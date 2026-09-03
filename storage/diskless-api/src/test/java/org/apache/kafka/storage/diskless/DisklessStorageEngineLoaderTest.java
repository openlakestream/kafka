/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.storage.diskless;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.utils.KafkaPluginClassLoader;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.storage.diskless.handlers.UrsaStorageConfig;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DisklessStorageEngineLoaderTest {

    private static final String SHARED_RESOURCE = "shared-resource.txt";
    private static final String PLUGIN_RESOURCE = "plugin-only-resource.txt";
    private static final String SERVICE_NAME = DisklessStorageProvider.class.getName();

    @TempDir
    Path tempDir;

    @Test
    void testKafkaPluginClassLoaderUsesChildResourceBeforeParentResource() throws Exception {
        Path parentDir = writeResource("parent", SHARED_RESOURCE, "parent.Provider");
        Path childDir = writeResource("child", SHARED_RESOURCE, "child.Provider");

        try (URLClassLoader parent = new URLClassLoader(new URL[] {parentDir.toUri().toURL()}, null);
             KafkaPluginClassLoader loader =
                     new KafkaPluginClassLoader(new URL[] {childDir.toUri().toURL()}, parent)) {
            URL resource = loader.getResource(SHARED_RESOURCE);
            assertNotNull(resource);
            assertEquals("child.Provider", Files.readString(Path.of(resource.toURI())));

            List<URL> resources = Collections.list(loader.getResources(SHARED_RESOURCE));
            assertEquals(2, resources.size());
            assertEquals("child.Provider", Files.readString(Path.of(resources.get(0).toURI())));
            assertEquals("parent.Provider", Files.readString(Path.of(resources.get(1).toURI())));
        }
    }

    @Test
    void testKafkaPluginClassLoaderFallsBackToParentResource() throws Exception {
        Path parentDir = writeResource("parent", SHARED_RESOURCE, "parent.Provider");
        Path childDir = Files.createDirectory(tempDir.resolve("child"));

        try (URLClassLoader parent = new URLClassLoader(new URL[] {parentDir.toUri().toURL()}, null);
             KafkaPluginClassLoader loader =
                     new KafkaPluginClassLoader(new URL[] {childDir.toUri().toURL()}, parent)) {
            URL resource = loader.getResource(SHARED_RESOURCE);
            assertNotNull(resource);
            assertEquals("parent.Provider", Files.readString(Path.of(resource.toURI())));

            List<URL> resources = Collections.list(loader.getResources(SHARED_RESOURCE));
            assertEquals(1, resources.size());
            assertEquals("parent.Provider", Files.readString(Path.of(resources.get(0).toURI())));
        }
    }

    @Test
    void testKafkaPluginClassLoaderLoadsSharedApiClassesFromParentFirst() throws Exception {
        Path childDir = Files.createDirectory(tempDir.resolve("child"));
        copyClassToDirectory(DisklessStorageEngineLoaderTest.class, childDir);

        try (KafkaPluginClassLoader loader =
                     new KafkaPluginClassLoader(
                             new URL[] {childDir.toUri().toURL()},
                             DisklessStorageEngineLoaderTest.class.getClassLoader())) {
            Class<?> loadedClass = loader.loadClass(DisklessStorageEngineLoaderTest.class.getName());
            assertSame(DisklessStorageEngineLoaderTest.class, loadedClass);
        }
    }

    @Test
    void testDisklessClassLoaderRegistryReusesLoaderUntilLastLeaseIsClosed() throws Exception {
        Path childDir = Files.createDirectory(tempDir.resolve("child"));
        URL[] urls = new URL[] {childDir.toUri().toURL()};
        ClassLoader parent = DisklessStorageEngineLoaderTest.class.getClassLoader();

        DisklessClassLoaderRegistry.Lease first = DisklessClassLoaderRegistry.acquire(urls, parent);
        DisklessClassLoaderRegistry.Lease second = DisklessClassLoaderRegistry.acquire(urls, parent);
        assertSame(first.classLoader(), second.classLoader());

        first.close();
        DisklessClassLoaderRegistry.Lease third = DisklessClassLoaderRegistry.acquire(urls, parent);
        assertSame(second.classLoader(), third.classLoader());

        ClassLoader sharedLoader = second.classLoader();
        second.close();
        third.close();

        DisklessClassLoaderRegistry.Lease fourth = DisklessClassLoaderRegistry.acquire(urls, parent);
        try {
            assertNotSame(sharedLoader, fourth.classLoader());
        } finally {
            fourth.close();
        }
    }

    @Test
    void testLeasedEngineReleasesClassLoaderAfterDelegateClose() throws Exception {
        Path childDir = Files.createDirectory(tempDir.resolve("child"));
        URL[] urls = new URL[] {childDir.toUri().toURL()};
        ClassLoader parent = DisklessStorageEngineLoaderTest.class.getClassLoader();
        DisklessClassLoaderRegistry.Lease lease = DisklessClassLoaderRegistry.acquire(urls, parent);
        ClassLoader firstLoader = lease.classLoader();
        CloseTrackingDisklessStorageEngine delegate = new CloseTrackingDisklessStorageEngine(false);

        DisklessClassLoaderContext.leased(DisklessStorageEngine.class, delegate, lease).close();
        assertTrue(delegate.closed.get());

        DisklessClassLoaderRegistry.Lease nextLease = DisklessClassLoaderRegistry.acquire(urls, parent);
        try {
            assertNotSame(firstLoader, nextLease.classLoader());
        } finally {
            nextLease.close();
        }
    }

    @Test
    void testLeasedEngineReleasesClassLoaderWhenDelegateCloseFails() throws Exception {
        Path childDir = Files.createDirectory(tempDir.resolve("child"));
        URL[] urls = new URL[] {childDir.toUri().toURL()};
        ClassLoader parent = DisklessStorageEngineLoaderTest.class.getClassLoader();
        DisklessClassLoaderRegistry.Lease lease = DisklessClassLoaderRegistry.acquire(urls, parent);
        ClassLoader firstLoader = lease.classLoader();
        CloseTrackingDisklessStorageEngine delegate = new CloseTrackingDisklessStorageEngine(true);
        DisklessStorageEngine engine =
                DisklessClassLoaderContext.leased(DisklessStorageEngine.class, delegate, lease);

        assertThrows(IOException.class, engine::close);
        assertTrue(delegate.closeAttempted.get());

        DisklessClassLoaderRegistry.Lease nextLease = DisklessClassLoaderRegistry.acquire(urls, parent);
        try {
            assertNotSame(firstLoader, nextLease.classLoader());
        } finally {
            nextLease.close();
        }

        // A second close is a no-op: the delegate is not closed twice.
        delegate.closeAttempted.set(false);
        engine.close();
        assertFalse(delegate.closeAttempted.get());
    }

    @Test
    void testDisklessRuntimeUsesPluginContextClassLoaderEndToEnd() throws Exception {
        Path pluginDir = compileTcclPlugin();

        try (DisklessStorageEngine engine = DisklessStorageEngineLoader.load(
                null,
                0,
                ursaConfig(pluginDir),
                null,
                Map.of(),
                topic -> Map.of(),
                topic -> OptionalInt.empty(),
                () -> 0L)) {
            engine.write(Map.of(), "").get();
            engine.fetch(null, Map.of()).get();
            engine.listOffsets(Map.of()).get();
            engine.cleanupPartition(null, false);
            engine.snapshotTrackedPartitions();
            engine.cleanupNonOwnedProducerStates(null, Set.of(), false);
        }

    }

    private Path writeResource(String directoryName, String resourceName, String value) throws Exception {
        Path directory = Files.createDirectory(tempDir.resolve(directoryName));
        Path resource = directory.resolve(resourceName);
        Files.createDirectories(resource.getParent());
        Files.writeString(resource, value);
        return directory;
    }

    private void copyClassToDirectory(Class<?> clazz, Path directory) throws Exception {
        String resourceName = clazz.getName().replace('.', '/') + ".class";
        Path classFile = directory.resolve(resourceName);
        Files.createDirectories(classFile.getParent());
        try (var input = clazz.getClassLoader().getResourceAsStream(resourceName)) {
            assertNotNull(input);
            Files.copy(input, classFile, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private Path compileTcclPlugin() throws Exception {
        Path pluginDir = Files.createDirectory(tempDir.resolve("tccl-plugin"));
        Path sourceDir = Files.createDirectories(pluginDir.resolve("plugin"));
        Files.writeString(pluginDir.resolve(PLUGIN_RESOURCE), "visible-to-plugin-tccl");
        Files.writeString(sourceDir.resolve("TcclDisklessStorageEngine.java"), tcclEngineSource());
        Files.writeString(sourceDir.resolve("TcclDisklessStorageProvider.java"), tcclProviderSource());

        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        assertNotNull(compiler);
        int result = compiler.run(
                null,
                null,
                null,
                "-classpath",
                System.getProperty("java.class.path"),
                "-d",
                pluginDir.toString(),
                sourceDir.resolve("TcclDisklessStorageEngine.java").toString(),
                sourceDir.resolve("TcclDisklessStorageProvider.java").toString());
        assertEquals(0, result);

        Path serviceFile = pluginDir.resolve("META-INF/services").resolve(SERVICE_NAME);
        Files.createDirectories(serviceFile.getParent());
        Files.writeString(serviceFile, "plugin.TcclDisklessStorageProvider\n");
        return pluginDir;
    }

    private String tcclProviderSource() {
        return """
                package plugin;

                import org.apache.kafka.storage.diskless.DisklessStorageEngine;
                import org.apache.kafka.storage.diskless.DisklessStorageProvider;
                import org.apache.kafka.storage.diskless.DisklessTopicLifecycle;
                import org.apache.kafka.storage.diskless.handlers.UrsaStorageConfig;

                public final class TcclDisklessStorageProvider implements DisklessStorageProvider {
                    @Override
                    public DisklessStorageEngine createStorageEngine(StorageEngineContext context) {
                        return new TcclDisklessStorageEngine();
                    }

                    @Override
                    public DisklessTopicLifecycle createTopicLifecycle(UrsaStorageConfig config) {
                        throw new UnsupportedOperationException("not used by this test plugin");
                    }
                }
                """;
    }

    private String tcclEngineSource() {
        return """
                package plugin;

                import org.apache.kafka.common.TopicIdPartition;
                import org.apache.kafka.common.Uuid;
                import org.apache.kafka.common.record.internal.MemoryRecords;
                import org.apache.kafka.common.requests.FetchRequest;
                import org.apache.kafka.common.requests.ProduceResponse;
                import org.apache.kafka.server.storage.log.FetchParams;
                import org.apache.kafka.server.storage.log.FetchPartitionData;
                import org.apache.kafka.storage.diskless.DisklessStorageEngine;
                import org.apache.kafka.storage.diskless.ListOffsetsPartitionRequest;
                import org.apache.kafka.storage.diskless.ListOffsetsPartitionResponse;

                import java.io.IOException;
                import java.util.Map;
                import java.util.Set;
                import java.util.concurrent.CompletableFuture;

                public final class TcclDisklessStorageEngine implements DisklessStorageEngine {
                    public TcclDisklessStorageEngine() {
                        requirePluginResource("constructor");
                    }

                    @Override
                    public CompletableFuture<Map<TopicIdPartition, ProduceResponse.PartitionResponse>> write(
                            Map<TopicIdPartition, MemoryRecords> entriesPerPartition,
                            String zone) {
                        requirePluginResource("write");
                        return CompletableFuture.completedFuture(Map.of());
                    }

                    @Override
                    public CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> fetch(
                            FetchParams params,
                            Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos) {
                        requirePluginResource("fetch");
                        return CompletableFuture.completedFuture(Map.of());
                    }

                    @Override
                    public CompletableFuture<Map<TopicIdPartition, ListOffsetsPartitionResponse>> listOffsets(
                            Map<TopicIdPartition, ListOffsetsPartitionRequest> requests) {
                        requirePluginResource("listOffsets");
                        return CompletableFuture.completedFuture(Map.of());
                    }

                    @Override
                    public boolean cleanupPartition(TopicIdPartition tp, boolean deletePartition) {
                        requirePluginResource("cleanupPartition");
                        return true;
                    }

                    @Override
                    public void applyTopicConfig(String topicName, Uuid topicId, Map<String, String> config) {
                        requirePluginResource("applyTopicConfig");
                    }

                    @Override
                    public void fenceDeletedTopic(String topicName, Uuid topicId) {
                        requirePluginResource("fenceDeletedTopic");
                    }

                    @Override
                    public Set<TopicIdPartition> snapshotTrackedPartitions() {
                        requirePluginResource("snapshotTrackedPartitions");
                        return Set.of();
                    }

                    @Override
                    public boolean cleanupNonOwnedProducerStates(
                            TopicIdPartition tp,
                            Set<String> retainedZones,
                            boolean deleteSnapshot) {
                        requirePluginResource("cleanupNonOwnedProducerStates");
                        return true;
                    }

                    @Override
                    public void close() throws IOException {
                        requirePluginResource("close");
                    }

                    private static void requirePluginResource(String operation) {
                        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
                        if (classLoader == null || classLoader.getResource("%s") == null) {
                            throw new IllegalStateException("Missing plugin TCCL resource during " + operation);
                        }
                    }
                }
                """.formatted(PLUGIN_RESOURCE);
    }

    private UrsaStorageConfig ursaConfig(Path classPath) throws Exception {
        return UrsaStorageConfig.fromConfigs(Map.of(
                ServerLogConfigs.URSA_STORAGE_CLASS_PATH_CONFIG,
                classPath.toString()));
    }

    private static final class CloseTrackingDisklessStorageEngine implements DisklessStorageEngine {
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final AtomicBoolean closeAttempted = new AtomicBoolean(false);
        private final AtomicBoolean failOnClose;

        private CloseTrackingDisklessStorageEngine(boolean failOnClose) {
            this.failOnClose = new AtomicBoolean(failOnClose);
        }

        @Override
        public CompletableFuture<Map<TopicIdPartition, ProduceResponse.PartitionResponse>> write(
                Map<TopicIdPartition, MemoryRecords> entriesPerPartition,
                String zone) {
            return CompletableFuture.completedFuture(Map.of());
        }

        @Override
        public CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> fetch(
                FetchParams params,
                Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos) {
            return CompletableFuture.completedFuture(Map.of());
        }

        @Override
        public boolean cleanupPartition(TopicIdPartition tp, boolean deletePartition) {
            return false;
        }

        @Override
        public void applyTopicConfig(String topicName, Uuid topicId, Map<String, String> config) {
        }

        @Override
        public void fenceDeletedTopic(String topicName, Uuid topicId) {
        }

        @Override
        public Set<TopicIdPartition> snapshotTrackedPartitions() {
            return Set.of();
        }

        @Override
        public boolean cleanupNonOwnedProducerStates(
                TopicIdPartition tp,
                Set<String> retainedZones,
                boolean deleteSnapshot) {
            return false;
        }

        @Override
        public void close() throws IOException {
            closeAttempted.set(true);
            if (failOnClose.get()) {
                throw new IOException("close failed");
            }
            closed.set(true);
        }
    }

}
