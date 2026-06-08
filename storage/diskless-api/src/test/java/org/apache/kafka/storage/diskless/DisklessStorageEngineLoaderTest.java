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
import org.apache.kafka.common.record.MemoryRecords;
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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DisklessStorageEngineLoaderTest {

    private static final String SERVICE_RESOURCE = "META-INF/services/example.Service";
    private static final String PLUGIN_RESOURCE = "plugin-only-resource.txt";
    private static final String TCCL_ENGINE_CLASS = "plugin.TcclDisklessStorageEngine";
    private static final String TCCL_METADATA_STORE_CLASS = "plugin.TcclDisklessMetadataStore";

    @TempDir
    Path tempDir;

    @Test
    void testKafkaPluginClassLoaderUsesChildResourceBeforeParentResource() throws Exception {
        Path parentDir = writeResource("parent", SERVICE_RESOURCE, "parent.Provider");
        Path childDir = writeResource("child", SERVICE_RESOURCE, "child.Provider");

        try (URLClassLoader parent = new URLClassLoader(new URL[] {parentDir.toUri().toURL()}, null);
             KafkaPluginClassLoader loader =
                     new KafkaPluginClassLoader(new URL[] {childDir.toUri().toURL()}, parent)) {
            URL resource = loader.getResource(SERVICE_RESOURCE);
            assertNotNull(resource);
            assertEquals("child.Provider", Files.readString(Path.of(resource.toURI())));

            List<URL> resources = Collections.list(loader.getResources(SERVICE_RESOURCE));
            assertEquals(2, resources.size());
            assertEquals("child.Provider", Files.readString(Path.of(resources.get(0).toURI())));
            assertEquals("parent.Provider", Files.readString(Path.of(resources.get(1).toURI())));
        }
    }

    @Test
    void testKafkaPluginClassLoaderFallsBackToParentResource() throws Exception {
        Path parentDir = writeResource("parent", SERVICE_RESOURCE, "parent.Provider");
        Path childDir = Files.createDirectory(tempDir.resolve("child"));

        try (URLClassLoader parent = new URLClassLoader(new URL[] {parentDir.toUri().toURL()}, null);
             KafkaPluginClassLoader loader =
                     new KafkaPluginClassLoader(new URL[] {childDir.toUri().toURL()}, parent)) {
            URL resource = loader.getResource(SERVICE_RESOURCE);
            assertNotNull(resource);
            assertEquals("parent.Provider", Files.readString(Path.of(resource.toURI())));

            List<URL> resources = Collections.list(loader.getResources(SERVICE_RESOURCE));
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
    void testDisklessStorageEngineLoaderReleasesClassLoaderWhenClassInitializationThrowsError() throws Exception {
        Path childDir = Files.createDirectory(tempDir.resolve("engine-error"));
        URL[] urls = DisklessStorageEngineLoader.classPathUrls(childDir.toString());
        ClassLoader parent = DisklessStorageEngineLoader.class.getClassLoader();

        assertThrows(ExceptionInInitializerError.class, () ->
                DisklessStorageEngineLoader.load(
                        null,
                        0,
                        ursaConfig(childDir),
                        null,
                        Map.of(),
                        topic -> Map.of(),
                        FailingEngine.class.getName()));

        assertClassLoaderLeaseReleased(urls, parent);
    }

    @Test
    void testDisklessMetadataStoreLoaderReleasesClassLoaderWhenClassInitializationThrowsError() throws Exception {
        Path childDir = Files.createDirectory(tempDir.resolve("metadata-error"));
        URL[] urls = DisklessMetadataStoreLoader.classPathUrls(childDir.toString());
        ClassLoader parent = DisklessMetadataStoreLoader.class.getClassLoader();

        assertThrows(ExceptionInInitializerError.class, () ->
                DisklessMetadataStoreLoader.load(
                        "oxia://localhost/default",
                        childDir.toString(),
                        FailingMetadataStore.class.getName()));

        assertClassLoaderLeaseReleased(urls, parent);
    }

    @Test
    void testLeasedDisklessStorageEngineReleasesClassLoaderAfterDelegateClose() throws Exception {
        Path childDir = Files.createDirectory(tempDir.resolve("child"));
        URL[] urls = new URL[] {childDir.toUri().toURL()};
        ClassLoader parent = DisklessStorageEngineLoaderTest.class.getClassLoader();
        DisklessClassLoaderRegistry.Lease lease = DisklessClassLoaderRegistry.acquire(urls, parent);
        ClassLoader firstLoader = lease.classLoader();
        CloseTrackingDisklessStorageEngine delegate = new CloseTrackingDisklessStorageEngine(false);

        new LeasedDisklessStorageEngine(delegate, lease).close();
        assertTrue(delegate.closed.get());

        DisklessClassLoaderRegistry.Lease nextLease = DisklessClassLoaderRegistry.acquire(urls, parent);
        try {
            assertNotSame(firstLoader, nextLease.classLoader());
        } finally {
            nextLease.close();
        }
    }

    @Test
    void testLeasedDisklessStorageEngineReleasesClassLoaderWhenDelegateCloseFails() throws Exception {
        Path childDir = Files.createDirectory(tempDir.resolve("child"));
        URL[] urls = new URL[] {childDir.toUri().toURL()};
        ClassLoader parent = DisklessStorageEngineLoaderTest.class.getClassLoader();
        DisklessClassLoaderRegistry.Lease lease = DisklessClassLoaderRegistry.acquire(urls, parent);
        ClassLoader firstLoader = lease.classLoader();
        CloseTrackingDisklessStorageEngine delegate = new CloseTrackingDisklessStorageEngine(true);

        try {
            new LeasedDisklessStorageEngine(delegate, lease).close();
        } catch (IOException expected) {
            // expected
        }
        assertTrue(delegate.closed.get());

        DisklessClassLoaderRegistry.Lease nextLease = DisklessClassLoaderRegistry.acquire(urls, parent);
        try {
            assertNotSame(firstLoader, nextLease.classLoader());
        } finally {
            nextLease.close();
        }
    }

    @Test
    void testLeasedDisklessMetadataStoreReleasesClassLoaderAfterDelegateClose() throws Exception {
        Path childDir = Files.createDirectory(tempDir.resolve("child"));
        URL[] urls = new URL[] {childDir.toUri().toURL()};
        ClassLoader parent = DisklessStorageEngineLoaderTest.class.getClassLoader();
        DisklessClassLoaderRegistry.Lease lease = DisklessClassLoaderRegistry.acquire(urls, parent);
        ClassLoader firstLoader = lease.classLoader();
        CloseTrackingDisklessMetadataStore delegate = new CloseTrackingDisklessMetadataStore();

        new LeasedDisklessMetadataStore(delegate, lease).close();
        assertTrue(delegate.closed.get());

        DisklessClassLoaderRegistry.Lease nextLease = DisklessClassLoaderRegistry.acquire(urls, parent);
        try {
            assertNotSame(firstLoader, nextLease.classLoader());
        } finally {
            nextLease.close();
        }
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
                TCCL_ENGINE_CLASS)) {
            engine.write(Map.of(), "").get();
            engine.fetch(null, Map.of()).get();
            engine.listOffsets(Map.of()).get();
            engine.cleanupPartition(null, false);
            engine.deletePartitionData(null);
            engine.snapshotTrackedPartitions();
            engine.cleanupNonOwnedProducerStates(null, Set.of(), false);
        }

        try (DisklessMetadataStore store = DisklessMetadataStoreLoader.load(
                "oxia://localhost/default",
                pluginDir.toString(),
                TCCL_METADATA_STORE_CLASS)) {
            store.put("key", new byte[0]).get();
            store.delete("key").get();
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
        Files.writeString(sourceDir.resolve("TcclDisklessMetadataStore.java"), tcclMetadataStoreSource());

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
                sourceDir.resolve("TcclDisklessMetadataStore.java").toString());
        assertEquals(0, result);
        return pluginDir;
    }

    private String tcclEngineSource() {
        return """
                package plugin;

                import org.apache.kafka.common.TopicIdPartition;
                import org.apache.kafka.common.record.MemoryRecords;
                import org.apache.kafka.common.requests.FetchRequest;
                import org.apache.kafka.common.requests.ProduceResponse;
                import org.apache.kafka.common.utils.Time;
                import org.apache.kafka.server.storage.log.FetchParams;
                import org.apache.kafka.server.storage.log.FetchPartitionData;
                import org.apache.kafka.storage.diskless.DisklessStorageEngine;
                import org.apache.kafka.storage.diskless.ListOffsetsPartitionRequest;
                import org.apache.kafka.storage.diskless.ListOffsetsPartitionResponse;
                import org.apache.kafka.storage.diskless.handlers.UrsaStorageConfig;
                import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

                import java.io.IOException;
                import java.util.Map;
                import java.util.Set;
                import java.util.concurrent.CompletableFuture;
                import java.util.function.Function;

                public final class TcclDisklessStorageEngine implements DisklessStorageEngine {
                    public TcclDisklessStorageEngine(
                            Time time,
                            int brokerId,
                            UrsaStorageConfig ursaConfig,
                            BrokerTopicStats brokerTopicStats,
                            Map<String, Object> logConfigDefaults,
                            Function<String, Map<String, String>> topicConfigSupplier) {
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
                    public void deletePartitionData(TopicIdPartition tp) {
                        requirePluginResource("deletePartitionData");
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

    private String tcclMetadataStoreSource() {
        return """
                package plugin;

                import org.apache.kafka.storage.diskless.DisklessMetadataStore;

                import java.util.concurrent.CompletableFuture;

                public final class TcclDisklessMetadataStore implements DisklessMetadataStore {
                    public TcclDisklessMetadataStore(String url) {
                        requirePluginResource("constructor");
                    }

                    @Override
                    public CompletableFuture<Void> put(String key, byte[] value) {
                        requirePluginResource("put");
                        return CompletableFuture.completedFuture(null);
                    }

                    @Override
                    public CompletableFuture<Void> delete(String key) {
                        requirePluginResource("delete");
                        return CompletableFuture.completedFuture(null);
                    }

                    @Override
                    public void close() {
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

    private void assertClassLoaderLeaseReleased(URL[] urls, ClassLoader parent) throws Exception {
        DisklessClassLoaderRegistry.Lease first = DisklessClassLoaderRegistry.acquire(urls, parent);
        ClassLoader firstLoader = first.classLoader();
        first.close();

        DisklessClassLoaderRegistry.Lease second = DisklessClassLoaderRegistry.acquire(urls, parent);
        try {
            assertNotSame(firstLoader, second.classLoader());
        } finally {
            second.close();
        }
    }

    private static final class CloseTrackingDisklessStorageEngine implements DisklessStorageEngine {
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final boolean failOnClose;

        private CloseTrackingDisklessStorageEngine(boolean failOnClose) {
            this.failOnClose = failOnClose;
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
        public void deletePartitionData(TopicIdPartition tp) {
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
            closed.set(true);
            if (failOnClose) {
                throw new IOException("close failed");
            }
        }
    }

    private static final class CloseTrackingDisklessMetadataStore implements DisklessMetadataStore {
        private final AtomicBoolean closed = new AtomicBoolean(false);

        @Override
        public CompletableFuture<Void> put(String key, byte[] value) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> delete(String key) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void close() {
            closed.set(true);
        }
    }

    private static final class FailingEngine {
        static {
            fail();
        }

        private static void fail() {
            throw new ExceptionInInitializerError("boom");
        }
    }

    private static final class FailingMetadataStore {
        static {
            fail();
        }

        private static void fail() {
            throw new ExceptionInInitializerError("boom");
        }
    }
}
