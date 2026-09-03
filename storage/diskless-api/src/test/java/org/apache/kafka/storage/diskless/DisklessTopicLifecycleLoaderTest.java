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
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.storage.diskless.handlers.UrsaStorageConfig;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DisklessTopicLifecycleLoaderTest {

    private static final String SERVICE_NAME = DisklessStorageProvider.class.getName();

    @TempDir
    Path tempDir;

    @BeforeEach
    void reset() {
        RecordingLifecycle.CLOSED.set(false);
        RecordingLifecycle.ensured = null;
        RecordingLifecycle.deletedTopicId = null;
        RecordingLifecycle.sweptOrphans = null;
        RecordingLifecycle.classLoader = null;
        RecordingLifecycle.inventoryClassLoader = null;
        RecordingLifecycle.invocationClassLoader = null;
        RecordingLifecycle.closeClassLoader = null;
        FailingCloseLifecycle.classLoader = null;
    }

    @Test
    void testLoadsLifecycleFromIsolatedRuntimeAndReleasesLease() throws Exception {
        writeProvider(RecordingProvider.class);
        UrsaStorageConfig config = ursaConfig();
        Uuid topicId = Uuid.randomUuid();

        try (DisklessTopicLifecycle lifecycle = DisklessTopicLifecycleLoader.load(config)) {
            assertTrue(lifecycle.listManagedTopics().get().isEmpty());
            lifecycle.ensureTopic("orders", topicId, 3, Map.of("owner", "kafka"), 11).get();
            lifecycle.deleteTopic("orders", topicId).get();
            lifecycle.sweepOrphans(Set.of(topicId), 42).get();
        }

        assertEquals("orders:" + topicId + ":3", RecordingLifecycle.ensured);
        assertEquals(topicId, RecordingLifecycle.deletedTopicId);
        assertEquals(Set.of(topicId) + ":42", RecordingLifecycle.sweptOrphans);
        assertTrue(RecordingLifecycle.CLOSED.get());

        ClassLoader observedLoader = RecordingLifecycle.classLoader;
        assertNotSame(Thread.currentThread().getContextClassLoader(), observedLoader);
        assertSame(observedLoader, RecordingLifecycle.inventoryClassLoader);
        assertSame(observedLoader, RecordingLifecycle.invocationClassLoader);
        assertSame(observedLoader, RecordingLifecycle.closeClassLoader);
        assertLeaseWasReleased(observedLoader);
    }

    @Test
    void testEngineAndLifecycleShareRuntimeUntilBothClose() throws Exception {
        writeProvider(RecordingProvider.class);
        UrsaStorageConfig config = ursaConfig();

        DisklessStorageEngine engine = DisklessStorageEngineLoader.load(
                null, 0, config, null, Map.of(), topic -> Map.of(), topic -> OptionalInt.empty(),
                () -> 0L);
        DisklessTopicLifecycle lifecycle = DisklessTopicLifecycleLoader.load(config);
        ClassLoader sharedLoader = RecordingLifecycle.classLoader;

        lifecycle.close();
        assertSame(sharedLoader, currentClassLoaderForClassPath());

        engine.close();
        assertLeaseWasReleased(sharedLoader);
    }

    @Test
    void testLifecycleCloseFailureStillReleasesLease() throws Exception {
        writeProvider(FailingCloseProvider.class);
        DisklessTopicLifecycle lifecycle = DisklessTopicLifecycleLoader.load(ursaConfig());
        ClassLoader failedLoader = FailingCloseLifecycle.classLoader;

        assertThrows(IOException.class, lifecycle::close);

        assertLeaseWasReleased(failedLoader);
    }

    @Test
    void testLoadLazilyDefersProviderDiscoveryUntilFirstOperation() throws Exception {
        // No provider is published on the configured class path, so an eager load would fail here.
        DisklessTopicLifecycle lifecycle = DisklessTopicLifecycleLoader.loadLazily(ursaConfig());
        try {
            ExecutionException failure = assertThrows(
                    ExecutionException.class,
                    () -> lifecycle.listManagedTopics().get());
            assertTrue(failure.getCause().getMessage().contains("No DisklessStorageProvider"));
        } finally {
            lifecycle.close();
        }
    }

    private void writeProvider(Class<?> provider) throws IOException {
        Path serviceFile = tempDir.resolve("META-INF/services").resolve(SERVICE_NAME);
        Files.createDirectories(serviceFile.getParent());
        Files.writeString(serviceFile, provider.getName() + "\n");
    }

    private UrsaStorageConfig ursaConfig() throws Exception {
        return UrsaStorageConfig.fromConfigs(Map.of(
                ServerLogConfigs.URSA_STORAGE_CLASS_PATH_CONFIG, tempDir.toString()));
    }

    private ClassLoader currentClassLoaderForClassPath() throws Exception {
        URL[] urls = DisklessStorageProviderLoader.classPathUrls(tempDir.toString());
        ClassLoader parent = DisklessStorageProviderLoader.class.getClassLoader();
        DisklessClassLoaderRegistry.Lease lease = DisklessClassLoaderRegistry.acquire(urls, parent);
        try {
            return lease.classLoader();
        } finally {
            lease.close();
        }
    }

    private void assertLeaseWasReleased(ClassLoader previousClassLoader) throws Exception {
        assertNotSame(previousClassLoader, currentClassLoaderForClassPath());
    }

    public static final class RecordingProvider implements DisklessStorageProvider {
        @Override
        public DisklessStorageEngine createStorageEngine(StorageEngineContext context) {
            return new NoOpEngine();
        }

        @Override
        public DisklessTopicLifecycle createTopicLifecycle(UrsaStorageConfig config) {
            return new RecordingLifecycle();
        }
    }

    public static final class FailingCloseProvider implements DisklessStorageProvider {
        @Override
        public DisklessStorageEngine createStorageEngine(StorageEngineContext context) {
            return new NoOpEngine();
        }

        @Override
        public DisklessTopicLifecycle createTopicLifecycle(UrsaStorageConfig config) {
            return new FailingCloseLifecycle();
        }
    }

    static final class RecordingLifecycle implements DisklessTopicLifecycle {
        static final AtomicBoolean CLOSED = new AtomicBoolean(false);
        static volatile String ensured;
        static volatile Uuid deletedTopicId;
        static volatile String sweptOrphans;
        static volatile ClassLoader classLoader;
        static volatile ClassLoader inventoryClassLoader;
        static volatile ClassLoader invocationClassLoader;
        static volatile ClassLoader closeClassLoader;

        RecordingLifecycle() {
            classLoader = Thread.currentThread().getContextClassLoader();
        }

        @Override
        public CompletableFuture<List<ManagedTopic>> listManagedTopics() {
            inventoryClassLoader = Thread.currentThread().getContextClassLoader();
            return CompletableFuture.completedFuture(List.of());
        }

        @Override
        public CompletableFuture<Void> ensureTopic(
                String topicName,
                Uuid topicId,
                int partitions,
                Map<String, String> configs,
                long sourceRevision) {
            ensured = topicName + ":" + topicId + ":" + partitions;
            invocationClassLoader = Thread.currentThread().getContextClassLoader();
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> deleteTopic(String topicName, Uuid topicId) {
            deletedTopicId = topicId;
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> sweepOrphans(Set<Uuid> liveTopicIds, long imageOffset) {
            sweptOrphans = liveTopicIds + ":" + imageOffset;
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void close() {
            closeClassLoader = Thread.currentThread().getContextClassLoader();
            CLOSED.set(true);
        }
    }

    static final class FailingCloseLifecycle implements DisklessTopicLifecycle {
        static volatile ClassLoader classLoader;

        FailingCloseLifecycle() {
            classLoader = Thread.currentThread().getContextClassLoader();
        }

        @Override
        public CompletableFuture<List<ManagedTopic>> listManagedTopics() {
            return CompletableFuture.completedFuture(List.of());
        }

        @Override
        public CompletableFuture<Void> ensureTopic(
                String topicName,
                Uuid topicId,
                int partitions,
                Map<String, String> configs,
                long sourceRevision) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> deleteTopic(String topicName, Uuid topicId) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> sweepOrphans(Set<Uuid> liveTopicIds, long imageOffset) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void close() throws IOException {
            throw new IOException("close failed");
        }
    }

    private static final class NoOpEngine implements DisklessStorageEngine {
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
            return true;
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
            return true;
        }

        @Override
        public void close() {
        }
    }
}
