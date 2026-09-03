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
package kafka.server.metadata;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.loader.LoaderManifest;
import org.apache.kafka.image.loader.LoaderManifestType;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.storage.diskless.DisklessTopicLifecycle;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class DisklessTopicLifecycleReconcilerTest {

    private static final int NODE_ID = 1;
    private static final Uuid A = Uuid.randomUuid();
    private static final Uuid B = Uuid.randomUuid();
    private static final Uuid C = Uuid.randomUuid();

    private static final long SWEEP_INTERVAL_MS = 60_000L;
    private static final long INITIAL_RETRY_MS = 10L;
    private static final long MAX_RETRY_MS = 50L;
    private static final long OPERATION_TIMEOUT_MS = 30_000L;
    private static final int MAX_CONCURRENT_OPERATIONS = 16;

    private final DisklessTopicLifecycle lifecycle = mock(DisklessTopicLifecycle.class);
    @SuppressWarnings("unchecked")
    private final BiConsumer<String, Throwable> faultHandler = mock(BiConsumer.class);
    private final List<DisklessTopicLifecycleReconciler> reconcilers = new ArrayList<>();
    private final List<ScheduledExecutorService> executors = new ArrayList<>();

    private DisklessTopicLifecycleReconciler reconciler;

    @BeforeEach
    void setUp() {
        when(lifecycle.ensureTopic(any(), any(), anyInt(), anyMap(), anyLong())).thenReturn(completedFuture(null));
        when(lifecycle.deleteTopic(any(), any())).thenReturn(completedFuture(null));
        when(lifecycle.sweepOrphans(anySet(), anyLong())).thenReturn(completedFuture(null));
        reconciler = newReconciler(SWEEP_INTERVAL_MS, MAX_CONCURRENT_OPERATIONS);
    }

    @AfterEach
    void tearDown() {
        reconcilers.forEach(DisklessTopicLifecycleReconciler::close);
        executors.forEach(ScheduledExecutorService::shutdownNow);
    }

    @Test
    void sweepCompletesWhileImagesKeepChanging() throws Exception {
        CompletableFuture<Void> sweep = new CompletableFuture<>();
        when(lifecycle.sweepOrphans(anySet(), anyLong())).thenReturn(sweep);
        when(lifecycle.ensureTopic(any(), any(), anyInt(), anyMap(), anyLong())).thenReturn(completedFuture(null));
        reconciler.onMetadataUpdate(delta(image(10L, disklessTopic("a", A, 1))), image(10L, disklessTopic("a", A, 1)), manifest());
        reconciler.onControllerChange(leader(NODE_ID));
        for (long offset = 11; offset < 20; offset++) {
            reconciler.onMetadataUpdate(delta(image(offset, disklessTopic("a", A, 1))), image(offset, disklessTopic("a", A, 1)), manifest());
        }
        sweep.complete(null);
        verify(lifecycle, timeout(5000).times(1)).sweepOrphans(Set.of(A), 10L);
        // Matching any revision is what makes this catch a dedupe regression: an extra ensureTopic
        // at revision 11..19 would not be counted by a matcher pinned to eq(10L).
        verify(lifecycle, timeout(5000).times(1)).ensureTopic(eq("a"), eq(A), eq(1), anyMap(), anyLong());
        verify(lifecycle).ensureTopic(eq("a"), eq(A), eq(1), anyMap(), eq(10L));
        TestUtils.waitForCondition(() -> reconciler.pendingOperationsForTesting() == 0,
            "the reconciled topic should have been retired from the pending set");
        verifyNoMoreInteractions(lifecycle);
    }

    @Test
    void createdTopicIsEnsuredAndDeletedTopicIsDeletedInOrder() throws Exception {
        when(lifecycle.ensureTopic(any(), any(), anyInt(), anyMap(), anyLong())).thenReturn(completedFuture(null));
        when(lifecycle.deleteTopic(any(), any())).thenReturn(completedFuture(null));
        activateWithEmptyImage();
        reconciler.onMetadataUpdate(delta(image(20L, disklessTopic("a", A, 3))), image(20L, disklessTopic("a", A, 3)), manifest());
        reconciler.onMetadataUpdate(deltaDeleting(A, image(20L, disklessTopic("a", A, 3))), image(21L), manifest());
        InOrder inOrder = inOrder(lifecycle);
        inOrder.verify(lifecycle, timeout(5000)).ensureTopic("a", A, 3, Map.of("ursa.storage.enable", "true"), 20L);
        inOrder.verify(lifecycle, timeout(5000)).deleteTopic("a", A);
    }

    @Test
    void failedOperationRetriesWithBackoffAndReportsFault() throws Exception {
        when(lifecycle.ensureTopic(any(), any(), anyInt(), anyMap(), anyLong()))
            .thenReturn(failedFuture(new IOException("oxia down")))
            .thenReturn(completedFuture(null));
        activateWithEmptyImage();
        reconciler.onMetadataUpdate(delta(image(5L, disklessTopic("a", A, 1))), image(5L, disklessTopic("a", A, 1)), manifest());
        verify(lifecycle, timeout(5000).times(2)).ensureTopic(eq("a"), eq(A), eq(1), anyMap(), eq(5L));
        verify(faultHandler, timeout(5000)).accept(contains("ensure diskless topic a"), any(IOException.class));
        TestUtils.waitForCondition(() -> reconciler.pendingOperationsForTesting() == 0,
            "the retried operation should have been retired from the pending set");
        assertEquals(0, reconciler.pendingOperationsForTesting());
    }

    @Test
    void concurrentOperationsAreBounded() throws Exception {
        List<CompletableFuture<Void>> inFlight = new CopyOnWriteArrayList<>();
        when(lifecycle.ensureTopic(any(), any(), anyInt(), anyMap(), anyLong())).thenAnswer(invocation -> {
            CompletableFuture<Void> future = new CompletableFuture<>();
            inFlight.add(future);
            return future;
        });
        reconciler = newReconciler(SWEEP_INTERVAL_MS, 2);
        MetadataImage image = image(9L, disklessTopic("a", A, 1), disklessTopic("b", B, 1), disklessTopic("c", C, 1));
        reconciler.onMetadataUpdate(delta(image), image, manifest());
        reconciler.onControllerChange(leader(NODE_ID));
        TestUtils.waitForCondition(() -> inFlight.size() == 2,
            "the concurrency limit should have started exactly two operations");
        // Nothing else may start while both permits are held.
        verify(lifecycle, times(2)).ensureTopic(any(), any(), anyInt(), anyMap(), anyLong());
        inFlight.get(0).complete(null);
        verify(lifecycle, timeout(5000).times(3)).ensureTopic(any(), any(), anyInt(), anyMap(), anyLong());
    }

    @Test
    void losingLeadershipCancelsRetriesAndSweeps() throws Exception {
        when(lifecycle.ensureTopic(any(), any(), anyInt(), anyMap(), anyLong())).thenReturn(failedFuture(new IOException("x")));
        // A backoff far longer than this test, so the attempt below stays pending on its retry
        // until losing leadership is what removes it.
        reconciler = newReconciler(SWEEP_INTERVAL_MS, MAX_CONCURRENT_OPERATIONS, OPERATION_TIMEOUT_MS,
            120_000L, 120_000L);
        activateWithEmptyImage();
        reconciler.onMetadataUpdate(delta(image(5L, disklessTopic("a", A, 1))), image(5L, disklessTopic("a", A, 1)), manifest());
        verify(lifecycle, timeout(5000).times(1)).ensureTopic(any(), any(), anyInt(), anyMap(), anyLong());

        reconciler.onControllerChange(leader(NODE_ID + 1));

        // Dropping the runner is what cancels its retry: a retry that still fired would find no
        // runner of its own generation and return without calling the lifecycle.
        TestUtils.waitForCondition(() -> reconciler.pendingOperationsForTesting() == 0,
            "losing leadership should have dropped every pending operation");
        verify(lifecycle, times(1)).ensureTopic(any(), any(), anyInt(), anyMap(), anyLong());
    }

    @Test
    void periodicSweepRunsOnConfiguredInterval() throws Exception {
        when(lifecycle.sweepOrphans(anySet(), anyLong())).thenReturn(completedFuture(null));
        reconciler = newReconciler(50L, MAX_CONCURRENT_OPERATIONS);
        activateWithEmptyImage();
        verify(lifecycle, timeout(2000).atLeast(3)).sweepOrphans(anySet(), anyLong());
    }

    @Test
    void disablingAndReenablingDisklessStorageReconcilesTheTopicAgain() throws Exception {
        activateWithEmptyImage();
        MetadataImage disklessImage = image(5L, disklessTopic("a", A, 1));
        reconciler.onMetadataUpdate(delta(disklessImage), disklessImage, manifest());
        verify(lifecycle, timeout(5000)).ensureTopic(eq("a"), eq(A), eq(1), anyMap(), eq(5L));

        MetadataImage classicImage = classicImage(6L, "a", A, 1);
        reconciler.onMetadataUpdate(configChange(disklessImage, "a", "false"), classicImage, manifest());

        // The desired state recorded while the topic was diskless must not dedupe this away.
        MetadataImage reenabledImage = image(7L, disklessTopic("a", A, 1));
        reconciler.onMetadataUpdate(configChange(classicImage, "a", "true"), reenabledImage, manifest());
        verify(lifecycle, timeout(5000)).ensureTopic(eq("a"), eq(A), eq(1), anyMap(), eq(7L));
    }

    @Test
    void timedOutOperationIsRetriedAndOriginalFutureIsCancelled() throws Exception {
        CompletableFuture<Void> stuck = new CompletableFuture<>();
        when(lifecycle.ensureTopic(any(), any(), anyInt(), anyMap(), anyLong()))
            .thenReturn(stuck)
            .thenReturn(completedFuture(null));
        reconciler = newReconciler(SWEEP_INTERVAL_MS, MAX_CONCURRENT_OPERATIONS, 50L);
        activateWithEmptyImage();
        reconciler.onMetadataUpdate(delta(image(5L, disklessTopic("a", A, 1))), image(5L, disklessTopic("a", A, 1)), manifest());
        verify(lifecycle, timeout(5000).times(2)).ensureTopic(eq("a"), eq(A), eq(1), anyMap(), eq(5L));
        verify(faultHandler, timeout(5000)).accept(contains("ensure diskless topic a"), any(TimeoutException.class));
        TestUtils.waitForCondition(stuck::isCancelled, "the timed-out attempt should have been cancelled");
    }

    @Test
    void aNewDesiredStateDuringBackoffDoesNotWaitForIt() throws Exception {
        when(lifecycle.ensureTopic(any(), any(), anyInt(), anyMap(), anyLong()))
            .thenReturn(failedFuture(new IOException("oxia down")))
            .thenReturn(completedFuture(null));
        // A backoff far longer than this test's own waits, so only cancelling it can explain the
        // second attempt arriving.
        reconciler = newReconciler(SWEEP_INTERVAL_MS, MAX_CONCURRENT_OPERATIONS, OPERATION_TIMEOUT_MS,
            120_000L, 120_000L);
        activateWithEmptyImage();

        MetadataImage created = image(5L, disklessTopic("a", A, 1));
        reconciler.onMetadataUpdate(delta(created), created, manifest());
        verify(lifecycle, timeout(5000)).ensureTopic(eq("a"), eq(A), eq(1), anyMap(), eq(5L));
        // The fault is reported after the retry is scheduled, so the backoff is pending from here.
        verify(faultHandler, timeout(5000)).accept(contains("ensure diskless topic a"), any(IOException.class));

        MetadataImage grown = image(6L, disklessTopic("a", A, 3));
        reconciler.onMetadataUpdate(delta(grown), grown, manifest());

        verify(lifecycle, timeout(5000)).ensureTopic(eq("a"), eq(A), eq(3), anyMap(), eq(6L));
    }

    @Test
    void aBlockingLifecycleCallDoesNotBlockTheMetadataThread() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        when(lifecycle.ensureTopic(any(), any(), anyInt(), anyMap(), anyLong())).thenAnswer(invocation -> {
            started.countDown();
            // A lifecycle implementation that is slow to hand its future back. Running it on the
            // metadata-loader thread would stall the controller's whole metadata pipeline.
            Thread.sleep(500L);
            return completedFuture(null);
        });
        activateWithEmptyImage();

        MetadataImage created = image(5L, disklessTopic("a", A, 1));
        long startNs = System.nanoTime();
        reconciler.onMetadataUpdate(delta(created), created, manifest());
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);

        assertTrue(started.await(5, TimeUnit.SECONDS), "the lifecycle call should have been started");
        assertTrue(elapsedMs < 250L, "onMetadataUpdate blocked for " + elapsedMs + " ms");
        verify(lifecycle, timeout(5000)).ensureTopic(eq("a"), eq(A), eq(1), anyMap(), eq(5L));
    }

    @Test
    void sweepProtectsEveryTopicInTheImageIncludingClassicOnes() throws Exception {
        MetadataImage image = new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build()
            .apply(new MetadataProvenance(0L, 0, 0, true));
        MetadataDelta delta = new MetadataDelta.Builder().setImage(image).build();
        replayCreation(delta, disklessTopic("a", A, 1));
        replayCreation(delta, disklessTopic("b", B, 1));
        delta.replay(disklessConfigRecord("b", "false"));
        MetadataImage mixed = delta.apply(new MetadataProvenance(12L, 0, 0, true));

        reconciler.onMetadataUpdate(delta, mixed, manifest());
        reconciler.onControllerChange(leader(NODE_ID));

        // The classic topic is still in Kafka metadata, and sweeping deletes storage permanently.
        verify(lifecycle, timeout(5000)).sweepOrphans(Set.of(A, B), 12L);
    }

    @Test
    void timedOutSweepIsRetriedAndOriginalFutureIsCancelled() throws Exception {
        CompletableFuture<Void> stuck = new CompletableFuture<>();
        when(lifecycle.sweepOrphans(anySet(), anyLong()))
            .thenReturn(stuck)
            .thenReturn(completedFuture(null));
        reconciler = newReconciler(50L, MAX_CONCURRENT_OPERATIONS, 50L);
        activateWithEmptyImage();

        verify(faultHandler, timeout(5000))
            .accept(contains("sweep orphaned diskless topic storage"), any(TimeoutException.class));
        TestUtils.waitForCondition(stuck::isCancelled, "the timed-out sweep should have been cancelled");
        verify(lifecycle, timeout(5000).atLeast(2)).sweepOrphans(anySet(), anyLong());
    }

    private void activateWithEmptyImage() {
        MetadataImage empty = image(0L);
        reconciler.onMetadataUpdate(delta(empty), empty, manifest());
        reconciler.onControllerChange(leader(NODE_ID));
    }

    private DisklessTopicLifecycleReconciler newReconciler(long sweepIntervalMs, int maxConcurrentOperations) {
        return newReconciler(sweepIntervalMs, maxConcurrentOperations, OPERATION_TIMEOUT_MS);
    }

    private DisklessTopicLifecycleReconciler newReconciler(long sweepIntervalMs, int maxConcurrentOperations,
                                                           long operationTimeoutMs) {
        return newReconciler(sweepIntervalMs, maxConcurrentOperations, operationTimeoutMs,
            INITIAL_RETRY_MS, MAX_RETRY_MS);
    }

    private DisklessTopicLifecycleReconciler newReconciler(long sweepIntervalMs, int maxConcurrentOperations,
                                                           long operationTimeoutMs, long initialRetryMs,
                                                           long maxRetryMs) {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "diskless-lifecycle-reconciler-test");
            thread.setDaemon(true);
            return thread;
        });
        executors.add(executor);
        DisklessTopicLifecycleReconciler created = new DisklessTopicLifecycleReconciler(
            NODE_ID, lifecycle, faultHandler, sweepIntervalMs,
            initialRetryMs, maxRetryMs, operationTimeoutMs, maxConcurrentOperations, executor);
        reconcilers.add(created);
        return created;
    }

    private record TopicSpec(String name, Uuid id, int partitions) { }

    private static TopicSpec disklessTopic(String name, Uuid id, int partitions) {
        return new TopicSpec(name, id, partitions);
    }

    /** An image at {@code offset} holding exactly {@code topics}, each with {@code ursa.storage.enable=true}. */
    private static MetadataImage image(long offset, TopicSpec... topics) {
        MetadataDelta delta = new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build();
        for (TopicSpec topic : topics) {
            replayCreation(delta, topic);
        }
        return delta.apply(new MetadataProvenance(offset, 0, 0, true));
    }

    /** The delta that creates everything in {@code newImage}, as the controller would replay it. */
    private static MetadataDelta delta(MetadataImage newImage) {
        MetadataDelta delta = new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build();
        for (TopicImage topic : newImage.topics().topicsById().values()) {
            replayCreation(delta, new TopicSpec(topic.name(), topic.id(), topic.partitions().size()));
        }
        return delta;
    }

    /** An image at {@code offset} holding one topic with {@code ursa.storage.enable=false}. */
    private static MetadataImage classicImage(long offset, String name, Uuid id, int partitions) {
        MetadataDelta delta = new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build();
        replayCreation(delta, new TopicSpec(name, id, partitions));
        delta.replay(disklessConfigRecord(name, "false"));
        return delta.apply(new MetadataProvenance(offset, 0, 0, true));
    }

    /** The delta that flips {@code ursa.storage.enable} on {@code topicName} to {@code enabled}. */
    private static MetadataDelta configChange(MetadataImage oldImage, String topicName, String enabled) {
        MetadataDelta delta = new MetadataDelta.Builder().setImage(oldImage).build();
        delta.replay(disklessConfigRecord(topicName, enabled));
        return delta;
    }

    /** The delta that removes {@code topicId} from {@code oldImage}. */
    private static MetadataDelta deltaDeleting(Uuid topicId, MetadataImage oldImage) {
        MetadataDelta delta = new MetadataDelta.Builder().setImage(oldImage).build();
        delta.replay(new RemoveTopicRecord().setTopicId(topicId));
        return delta;
    }

    private static void replayCreation(MetadataDelta delta, TopicSpec topic) {
        delta.replay(new TopicRecord().setName(topic.name()).setTopicId(topic.id()));
        for (int partition = 0; partition < topic.partitions(); partition++) {
            delta.replay(new PartitionRecord()
                .setTopicId(topic.id())
                .setPartitionId(partition)
                .setReplicas(List.of(0))
                .setIsr(List.of(0))
                .setRemovingReplicas(List.of())
                .setAddingReplicas(List.of())
                .setLeader(0)
                .setLeaderEpoch(0)
                .setPartitionEpoch(0));
        }
        delta.replay(disklessConfigRecord(topic.name(), "true"));
    }

    private static ConfigRecord disklessConfigRecord(String topicName, String value) {
        return new ConfigRecord()
            .setResourceType(ConfigResource.Type.TOPIC.id())
            .setResourceName(topicName)
            .setName(TopicConfig.URSA_STORAGE_ENABLE_CONFIG)
            .setValue(value);
    }

    private static LoaderManifest manifest() {
        return new LoaderManifest() {
            @Override
            public LoaderManifestType type() {
                return LoaderManifestType.LOG_DELTA;
            }

            @Override
            public MetadataProvenance provenance() {
                return MetadataProvenance.EMPTY;
            }
        };
    }

    private static LeaderAndEpoch leader(int nodeId) {
        return new LeaderAndEpoch(OptionalInt.of(nodeId), 1);
    }
}
