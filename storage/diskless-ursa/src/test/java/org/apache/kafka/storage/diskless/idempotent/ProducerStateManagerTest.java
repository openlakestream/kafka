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
package org.apache.kafka.storage.diskless.idempotent;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.storage.diskless.DisklessClientZone;
import org.apache.kafka.storage.diskless.handlers.KafkaRecordsPayload;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import io.lakestream.api.Log;
import io.lakestream.api.LogCursor;
import io.lakestream.api.LogEntry;
import io.lakestream.api.LogOffset;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.Version;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.DeleteOption;
import io.oxia.client.api.options.PutOption;
import io.oxia.client.api.options.defs.OptionVersionId;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ProducerStateManagerTest {

    @Test
    void testFirstWriteMustStartAtSequenceZero() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        ProducerStateManager manager = newManager(tp, () -> null, emptyLogSupplier());
        try {
            ProducerStateManager.PrepareResult result = manager.prepareAppend(List.of(
                batch(1L, (short) 0, 5, 9, 5, 1000L)
            )).get();

            assertInstanceOf(ProducerStateManager.OutOfOrderSequence.class, result);
            ProducerStateManager.OutOfOrderSequence outOfOrder = (ProducerStateManager.OutOfOrderSequence) result;
            assertEquals(0, outOfOrder.expectedSequence());
            assertEquals(5, outOfOrder.actualSequence());
        } finally {
            manager.close();
        }
    }

    @Test
    void testStaleEpochRejected() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        ProducerStateManager manager = newManager(tp, () -> null, emptyLogSupplier());
        try {
            ProducerStateManager.PendingAppend firstPending =
                ready(manager.prepareAppend(List.of(batch(1L, (short) 5, 0, 9, 10, 1000L))).get());
            manager.completeAppend(firstPending, 0L, 1000L);

            ProducerStateManager.PrepareResult staleEpoch = manager.prepareAppend(List.of(
                batch(1L, (short) 3, 10, 19, 10, 2000L)
            )).get();

            assertInstanceOf(ProducerStateManager.InvalidEpoch.class, staleEpoch);
            ProducerStateManager.InvalidEpoch invalidEpoch = (ProducerStateManager.InvalidEpoch) staleEpoch;
            assertEquals((short) 5, invalidEpoch.currentEpoch());
            assertEquals((short) 3, invalidEpoch.requestEpoch());
        } finally {
            manager.close();
        }
    }

    @Test
    void testOutOfOrderSequenceRejected() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        ProducerStateManager manager = newManager(tp, () -> null, emptyLogSupplier());
        try {
            ProducerStateManager.PendingAppend firstPending =
                ready(manager.prepareAppend(List.of(batch(1L, (short) 0, 0, 9, 10, 1000L))).get());
            manager.completeAppend(firstPending, 0L, 1000L);

            ProducerStateManager.PrepareResult outOfOrder = manager.prepareAppend(List.of(
                batch(1L, (short) 0, 20, 29, 10, 2000L)
            )).get();

            assertInstanceOf(ProducerStateManager.OutOfOrderSequence.class, outOfOrder);
            ProducerStateManager.OutOfOrderSequence result = (ProducerStateManager.OutOfOrderSequence) outOfOrder;
            assertEquals(10, result.expectedSequence());
            assertEquals(20, result.actualSequence());
        } finally {
            manager.close();
        }
    }

    @Test
    void testDuplicateReturnsCachedOffset() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        ProducerStateManager manager = newManager(tp, () -> null, emptyLogSupplier());
        try {
            ProducerStateManager.PendingAppend firstPending =
                ready(manager.prepareAppend(List.of(batch(1L, (short) 0, 0, 9, 10, 1000L))).get());
            manager.completeAppend(firstPending, 100L, 777L);

            ProducerStateManager.PrepareResult duplicate = manager.prepareAppend(List.of(
                batch(1L, (short) 0, 0, 9, 10, 2000L)
            )).get();

            assertInstanceOf(ProducerStateManager.Duplicate.class, duplicate);
            ProducerStateManager.AppendResult appendResult =
                ((ProducerStateManager.Duplicate) duplicate).appendResultFuture().get();
            assertEquals(100L, appendResult.baseOffset());
            assertEquals(777L, appendResult.timestamp());
        } finally {
            manager.close();
        }
    }

    @Test
    void testInFlightDuplicateReusesFuture() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        ProducerStateManager manager = newManager(tp, () -> null, emptyLogSupplier());
        try {
            ProducerStateManager.PendingAppend firstPending =
                ready(manager.prepareAppend(List.of(batch(1L, (short) 0, 0, 9, 10, 1000L))).get());

            ProducerStateManager.PrepareResult duplicate = manager.prepareAppend(List.of(
                batch(1L, (short) 0, 0, 9, 10, 2000L)
            )).get();
            assertInstanceOf(ProducerStateManager.Duplicate.class, duplicate);

            CompletableFuture<ProducerStateManager.AppendResult> duplicateFuture =
                ((ProducerStateManager.Duplicate) duplicate).appendResultFuture();
            assertFalse(duplicateFuture.isDone());

            manager.completeAppend(firstPending, 50L, 555L);
            ProducerStateManager.AppendResult appendResult = duplicateFuture.get();
            assertEquals(50L, appendResult.baseOffset());
            assertEquals(555L, appendResult.timestamp());
        } finally {
            manager.close();
        }
    }

    @Test
    void testSnapshotAndReplayRecovery() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        InMemorySnapshotStore snapshotStore = new InMemorySnapshotStore();

        ProducerStateManager manager1 = newManager(tp, snapshotStore::client, emptyLogSupplier());
        ProducerStateManager manager2 = null;
        try {
            ProducerStateManager.PendingAppend firstPending =
                ready(manager1.prepareAppend(List.of(batch(1L, (short) 0, 0, 1, 2, 1000L))).get());
            manager1.completeAppend(firstPending, 0L, 1000L);
            manager1.takeSnapshot("test").get();
            assertTrue(snapshotStore.data().containsKey(
                    ProducerStateSnapshotKeys.snapshotKey(tp.topicId().toString(), tp.partition())));

            LogEntry firstReplayEntry = newReplayEntry(1L, (short) 0, 2, "r2", 2L, 1);
            LogEntry secondReplayEntry = newReplayEntry(1L, (short) 0, 3, "r3", 3L, 1);
            Log replayLog = replayLog(List.of(firstReplayEntry, secondReplayEntry), 3L, 1);
            manager2 = newManager(tp, snapshotStore::client, () -> CompletableFuture.completedFuture(replayLog));

            ProducerStateManager.PrepareResult recovered = manager2.prepareAppend(List.of(
                batch(1L, (short) 0, 4, 4, 1, 3000L)
            )).get();
            assertInstanceOf(ProducerStateManager.Ready.class, recovered);
            verify(firstReplayEntry).close();
            verify(secondReplayEntry).close();
        } finally {
            manager1.close();
            if (manager2 != null) {
                manager2.close();
            }
        }
    }

    @Test
    void testCleanupWaitsForInFlightSnapshotBeforeDeleting() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        String topicId = tp.topicId().toString();
        String snapshotKey = ProducerStateSnapshotKeys.snapshotKey(topicId, tp.partition());
        String deletedTopicMarkerKey = ProducerStateSnapshotKeys.deletedTopicMarkerKey(topicId);
        InMemorySnapshotStore snapshotStore = new InMemorySnapshotStore();

        ProducerStateManager manager = newManager(tp, snapshotStore::client, emptyLogSupplier());
        try {
            ProducerStateManager.PendingAppend pendingAppend = ready(manager.prepareAppend(List.of(
                    batch(1L, (short) 0, 0, 0, 1, 1000L)
            )).get());
            manager.completeAppend(pendingAppend, 0L, 1000L);

            CompletableFuture<GetResult> pendingMarkerRead = new CompletableFuture<>();
            snapshotStore.queueGet(deletedTopicMarkerKey, pendingMarkerRead);
            CompletableFuture<Void> snapshotFuture = manager.takeSnapshot("test");
            CompletableFuture<Void> cleanupFuture = manager.cleanup(true);

            assertFalse(snapshotFuture.isDone());
            assertFalse(cleanupFuture.isDone());
            assertTrue(snapshotStore.data().containsKey(snapshotKey));

            pendingMarkerRead.complete(null);
            cleanupFuture.get();

            assertFalse(snapshotStore.data().containsKey(snapshotKey));
        } finally {
            manager.close();
        }
    }

    @Test
    void testCleanupWithoutDeletePersistsFinalSnapshot() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        String snapshotKey = ProducerStateSnapshotKeys.snapshotKey(
                tp.topicId().toString(), tp.partition());
        InMemorySnapshotStore snapshotStore = new InMemorySnapshotStore();
        ProducerStateManager manager = newManager(tp, snapshotStore::client, emptyLogSupplier());

        ProducerStateManager.PendingAppend append = ready(manager.prepareAppend(List.of(
                batch(1L, (short) 0, 0, 0, 1, 1000L)
        )).get());
        manager.completeAppend(append, 0L, 1000L);

        manager.cleanup(false).get();

        assertEquals(0,
                ProducerStateSerDes.deserialize(snapshotStore.data().get(snapshotKey)).get(1L).lastSequence());
        verify(snapshotStore.client(), never()).delete(snapshotKey);
        verify(snapshotStore.client(), never()).delete(eq(snapshotKey), anySet());
    }

    @Test
    void testCleanupWithoutDeleteDrainsCoalescedFinalSnapshot() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        String topicId = tp.topicId().toString();
        String snapshotKey = ProducerStateSnapshotKeys.snapshotKey(topicId, tp.partition());
        String deletedTopicMarkerKey = ProducerStateSnapshotKeys.deletedTopicMarkerKey(topicId);
        InMemorySnapshotStore snapshotStore = new InMemorySnapshotStore();
        ProducerStateManager manager = newManager(tp, snapshotStore::client, emptyLogSupplier());

        ProducerStateManager.PendingAppend firstAppend = ready(manager.prepareAppend(List.of(
                batch(1L, (short) 0, 0, 0, 1, 1000L)
        )).get());
        manager.completeAppend(firstAppend, 0L, 1000L);
        CompletableFuture<GetResult> delayedMarkerRead = new CompletableFuture<>();
        snapshotStore.queueGet(deletedTopicMarkerKey, delayedMarkerRead);
        CompletableFuture<Void> firstSnapshot = manager.takeSnapshot("first");

        ProducerStateManager.PendingAppend secondAppend = ready(manager.prepareAppend(List.of(
                batch(1L, (short) 0, 1, 1, 1, 2000L)
        )).get());
        manager.completeAppend(secondAppend, 1L, 2000L);
        CompletableFuture<Void> cleanupFuture = manager.cleanup(false);

        assertFalse(firstSnapshot.isDone());
        assertFalse(cleanupFuture.isDone());
        delayedMarkerRead.complete(null);
        cleanupFuture.get();

        assertEquals(1,
                ProducerStateSerDes.deserialize(snapshotStore.data().get(snapshotKey)).get(1L).lastSequence());
        verify(snapshotStore.client(), never()).delete(snapshotKey);
        verify(snapshotStore.client(), never()).delete(eq(snapshotKey), anySet());
    }

    @Test
    void testCleanupWithoutDeleteWaitsForRecoveryClaimAndRetainsSnapshot() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        String snapshotKey = ProducerStateSnapshotKeys.snapshotKey(
                tp.topicId().toString(), tp.partition());
        byte[] snapshot = ProducerStateSerDes.emptySnapshot();
        InMemorySnapshotStore snapshotStore = new InMemorySnapshotStore();
        snapshotStore.storeValue(snapshotKey, snapshot, 1L);
        CompletableFuture<PutResult> claimGate = new CompletableFuture<>();
        snapshotStore.queuePut(snapshotKey, claimGate);
        ProducerStateManager manager = newManager(tp, snapshotStore::client, emptyLogSupplier());

        CompletableFuture<ProducerStateManager.PrepareResult> prepareFuture = manager.prepareAppend(List.of(
                batch(1L, (short) 0, 0, 0, 1, 1000L)
        ));
        verify(snapshotStore.client(), timeout(5_000))
                .put(eq(snapshotKey), any(byte[].class), anySet());

        CompletableFuture<Void> cleanupFuture = manager.cleanup(false);
        assertFalse(cleanupFuture.isDone(), "Cleanup must drain the in-flight recovery claim");

        snapshotStore.storeValue(snapshotKey, snapshot, 2L);
        claimGate.complete(new PutResult(snapshotKey, InMemorySnapshotStore.version(2L)));
        cleanupFuture.get();

        assertTrue(snapshotStore.data().containsKey(snapshotKey));
        ExecutionException prepareError = assertThrows(ExecutionException.class, prepareFuture::get);
        assertInstanceOf(IllegalStateException.class, prepareError.getCause());
        verify(snapshotStore.client(), never()).delete(eq(snapshotKey), anySet());
    }

    @Test
    void testCleanupWithDeleteWaitsForRecoveryClaimAndDeletesOwnedSnapshot() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        String snapshotKey = ProducerStateSnapshotKeys.snapshotKey(
                tp.topicId().toString(), tp.partition());
        byte[] snapshot = ProducerStateSerDes.emptySnapshot();
        InMemorySnapshotStore snapshotStore = new InMemorySnapshotStore();
        snapshotStore.storeValue(snapshotKey, snapshot, 1L);
        CompletableFuture<PutResult> claimGate = new CompletableFuture<>();
        snapshotStore.queuePut(snapshotKey, claimGate);
        ProducerStateManager manager = newManager(tp, snapshotStore::client, emptyLogSupplier());

        CompletableFuture<ProducerStateManager.PrepareResult> prepareFuture = manager.prepareAppend(List.of(
                batch(1L, (short) 0, 0, 0, 1, 1000L)
        ));
        verify(snapshotStore.client(), timeout(5_000))
                .put(eq(snapshotKey), any(byte[].class), anySet());

        CompletableFuture<Void> cleanupFuture = manager.cleanup(true);
        assertFalse(cleanupFuture.isDone(), "Cleanup must drain the in-flight recovery claim");

        snapshotStore.storeValue(snapshotKey, snapshot, 2L);
        claimGate.complete(new PutResult(snapshotKey, InMemorySnapshotStore.version(2L)));
        cleanupFuture.get();

        assertFalse(snapshotStore.data().containsKey(snapshotKey));
        ExecutionException prepareError = assertThrows(ExecutionException.class, prepareFuture::get);
        assertInstanceOf(IllegalStateException.class, prepareError.getCause());
        verify(snapshotStore.client()).delete(eq(snapshotKey), anySet());
    }

    @Test
    void testAmbiguousSnapshotPutFencesOwnershipWithoutDeleting() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        String topicId = tp.topicId().toString();
        String snapshotKey = ProducerStateSnapshotKeys.snapshotKey(topicId, tp.partition());
        String deletedTopicMarkerKey = ProducerStateSnapshotKeys.deletedTopicMarkerKey(topicId);
        RuntimeException ambiguousPutFailure = new RuntimeException("put outcome is unknown");
        InMemorySnapshotStore snapshotStore = new InMemorySnapshotStore();

        ProducerStateManager manager = newManager(tp, snapshotStore::client, emptyLogSupplier());
        try {
            ProducerStateManager.PendingAppend pendingAppend = ready(manager.prepareAppend(List.of(
                    batch(1L, (short) 0, 0, 0, 1, 1000L)
            )).get());
            manager.completeAppend(pendingAppend, 0L, 1000L);

            snapshotStore.queueGet(deletedTopicMarkerKey, CompletableFuture.completedFuture(null));
            snapshotStore.queuePut(snapshotKey, CompletableFuture.failedFuture(ambiguousPutFailure));
            ExecutionException error = assertThrows(
                    ExecutionException.class, () -> manager.takeSnapshot("test").get());

            assertSame(ambiguousPutFailure, error.getCause());
            assertTrue(ProducerStateSerDes.deserialize(snapshotStore.data().get(snapshotKey)).isEmpty());
            verify(snapshotStore.client(), never()).delete(snapshotKey);
            verify(snapshotStore.client(), never()).delete(eq(snapshotKey), anySet());

            ExecutionException fencedError = assertThrows(
                    ExecutionException.class, () -> manager.takeSnapshot("after-fence").get());
            assertInstanceOf(IllegalStateException.class, fencedError.getCause());

            ExecutionException appendError = assertThrows(
                    ExecutionException.class, () -> manager.prepareAppend(List.of(
                            batch(1L, (short) 0, 1, 1, 1, 2000L)
                    )).get());
            assertInstanceOf(NotLeaderOrFollowerException.class, appendError.getCause());
            manager.cleanup(true).get();
        } finally {
            manager.close();
        }
    }

    @Test
    void testFailedDeletionFenceReadDoesNotCostSnapshotOwnership() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        String topicId = tp.topicId().toString();
        String snapshotKey = ProducerStateSnapshotKeys.snapshotKey(topicId, tp.partition());
        String deletedTopicMarkerKey = ProducerStateSnapshotKeys.deletedTopicMarkerKey(topicId);
        RuntimeException fenceReadFailure = new RuntimeException("fence read unavailable");
        InMemorySnapshotStore snapshotStore = new InMemorySnapshotStore();

        ProducerStateManager manager = newManager(tp, snapshotStore::client, emptyLogSupplier());
        try {
            ProducerStateManager.PendingAppend pendingAppend = ready(manager.prepareAppend(List.of(
                    batch(1L, (short) 0, 0, 0, 1, 1000L)
            )).get());
            manager.completeAppend(pendingAppend, 0L, 1000L);

            snapshotStore.queueGet(deletedTopicMarkerKey, CompletableFuture.failedFuture(fenceReadFailure));
            ExecutionException error = assertThrows(
                    ExecutionException.class, () -> manager.takeSnapshot("fence-read-failure").get());
            assertSame(fenceReadFailure, error.getCause());
            // The failed read stopped the write, so the claim's empty snapshot still stands.
            assertTrue(ProducerStateSerDes.deserialize(snapshotStore.data().get(snapshotKey)).isEmpty());

            // The read told us nothing about the deletion fence, so ownership is untouched and the
            // next attempt writes normally.
            manager.takeSnapshot("retry").get();
            assertEquals(1, ProducerStateSerDes.deserialize(snapshotStore.data().get(snapshotKey)).size());
            manager.cleanup(true).get();
        } finally {
            manager.close();
        }
    }

    @Test
    void testSnapshotIsNotWrittenWhenTopicIsFenced() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        String topicId = tp.topicId().toString();
        String snapshotKey = ProducerStateSnapshotKeys.snapshotKey(topicId, tp.partition());
        InMemorySnapshotStore snapshotStore = new InMemorySnapshotStore();

        ProducerStateManager manager = newManager(tp, snapshotStore::client, emptyLogSupplier());
        try {
            ProducerStateManager.PendingAppend pendingAppend = ready(manager.prepareAppend(List.of(
                    batch(1L, (short) 0, 0, 0, 1, 1000L)
            )).get());
            manager.completeAppend(pendingAppend, 0L, 1000L);
            // Only the empty recovery claim has been written so far.
            assertTrue(ProducerStateSerDes.deserialize(snapshotStore.data().get(snapshotKey)).isEmpty());
            clearInvocations(snapshotStore.client());
            snapshotStore.putFence(tp.topicId());

            manager.takeSnapshot("test").get();

            assertTrue(ProducerStateSerDes.deserialize(snapshotStore.data().get(snapshotKey)).isEmpty());
            verify(snapshotStore.client(), never()).put(
                    startsWith(ProducerStateSnapshotKeys.topicSnapshotPrefix(topicId)),
                    any(byte[].class),
                    anySet());

            ExecutionException appendError = assertThrows(
                    ExecutionException.class, () -> manager.prepareAppend(List.of(
                            batch(1L, (short) 0, 1, 1, 1, 2000L)
                    )).get());
            assertInstanceOf(NotLeaderOrFollowerException.class, appendError.getCause());
        } finally {
            manager.close();
        }
    }

    @Test
    void testPeriodicSnapshotStartsOnlyAfterRecoveryClaimCompletes() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        String snapshotKey = ProducerStateSnapshotKeys.snapshotKey(
                tp.topicId().toString(), tp.partition());
        InMemorySnapshotStore snapshotStore = new InMemorySnapshotStore();
        CompletableFuture<PutResult> claimGate = new CompletableFuture<>();
        snapshotStore.queuePut(snapshotKey, claimGate);
        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        ScheduledFuture<?> periodicTask = mock(ScheduledFuture.class);
        doAnswer(invocation -> periodicTask).when(scheduler).scheduleAtFixedRate(
                any(Runnable.class), anyLong(), anyLong(), eq(TimeUnit.MILLISECONDS));
        ProducerStateManager manager = new ProducerStateManager(
                tp,
                snapshotStore::client,
                emptyLogSupplier()::get,
                DisklessClientZone.NO_ZONE,
                ProducerStateManager.DEFAULT_SNAPSHOT_INTERVAL_MS,
                ProducerStateManager.DEFAULT_SNAPSHOT_RECORD_THRESHOLD,
                scheduler);
        try {
            CompletableFuture<ProducerStateManager.PrepareResult> prepareFuture = manager.prepareAppend(List.of(
                    batch(1L, (short) 0, 0, 0, 1, 1000L)
            ));

            verify(scheduler, never()).scheduleAtFixedRate(
                    any(Runnable.class), anyLong(), anyLong(), eq(TimeUnit.MILLISECONDS));
            snapshotStore.storeValue(snapshotKey, ProducerStateSerDes.emptySnapshot(), 1L);
            claimGate.complete(new PutResult(snapshotKey, InMemorySnapshotStore.version(1L)));

            assertInstanceOf(ProducerStateManager.Ready.class, prepareFuture.get());
            verify(scheduler).scheduleAtFixedRate(
                    any(Runnable.class),
                    eq(ProducerStateManager.DEFAULT_SNAPSHOT_INTERVAL_MS),
                    eq(ProducerStateManager.DEFAULT_SNAPSHOT_INTERVAL_MS),
                    eq(TimeUnit.MILLISECONDS));
        } finally {
            manager.cleanup(true).get();
        }
    }

    @Test
    void testOwnershipLossCancelsPeriodicSnapshotTask() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        String topicId = tp.topicId().toString();
        String deletedTopicMarkerKey = ProducerStateSnapshotKeys.deletedTopicMarkerKey(topicId);
        InMemorySnapshotStore snapshotStore = new InMemorySnapshotStore();
        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        ScheduledFuture<?> periodicTask = mock(ScheduledFuture.class);
        ArgumentCaptor<Runnable> periodicTaskCaptor = ArgumentCaptor.forClass(Runnable.class);
        doAnswer(invocation -> periodicTask).when(scheduler).scheduleAtFixedRate(
                any(Runnable.class), anyLong(), anyLong(), eq(TimeUnit.MILLISECONDS));
        ProducerStateManager manager = new ProducerStateManager(
                tp,
                snapshotStore::client,
                emptyLogSupplier()::get,
                DisklessClientZone.NO_ZONE,
                ProducerStateManager.DEFAULT_SNAPSHOT_INTERVAL_MS,
                ProducerStateManager.DEFAULT_SNAPSHOT_RECORD_THRESHOLD,
                scheduler);
        try {
            ProducerStateManager.PendingAppend pendingAppend = ready(manager.prepareAppend(List.of(
                    batch(1L, (short) 0, 0, 0, 1, 1000L)
            )).get());
            manager.completeAppend(pendingAppend, 0L, 1000L);
            verify(scheduler).scheduleAtFixedRate(
                    periodicTaskCaptor.capture(),
                    eq(ProducerStateManager.DEFAULT_SNAPSHOT_INTERVAL_MS),
                    eq(ProducerStateManager.DEFAULT_SNAPSHOT_INTERVAL_MS),
                    eq(TimeUnit.MILLISECONDS));

            clearInvocations(snapshotStore.client());
            snapshotStore.queueGet(
                    deletedTopicMarkerKey, CompletableFuture.completedFuture(mock(GetResult.class)));
            periodicTaskCaptor.getValue().run();

            verify(periodicTask).cancel(false);
            verify(snapshotStore.client()).get(deletedTopicMarkerKey);

            periodicTaskCaptor.getValue().run();
            verify(snapshotStore.client()).get(deletedTopicMarkerKey);
        } finally {
            manager.cleanup(true).get();
        }
    }

    @Test
    void testSlowSnapshotCoalescesThresholdRequestsAndKeepsLatestState() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        String topicId = tp.topicId().toString();
        String snapshotKey = ProducerStateSnapshotKeys.snapshotKey(topicId, tp.partition());
        String deletedTopicMarkerKey = ProducerStateSnapshotKeys.deletedTopicMarkerKey(topicId);
        InMemorySnapshotStore snapshotStore = new InMemorySnapshotStore();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        ProducerStateManager manager = new ProducerStateManager(
                tp,
                snapshotStore::client,
                emptyLogSupplier()::get,
                DisklessClientZone.NO_ZONE,
                0L,
                1,
                scheduler);
        try {
            ProducerStateManager.PendingAppend firstAppend = ready(manager.prepareAppend(List.of(
                    batch(1L, (short) 0, 0, 0, 1, 1000L)
            )).get());
            CompletableFuture<GetResult> pendingMarkerRead = new CompletableFuture<>();
            snapshotStore.queueGet(deletedTopicMarkerKey, pendingMarkerRead);
            manager.completeAppend(firstAppend, 0L, 1000L);

            for (int sequence = 1; sequence <= 100; sequence++) {
                ProducerStateManager.PendingAppend append = ready(manager.prepareAppend(List.of(
                        batch(1L, (short) 0, sequence, sequence, 1, 1000L + sequence)
                )).get());
                manager.completeAppend(append, sequence, 1000L + sequence);
            }
            CompletableFuture<Void> latestSnapshot = manager.takeSnapshot("latest");
            assertSame(latestSnapshot, manager.takeSnapshot("also-latest"));

            verify(snapshotStore.client()).put(eq(snapshotKey), any(byte[].class), anySet());
            assertFalse(latestSnapshot.isDone());

            pendingMarkerRead.complete(null);
            latestSnapshot.get();

            ArgumentCaptor<byte[]> payloadCaptor = ArgumentCaptor.forClass(byte[].class);
            verify(snapshotStore.client(), times(3))
                    .put(eq(snapshotKey), payloadCaptor.capture(), anySet());
            List<byte[]> payloads = payloadCaptor.getAllValues();
            assertTrue(ProducerStateSerDes.deserialize(payloads.get(0)).isEmpty());
            assertEquals(0, ProducerStateSerDes.deserialize(payloads.get(1)).get(1L).lastSequence());
            assertEquals(100, ProducerStateSerDes.deserialize(payloads.get(2)).get(1L).lastSequence());
            manager.cleanup(true).get();
        } finally {
            manager.close();
            scheduler.shutdownNow();
        }
    }

    @Test
    void testNewManagerClaimFencesQueuedWriteFromOldManager() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        String topicId = tp.topicId().toString();
        String snapshotKey = ProducerStateSnapshotKeys.snapshotKey(topicId, tp.partition());
        String deletedTopicMarkerKey = ProducerStateSnapshotKeys.deletedTopicMarkerKey(topicId);
        InMemorySnapshotStore snapshotStore = new InMemorySnapshotStore();
        ProducerStateManager oldManager = newManager(tp, snapshotStore::client, emptyLogSupplier());
        ProducerStateManager newManager = null;
        try {
            ProducerStateManager.PendingAppend oldAppend = ready(oldManager.prepareAppend(List.of(
                    batch(1L, (short) 0, 0, 0, 1, 1000L)
            )).get());
            oldManager.completeAppend(oldAppend, 0L, 1000L);

            CompletableFuture<GetResult> delayedOldMarkerRead = new CompletableFuture<>();
            snapshotStore.queueGet(deletedTopicMarkerKey, delayedOldMarkerRead);
            CompletableFuture<Void> oldSnapshot = oldManager.takeSnapshot("old-owner");

            newManager = newManager(tp, snapshotStore::client, emptyLogSupplier());
            ProducerStateManager.PrepareResult newOwnerResult = newManager.prepareAppend(List.of(
                    batch(1L, (short) 0, 1, 1, 1, 2000L)
            )).get();
            assertInstanceOf(ProducerStateManager.OutOfOrderSequence.class, newOwnerResult);

            delayedOldMarkerRead.complete(null);
            ExecutionException oldWriteError = assertThrows(ExecutionException.class, oldSnapshot::get);
            assertInstanceOf(UnexpectedVersionIdException.class, oldWriteError.getCause());
            assertTrue(ProducerStateSerDes.deserialize(snapshotStore.data().get(snapshotKey)).isEmpty());

            ExecutionException fencedError = assertThrows(
                    ExecutionException.class, () -> oldManager.takeSnapshot("old-owner-again").get());
            assertInstanceOf(IllegalStateException.class, fencedError.getCause());
        } finally {
            oldManager.cleanup(true).get();
            if (newManager != null) {
                newManager.cleanup(true).get();
            }
        }
    }

    @Test
    void testClaimConflictRereadsAndRetries() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        String topicId = tp.topicId().toString();
        String snapshotKey = ProducerStateSnapshotKeys.snapshotKey(topicId, tp.partition());
        InMemorySnapshotStore snapshotStore = new InMemorySnapshotStore();
        CompletableFuture<PutResult> conflictingClaim = new CompletableFuture<>();
        snapshotStore.queuePut(snapshotKey, conflictingClaim);

        ProducerStateManager manager = newManager(tp, snapshotStore::client, emptyLogSupplier());
        try {
            CompletableFuture<ProducerStateManager.PrepareResult> prepareFuture = manager.prepareAppend(List.of(
                    batch(1L, (short) 0, 0, 0, 1, 1000L)
            ));
            snapshotStore.data().put(snapshotKey, ProducerStateSerDes.emptySnapshot());
            conflictingClaim.completeExceptionally(new KeyAlreadyExistsException(snapshotKey));

            ProducerStateManager.PendingAppend append = ready(prepareFuture.get());
            manager.completeAppend(append, 0L, 1000L);
            manager.takeSnapshot("after-claim-retry").get();

            assertEquals(0,
                    ProducerStateSerDes.deserialize(snapshotStore.data().get(snapshotKey)).get(1L).lastSequence());
            verify(snapshotStore.client(), times(3))
                    .put(eq(snapshotKey), any(byte[].class), anySet());
        } finally {
            manager.cleanup(true).get();
        }
    }

    @Test
    void testReplayRebasesEveryBatchInRawMemoryRecords() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        LogEntry replayEntry = newMultiBatchReplayEntry(10L);
        Log replayLog = replayLog(replayEntry, 10L, 3);
        ProducerStateManager manager = newManager(
                tp, () -> null, () -> CompletableFuture.completedFuture(replayLog));

        try {
            ProducerStateManager.PrepareResult duplicate = manager.prepareAppend(List.of(
                    batch(1L, (short) 0, 2, 2, 1, 3000L)
            )).get();
            assertInstanceOf(ProducerStateManager.Duplicate.class, duplicate);
            ProducerStateManager.AppendResult duplicateResult =
                    ((ProducerStateManager.Duplicate) duplicate).appendResultFuture().get();
            assertEquals(12L, duplicateResult.baseOffset());
            verify(replayEntry).close();
        } finally {
            manager.close();
        }
    }

    @Test
    void testCorruptedSnapshotIgnoredAndOverwritten() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        InMemorySnapshotStore snapshotStore = new InMemorySnapshotStore();
        String snapshotKey = ProducerStateSnapshotKeys.snapshotKey(tp.topicId().toString(), tp.partition());
        snapshotStore.data().put(snapshotKey, "bad-snapshot".getBytes(StandardCharsets.UTF_8));

        ProducerStateManager manager = newManager(tp, snapshotStore::client, emptyLogSupplier());
        try {
            ProducerStateManager.PendingAppend pending =
                ready(manager.prepareAppend(List.of(batch(1L, (short) 0, 0, 0, 1, 1000L))).get());
            manager.completeAppend(pending, 0L, 1000L);
            manager.takeSnapshot("overwrite").get();

            byte[] updated = snapshotStore.data().get(snapshotKey);
            Map<Long, ProducerStateEntry> snapshot = ProducerStateSerDes.deserialize(updated);
            assertEquals(1, snapshot.size());
        } finally {
            manager.close();
        }
    }

    @Test
    void testSnapshotRecoveryIsScopedByZone() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        InMemorySnapshotStore snapshotStore = new InMemorySnapshotStore();
        SupplierWithException<CompletableFuture<Log>> logSupplier = emptyLogSupplier();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        ProducerStateManager zoneAManager = null;
        ProducerStateManager zoneBManager = null;
        ProducerStateManager zoneAReloadedManager = null;
        try {
            zoneAManager = new ProducerStateManager(
                tp,
                snapshotStore::client,
                logSupplier::get,
                " zone-a ",
                0L,
                ProducerStateManager.DEFAULT_SNAPSHOT_RECORD_THRESHOLD,
                scheduler);
            ProducerStateManager.PendingAppend initialAppend = ready(zoneAManager.prepareAppend(List.of(
                batch(1L, (short) 0, 0, 0, 1, 1000L)
            )).get());
            zoneAManager.completeAppend(initialAppend, 0L, 1000L);
            zoneAManager.takeSnapshot("zone-a").get();
            zoneAManager.close();
            zoneAManager = null;

            String zoneAKey = ProducerStateSnapshotKeys.snapshotKey(
                tp.topicId().toString(), tp.partition(), "zone-a");
            assertTrue(snapshotStore.data().containsKey(zoneAKey));

            zoneBManager = new ProducerStateManager(
                tp,
                snapshotStore::client,
                logSupplier::get,
                "zone-b",
                0L,
                ProducerStateManager.DEFAULT_SNAPSHOT_RECORD_THRESHOLD,
                scheduler);
            ProducerStateManager.PrepareResult zoneBResult = zoneBManager.prepareAppend(List.of(
                batch(1L, (short) 0, 1, 1, 1, 2000L)
            )).get();
            assertInstanceOf(ProducerStateManager.OutOfOrderSequence.class, zoneBResult);
            ProducerStateManager.OutOfOrderSequence zoneBOutOfOrder =
                (ProducerStateManager.OutOfOrderSequence) zoneBResult;
            assertEquals(0, zoneBOutOfOrder.expectedSequence());
            zoneBManager.close();
            zoneBManager = null;

            zoneAReloadedManager = new ProducerStateManager(
                tp,
                snapshotStore::client,
                logSupplier::get,
                "zone-a",
                0L,
                ProducerStateManager.DEFAULT_SNAPSHOT_RECORD_THRESHOLD,
                scheduler);
            ProducerStateManager.PendingAppend recoveredAppend = ready(zoneAReloadedManager.prepareAppend(List.of(
                batch(1L, (short) 0, 1, 1, 1, 3000L)
            )).get());
            zoneAReloadedManager.completeAppend(recoveredAppend, 1L, 3000L);
        } finally {
            if (zoneAManager != null) {
                zoneAManager.close();
            }
            if (zoneBManager != null) {
                zoneBManager.close();
            }
            if (zoneAReloadedManager != null) {
                zoneAReloadedManager.close();
            }
            scheduler.shutdownNow();
        }
    }

    @Test
    void testReplaySkipsMalformedEntryAndAdvances() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        LogEntry firstEntry = newReplayEntry(1L, (short) 0, 0, "first", 0L, 1);
        LogEntry malformedEntry = newMalformedReplayEntry(1L, 1);
        LogEntry lastEntry = newReplayEntry(1L, (short) 0, 1, "last", 2L, 1);
        Log replayLog = replayLog(List.of(firstEntry, malformedEntry, lastEntry), 2L, 1);

        ProducerStateManager manager = newManager(
                tp, () -> null, () -> CompletableFuture.completedFuture(replayLog));
        try {
            ProducerStateManager.PrepareResult result = manager.prepareAppend(List.of(
                    batch(1L, (short) 0, 2, 2, 1, 3000L)
            )).get();

            assertInstanceOf(ProducerStateManager.Ready.class, result);
            assertEquals(3L, manager.nextOffsetToRecoverForTesting());
            verify(malformedEntry).close();
            verify(lastEntry).close();
        } finally {
            manager.close();
        }
    }

    @Test
    void testReplayDoesNotStopAtLeadingNonIdempotentEntry() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        LogEntry nonIdempotentEntry = newReplayEntryWithoutProducer("legacy", 0L, 1);
        LogEntry idempotentEntry = newReplayEntry(7L, (short) 0, 0, "idempotent", 1L, 1);
        Log replayLog = replayLog(List.of(nonIdempotentEntry, idempotentEntry), 1L, 1);

        ProducerStateManager manager = newManager(
                tp, () -> null, () -> CompletableFuture.completedFuture(replayLog));
        try {
            // Sequence 1 follows the sequence 0 replayed from behind the non-idempotent entry.
            ProducerStateManager.PrepareResult result = manager.prepareAppend(List.of(
                    batch(7L, (short) 0, 1, 1, 1, 3000L)
            )).get();

            assertInstanceOf(ProducerStateManager.Ready.class, result);
            assertEquals(2L, manager.nextOffsetToRecoverForTesting());
            verify(nonIdempotentEntry).close();
            verify(idempotentEntry).close();
        } finally {
            manager.close();
        }
    }

    @Test
    void testReplayFailureClosesEntireEntryBatch() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        RuntimeException entryFailure = new RuntimeException("entry offset unavailable");
        LogEntry failingEntry = mock(LogEntry.class);
        when(failingEntry.offset()).thenThrow(entryFailure);
        LogEntry unvisitedEntry = newReplayEntry(1L, (short) 0, 0, "unvisited", 1L, 1);
        Log replayLog = replayLog(List.of(failingEntry, unvisitedEntry), 1L, 1);

        ProducerStateManager manager = newManager(
                tp, () -> null, () -> CompletableFuture.completedFuture(replayLog));
        try {
            ExecutionException prepareError = assertThrows(ExecutionException.class, () -> manager.prepareAppend(List.of(
                    batch(1L, (short) 0, 0, 0, 1, 3000L)
            )).get());
            Throwable rootCause = prepareError;
            while (rootCause.getCause() != null) {
                rootCause = rootCause.getCause();
            }

            assertSame(entryFailure, rootCause);
            verify(failingEntry).close();
            verify(unvisitedEntry).close();
        } finally {
            manager.close();
        }
    }

    @Test
    void testLateReplayReadAfterManagerCloseClosesEntriesAndCursor() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        Log logInstance = mock(Log.class);
        LogCursor cursor = mock(LogCursor.class);
        LogOffset lastOffset = mockLogOffset(0L, 1);
        CompletableFuture<List<LogEntry>> readFuture = new CompletableFuture<>();
        when(logInstance.getLastOffset()).thenReturn(
                CompletableFuture.completedFuture(lastOffset));
        when(logInstance.openEphemeralCursor(anyString(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(cursor));
        when(cursor.readEntries(anyInt(), anyLong(), any(), anyLong())).thenReturn(readFuture);

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        ProducerStateManager manager = new ProducerStateManager(
                tp,
                () -> null,
                () -> CompletableFuture.completedFuture(logInstance),
                "test-zone",
                0L,
                ProducerStateManager.DEFAULT_SNAPSHOT_RECORD_THRESHOLD,
                scheduler);
        try {
            CompletableFuture<ProducerStateManager.PrepareResult> prepareFuture = manager.prepareAppend(List.of(
                    batch(1L, (short) 0, 0, 0, 1, 3000L)
            ));
            verify(cursor, timeout(5_000)).readEntries(anyInt(), anyLong(), any(), anyLong());
            assertFalse(prepareFuture.isDone());

            manager.close();
            assertThrows(ExecutionException.class, prepareFuture::get);

            LogEntry lateEntry = newReplayEntry(1L, (short) 0, 0, "late", 0L, 1);
            readFuture.complete(List.of(lateEntry));

            verify(lateEntry, timeout(5_000)).close();
            verify(lateEntry, never()).payload();
            verify(cursor, timeout(5_000)).close();
            verify(cursor).readEntries(anyInt(), anyLong(), any(), anyLong());
        } finally {
            manager.close();
            scheduler.shutdownNow();
        }
    }

    @Test
    void testSynchronousReplayReadFailureCompletesPreparationAndClosesCursor() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        Log logInstance = mock(Log.class);
        LogCursor cursor = mock(LogCursor.class);
        RuntimeException readFailure = new RuntimeException("synchronous replay read failure");
        LogOffset lastOffset = mockLogOffset(0L, 1);
        when(logInstance.getLastOffset()).thenReturn(
                CompletableFuture.completedFuture(lastOffset));
        when(logInstance.openEphemeralCursor(anyString(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(cursor));
        when(cursor.readEntries(anyInt(), anyLong(), any(), anyLong())).thenThrow(readFailure);

        ProducerStateManager manager = newManager(
                tp, () -> null, () -> CompletableFuture.completedFuture(logInstance));
        try {
            CompletableFuture<ProducerStateManager.PrepareResult> prepareFuture = manager.prepareAppend(List.of(
                    batch(1L, (short) 0, 0, 0, 1, 3000L)
            ));

            ExecutionException prepareError = assertThrows(
                    ExecutionException.class,
                    () -> prepareFuture.get(5, TimeUnit.SECONDS));
            Throwable rootCause = prepareError;
            while (rootCause.getCause() != null) {
                rootCause = rootCause.getCause();
            }
            assertSame(readFailure, rootCause);
            assertTrue(prepareFuture.isCompletedExceptionally());
            verify(cursor, timeout(5_000)).close();
        } finally {
            manager.close();
        }
    }

    @Test
    void testTransientReplayCursorCloseFailureRetainsHandleAndRetries() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        Log logInstance = mock(Log.class);
        LogCursor cursor = mock(LogCursor.class);
        RuntimeException readFailure = new RuntimeException("synchronous replay read failure");
        LogOffset lastOffset = mockLogOffset(0L, 1);
        when(logInstance.getLastOffset()).thenReturn(
                CompletableFuture.completedFuture(lastOffset));
        when(logInstance.openEphemeralCursor(anyString(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(cursor));
        when(cursor.readEntries(anyInt(), anyLong(), any(), anyLong())).thenThrow(readFailure);
        doThrow(new IOException("transient cursor close failure"))
                .doNothing()
                .when(cursor)
                .close();

        ProducerStateManager manager = newManager(
                tp, () -> null, () -> CompletableFuture.completedFuture(logInstance));
        try {
            CompletableFuture<ProducerStateManager.PrepareResult> prepareFuture = manager.prepareAppend(List.of(
                    batch(1L, (short) 0, 0, 0, 1, 3000L)
            ));

            ExecutionException prepareError = assertThrows(
                    ExecutionException.class,
                    () -> prepareFuture.get(5, TimeUnit.SECONDS));
            Throwable rootCause = prepareError;
            while (rootCause.getCause() != null) {
                rootCause = rootCause.getCause();
            }
            assertSame(readFailure, rootCause);
            verify(cursor, timeout(5_000).times(2)).close();
        } finally {
            manager.close();
        }
    }

    @Test
    void testReplaySkippedWhenNoSnapshotAndMessagesExceedThreshold() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        Log logInstance = mock(Log.class);
        LogOffset lastOffset = mockLogOffset(0L, 10_001);
        when(logInstance.getLastOffset()).thenReturn(CompletableFuture.completedFuture(lastOffset));

        ProducerStateManager manager = newManager(tp, () -> null, () -> CompletableFuture.completedFuture(logInstance));
        try {
            // Producer 1: first append bypasses sequence check once after recovery is skipped.
            ProducerStateManager.PrepareResult firstProducerFirst = manager.prepareAppend(List.of(
                batch(1L, (short) 0, 5, 5, 1, 3000L)
            )).get();
            assertInstanceOf(ProducerStateManager.Ready.class, firstProducerFirst);
            manager.completeAppend(((ProducerStateManager.Ready) firstProducerFirst).pendingAppend(), 0L, 3000L);

            // Producer 2: also gets one bypass because recovery-skip bypass is tracked per producer.
            ProducerStateManager.PrepareResult secondProducerFirst = manager.prepareAppend(List.of(
                batch(2L, (short) 0, 7, 7, 1, 3001L)
            )).get();
            assertInstanceOf(ProducerStateManager.Ready.class, secondProducerFirst);
            manager.completeAppend(((ProducerStateManager.Ready) secondProducerFirst).pendingAppend(), 1L, 3001L);

            // Producer 1: after its first bypass, validation should be strict again.
            ProducerStateManager.PrepareResult firstProducerSecond = manager.prepareAppend(List.of(
                batch(1L, (short) 0, 7, 7, 1, 3002L)
            )).get();
            assertInstanceOf(ProducerStateManager.OutOfOrderSequence.class, firstProducerSecond);
            verify(logInstance, never()).openEphemeralCursor(anyString(), anyLong());
        } finally {
            manager.close();
        }
    }

    @Test
    void testBypassedPrepareAbortDoesNotGrantSecondBypassForSameProducer() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        Log logInstance = mock(Log.class);
        LogOffset lastOffset = mockLogOffset(0L, 10_001);
        when(logInstance.getLastOffset()).thenReturn(CompletableFuture.completedFuture(lastOffset));

        try (ProducerStateManager manager = newManager(
                tp, () -> null, () -> CompletableFuture.completedFuture(logInstance))) {
            ProducerStateManager.PrepareResult first = manager.prepareAppend(List.of(
                    batch(1L, (short) 0, 5, 5, 1, 3000L)
            )).get();
            assertInstanceOf(ProducerStateManager.Ready.class, first);

            ProducerStateManager.PendingAppend pendingAppend = ((ProducerStateManager.Ready) first).pendingAppend();
            // Simulate append failure and rollback to verify boundary behavior after a bypassed prepare.
            manager.abortAppend(pendingAppend, new RuntimeException("abort first bypassed append"));

            ProducerStateManager.PrepareResult second = manager.prepareAppend(List.of(
                    batch(1L, (short) 0, 5, 5, 1, 3001L)
            )).get();
            assertInstanceOf(ProducerStateManager.OutOfOrderSequence.class, second);

            ProducerStateManager.PrepareResult third = manager.prepareAppend(List.of(
                    batch(1L, (short) 0, 0, 0, 1, 3002L)
            )).get();
            assertInstanceOf(ProducerStateManager.Ready.class, third);
            manager.completeAppend(((ProducerStateManager.Ready) third).pendingAppend(), 42L, 3002L);

            Optional<ProducerStateManager.ProducerState> producerState = manager.producerState(1L);
            assertTrue(producerState.isPresent());
            assertEquals((short) 0, producerState.get().producerEpoch());
            assertEquals(0, producerState.get().lastSequence());
            assertEquals(42L, producerState.get().lastOffset());
            assertEquals(1, producerState.get().retainedBatchCount());

            verify(logInstance, never()).openEphemeralCursor(anyString(), anyLong());
        }
    }

    private static ProducerStateManager newManager(
            TopicIdPartition tp,
            SupplierWithException<AsyncOxiaClient> oxiaClientSupplier,
            SupplierWithException<CompletableFuture<Log>> logSupplier) {
        return new ProducerStateManager(tp, oxiaClientSupplier::get, logSupplier::get);
    }

    private static ProducerStateManager.AppendBatch batch(
            long producerId,
            short producerEpoch,
            int firstSeq,
            int lastSeq,
            int recordCount,
            long timestamp) {
        return new ProducerStateManager.AppendBatch(
            producerId,
            producerEpoch,
            firstSeq,
            lastSeq,
            recordCount,
            timestamp);
    }

    private static ProducerStateManager.PendingAppend ready(ProducerStateManager.PrepareResult result) {
        return ((ProducerStateManager.Ready) result).pendingAppend();
    }

    private static TopicIdPartition testTopicPartition() {
        return new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("test-topic", 0));
    }

    private static SupplierWithException<CompletableFuture<Log>> emptyLogSupplier() {
        return () -> {
            Log logInstance = mock(Log.class);
            LogOffset emptyOffset = mockLogOffset(-1L, 0);
            when(logInstance.getLastOffset()).thenReturn(CompletableFuture.completedFuture(emptyOffset));
            return CompletableFuture.completedFuture(logInstance);
        };
    }

    private static LogOffset mockLogOffset(long offset, int numberOfRecords) {
        LogOffset logOffset = mock(LogOffset.class);
        when(logOffset.offset()).thenReturn(offset);
        when(logOffset.numberOfRecords()).thenReturn(numberOfRecords);
        when(logOffset.timestamp()).thenReturn(-1L);
        return logOffset;
    }

    private static Log replayLog(LogEntry replayEntry, long baseOffset, int numMessages) throws Exception {
        return replayLog(List.of(replayEntry), baseOffset, numMessages);
    }

    private static Log replayLog(List<LogEntry> replayEntries, long baseOffset, int numMessages) throws Exception {
        Log logInstance = mock(Log.class);
        LogCursor cursor = mock(LogCursor.class);
        LogOffset lastOffset = mockLogOffset(baseOffset, numMessages);

        when(logInstance.getLastOffset()).thenReturn(CompletableFuture.completedFuture(lastOffset));
        when(logInstance.openEphemeralCursor(anyString(), anyLong())).thenReturn(
                CompletableFuture.completedFuture(cursor));

        AtomicInteger readCount = new AtomicInteger(0);
        when(cursor.readEntries(anyInt(), anyLong(), any(), anyLong())).thenAnswer(invocation -> {
            if (readCount.getAndIncrement() == 0) {
                return CompletableFuture.completedFuture(replayEntries);
            } else {
                return CompletableFuture.completedFuture(List.of());
            }
        });

        return logInstance;
    }

    private static LogEntry newReplayEntry(
            long producerId,
            short producerEpoch,
            int firstSeq,
            String value,
            long baseOffset,
            int numMessages) {
        MemoryRecords memoryRecords = MemoryRecords.withIdempotentRecords(
            Compression.NONE,
            producerId,
            producerEpoch,
            firstSeq,
            new SimpleRecord(value.getBytes(StandardCharsets.UTF_8)));

        ByteBuf data = KafkaRecordsPayload.copyForAppend(memoryRecords);

        LogEntry entry = mock(LogEntry.class);
        AtomicBoolean closed = new AtomicBoolean();
        when(entry.offset()).thenReturn(baseOffset);
        when(entry.numberOfRecords()).thenReturn(numMessages);
        when(entry.size()).thenReturn(data.readableBytes());
        when(entry.payload()).thenReturn(data.asReadOnly());
        doAnswer(invocation -> {
            if (closed.compareAndSet(false, true)) {
                data.release();
            }
            return null;
        }).when(entry).close();
        return entry;
    }

    private static LogEntry newMultiBatchReplayEntry(long baseOffset) {
        MemoryRecords firstBatch = MemoryRecords.withIdempotentRecords(
                Compression.NONE,
                1L,
                (short) 0,
                0,
                new SimpleRecord("first".getBytes(StandardCharsets.UTF_8)),
                new SimpleRecord("second".getBytes(StandardCharsets.UTF_8)));
        MemoryRecords secondBatch = MemoryRecords.withIdempotentRecords(
                Compression.NONE,
                1L,
                (short) 0,
                2,
                new SimpleRecord("third".getBytes(StandardCharsets.UTF_8)));
        ByteBuffer combined = ByteBuffer.allocate(
                firstBatch.buffer().remaining() + secondBatch.buffer().remaining());
        combined.put(firstBatch.buffer()).put(secondBatch.buffer()).flip();
        ByteBuf data = KafkaRecordsPayload.copyForAppend(MemoryRecords.readableRecords(combined));

        LogEntry entry = mock(LogEntry.class);
        AtomicBoolean closed = new AtomicBoolean();
        when(entry.offset()).thenReturn(baseOffset);
        when(entry.numberOfRecords()).thenReturn(3);
        when(entry.size()).thenReturn(data.readableBytes());
        when(entry.payload()).thenReturn(data.asReadOnly());
        doAnswer(invocation -> {
            if (closed.compareAndSet(false, true)) {
                data.release();
            }
            return null;
        }).when(entry).close();
        return entry;
    }

    /** A storage entry whose payload cannot be decoded as Kafka record batches. */
    private static LogEntry newMalformedReplayEntry(long baseOffset, int numMessages) {
        ByteBuf data = Unpooled.copiedBuffer("garbage-payload".getBytes(StandardCharsets.UTF_8));

        LogEntry entry = mock(LogEntry.class);
        AtomicBoolean closed = new AtomicBoolean();
        when(entry.offset()).thenReturn(baseOffset);
        when(entry.numberOfRecords()).thenReturn(numMessages);
        when(entry.size()).thenReturn(data.readableBytes());
        when(entry.payload()).thenReturn(data.asReadOnly());
        doAnswer(invocation -> {
            if (closed.compareAndSet(false, true)) {
                data.release();
            }
            return null;
        }).when(entry).close();
        return entry;
    }

    private static LogEntry newReplayEntryWithoutProducer(
            String value,
            long baseOffset,
            int numMessages) {
        MemoryRecords memoryRecords = MemoryRecords.withRecords(
            Compression.NONE,
            new SimpleRecord(value.getBytes(StandardCharsets.UTF_8)));

        ByteBuf data = KafkaRecordsPayload.copyForAppend(memoryRecords);

        LogEntry entry = mock(LogEntry.class);
        AtomicBoolean closed = new AtomicBoolean();
        when(entry.offset()).thenReturn(baseOffset);
        when(entry.numberOfRecords()).thenReturn(numMessages);
        when(entry.size()).thenReturn(data.readableBytes());
        when(entry.payload()).thenReturn(data.asReadOnly());
        doAnswer(invocation -> {
            if (closed.compareAndSet(false, true)) {
                data.release();
            }
            return null;
        }).when(entry).close();
        return entry;
    }

    @FunctionalInterface
    private interface SupplierWithException<T> {
        T get();
    }

    private static final class InMemorySnapshotStore {
        private final Map<String, byte[]> data = new ConcurrentHashMap<>();
        private final Map<String, Long> versions = new ConcurrentHashMap<>();
        private final Map<String, ConcurrentLinkedQueue<CompletableFuture<GetResult>>> queuedGets =
                new ConcurrentHashMap<>();
        private final Map<String, ConcurrentLinkedQueue<CompletableFuture<PutResult>>> queuedPuts =
                new ConcurrentHashMap<>();
        private final AtomicLong nextVersionId = new AtomicLong();
        private final AsyncOxiaClient client = mock(AsyncOxiaClient.class);

        InMemorySnapshotStore() throws Exception {
            when(client.get(anyString())).thenAnswer(invocation -> {
                String key = invocation.getArgument(0);
                ConcurrentLinkedQueue<CompletableFuture<GetResult>> queue = queuedGets.get(key);
                CompletableFuture<GetResult> queued = queue == null ? null : queue.poll();
                if (queued != null) {
                    return queued;
                }
                return CompletableFuture.completedFuture(currentValue(key));
            });
            when(client.put(anyString(), any(byte[].class), anySet())).thenAnswer(invocation -> {
                String key = invocation.getArgument(0);
                ConcurrentLinkedQueue<CompletableFuture<PutResult>> queue = queuedPuts.get(key);
                CompletableFuture<PutResult> queued = queue == null ? null : queue.poll();
                if (queued != null) {
                    return queued;
                }
                byte[] value = invocation.getArgument(1);
                Set<PutOption> options = invocation.getArgument(2);
                synchronized (this) {
                    long currentVersionId = currentVersionId(key);
                    Long expectedVersionId = expectedVersionId(options);
                    if (expectedVersionId != null && expectedVersionId != currentVersionId) {
                        return CompletableFuture.failedFuture(
                                new UnexpectedVersionIdException(key, expectedVersionId));
                    }
                    long writtenVersionId = nextVersionId.incrementAndGet();
                    data.put(key, value.clone());
                    versions.put(key, writtenVersionId);
                    return CompletableFuture.completedFuture(
                            new PutResult(key, version(writtenVersionId)));
                }
            });
            when(client.delete(anyString())).thenAnswer(invocation -> {
                String key = invocation.getArgument(0);
                synchronized (this) {
                    boolean removed = data.remove(key) != null;
                    versions.remove(key);
                    return CompletableFuture.completedFuture(removed);
                }
            });
            when(client.delete(anyString(), anySet())).thenAnswer(invocation -> {
                String key = invocation.getArgument(0);
                Set<DeleteOption> options = invocation.getArgument(1);
                synchronized (this) {
                    long currentVersionId = currentVersionId(key);
                    Long expectedVersionId = expectedVersionId(options);
                    if (expectedVersionId != null && expectedVersionId != currentVersionId) {
                        return CompletableFuture.failedFuture(
                                new UnexpectedVersionIdException(key, expectedVersionId));
                    }
                    boolean removed = data.remove(key) != null;
                    versions.remove(key);
                    return CompletableFuture.completedFuture(removed);
                }
            });
        }

        private synchronized GetResult currentValue(String key) {
            byte[] value = data.get(key);
            if (value == null) {
                return null;
            }
            long versionId = versions.computeIfAbsent(key, ignored -> nextVersionId.incrementAndGet());
            return new GetResult(key, value.clone(), version(versionId));
        }

        private synchronized long currentVersionId(String key) {
            if (!data.containsKey(key)) {
                return OptionVersionId.KEY_NOT_EXISTS;
            }
            return versions.computeIfAbsent(key, ignored -> nextVersionId.incrementAndGet());
        }

        private static Long expectedVersionId(Set<?> options) {
            return options.stream()
                    .filter(OptionVersionId.class::isInstance)
                    .map(OptionVersionId.class::cast)
                    .map(OptionVersionId::versionId)
                    .findFirst()
                    .orElse(null);
        }

        private static Version version(long versionId) {
            return new Version(versionId, 0L, 0L, 0L, Optional.empty(), Optional.empty());
        }

        void queueGet(String key, CompletableFuture<GetResult> result) {
            queuedGets.computeIfAbsent(key, ignored -> new ConcurrentLinkedQueue<>()).add(result);
        }

        void queuePut(String key, CompletableFuture<PutResult> result) {
            queuedPuts.computeIfAbsent(key, ignored -> new ConcurrentLinkedQueue<>()).add(result);
        }

        synchronized void storeValue(String key, byte[] value, long versionId) {
            data.put(key, value.clone());
            versions.put(key, versionId);
            nextVersionId.accumulateAndGet(versionId, Math::max);
        }

        /** Writes the durable deletion fence that permanently retires a topic incarnation. */
        void putFence(Uuid topicId) {
            storeValue(
                    ProducerStateSnapshotKeys.deletedTopicMarkerKey(topicId.toString()),
                    new byte[] {1},
                    nextVersionId.incrementAndGet());
        }

        Map<String, byte[]> data() {
            return data;
        }

        AsyncOxiaClient client() {
            return client;
        }
    }
}
