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
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.storage.diskless.handlers.KafkaEntryFormatter;
import org.apache.kafka.storage.diskless.handlers.RecordAnalyzer;

import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.Position;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.ByteBuf;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.Version;
import io.streamnative.ursa.mledger.UrsaPosition;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ProducerStateManagerTest {

    @Test
    void testFirstWriteMustStartAtSequenceZero() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        ProducerStateManager manager = newManager(tp, () -> null, emptyManagedLedgerSupplier());
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
        ProducerStateManager manager = newManager(tp, () -> null, emptyManagedLedgerSupplier());
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
        ProducerStateManager manager = newManager(tp, () -> null, emptyManagedLedgerSupplier());
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
        ProducerStateManager manager = newManager(tp, () -> null, emptyManagedLedgerSupplier());
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
        ProducerStateManager manager = newManager(tp, () -> null, emptyManagedLedgerSupplier());
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

        ProducerStateManager manager1 = newManager(tp, snapshotStore::client, emptyManagedLedgerSupplier());
        ProducerStateManager manager2 = null;
        try {
            ProducerStateManager.PendingAppend firstPending =
                ready(manager1.prepareAppend(List.of(batch(1L, (short) 0, 0, 1, 2, 1000L))).get());
            manager1.completeAppend(firstPending, 0L, 1000L);
            manager1.takeSnapshot("test").get();

            Entry replayEntry = newReplayEntry(tp, 1L, (short) 0, 2, "r2", 2L, 1L);
            ManagedLedger replayLedger = replayLedger(replayEntry, 2L, 1L);
            manager2 = newManager(tp, snapshotStore::client, () -> CompletableFuture.completedFuture(replayLedger));

            ProducerStateManager.PrepareResult recovered = manager2.prepareAppend(List.of(
                batch(1L, (short) 0, 3, 3, 1, 3000L)
            )).get();
            assertInstanceOf(ProducerStateManager.Ready.class, recovered);
        } finally {
            manager1.close();
            if (manager2 != null) {
                manager2.close();
            }
        }
    }

    @Test
    void testCorruptedSnapshotIgnoredAndOverwritten() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        InMemorySnapshotStore snapshotStore = new InMemorySnapshotStore();
        String snapshotKey = ProducerStateSnapshotKeys.snapshotKey(tp.topicId().toString(), tp.partition());
        snapshotStore.data().put(snapshotKey, "bad-snapshot".getBytes(StandardCharsets.UTF_8));

        ProducerStateManager manager = newManager(tp, snapshotStore::client, emptyManagedLedgerSupplier());
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
    void testReplayStopsWhenFirstEntryHasNoProducerAndNoSnapshot() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        Entry nonIdempotentReplayEntry = newReplayEntryWithoutProducer(tp, "legacy", 0L, 1L);
        Entry idempotentReplayEntry = newReplayEntry(tp, 1L, (short) 0, 0, "idempotent", 1L, 1L);
        ManagedLedger replayLedger = replayLedger(List.of(nonIdempotentReplayEntry, idempotentReplayEntry), 1L, 1L);

        ProducerStateManager manager = newManager(tp, () -> null, () -> CompletableFuture.completedFuture(replayLedger));
        try {
            ProducerStateManager.PrepareResult result = manager.prepareAppend(List.of(
                batch(1L, (short) 0, 0, 0, 1, 3000L)
            )).get();
            assertInstanceOf(ProducerStateManager.Ready.class, result);
        } finally {
            manager.close();
        }
    }

    @Test
    void testReplaySkippedWhenNoSnapshotAndMessagesExceedThreshold() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        ManagedLedger managedLedger = mock(ManagedLedger.class);
        // No snapshot -> startOffset is 0. With default threshold 10_000, this should skip replay.
        UrsaPosition lastConfirmed = new UrsaPosition(1L, 0L, 10_001L);
        when(managedLedger.getLastConfirmedEntry()).thenReturn(lastConfirmed);

        ProducerStateManager manager = newManager(tp, () -> null, () -> CompletableFuture.completedFuture(managedLedger));
        try {
            // First append bypasses sequence check once after recovery is skipped.
            ProducerStateManager.PrepareResult first = manager.prepareAppend(List.of(
                batch(1L, (short) 0, 5, 5, 1, 3000L)
            )).get();
            assertInstanceOf(ProducerStateManager.Ready.class, first);
            manager.completeAppend(((ProducerStateManager.Ready) first).pendingAppend(), 0L, 3000L);

            // After the first append, sequence checks should return to normal.
            ProducerStateManager.PrepareResult second = manager.prepareAppend(List.of(
                batch(2L, (short) 0, 7, 7, 1, 3001L)
            )).get();
            assertInstanceOf(ProducerStateManager.OutOfOrderSequence.class, second);
            verify(managedLedger, never()).newNonDurableCursor(any(), anyString());
        } finally {
            manager.close();
        }
    }

    private static ProducerStateManager newManager(
            TopicIdPartition tp,
            SupplierWithException<AsyncOxiaClient> oxiaClientSupplier,
            SupplierWithException<CompletableFuture<ManagedLedger>> managedLedgerSupplier) {
        return new ProducerStateManager(
            tp,
            () -> oxiaClientSupplier.get(),
            () -> managedLedgerSupplier.get());
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

    private static SupplierWithException<CompletableFuture<ManagedLedger>> emptyManagedLedgerSupplier() {
        return () -> {
            ManagedLedger managedLedger = mock(ManagedLedger.class);
            Position empty = mock(Position.class);
            when(empty.getEntryId()).thenReturn(-1L);
            when(empty.getLedgerId()).thenReturn(-1L);
            when(managedLedger.getLastConfirmedEntry()).thenReturn(empty);
            return CompletableFuture.completedFuture(managedLedger);
        };
    }

    private static ManagedLedger replayLedger(Entry replayEntry, long baseOffset, long numMessages) throws Exception {
        return replayLedger(List.of(replayEntry), baseOffset, numMessages);
    }

    private static ManagedLedger replayLedger(List<Entry> replayEntries, long baseOffset, long numMessages) throws Exception {
        ManagedLedger managedLedger = mock(ManagedLedger.class);
        ManagedCursor cursor = mock(ManagedCursor.class);
        UrsaPosition lastConfirmed = new UrsaPosition(1L, baseOffset, numMessages);

        when(managedLedger.getLastConfirmedEntry()).thenReturn(lastConfirmed);
        when(managedLedger.newNonDurableCursor(any(Position.class), anyString())).thenReturn(cursor);

        AtomicInteger readCount = new AtomicInteger(0);
        doAnswer(invocation -> {
            AsyncCallbacks.ReadEntriesCallback callback = invocation.getArgument(2);
            Object ctx = invocation.getArgument(3);
            if (readCount.getAndIncrement() == 0) {
                callback.readEntriesComplete(replayEntries, ctx);
            } else {
                callback.readEntriesComplete(List.of(), ctx);
            }
            return null;
        }).when(cursor).asyncReadEntries(
            anyInt(),
            anyLong(),
            any(AsyncCallbacks.ReadEntriesCallback.class),
            any(),
            any(Position.class));

        doAnswer(invocation -> {
            AsyncCallbacks.DeleteCursorCallback callback = invocation.getArgument(1);
            callback.deleteCursorComplete(invocation.getArgument(2));
            return null;
        }).when(managedLedger).asyncDeleteCursor(anyString(), any(AsyncCallbacks.DeleteCursorCallback.class), any());

        return managedLedger;
    }

    private static Entry newReplayEntry(
            TopicIdPartition tp,
            long producerId,
            short producerEpoch,
            int firstSeq,
            String value,
            long baseOffset,
            long numMessages) {
        MemoryRecords memoryRecords = MemoryRecords.withIdempotentRecords(
            Compression.NONE,
            producerId,
            producerEpoch,
            firstSeq,
            new SimpleRecord(value.getBytes(StandardCharsets.UTF_8)));

        RecordAnalyzer.RecordAnalysisResult analysisResult = RecordAnalyzer.analyzeAndValidateRecords(
            memoryRecords,
            new TopicPartition(tp.topic(), tp.partition()),
            0);
        ByteBuf data = KafkaEntryFormatter.encode(memoryRecords, analysisResult);

        Entry entry = mock(Entry.class);
        UrsaPosition position = new UrsaPosition(1L, baseOffset, numMessages);
        when(entry.getPosition()).thenReturn(position);
        when(entry.getEntryId()).thenReturn(baseOffset);
        when(entry.getDataBuffer()).thenReturn(data.duplicate());
        when(entry.release()).thenReturn(true);
        return entry;
    }

    private static Entry newReplayEntryWithoutProducer(
            TopicIdPartition tp,
            String value,
            long baseOffset,
            long numMessages) {
        MemoryRecords memoryRecords = MemoryRecords.withRecords(
            Compression.NONE,
            new SimpleRecord(value.getBytes(StandardCharsets.UTF_8)));

        RecordAnalyzer.RecordAnalysisResult analysisResult = RecordAnalyzer.analyzeAndValidateRecords(
            memoryRecords,
            new TopicPartition(tp.topic(), tp.partition()),
            0);
        ByteBuf data = KafkaEntryFormatter.encode(memoryRecords, analysisResult);

        Entry entry = mock(Entry.class);
        UrsaPosition position = new UrsaPosition(1L, baseOffset, numMessages);
        when(entry.getPosition()).thenReturn(position);
        when(entry.getEntryId()).thenReturn(baseOffset);
        when(entry.getDataBuffer()).thenReturn(data.duplicate());
        when(entry.release()).thenReturn(true);
        return entry;
    }

    @FunctionalInterface
    private interface SupplierWithException<T> {
        T get();
    }

    private static final class InMemorySnapshotStore {
        private static final Version VERSION = new Version(
            1L, 0L, 0L, 0L, Optional.empty(), Optional.empty());

        private final Map<String, byte[]> data = new ConcurrentHashMap<>();
        private final AsyncOxiaClient client = mock(AsyncOxiaClient.class);

        InMemorySnapshotStore() throws Exception {
            when(client.get(anyString())).thenAnswer(invocation -> {
                String key = invocation.getArgument(0);
                byte[] value = data.get(key);
                if (value == null) {
                    return CompletableFuture.completedFuture(null);
                }
                return CompletableFuture.completedFuture(new GetResult(key, value, VERSION));
            });
            when(client.put(anyString(), any(byte[].class))).thenAnswer(invocation -> {
                String key = invocation.getArgument(0);
                byte[] value = invocation.getArgument(1);
                data.put(key, value);
                return CompletableFuture.completedFuture(new PutResult(key, VERSION));
            });
            when(client.delete(anyString())).thenAnswer(invocation -> {
                data.remove(invocation.getArgument(0));
                return CompletableFuture.completedFuture(true);
            });
        }

        Map<String, byte[]> data() {
            return data;
        }

        AsyncOxiaClient client() {
            return client;
        }
    }
}
