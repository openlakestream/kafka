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
package org.apache.kafka.storage.diskless.handlers;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.diskless.DisklessClientZone;
import org.apache.kafka.storage.diskless.idempotent.ProducerStateManager;
import org.apache.kafka.test.TestUtils;

import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.buffer.ByteBuf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

class UrsaManagedLedgerWriterIdempotentTest {

    @Test
    void testIdempotentWritesAreSequencedPerPartition() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        ManagedLedger managedLedger = mock(ManagedLedger.class);
        Position emptyPosition = position(-1L, -1L);
        when(managedLedger.getLastConfirmedEntry()).thenReturn(emptyPosition);

        CountDownLatch firstAppendSubmitted = new CountDownLatch(1);
        CountDownLatch secondAppendSubmitted = new CountDownLatch(1);
        AtomicReference<AsyncCallbacks.AddEntryCallback> firstCallbackRef = new AtomicReference<>();
        AtomicReference<AsyncCallbacks.AddEntryCallback> secondCallbackRef = new AtomicReference<>();
        AtomicReference<Position> firstPositionRef = new AtomicReference<>();
        AtomicReference<Position> secondPositionRef = new AtomicReference<>();
        AtomicLong nextEntryId = new AtomicLong(0L);

        doAnswer(invocation -> {
            ByteBuf buffer = invocation.getArgument(0);
            int numberOfMessages = invocation.getArgument(1);
            AsyncCallbacks.AddEntryCallback callback = invocation.getArgument(2);

            long sequenceId = readSequenceId(buffer);
            long entryId = nextEntryId.getAndAdd(numberOfMessages);
            Position position = position(entryId, 1L);
            if (sequenceId == 0L) {
                firstCallbackRef.set(callback);
                firstPositionRef.set(position);
                firstAppendSubmitted.countDown();
                return null;
            }
            if (sequenceId == 1L) {
                secondCallbackRef.set(callback);
                secondPositionRef.set(position);
                secondAppendSubmitted.countDown();
                return null;
            }

            callback.addFailed(new ManagedLedgerException("Unexpected sequenceId " + sequenceId), null);
            return null;
        }).when(managedLedger).asyncAddEntry(
            any(ByteBuf.class),
            anyInt(),
            any(AsyncCallbacks.AddEntryCallback.class),
            any());

        ProducerStateManager producerStateManager = spy(new ProducerStateManager(
            tp,
            () -> null,
            () -> CompletableFuture.completedFuture(managedLedger)));

        UrsaStorageState state = mock(UrsaStorageState.class);
        UrsaPartitionLog partitionLog = attachPartitionLog(state, tp, managedLedger);
        partitionLog.installProducerStateManager(DisklessClientZone.NO_ZONE, producerStateManager);
        AtomicInteger prepareAppendCalls = new AtomicInteger(0);
        CountDownLatch firstPrepareAppendCall = new CountDownLatch(1);
        CountDownLatch allowFirstPrepareAppendReturn = new CountDownLatch(1);
        doAnswer(invocation -> {
            if (prepareAppendCalls.incrementAndGet() == 1) {
                firstPrepareAppendCall.countDown();
                assertTrue(
                    await(allowFirstPrepareAppendReturn, "Timed out waiting to release first prepareAppend call"),
                    "Timed out waiting to release first prepareAppend call");
            }
            return invocation.callRealMethod();
        }).when(producerStateManager).prepareAppend(any());

        UrsaManagedLedgerWriter writer = new UrsaManagedLedgerWriter(state);

        MemoryRecords records1 = MemoryRecords.withIdempotentRecords(
            Compression.NONE,
            3333L,
            (short) 0,
            0,
            new SimpleRecord("a".getBytes(StandardCharsets.UTF_8))
        );
        MemoryRecords records2 = MemoryRecords.withIdempotentRecords(
            Compression.NONE,
            3333L,
            (short) 0,
            1,
            new SimpleRecord("b".getBytes(StandardCharsets.UTF_8))
        );

        ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            CompletableFuture<Map<TopicIdPartition, PartitionResponse>> firstWrite =
                CompletableFuture.supplyAsync(() -> writer.write(Map.of(tp, records1), DisklessClientZone.NO_ZONE), executor)
                    .thenCompose(future -> future);

            TestUtils.waitForCondition(
                () -> firstPrepareAppendCall.getCount() == 0,
                5_000L,
                "Timed out waiting for first prepareAppend call");

            CompletableFuture<Map<TopicIdPartition, PartitionResponse>> secondWrite =
                CompletableFuture.supplyAsync(() -> writer.write(Map.of(tp, records2), DisklessClientZone.NO_ZONE), executor)
                    .thenCompose(future -> future);

            // Ensure the second write can't start preparing appends until the first one has been submitted.
            assertEquals(1, prepareAppendCalls.get());

            allowFirstPrepareAppendReturn.countDown();

            TestUtils.waitForCondition(
                () -> firstAppendSubmitted.getCount() == 0,
                5_000L,
                "Timed out waiting for first asyncAddEntry call");

            assertTrue(
                secondAppendSubmitted.await(5, TimeUnit.SECONDS),
                "Timed out waiting for second asyncAddEntry call");

            // Complete the second append first to simulate out-of-order completion.
            secondCallbackRef.get().addComplete(secondPositionRef.get(), null, null);
            firstCallbackRef.get().addComplete(firstPositionRef.get(), null, null);

            PartitionResponse firstResponse = firstWrite.get().get(tp);
            PartitionResponse secondResponse = secondWrite.get().get(tp);

            assertEquals(Errors.NONE, firstResponse.error);
            assertEquals(Errors.NONE, secondResponse.error);
            assertEquals(0L, firstResponse.baseOffset);
            assertEquals(1L, secondResponse.baseOffset);
        } finally {
            executor.shutdownNow();
            producerStateManager.close();
            writer.close();
        }
    }

    @Test
    void testIdempotentWritesOnOtherPartitionsAreNotBlockedByTail() throws Exception {
        Uuid topicId = Uuid.randomUuid();
        TopicIdPartition tp0 = new TopicIdPartition(topicId, new TopicPartition("test-topic", 0));
        TopicIdPartition tp1 = new TopicIdPartition(topicId, new TopicPartition("test-topic", 1));

        ManagedLedger managedLedger0 = mock(ManagedLedger.class);
        ManagedLedger managedLedger1 = mock(ManagedLedger.class);
        Position emptyPosition = position(-1L, -1L);
        when(managedLedger0.getLastConfirmedEntry()).thenReturn(emptyPosition);
        when(managedLedger1.getLastConfirmedEntry()).thenReturn(emptyPosition);

        AtomicLong nextOffset0 = new AtomicLong(0L);
        doAnswer(invocation -> {
            int numberOfMessages = invocation.getArgument(1);
            AsyncCallbacks.AddEntryCallback callback = invocation.getArgument(2);
            long baseOffset = nextOffset0.getAndAdd(numberOfMessages);
            callback.addComplete(position(baseOffset, 1L), null, null);
            return null;
        }).when(managedLedger0).asyncAddEntry(
            any(ByteBuf.class),
            anyInt(),
            any(AsyncCallbacks.AddEntryCallback.class),
            any());

        AtomicLong nextOffset1 = new AtomicLong(0L);
        CountDownLatch tp1Seq0Submitted = new CountDownLatch(1);
        doAnswer(invocation -> {
            ByteBuf buffer = invocation.getArgument(0);
            int numberOfMessages = invocation.getArgument(1);
            AsyncCallbacks.AddEntryCallback callback = invocation.getArgument(2);

            long sequenceId = readSequenceId(buffer);
            if (sequenceId == 0L) {
                tp1Seq0Submitted.countDown();
            }

            long baseOffset = nextOffset1.getAndAdd(numberOfMessages);
            callback.addComplete(position(baseOffset, 1L), null, null);
            return null;
        }).when(managedLedger1).asyncAddEntry(
            any(ByteBuf.class),
            anyInt(),
            any(AsyncCallbacks.AddEntryCallback.class),
            any());

        ProducerStateManager producerStateManager0 = spy(new ProducerStateManager(
            tp0,
            () -> null,
            () -> CompletableFuture.completedFuture(managedLedger0)));
        ProducerStateManager producerStateManager1 = new ProducerStateManager(
            tp1,
            () -> null,
            () -> CompletableFuture.completedFuture(managedLedger1));

        UrsaStorageState state = mock(UrsaStorageState.class);

        UrsaPartitionLog tp0PartitionLog = attachPartitionLog(state, tp0, managedLedger0);
        tp0PartitionLog.installProducerStateManager(DisklessClientZone.NO_ZONE, producerStateManager0);
        CountDownLatch tp0PrepareAppendCalled = new CountDownLatch(1);
        CountDownLatch releaseTp0PrepareAppend = new CountDownLatch(1);
        doAnswer(invocation -> {
            tp0PrepareAppendCalled.countDown();
            assertTrue(
                await(releaseTp0PrepareAppend, "Timed out waiting to release prepareAppend for tp0"),
                "Timed out waiting to release prepareAppend for tp0");
            return invocation.callRealMethod();
        }).when(producerStateManager0).prepareAppend(any());
        UrsaPartitionLog tp1PartitionLog = attachPartitionLog(state, tp1, managedLedger1);
        tp1PartitionLog.installProducerStateManager(DisklessClientZone.NO_ZONE, producerStateManager1);

        UrsaManagedLedgerWriter writer = new UrsaManagedLedgerWriter(state);

        MemoryRecords tp0Records = MemoryRecords.withIdempotentRecords(
            Compression.NONE,
            3333L,
            (short) 0,
            0,
            new SimpleRecord("x".getBytes(StandardCharsets.UTF_8))
        );
        MemoryRecords tp1Seq0 = MemoryRecords.withIdempotentRecords(
            Compression.NONE,
            3333L,
            (short) 0,
            0,
            new SimpleRecord("a".getBytes(StandardCharsets.UTF_8))
        );
        MemoryRecords tp1Seq1 = MemoryRecords.withIdempotentRecords(
            Compression.NONE,
            3333L,
            (short) 0,
            1,
            new SimpleRecord("b".getBytes(StandardCharsets.UTF_8))
        );

        LinkedHashMap<TopicIdPartition, MemoryRecords> firstRequest = new LinkedHashMap<>();
        firstRequest.put(tp0, tp0Records);
        firstRequest.put(tp1, tp1Seq0);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            CompletableFuture<Map<TopicIdPartition, PartitionResponse>> firstWrite =
                CompletableFuture.supplyAsync(() -> writer.write(firstRequest, DisklessClientZone.NO_ZONE), executor)
                    .thenCompose(future -> future);

            TestUtils.waitForCondition(
                () -> tp0PrepareAppendCalled.getCount() == 0,
                5_000L,
                "Timed out waiting for tp0 prepareAppend call");

            assertTrue(
                tp1Seq0Submitted.await(5, TimeUnit.SECONDS),
                "Timed out waiting for tp1 sequence 0 to be submitted while tp0 is blocked");

            CompletableFuture<Map<TopicIdPartition, PartitionResponse>> secondWrite =
                CompletableFuture.supplyAsync(() -> writer.write(Map.of(tp1, tp1Seq1), DisklessClientZone.NO_ZONE), executor)
                    .thenCompose(future -> future);

            releaseTp0PrepareAppend.countDown();

            PartitionResponse firstResponse = firstWrite.get().get(tp1);
            PartitionResponse secondResponse = secondWrite.get().get(tp1);

            assertEquals(Errors.NONE, firstResponse.error);
            assertEquals(Errors.NONE, secondResponse.error);
            assertEquals(0L, firstResponse.baseOffset);
            assertEquals(1L, secondResponse.baseOffset);
        } finally {
            releaseTp0PrepareAppend.countDown();
            executor.shutdownNow();
            producerStateManager0.close();
            producerStateManager1.close();
            writer.close();
        }
    }

    @Test
    void testInFlightDuplicateReusesFuture() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        ManagedLedger managedLedger = mock(ManagedLedger.class);
        Position emptyPosition = position(-1L, -1L);
        Position appendPosition = position(200L, 0L);
        when(managedLedger.getLastConfirmedEntry()).thenReturn(emptyPosition);

        AtomicReference<AsyncCallbacks.AddEntryCallback> addEntryCallbackRef = new AtomicReference<>();
        AtomicInteger addEntryCalls = new AtomicInteger(0);
        doAnswer(invocation -> {
            addEntryCalls.incrementAndGet();
            addEntryCallbackRef.set(invocation.getArgument(2));
            return null;
        }).when(managedLedger).asyncAddEntry(
            any(ByteBuf.class),
            anyInt(),
            any(AsyncCallbacks.AddEntryCallback.class),
            any());

        ProducerStateManager producerStateManager = new ProducerStateManager(
            tp,
            () -> null,
            () -> CompletableFuture.completedFuture(managedLedger));

        UrsaStorageState state = mock(UrsaStorageState.class);
        attachPartitionLog(state, tp, managedLedger).installProducerStateManager(DisklessClientZone.NO_ZONE, producerStateManager);

        UrsaManagedLedgerWriter writer = new UrsaManagedLedgerWriter(state);

        MemoryRecords records = MemoryRecords.withIdempotentRecords(
            Compression.NONE,
            1234L,
            (short) 0,
            0,
            new SimpleRecord("a".getBytes(StandardCharsets.UTF_8)),
            new SimpleRecord("b".getBytes(StandardCharsets.UTF_8))
        );

        try {
            CompletableFuture<Map<TopicIdPartition, PartitionResponse>> firstWrite =
                    writer.write(Map.of(tp, records), DisklessClientZone.NO_ZONE);
            CompletableFuture<Map<TopicIdPartition, PartitionResponse>> secondWrite =
                    writer.write(Map.of(tp, records), DisklessClientZone.NO_ZONE);

            assertFalse(firstWrite.isDone());
            assertFalse(secondWrite.isDone());
            TestUtils.waitForCondition(
                () -> addEntryCalls.get() == 1,
                5_000L,
                "Timed out waiting for first asyncAddEntry call");
            assertEquals(1, addEntryCalls.get());

            AsyncCallbacks.AddEntryCallback addEntryCallback = addEntryCallbackRef.get();
            addEntryCallback.addComplete(appendPosition, null, null);

            PartitionResponse first = firstWrite.get().get(tp);
            PartitionResponse second = secondWrite.get().get(tp);
            assertEquals(Errors.NONE, first.error);
            assertEquals(Errors.NONE, second.error);
            assertEquals(200L, first.baseOffset);
            assertEquals(200L, second.baseOffset);
        } finally {
            producerStateManager.close();
            writer.close();
        }
    }

    @Test
    void testSingleRequestMultiBatchAssignsBatchBaseOffsets() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        ManagedLedger managedLedger = mock(ManagedLedger.class);
        Position emptyPosition = position(-1L, -1L);
        Position appendPosition = position(100L, 0L);
        when(managedLedger.getLastConfirmedEntry()).thenReturn(emptyPosition);
        doAnswer(invocation -> {
            AsyncCallbacks.AddEntryCallback callback = invocation.getArgument(2);
            callback.addComplete(appendPosition, null, invocation.getArgument(3));
            return null;
        }).when(managedLedger).asyncAddEntry(
            any(ByteBuf.class),
            anyInt(),
            any(AsyncCallbacks.AddEntryCallback.class),
            any());

        ProducerStateManager producerStateManager = new ProducerStateManager(
            tp,
            () -> null,
            () -> CompletableFuture.completedFuture(managedLedger));

        UrsaStorageState state = mock(UrsaStorageState.class);
        attachPartitionLog(state, tp, managedLedger).installProducerStateManager(DisklessClientZone.NO_ZONE, producerStateManager);

        UrsaManagedLedgerWriter writer = new UrsaManagedLedgerWriter(state);

        MemoryRecords firstBatch = MemoryRecords.withIdempotentRecords(
            Compression.NONE,
            2222L,
            (short) 0,
            0,
            new SimpleRecord("a".getBytes(StandardCharsets.UTF_8)),
            new SimpleRecord("b".getBytes(StandardCharsets.UTF_8))
        );
        MemoryRecords secondBatch = MemoryRecords.withIdempotentRecords(
            Compression.NONE,
            2222L,
            (short) 0,
            2,
            new SimpleRecord("c".getBytes(StandardCharsets.UTF_8)),
            new SimpleRecord("d".getBytes(StandardCharsets.UTF_8)),
            new SimpleRecord("e".getBytes(StandardCharsets.UTF_8))
        );
        MemoryRecords mergedRecords = mergeRecords(firstBatch, secondBatch);

        try {
            Map<TopicIdPartition, PartitionResponse> response =
                    writer.write(Map.of(tp, mergedRecords), DisklessClientZone.NO_ZONE).get();
            PartitionResponse partitionResponse = response.get(tp);
            assertEquals(Errors.NONE, partitionResponse.error);
            assertEquals(100L, partitionResponse.baseOffset);

            Optional<ProducerStateManager.ProducerState> producerState = producerStateManager.producerState(2222L);
            assertTrue(producerState.isPresent());
            assertEquals(4, producerState.get().lastSequence());
            assertEquals(102L, producerState.get().lastOffset());
        } finally {
            producerStateManager.close();
            writer.close();
        }
    }

    private static TopicIdPartition testTopicPartition() {
        return new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("test-topic", 0));
    }

    private static Position position(long entryId, long ledgerId) {
        Position position = mock(Position.class);
        when(position.getEntryId()).thenReturn(entryId);
        when(position.getLedgerId()).thenReturn(ledgerId);
        return position;
    }

    private static MemoryRecords mergeRecords(MemoryRecords first, MemoryRecords second) {
        ByteBuffer merged = ByteBuffer.allocate(first.sizeInBytes() + second.sizeInBytes());
        merged.put(first.buffer().duplicate());
        merged.put(second.buffer().duplicate());
        merged.flip();
        return MemoryRecords.readableRecords(merged);
    }

    private static long readSequenceId(ByteBuf buffer) {
        MessageMetadata metadata = Commands.parseMessageMetadata(buffer.duplicate());
        if (metadata.hasSequenceId()) {
            return metadata.getSequenceId();
        }
        return -1L;
    }

    private static boolean await(CountDownLatch latch, String failureMessage) {
        try {
            return latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(failureMessage, e);
        }
    }

    private static UrsaPartitionLog attachPartitionLog(
            UrsaStorageState state,
            TopicIdPartition tp,
            ManagedLedger managedLedger) {
        UrsaPartitionLog partitionLog = new UrsaPartitionLog(
                tp,
                state,
                new DisklessLogMetrics(),
                CompletableFuture.completedFuture(managedLedger),
                null,
                0L,
                0,
                null);
        when(state.time()).thenReturn(Time.SYSTEM);
        when(state.getOrCreatePartitionLog(tp)).thenReturn(partitionLog);
        return partitionLog;
    }
}
