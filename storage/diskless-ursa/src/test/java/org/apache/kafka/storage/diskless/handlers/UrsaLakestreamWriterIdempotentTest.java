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
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.diskless.DisklessClientZone;
import org.apache.kafka.storage.diskless.idempotent.ProducerStateManager;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import io.lakestream.api.Log;
import io.lakestream.api.LogEntryHeader;
import io.lakestream.api.LogOffset;
import io.netty.buffer.ByteBuf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class UrsaLakestreamWriterIdempotentTest {

    private static ScheduledExecutorService timer;

    @BeforeAll
    static void startTimer() {
        timer = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "diskless-timer-test");
            thread.setDaemon(true);
            return thread;
        });
    }

    @AfterAll
    static void stopTimer() {
        timer.shutdownNow();
    }

    @Test
    void testProducerSnapshotOwnershipLossInvalidatesPartition() throws Exception {
        TopicIdPartition tp = testTopicPartition(0);
        Log logInstance = emptyLog();
        ProducerStateManager producerStateManager = spy(newProducerStateManager(tp, logInstance));
        doAnswer(invocation -> CompletableFuture.failedFuture(
                new NotLeaderOrFollowerException("producer snapshot ownership lost")))
                .when(producerStateManager).prepareAppend(any());
        UrsaStorageState state = mock(UrsaStorageState.class);
        UrsaPartitionLog partitionLog = attachPartitionLog(state, tp, logInstance);
        partitionLog.installProducerStateManager(DisklessClientZone.NO_ZONE, producerStateManager);
        UrsaLakestreamWriter writer = new UrsaLakestreamWriter(state);

        try {
            PartitionResponse response = writer.write(
                    Map.of(tp, idempotentRecords(4_444L, 0, "fenced")),
                    DisklessClientZone.NO_ZONE).get(5, TimeUnit.SECONDS).get(tp);

            assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, response.error);
            verify(state).removePartitionLog(tp, partitionLog);
            verify(logInstance, never()).append(anyInt(), any(ByteBuf.class));
        } finally {
            producerStateManager.close();
            writer.close();
        }
    }

    @Test
    void testQueuedWriteDoesNotPrepareOrAppendAfterPartitionClose() throws Exception {
        TopicIdPartition tp = testTopicPartition(0);
        Log logInstance = emptyLog();
        ProducerStateManager producerStateManager = spy(newProducerStateManager(tp, logInstance));
        UrsaStorageState state = mock(UrsaStorageState.class);
        UrsaPartitionLog partitionLog = attachPartitionLog(state, tp, logInstance);
        partitionLog.installProducerStateManager(DisklessClientZone.NO_ZONE, producerStateManager);

        AtomicInteger prepareAppendCalls = new AtomicInteger();
        CountDownLatch firstPrepareEntered = new CountDownLatch(1);
        CountDownLatch releaseFirstPrepare = new CountDownLatch(1);
        doAnswer(invocation -> {
            prepareAppendCalls.incrementAndGet();
            firstPrepareEntered.countDown();
            assertTrue(await(releaseFirstPrepare),
                    "Timed out waiting to release the in-flight prepareAppend call");
            return invocation.callRealMethod();
        }).when(producerStateManager).prepareAppend(any());

        UrsaLakestreamWriter writer = new UrsaLakestreamWriter(state);
        CompletableFuture<Map<TopicIdPartition, PartitionResponse>> firstWrite = null;
        CompletableFuture<Map<TopicIdPartition, PartitionResponse>> queuedWrite = null;
        try {
            firstWrite = writer.write(
                    Map.of(tp, idempotentRecords(4_444L, 0, "first")),
                    DisklessClientZone.NO_ZONE);
            assertTrue(firstPrepareEntered.await(5, TimeUnit.SECONDS),
                    "Timed out waiting for the first prepareAppend call");

            queuedWrite = writer.write(
                    Map.of(tp, idempotentRecords(4_444L, 1, "queued")),
                    DisklessClientZone.NO_ZONE);
            assertEquals(1, prepareAppendCalls.get(), "The second write should still be queued");
            assertEquals(2, partitionLog.ownedWritePayloadCount());

            partitionLog.close(false);
            assertEquals(0, partitionLog.ownedWritePayloadCount());
            releaseFirstPrepare.countDown();

            PartitionResponse firstResponse = firstWrite.get(5, TimeUnit.SECONDS).get(tp);
            PartitionResponse queuedResponse = queuedWrite.get(5, TimeUnit.SECONDS).get(tp);
            assertFalse(firstResponse.error == Errors.NONE);
            assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, queuedResponse.error);
            assertEquals(1, prepareAppendCalls.get(),
                    "A queued write must not enter producer-state preparation after close");
            verify(logInstance, never()).append(anyInt(), any(ByteBuf.class));
        } finally {
            releaseFirstPrepare.countDown();
            producerStateManager.close();
            writer.close();
        }
    }

    @Test
    void testIdempotentWritesAreSequencedPerPartition() throws Exception {
        TopicIdPartition tp = testTopicPartition(0);
        Log logInstance = emptyLog();

        CountDownLatch firstAppendSubmitted = new CountDownLatch(1);
        CountDownLatch secondAppendSubmitted = new CountDownLatch(1);
        AtomicReference<CompletableFuture<LogEntryHeader>> firstAppendFuture = new AtomicReference<>();
        AtomicReference<CompletableFuture<LogEntryHeader>> secondAppendFuture = new AtomicReference<>();
        AtomicLong nextOffset = new AtomicLong(0L);

        when(logInstance.append(anyInt(), any(ByteBuf.class))).thenAnswer(invocation -> {
            int numberOfMessages = invocation.getArgument(0);
            long baseOffset = nextOffset.getAndAdd(numberOfMessages);
            CompletableFuture<LogEntryHeader> future = new CompletableFuture<>();
            if (baseOffset == 0L) {
                firstAppendFuture.set(future);
                firstAppendSubmitted.countDown();
            } else {
                secondAppendFuture.set(future);
                secondAppendSubmitted.countDown();
            }
            return future;
        });

        ProducerStateManager producerStateManager = spy(newProducerStateManager(tp, logInstance));
        UrsaStorageState state = mock(UrsaStorageState.class);
        UrsaPartitionLog partitionLog = attachPartitionLog(state, tp, logInstance);
        partitionLog.installProducerStateManager(DisklessClientZone.NO_ZONE, producerStateManager);

        AtomicInteger prepareAppendCalls = new AtomicInteger(0);
        CountDownLatch firstPrepareAppendCall = new CountDownLatch(1);
        CountDownLatch allowFirstPrepareAppendReturn = new CountDownLatch(1);
        doAnswer(invocation -> {
            if (prepareAppendCalls.incrementAndGet() == 1) {
                firstPrepareAppendCall.countDown();
                assertTrue(await(allowFirstPrepareAppendReturn),
                        "Timed out waiting to release first prepareAppend call");
            }
            return invocation.callRealMethod();
        }).when(producerStateManager).prepareAppend(any());

        UrsaLakestreamWriter writer = new UrsaLakestreamWriter(state);
        MemoryRecords records1 = idempotentRecords(3333L, 0, "a");
        MemoryRecords records2 = idempotentRecords(3333L, 1, "b");
        ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            CompletableFuture<Map<TopicIdPartition, PartitionResponse>> firstWrite =
                    CompletableFuture.supplyAsync(
                            () -> writer.write(Map.of(tp, records1), DisklessClientZone.NO_ZONE), executor)
                            .thenCompose(future -> future);

            TestUtils.waitForCondition(
                    () -> firstPrepareAppendCall.getCount() == 0,
                    5_000L,
                    "Timed out waiting for first prepareAppend call");

            CompletableFuture<Map<TopicIdPartition, PartitionResponse>> secondWrite =
                    CompletableFuture.supplyAsync(
                            () -> writer.write(Map.of(tp, records2), DisklessClientZone.NO_ZONE), executor)
                            .thenCompose(future -> future);

            assertEquals(1, prepareAppendCalls.get());
            allowFirstPrepareAppendReturn.countDown();

            assertTrue(firstAppendSubmitted.await(5, TimeUnit.SECONDS),
                    "Timed out waiting for first append call");
            assertTrue(secondAppendSubmitted.await(5, TimeUnit.SECONDS),
                    "Timed out waiting for second append call");

            // Submission is sequenced, but storage completions may arrive out of order.
            secondAppendFuture.get().complete(mockLogEntryHeader(1L));
            firstAppendFuture.get().complete(mockLogEntryHeader(0L));

            PartitionResponse firstResponse = firstWrite.get(5, TimeUnit.SECONDS).get(tp);
            PartitionResponse secondResponse = secondWrite.get(5, TimeUnit.SECONDS).get(tp);
            assertEquals(Errors.NONE, firstResponse.error);
            assertEquals(Errors.NONE, secondResponse.error);
            assertEquals(0L, firstResponse.baseOffset);
            assertEquals(1L, secondResponse.baseOffset);
        } finally {
            allowFirstPrepareAppendReturn.countDown();
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
        Log log0 = emptyLog();
        Log log1 = emptyLog();

        AtomicLong nextOffset0 = new AtomicLong(0L);
        when(log0.append(anyInt(), any(ByteBuf.class))).thenAnswer(invocation -> {
            int numberOfMessages = invocation.getArgument(0);
            long baseOffset = nextOffset0.getAndAdd(numberOfMessages);
            return CompletableFuture.completedFuture(mockLogEntryHeader(baseOffset));
        });

        AtomicLong nextOffset1 = new AtomicLong(0L);
        CountDownLatch tp1SequenceZeroSubmitted = new CountDownLatch(1);
        when(log1.append(anyInt(), any(ByteBuf.class))).thenAnswer(invocation -> {
            int numberOfMessages = invocation.getArgument(0);
            long baseOffset = nextOffset1.getAndAdd(numberOfMessages);
            if (baseOffset == 0L) {
                tp1SequenceZeroSubmitted.countDown();
            }
            return CompletableFuture.completedFuture(mockLogEntryHeader(baseOffset));
        });

        ProducerStateManager producerStateManager0 = spy(newProducerStateManager(tp0, log0));
        ProducerStateManager producerStateManager1 = newProducerStateManager(tp1, log1);
        UrsaStorageState state = mock(UrsaStorageState.class);
        UrsaPartitionLog partitionLog0 = attachPartitionLog(state, tp0, log0);
        UrsaPartitionLog partitionLog1 = attachPartitionLog(state, tp1, log1);
        partitionLog0.installProducerStateManager(DisklessClientZone.NO_ZONE, producerStateManager0);
        partitionLog1.installProducerStateManager(DisklessClientZone.NO_ZONE, producerStateManager1);

        CountDownLatch tp0PrepareAppendCalled = new CountDownLatch(1);
        CountDownLatch releaseTp0PrepareAppend = new CountDownLatch(1);
        doAnswer(invocation -> {
            tp0PrepareAppendCalled.countDown();
            assertTrue(await(releaseTp0PrepareAppend),
                    "Timed out waiting to release prepareAppend for tp0");
            return invocation.callRealMethod();
        }).when(producerStateManager0).prepareAppend(any());

        UrsaLakestreamWriter writer = new UrsaLakestreamWriter(state);
        MemoryRecords tp0Records = idempotentRecords(3333L, 0, "x");
        MemoryRecords tp1SequenceZero = idempotentRecords(3333L, 0, "a");
        MemoryRecords tp1SequenceOne = idempotentRecords(3333L, 1, "b");
        LinkedHashMap<TopicIdPartition, MemoryRecords> firstRequest = new LinkedHashMap<>();
        firstRequest.put(tp0, tp0Records);
        firstRequest.put(tp1, tp1SequenceZero);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            CompletableFuture<Map<TopicIdPartition, PartitionResponse>> firstWrite =
                    CompletableFuture.supplyAsync(
                            () -> writer.write(firstRequest, DisklessClientZone.NO_ZONE), executor)
                            .thenCompose(future -> future);

            TestUtils.waitForCondition(
                    () -> tp0PrepareAppendCalled.getCount() == 0,
                    5_000L,
                    "Timed out waiting for tp0 prepareAppend call");
            assertTrue(tp1SequenceZeroSubmitted.await(5, TimeUnit.SECONDS),
                    "tp1 was blocked by tp0's write tail");

            CompletableFuture<Map<TopicIdPartition, PartitionResponse>> secondWrite =
                    CompletableFuture.supplyAsync(
                            () -> writer.write(Map.of(tp1, tp1SequenceOne), DisklessClientZone.NO_ZONE), executor)
                            .thenCompose(future -> future);

            releaseTp0PrepareAppend.countDown();

            PartitionResponse firstResponse = firstWrite.get(5, TimeUnit.SECONDS).get(tp1);
            PartitionResponse secondResponse = secondWrite.get(5, TimeUnit.SECONDS).get(tp1);
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
        TopicIdPartition tp = testTopicPartition(0);
        Log logInstance = emptyLog();
        AtomicReference<CompletableFuture<LogEntryHeader>> appendFutureRef = new AtomicReference<>();
        AtomicInteger appendCalls = new AtomicInteger(0);
        when(logInstance.append(anyInt(), any(ByteBuf.class))).thenAnswer(invocation -> {
            appendCalls.incrementAndGet();
            CompletableFuture<LogEntryHeader> future = new CompletableFuture<>();
            appendFutureRef.set(future);
            return future;
        });

        ProducerStateManager producerStateManager = newProducerStateManager(tp, logInstance);
        UrsaStorageState state = mock(UrsaStorageState.class);
        attachPartitionLog(state, tp, logInstance)
                .installProducerStateManager(DisklessClientZone.NO_ZONE, producerStateManager);
        UrsaLakestreamWriter writer = new UrsaLakestreamWriter(state);
        MemoryRecords records = MemoryRecords.withIdempotentRecords(
                Compression.NONE,
                1234L,
                (short) 0,
                0,
                new SimpleRecord("a".getBytes(StandardCharsets.UTF_8)),
                new SimpleRecord("b".getBytes(StandardCharsets.UTF_8)));

        try {
            CompletableFuture<Map<TopicIdPartition, PartitionResponse>> firstWrite =
                    writer.write(Map.of(tp, records), DisklessClientZone.NO_ZONE);
            CompletableFuture<Map<TopicIdPartition, PartitionResponse>> secondWrite =
                    writer.write(Map.of(tp, records), DisklessClientZone.NO_ZONE);

            assertFalse(firstWrite.isDone());
            assertFalse(secondWrite.isDone());
            TestUtils.waitForCondition(
                    () -> appendCalls.get() == 1,
                    5_000L,
                    "Timed out waiting for the first append call");
            assertEquals(1, appendCalls.get());

            appendFutureRef.get().complete(mockLogEntryHeader(200L));

            PartitionResponse first = firstWrite.get(5, TimeUnit.SECONDS).get(tp);
            PartitionResponse second = secondWrite.get(5, TimeUnit.SECONDS).get(tp);
            assertEquals(Errors.NONE, first.error);
            assertEquals(Errors.NONE, second.error);
            assertEquals(200L, first.baseOffset);
            assertEquals(200L, second.baseOffset);
            assertEquals(RecordBatch.NO_TIMESTAMP, first.logAppendTime);
            assertEquals(RecordBatch.NO_TIMESTAMP, second.logAppendTime, "a duplicate reports no create-time");
        } finally {
            producerStateManager.close();
            writer.close();
        }
    }

    @Test
    void testSingleRequestMultiBatchAssignsBatchBaseOffsets() throws Exception {
        TopicIdPartition tp = testTopicPartition(0);
        Log logInstance = emptyLog();
        LogEntryHeader appendHeader = mockLogEntryHeader(100L);
        when(logInstance.append(anyInt(), any(ByteBuf.class)))
                .thenReturn(CompletableFuture.completedFuture(appendHeader));

        ProducerStateManager producerStateManager = newProducerStateManager(tp, logInstance);
        UrsaStorageState state = mock(UrsaStorageState.class);
        attachPartitionLog(state, tp, logInstance)
                .installProducerStateManager(DisklessClientZone.NO_ZONE, producerStateManager);
        UrsaLakestreamWriter writer = new UrsaLakestreamWriter(state);

        MemoryRecords firstBatch = MemoryRecords.withIdempotentRecords(
                Compression.NONE,
                2222L,
                (short) 0,
                0,
                new SimpleRecord("a".getBytes(StandardCharsets.UTF_8)),
                new SimpleRecord("b".getBytes(StandardCharsets.UTF_8)));
        MemoryRecords secondBatch = MemoryRecords.withIdempotentRecords(
                Compression.NONE,
                2222L,
                (short) 0,
                2,
                new SimpleRecord("c".getBytes(StandardCharsets.UTF_8)),
                new SimpleRecord("d".getBytes(StandardCharsets.UTF_8)),
                new SimpleRecord("e".getBytes(StandardCharsets.UTF_8)));

        try {
            Map<TopicIdPartition, PartitionResponse> response = writer.write(
                    Map.of(tp, mergeRecords(firstBatch, secondBatch)),
                    DisklessClientZone.NO_ZONE).get(5, TimeUnit.SECONDS);
            assertEquals(Errors.NONE, response.get(tp).error);
            assertEquals(100L, response.get(tp).baseOffset);

            Optional<ProducerStateManager.ProducerState> producerState = producerStateManager.producerState(2222L);
            assertTrue(producerState.isPresent());
            assertEquals(4, producerState.get().lastSequence());
            assertEquals(102L, producerState.get().lastOffset());
        } finally {
            producerStateManager.close();
            writer.close();
        }
    }

    @Test
    void testProducerSequenceStateIsIndependentAcrossZones() throws Exception {
        TopicIdPartition tp = testTopicPartition(0);
        Log logInstance = emptyLog();
        AtomicLong nextOffset = new AtomicLong();
        when(logInstance.append(anyInt(), any(ByteBuf.class))).thenAnswer(invocation -> {
            int recordCount = invocation.getArgument(0);
            return CompletableFuture.completedFuture(mockLogEntryHeader(nextOffset.getAndAdd(recordCount)));
        });

        ProducerStateManager zoneAState = newProducerStateManager(tp, logInstance);
        ProducerStateManager zoneBState = newProducerStateManager(tp, logInstance);
        UrsaStorageState state = mock(UrsaStorageState.class);
        UrsaPartitionLog partitionLog = attachPartitionLog(state, tp, logInstance);
        partitionLog.installProducerStateManager("zone-a", zoneAState);
        partitionLog.installProducerStateManager("zone-b", zoneBState);
        UrsaLakestreamWriter writer = new UrsaLakestreamWriter(state);
        MemoryRecords firstSequence = idempotentRecords(9876L, 0, "a");

        try {
            PartitionResponse zoneAResponse = writer.write(Map.of(tp, firstSequence), "zone-a")
                    .get(5, TimeUnit.SECONDS).get(tp);
            PartitionResponse zoneBResponse = writer.write(Map.of(tp, firstSequence), "zone-b")
                    .get(5, TimeUnit.SECONDS).get(tp);

            assertEquals(Errors.NONE, zoneAResponse.error);
            assertEquals(Errors.NONE, zoneBResponse.error);
            assertEquals(0L, zoneAResponse.baseOffset);
            assertEquals(1L, zoneBResponse.baseOffset);
            assertEquals(0, zoneAState.producerState(9876L).orElseThrow().lastSequence());
            assertEquals(0, zoneBState.producerState(9876L).orElseThrow().lastSequence());
        } finally {
            zoneAState.close();
            zoneBState.close();
            writer.close();
        }
    }

    @Test
    void testLogAppendTimeDuplicateReportsTheOriginalStamp() throws Exception {
        TopicIdPartition tp = testTopicPartition(0);
        Log logInstance = emptyLog();
        AtomicLong nextOffset = new AtomicLong();
        when(logInstance.append(anyInt(), any(ByteBuf.class))).thenAnswer(invocation -> {
            int recordCount = invocation.getArgument(0);
            return CompletableFuture.completedFuture(mockLogEntryHeader(nextOffset.getAndAdd(recordCount)));
        });

        ProducerStateManager producerStateManager = newProducerStateManager(tp, logInstance);
        // The clock ticks on every read, so a stored bookkeeping timestamp would differ from the
        // stamp written into the records.
        MockTime time = new MockTime(1L, 7_000L, 0L);
        UrsaStorageState state = mock(UrsaStorageState.class);
        attachPartitionLog(state, tp, logInstance, time, TimestampType.LOG_APPEND_TIME)
                .installProducerStateManager(DisklessClientZone.NO_ZONE, producerStateManager);
        UrsaLakestreamWriter writer = new UrsaLakestreamWriter(state);

        try {
            PartitionResponse first = writer.write(
                    Map.of(tp, idempotentRecords(5555L, 0, "a")),
                    DisklessClientZone.NO_ZONE).get(5, TimeUnit.SECONDS).get(tp);
            assertEquals(Errors.NONE, first.error);
            long stamped = first.logAppendTime;
            assertTrue(stamped >= 7_000L, "The append is stamped with the broker clock");

            // The retry is stamped later, but the duplicate must report the original append.
            time.sleep(1_000L);
            PartitionResponse duplicate = writer.write(
                    Map.of(tp, idempotentRecords(5555L, 0, "a")),
                    DisklessClientZone.NO_ZONE).get(5, TimeUnit.SECONDS).get(tp);

            assertEquals(Errors.NONE, duplicate.error);
            assertEquals(first.baseOffset, duplicate.baseOffset);
            assertEquals(stamped, duplicate.logAppendTime);
            assertEquals(1, appendCalls(logInstance));
        } finally {
            producerStateManager.close();
            writer.close();
        }
    }

    private static int appendCalls(Log logInstance) {
        return (int) mockingDetails(logInstance).getInvocations().stream()
                .filter(invocation -> "append".equals(invocation.getMethod().getName()))
                .count();
    }

    private static ProducerStateManager newProducerStateManager(TopicIdPartition tp, Log logInstance) {
        return new ProducerStateManager(tp, () -> null, () -> CompletableFuture.completedFuture(logInstance));
    }

    private static UrsaPartitionLog attachPartitionLog(
            UrsaStorageState state,
            TopicIdPartition tp,
            Log logInstance) {
        return attachPartitionLog(state, tp, logInstance, Time.SYSTEM, TimestampType.CREATE_TIME);
    }

    private static UrsaPartitionLog attachPartitionLog(
            UrsaStorageState state,
            TopicIdPartition tp,
            Log logInstance,
            Time time,
            TimestampType timestampType) {
        when(state.time()).thenReturn(time);
        when(state.timer()).thenReturn(timer);
        when(state.timestampType(tp.topic())).thenReturn(timestampType);
        UrsaPartitionLog partitionLog = new UrsaPartitionLog(
                tp,
                state,
                mock(DisklessLogMetrics.class),
                CompletableFuture.completedFuture(logInstance),
                null,
                0L,
                0,
                null);
        when(state.getOrCreatePartitionLog(tp)).thenReturn(partitionLog);
        return partitionLog;
    }

    private static Log emptyLog() {
        Log logInstance = mock(Log.class);
        LogOffset emptyOffset = mockLogOffset(-1L, 0);
        when(logInstance.getLastOffset())
                .thenReturn(CompletableFuture.completedFuture(emptyOffset));
        return logInstance;
    }

    private static MemoryRecords idempotentRecords(long producerId, int baseSequence, String value) {
        return MemoryRecords.withIdempotentRecords(
                Compression.NONE,
                producerId,
                (short) 0,
                baseSequence,
                new SimpleRecord(value.getBytes(StandardCharsets.UTF_8)));
    }

    private static TopicIdPartition testTopicPartition(int partition) {
        return new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("test-topic", partition));
    }

    private static LogOffset mockLogOffset(long offset, int numberOfRecords) {
        LogOffset logOffset = mock(LogOffset.class);
        when(logOffset.offset()).thenReturn(offset);
        when(logOffset.numberOfRecords()).thenReturn(numberOfRecords);
        return logOffset;
    }

    private static LogEntryHeader mockLogEntryHeader(long offset) {
        LogEntryHeader header = mock(LogEntryHeader.class);
        when(header.offset()).thenReturn(offset);
        return header;
    }

    private static MemoryRecords mergeRecords(MemoryRecords first, MemoryRecords second) {
        ByteBuffer merged = ByteBuffer.allocate(first.sizeInBytes() + second.sizeInBytes());
        merged.put(first.buffer().duplicate());
        merged.put(second.buffer().duplicate());
        merged.flip();
        return MemoryRecords.readableRecords(merged);
    }

    private static boolean await(CountDownLatch latch) {
        try {
            return latch.await(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }
}
