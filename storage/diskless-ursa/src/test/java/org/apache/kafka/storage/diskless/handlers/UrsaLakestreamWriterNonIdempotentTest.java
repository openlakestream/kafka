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
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.lakestream.api.Log;
import io.lakestream.api.LogEntryHeader;
import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.exception.LogFencedException;
import io.lakestream.api.exception.NoSuchStreamException;
import io.netty.buffer.ByteBuf;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class UrsaLakestreamWriterNonIdempotentTest {

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
    void testNewPartitionLogCloseOnlyClosesLeasedHandle() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        Log logInstance = mock(Log.class);
        UrsaStorageState state = mock(UrsaStorageState.class);

        UrsaPartitionLog partitionLog = newPartitionLog(
                state,
                tp,
                CompletableFuture.completedFuture(logInstance));

        partitionLog.close(false);

        verify(logInstance, never()).fence();
        verify(logInstance, timeout(5_000)).closeAsync();
    }

    @Test
    void testWriteAfterCloseReturnsNotLeaderWithoutAppending() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        Log logInstance = mock(Log.class);
        UrsaStorageState state = mock(UrsaStorageState.class);
        UrsaPartitionLog partitionLog = attachPartitionLog(state, tp, logInstance);
        MemoryRecords records = MemoryRecords.withRecords(
                Compression.NONE,
                new SimpleRecord("after-close".getBytes(StandardCharsets.UTF_8)));

        partitionLog.close(false);

        PartitionResponse response = partitionLog.write(
                records,
                DisklessClientZone.NO_ZONE,
                "test").get(5, TimeUnit.SECONDS);

        assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, response.error);
        verify(logInstance, never()).append(anyInt(), any(ByteBuf.class));
    }

    @Test
    void testPartitionThatCannotBeResolvedFailsOnItsOwn() throws Exception {
        TopicIdPartition healthy = testTopicPartition();
        TopicIdPartition deleted = new TopicIdPartition(
                Uuid.randomUuid(), new TopicPartition("deleted-topic", 0));
        TopicIdPartition closing = new TopicIdPartition(
                Uuid.randomUuid(), new TopicPartition("closing-topic", 0));
        Log logInstance = mock(Log.class);
        LogEntryHeader appended = mockLogEntryHeader(4L);
        when(logInstance.append(anyInt(), any(ByteBuf.class)))
                .thenReturn(CompletableFuture.completedFuture(appended));
        UrsaStorageState state = newWriterState(healthy, logInstance);
        when(state.getOrCreatePartitionLog(deleted))
                .thenThrow(new NotLeaderOrFollowerException("Topic deleted-topic was deleted"));
        when(state.getOrCreatePartitionLog(closing))
                .thenThrow(new IllegalStateException("Ursa storage state is closed"));

        // The healthy partition is resolved and appended first, so a throw from a later partition
        // must not take down records that already reached storage: the producer would retry them.
        Map<TopicIdPartition, MemoryRecords> entries = new LinkedHashMap<>();
        entries.put(healthy, MemoryRecords.withRecords(
                Compression.NONE, new SimpleRecord("a".getBytes(StandardCharsets.UTF_8))));
        entries.put(deleted, MemoryRecords.withRecords(
                Compression.NONE, new SimpleRecord("b".getBytes(StandardCharsets.UTF_8))));
        entries.put(closing, MemoryRecords.withRecords(
                Compression.NONE, new SimpleRecord("c".getBytes(StandardCharsets.UTF_8))));

        UrsaLakestreamWriter writer = new UrsaLakestreamWriter(state);
        try {
            Map<TopicIdPartition, PartitionResponse> responses =
                    writer.write(entries, DisklessClientZone.NO_ZONE).get(5, TimeUnit.SECONDS);

            assertEquals(Errors.NONE, responses.get(healthy).error);
            assertEquals(4L, responses.get(healthy).baseOffset);
            assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, responses.get(deleted).error);
            assertEquals(Errors.KAFKA_STORAGE_ERROR, responses.get(closing).error);
            verify(logInstance).append(anyInt(), any(ByteBuf.class));
        } finally {
            writer.close();
        }
    }

    @Test
    void testWriteWhileStreamIsProvisioningReturnsUnknownTopicAndInvalidates() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        UrsaStorageState state = mock(UrsaStorageState.class);
        when(state.time()).thenReturn(Time.SYSTEM);
        UrsaPartitionLog partitionLog = newPartitionLog(
                state,
                tp,
                CompletableFuture.failedFuture(new NoSuchStreamException(
                        StreamIdentifier.of("default", "stream-is-provisioning"))));
        MemoryRecords records = MemoryRecords.withRecords(
                Compression.NONE,
                new SimpleRecord("before-provisioning-completes".getBytes(StandardCharsets.UTF_8)));

        PartitionResponse response = partitionLog.write(
                records,
                DisklessClientZone.NO_ZONE,
                "test").get(5, TimeUnit.SECONDS);

        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, response.error);
        verify(state).removePartitionLog(eq(tp), same(partitionLog));
    }

    @Test
    void testWriteRejectsTrailingBytesWithoutAppending() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        Log logInstance = mock(Log.class);
        UrsaStorageState state = mock(UrsaStorageState.class);
        UrsaPartitionLog partitionLog = attachPartitionLog(state, tp, logInstance);
        MemoryRecords validRecords = MemoryRecords.withRecords(
                Compression.NONE,
                new SimpleRecord("valid".getBytes(StandardCharsets.UTF_8)));
        ByteBuffer malformedBuffer = ByteBuffer.allocate(validRecords.buffer().remaining() + 1);
        malformedBuffer.put(validRecords.buffer()).put((byte) 1).flip();
        MemoryRecords malformedRecords = MemoryRecords.readableRecords(malformedBuffer);

        try {
            PartitionResponse response = partitionLog.write(
                    malformedRecords,
                    DisklessClientZone.NO_ZONE,
                    "test-writer").get(5, TimeUnit.SECONDS);

            assertEquals(Errors.INVALID_RECORD, response.error);
            verify(logInstance, never()).append(anyInt(), any(ByteBuf.class));
        } finally {
            partitionLog.close(false);
        }
    }

    @Test
    void testLateStaleLogOpenClosesWithoutFencingActiveReplacement() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        UrsaStorageState state = mock(UrsaStorageState.class);
        AtomicReference<UrsaPartitionLog> activePartitionLog = new AtomicReference<>();
        when(state.partitionLog(tp)).thenAnswer(invocation -> activePartitionLog.get());
        when(state.time()).thenReturn(Time.SYSTEM);

        CompletableFuture<Log> staleOpenFuture = new CompletableFuture<>();
        Log staleLog = mock(Log.class);
        UrsaPartitionLog stalePartitionLog = newPartitionLog(state, tp, staleOpenFuture);
        activePartitionLog.set(stalePartitionLog);

        stalePartitionLog.invalidate();
        activePartitionLog.compareAndSet(stalePartitionLog, null);

        Log replacementLog = mock(Log.class);
        UrsaPartitionLog replacementPartitionLog = newPartitionLog(
                state,
                tp,
                CompletableFuture.completedFuture(replacementLog));
        activePartitionLog.set(replacementPartitionLog);

        staleOpenFuture.complete(staleLog);

        verify(replacementLog, never()).fence();
        verify(staleLog, never()).fence();
        verify(staleLog, timeout(5_000)).closeAsync();

        replacementPartitionLog.close(false);
    }

    @Test
    void testNonIdempotentWriteAppendsValidRecordsAndReleasesBuffer() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        Log logInstance = mock(Log.class);
        CompletableFuture<LogEntryHeader> appendFuture = new CompletableFuture<>();
        AtomicReference<ByteBuf> appendedBuffer = new AtomicReference<>();

        when(logInstance.append(anyInt(), any(ByteBuf.class))).thenAnswer(invocation -> {
            int numberOfMessages = invocation.getArgument(0);
            ByteBuf buffer = invocation.getArgument(1);
            appendedBuffer.set(buffer);

            MemoryRecords appendedRecords = decodeKafkaRecords(buffer);
            RecordBatch batch = appendedRecords.batches().iterator().next();
            assertEquals(2, numberOfMessages);
            assertTrue(batch.isValid());
            assertFalse(batch.hasProducerId());
            assertEquals(1, buffer.refCnt());
            return appendFuture;
        });

        UrsaLakestreamWriter writer = new UrsaLakestreamWriter(newWriterState(tp, logInstance));
        MemoryRecords records = MemoryRecords.withRecords(
                Compression.NONE,
                new SimpleRecord("a".getBytes(StandardCharsets.UTF_8)),
                new SimpleRecord("b".getBytes(StandardCharsets.UTF_8)));

        try {
            CompletableFuture<Map<TopicIdPartition, PartitionResponse>> writeFuture =
                    writer.write(Map.of(tp, records), DisklessClientZone.NO_ZONE);
            assertFalse(writeFuture.isDone());
            TestUtils.waitForCondition(
                    () -> appendedBuffer.get() != null,
                    5_000L,
                    "Timed out waiting for the append call");
            assertEquals(1, appendedBuffer.get().refCnt());

            appendFuture.complete(mockLogEntryHeader(0L));

            PartitionResponse response = writeFuture.get(5, TimeUnit.SECONDS).get(tp);
            assertEquals(Errors.NONE, response.error);
            assertEquals(0L, response.baseOffset);
            assertEquals(RecordBatch.NO_TIMESTAMP, response.logAppendTime);
            assertEquals(0, appendedBuffer.get().refCnt());
        } finally {
            writer.close();
        }
    }

    @Test
    void testNonIdempotentWritesSamePartitionCanSubmitThroughSequencedWrites() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        Log logInstance = mock(Log.class);
        CountDownLatch firstAppendSubmitted = new CountDownLatch(1);
        CountDownLatch secondAppendSubmitted = new CountDownLatch(1);
        AtomicReference<CompletableFuture<LogEntryHeader>> firstAppendFuture = new AtomicReference<>();
        AtomicReference<CompletableFuture<LogEntryHeader>> secondAppendFuture = new AtomicReference<>();
        AtomicInteger appendCalls = new AtomicInteger(0);

        when(logInstance.append(anyInt(), any(ByteBuf.class))).thenAnswer(invocation -> {
            ByteBuf buffer = invocation.getArgument(1);
            MemoryRecords appendedRecords = decodeKafkaRecords(buffer);
            RecordBatch batch = appendedRecords.batches().iterator().next();
            assertTrue(batch.isValid());
            assertFalse(batch.hasProducerId());

            CompletableFuture<LogEntryHeader> future = new CompletableFuture<>();
            int appendIndex = appendCalls.getAndIncrement();
            if (appendIndex == 0) {
                firstAppendFuture.set(future);
                firstAppendSubmitted.countDown();
            } else if (appendIndex == 1) {
                secondAppendFuture.set(future);
                secondAppendSubmitted.countDown();
            } else {
                fail("Unexpected append index " + appendIndex);
            }
            return future;
        });

        UrsaLakestreamWriter writer = new UrsaLakestreamWriter(newWriterState(tp, logInstance));
        MemoryRecords records1 = MemoryRecords.withRecords(
                Compression.NONE,
                new SimpleRecord("first".getBytes(StandardCharsets.UTF_8)));
        MemoryRecords records2 = MemoryRecords.withRecords(
                Compression.NONE,
                new SimpleRecord("second".getBytes(StandardCharsets.UTF_8)));

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            CompletableFuture<Map<TopicIdPartition, PartitionResponse>> firstWrite =
                    CompletableFuture.supplyAsync(
                            () -> writer.write(Map.of(tp, records1), DisklessClientZone.NO_ZONE), executor)
                            .thenCompose(future -> future);

            assertTrue(firstAppendSubmitted.await(5, TimeUnit.SECONDS),
                    "Timed out waiting for first non-idempotent append submission");

            CompletableFuture<Map<TopicIdPartition, PartitionResponse>> secondWrite =
                    CompletableFuture.supplyAsync(
                            () -> writer.write(Map.of(tp, records2), DisklessClientZone.NO_ZONE), executor)
                            .thenCompose(future -> future);

            assertTrue(secondAppendSubmitted.await(5, TimeUnit.SECONDS),
                    "Timed out waiting for second non-idempotent append submission");

            firstAppendFuture.get().complete(mockLogEntryHeader(0L));
            secondAppendFuture.get().complete(mockLogEntryHeader(1L));

            PartitionResponse first = firstWrite.get(5, TimeUnit.SECONDS).get(tp);
            PartitionResponse second = secondWrite.get(5, TimeUnit.SECONDS).get(tp);
            assertEquals(Errors.NONE, first.error);
            assertEquals(Errors.NONE, second.error);
            assertEquals(0L, first.baseOffset);
            assertEquals(1L, second.baseOffset);
        } finally {
            executor.shutdownNow();
            writer.close();
        }
    }

    @Test
    void testQueuedWriteOwnsRecordsBeforeReturning() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        Log logInstance = mock(Log.class);
        CompletableFuture<Log> logFuture = new CompletableFuture<>();
        AtomicInteger appendCalls = new AtomicInteger();
        AtomicReference<byte[]> secondAppendedPayload = new AtomicReference<>();

        when(logInstance.append(anyInt(), any(ByteBuf.class))).thenAnswer(invocation -> {
            int appendIndex = appendCalls.getAndIncrement();
            ByteBuf buffer = invocation.getArgument(1);
            byte[] appendedPayload = new byte[buffer.readableBytes()];
            buffer.getBytes(buffer.readerIndex(), appendedPayload);
            if (appendIndex == 1) {
                secondAppendedPayload.set(appendedPayload);
            }
            return CompletableFuture.completedFuture(mockLogEntryHeader(appendIndex));
        });

        UrsaStorageState state = mock(UrsaStorageState.class);
        when(state.time()).thenReturn(Time.SYSTEM);
        UrsaPartitionLog partitionLog = newPartitionLog(state, tp, logFuture);
        MemoryRecords firstRecords = MemoryRecords.withRecords(
                Compression.NONE,
                new SimpleRecord("first".getBytes(StandardCharsets.UTF_8)));
        MemoryRecords queuedRecords = MemoryRecords.withRecords(
                Compression.NONE,
                new SimpleRecord("queued".getBytes(StandardCharsets.UTF_8)));
        byte[] expectedQueuedPayload = copyBytes(queuedRecords.buffer());

        try {
            CompletableFuture<PartitionResponse> firstWrite = partitionLog.write(
                    firstRecords,
                    DisklessClientZone.NO_ZONE,
                    "test");
            CompletableFuture<PartitionResponse> queuedWrite = partitionLog.write(
                    queuedRecords,
                    DisklessClientZone.NO_ZONE,
                    "test");
            assertEquals(2, partitionLog.ownedWritePayloadCount());

            ByteBuffer callerBuffer = queuedRecords.buffer();
            assertFalse(callerBuffer.isReadOnly());
            for (int index = callerBuffer.position(); index < callerBuffer.limit(); index++) {
                callerBuffer.put(index, (byte) 0);
            }

            logFuture.complete(logInstance);

            assertEquals(Errors.NONE, firstWrite.get(5, TimeUnit.SECONDS).error);
            assertEquals(Errors.NONE, queuedWrite.get(5, TimeUnit.SECONDS).error);
            assertEquals(2, appendCalls.get());
            assertArrayEquals(expectedQueuedPayload, secondAppendedPayload.get());
            assertEquals(0, partitionLog.ownedWritePayloadCount());
        } finally {
            partitionLog.close(false);
        }
    }

    @Test
    void testAppendInitializationFailureReleasesOwnedPayload() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        Log logInstance = mock(Log.class);
        AtomicReference<ByteBuf> appendedBuffer = new AtomicReference<>();
        when(logInstance.append(anyInt(), any(ByteBuf.class))).thenAnswer(invocation -> {
            appendedBuffer.set(invocation.getArgument(1));
            throw new IOException("append initialization failed");
        });

        UrsaStorageState state = mock(UrsaStorageState.class);
        UrsaPartitionLog partitionLog = attachPartitionLog(state, tp, logInstance);
        MemoryRecords records = MemoryRecords.withRecords(
                Compression.NONE,
                new SimpleRecord("a".getBytes(StandardCharsets.UTF_8)));

        try {
            PartitionResponse response = partitionLog.write(
                    records,
                    DisklessClientZone.NO_ZONE,
                    "test").get(5, TimeUnit.SECONDS);

            assertEquals(Errors.KAFKA_STORAGE_ERROR, response.error);
            assertEquals(0, appendedBuffer.get().refCnt());
            assertEquals(0, partitionLog.ownedWritePayloadCount());
        } finally {
            partitionLog.close(false);
        }
    }

    @Test
    void testFencedLogReturnsNotLeaderAndInvalidatesPartitionLog() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        Log logInstance = mock(Log.class);
        when(logInstance.append(anyInt(), any(ByteBuf.class)))
                .thenReturn(CompletableFuture.failedFuture(new LogFencedException("stream 17 is fenced")));

        UrsaStorageState state = mock(UrsaStorageState.class);
        UrsaPartitionLog partitionLog = attachPartitionLog(state, tp, logInstance);
        UrsaLakestreamWriter writer = new UrsaLakestreamWriter(state);
        MemoryRecords records = MemoryRecords.withRecords(
                Compression.NONE,
                new SimpleRecord("a".getBytes(StandardCharsets.UTF_8)));

        try {
            PartitionResponse response = writer.write(Map.of(tp, records), DisklessClientZone.NO_ZONE)
                    .get(5, TimeUnit.SECONDS).get(tp);
            assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, response.error);
            verify(state).removePartitionLog(eq(tp), same(partitionLog));
            verify(logInstance, never()).fence();
            verify(logInstance, timeout(5_000)).closeAsync();
        } finally {
            writer.close();
        }
    }

    @Test
    void testUnrelatedClosedFailureDoesNotInvalidatePartitionLog() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        Log logInstance = mock(Log.class);
        when(logInstance.append(anyInt(), any(ByteBuf.class)))
                .thenReturn(CompletableFuture.failedFuture(
                        new ChannelClosedException("storage channel closed")));

        UrsaStorageState state = mock(UrsaStorageState.class);
        UrsaPartitionLog partitionLog = attachPartitionLog(state, tp, logInstance);
        UrsaLakestreamWriter writer = new UrsaLakestreamWriter(state);
        MemoryRecords records = MemoryRecords.withRecords(
                Compression.NONE,
                new SimpleRecord("a".getBytes(StandardCharsets.UTF_8)));

        try {
            PartitionResponse response = writer.write(Map.of(tp, records), DisklessClientZone.NO_ZONE)
                    .get(5, TimeUnit.SECONDS).get(tp);
            assertEquals(Errors.KAFKA_STORAGE_ERROR, response.error);
            verify(state, never()).removePartitionLog(eq(tp), same(partitionLog));
        } finally {
            writer.close();
        }
    }

    @Test
    void testAppendFailureReturnsStorageErrorWithoutInvalidationAndReleasesBuffer() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        Log logInstance = mock(Log.class);
        CompletableFuture<LogEntryHeader> appendFuture = new CompletableFuture<>();
        AtomicReference<ByteBuf> appendedBuffer = new AtomicReference<>();
        when(logInstance.append(anyInt(), any(ByteBuf.class))).thenAnswer(invocation -> {
            appendedBuffer.set(invocation.getArgument(1));
            return appendFuture;
        });

        UrsaStorageState state = mock(UrsaStorageState.class);
        UrsaPartitionLog partitionLog = attachPartitionLog(state, tp, logInstance);
        UrsaLakestreamWriter writer = new UrsaLakestreamWriter(state);
        MemoryRecords records = MemoryRecords.withRecords(
                Compression.NONE,
                new SimpleRecord("a".getBytes(StandardCharsets.UTF_8)));

        try {
            CompletableFuture<Map<TopicIdPartition, PartitionResponse>> writeFuture =
                    writer.write(Map.of(tp, records), DisklessClientZone.NO_ZONE);
            TestUtils.waitForCondition(
                    () -> appendedBuffer.get() != null,
                    5_000L,
                    "Timed out waiting for the append call");
            assertEquals(1, appendedBuffer.get().refCnt());

            appendFuture.completeExceptionally(new IOException("temporary storage failure"));

            PartitionResponse response = writeFuture.get(5, TimeUnit.SECONDS).get(tp);
            assertEquals(Errors.KAFKA_STORAGE_ERROR, response.error);
            assertEquals(0, appendedBuffer.get().refCnt());
            verify(state, never()).removePartitionLog(eq(tp), same(partitionLog));
        } finally {
            writer.close();
        }
    }

    @Test
    void testLogAppendTimeTopicStampsRecordsAndReportsTheAppendTime() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        Log logInstance = mock(Log.class);
        AtomicReference<MemoryRecords> appendedRecords = new AtomicReference<>();
        when(logInstance.append(anyInt(), any(ByteBuf.class))).thenAnswer(invocation -> {
            appendedRecords.set(decodeKafkaRecords(invocation.getArgument(1)));
            return CompletableFuture.completedFuture(mockLogEntryHeader(0L));
        });

        UrsaStorageState state = mock(UrsaStorageState.class);
        UrsaPartitionLog partitionLog = newPartitionLog(
                state,
                tp,
                CompletableFuture.completedFuture(logInstance),
                new MockTime(0L, 7_000L, 0L),
                TimestampType.LOG_APPEND_TIME);
        when(state.getOrCreatePartitionLog(tp)).thenReturn(partitionLog);
        UrsaLakestreamWriter writer = new UrsaLakestreamWriter(state);
        MemoryRecords records = MemoryRecords.withRecords(
                Compression.NONE,
                new SimpleRecord(5L, "a".getBytes(StandardCharsets.UTF_8)));

        try {
            PartitionResponse response = writer.write(Map.of(tp, records), DisklessClientZone.NO_ZONE)
                    .get(5, TimeUnit.SECONDS).get(tp);

            assertEquals(Errors.NONE, response.error);
            assertEquals(7_000L, response.logAppendTime);
            RecordBatch batch = appendedRecords.get().batches().iterator().next();
            assertEquals(7_000L, batch.maxTimestamp());
            assertEquals(TimestampType.LOG_APPEND_TIME, batch.timestampType());
            assertTrue(batch.isValid());
        } finally {
            writer.close();
        }
    }

    private static UrsaStorageState newWriterState(TopicIdPartition tp, Log logInstance) {
        UrsaStorageState state = mock(UrsaStorageState.class);
        attachPartitionLog(state, tp, logInstance);
        return state;
    }

    private static final class ChannelClosedException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        private ChannelClosedException(String message) {
            super(message);
        }
    }

    private static UrsaPartitionLog attachPartitionLog(
            UrsaStorageState state,
            TopicIdPartition tp,
            Log logInstance) {
        UrsaPartitionLog partitionLog = newPartitionLog(
                state,
                tp,
                CompletableFuture.completedFuture(logInstance));
        when(state.getOrCreatePartitionLog(tp)).thenReturn(partitionLog);
        return partitionLog;
    }

    private static UrsaPartitionLog newPartitionLog(
            UrsaStorageState state,
            TopicIdPartition tp,
            CompletableFuture<Log> logFuture) {
        return newPartitionLog(state, tp, logFuture, Time.SYSTEM, TimestampType.CREATE_TIME);
    }

    private static UrsaPartitionLog newPartitionLog(
            UrsaStorageState state,
            TopicIdPartition tp,
            CompletableFuture<Log> logFuture,
            Time time,
            TimestampType timestampType) {
        when(state.time()).thenReturn(time);
        when(state.timer()).thenReturn(timer);
        when(state.timestampType(tp.topic())).thenReturn(timestampType);
        return new UrsaPartitionLog(
                tp,
                state,
                mock(DisklessLogMetrics.class),
                logFuture,
                null,
                0L,
                0,
                null);
    }

    private static TopicIdPartition testTopicPartition() {
        return new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("test-topic", 0));
    }

    private static LogEntryHeader mockLogEntryHeader(long offset) {
        LogEntryHeader header = mock(LogEntryHeader.class);
        when(header.offset()).thenReturn(offset);
        return header;
    }

    private static byte[] copyBytes(ByteBuffer buffer) {
        ByteBuffer copy = buffer.duplicate();
        byte[] data = new byte[copy.remaining()];
        copy.get(data);
        return data;
    }

    private static MemoryRecords decodeKafkaRecords(ByteBuf buffer) {
        return MemoryRecords.readableRecords(buffer.nioBuffer(buffer.readerIndex(), buffer.readableBytes()));
    }
}
