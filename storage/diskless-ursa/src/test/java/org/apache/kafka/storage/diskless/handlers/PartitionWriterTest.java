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
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.diskless.DisklessClientZone;
import org.apache.kafka.storage.diskless.idempotent.ProducerStateManager;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import io.lakestream.api.Log;
import io.lakestream.api.LogEntryHeader;
import io.lakestream.api.LogOffset;
import io.netty.buffer.ByteBuf;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PartitionWriterTest {

    private static final TopicIdPartition TP =
            new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("test-topic", 0));
    private static final String NO_ZONE = DisklessClientZone.NO_ZONE;

    @Test
    void createTimeTopicReportsNoLogAppendTime() throws Exception {
        Log log = mock(Log.class);
        LogEntryHeader header = header(7L, 1, 1000L);
        when(log.append(eq(1), any())).thenReturn(completedFuture(header));
        PartitionWriter writer = writer(log, TimestampType.CREATE_TIME);

        PartitionResponse response = writer
                .write(records(new SimpleRecord(5L, "a".getBytes(StandardCharsets.UTF_8))), NO_ZONE)
                .get(5, TimeUnit.SECONDS);

        assertEquals(Errors.NONE, response.error);
        assertEquals(7L, response.baseOffset);
        assertEquals(RecordBatch.NO_TIMESTAMP, response.logAppendTime);
    }

    @Test
    void logAppendTimeTopicRewritesBatchTimestampBeforeAppend() throws Exception {
        Log log = mock(Log.class);
        // The payload is released as soon as the append future completes, so decode it inside the answer.
        AtomicReference<MemoryRecords> stored = new AtomicReference<>();
        when(log.append(eq(1), any())).thenAnswer(invocation -> {
            stored.set(copyOf(invocation.getArgument(1)));
            return completedFuture(header(0L, 1, 1000L));
        });
        MockTime time = new MockTime(0L, 5000L, 0L);
        PartitionWriter writer = writer(log, TimestampType.LOG_APPEND_TIME, time);

        PartitionResponse response = writer
                .write(records(new SimpleRecord(5L, "a".getBytes(StandardCharsets.UTF_8))), NO_ZONE)
                .get(5, TimeUnit.SECONDS);

        assertEquals(Errors.NONE, response.error);
        assertEquals(5000L, response.logAppendTime);
        RecordBatch appended = stored.get().batches().iterator().next();
        assertEquals(5000L, appended.maxTimestamp());
        assertEquals(TimestampType.LOG_APPEND_TIME, appended.timestampType());
        assertTrue(appended.isValid());
    }

    @Test
    void aTimestampTypeChangeTakesEffectOnTheNextAppend() throws Exception {
        Log log = mock(Log.class);
        when(log.append(eq(1), any())).thenAnswer(invocation -> completedFuture(header(0L, 1, 1000L)));
        PartitionWriter writer = writer(log, TimestampType.CREATE_TIME, new MockTime(0L, 5000L, 0L));

        PartitionResponse beforeChange = writer
                .write(records(new SimpleRecord(5L, "a".getBytes(StandardCharsets.UTF_8))), NO_ZONE)
                .get(5, TimeUnit.SECONDS);
        assertEquals(RecordBatch.NO_TIMESTAMP, beforeChange.logAppendTime);

        writer.applyTimestampType(TimestampType.LOG_APPEND_TIME);

        PartitionResponse afterChange = writer
                .write(records(new SimpleRecord(5L, "b".getBytes(StandardCharsets.UTF_8))), NO_ZONE)
                .get(5, TimeUnit.SECONDS);
        assertEquals(5000L, afterChange.logAppendTime);
    }

    @Test
    void awaitAppendCompletesWhenAnAppendLands() throws Exception {
        Log log = mock(Log.class);
        CompletableFuture<LogEntryHeader> pending = new CompletableFuture<>();
        when(log.append(eq(1), any())).thenReturn(pending);
        PartitionWriter writer = writer(log, TimestampType.CREATE_TIME);

        CompletableFuture<Void> waiter = writer.awaitAppend();
        CompletableFuture<PartitionResponse> write =
                writer.write(records(new SimpleRecord("a".getBytes(StandardCharsets.UTF_8))), NO_ZONE);
        assertFalse(waiter.isDone());

        pending.complete(header(0L, 1, 1L));

        assertNull(waiter.get(5, TimeUnit.SECONDS));
        assertEquals(Errors.NONE, write.get(5, TimeUnit.SECONDS).error);
    }

    @Test
    void awaitAppendIsNotCompletedByAFailedAppend() throws Exception {
        Log log = mock(Log.class);
        CompletableFuture<LogEntryHeader> pending = new CompletableFuture<>();
        when(log.append(eq(1), any())).thenReturn(pending);
        PartitionWriter writer = writer(log, TimestampType.CREATE_TIME);

        CompletableFuture<Void> waiter = writer.awaitAppend();
        CompletableFuture<PartitionResponse> write =
                writer.write(records(new SimpleRecord("a".getBytes(StandardCharsets.UTF_8))), NO_ZONE);
        pending.completeExceptionally(new IllegalStateException("append failed"));

        assertThrows(ExecutionException.class, () -> write.get(5, TimeUnit.SECONDS));
        assertFalse(waiter.isDone(), "A failed append must not wake a long-poll waiter");
        writer.close();
        assertNull(waiter.get(5, TimeUnit.SECONDS));
    }

    @Test
    void closeCompletesWaitersAndReleasesPendingPayloads() throws Exception {
        CompletableFuture<Log> logFuture = new CompletableFuture<>();
        PartitionWriter writer = new PartitionWriter(
                TP,
                () -> logFuture,
                zone -> {
                    throw new IllegalStateException("no producer state expected for " + zone);
                },
                TimestampType.CREATE_TIME,
                new MockTime(0L, 1000L, 0L),
                appended -> { });

        CompletableFuture<PartitionResponse> write =
                writer.write(records(new SimpleRecord("a".getBytes(StandardCharsets.UTF_8))), NO_ZONE);
        CompletableFuture<Void> waiter = writer.awaitAppend();
        assertEquals(1, writer.ownedWritePayloadCount());
        assertFalse(write.isDone());

        writer.close();

        assertNull(waiter.get(5, TimeUnit.SECONDS));
        assertEquals(0, writer.ownedWritePayloadCount());
        assertNull(writer.awaitAppend().get(5, TimeUnit.SECONDS));
        PartitionResponse afterClose = writer
                .write(records(new SimpleRecord("b".getBytes(StandardCharsets.UTF_8))), NO_ZONE)
                .get(5, TimeUnit.SECONDS);
        assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, afterClose.error);
    }

    @Test
    void invalidRecordsAreRejectedWithoutAppending() throws Exception {
        Log log = mock(Log.class);
        PartitionWriter writer = writer(log, TimestampType.CREATE_TIME);
        MemoryRecords valid = records(new SimpleRecord("a".getBytes(StandardCharsets.UTF_8)));
        ByteBuffer malformed = ByteBuffer.allocate(valid.buffer().remaining() + 1);
        malformed.put(valid.buffer()).put((byte) 1).flip();

        PartitionResponse response = writer
                .write(MemoryRecords.readableRecords(malformed), NO_ZONE)
                .get(5, TimeUnit.SECONDS);

        assertEquals(Errors.INVALID_RECORD, response.error);
        assertEquals(0, writer.ownedWritePayloadCount());
    }

    @Test
    void anAppendThatNeverReachesStorageAbortsTheProducerState() throws Exception {
        ProducerStateManager producerStateManager = mock(ProducerStateManager.class);
        when(producerStateManager.prepareAppend(anyList()))
                .thenReturn(completedFuture(new ProducerStateManager.Ready(null)));
        IOException openFailure = new IOException("log unavailable");
        PartitionWriter writer = new PartitionWriter(
                TP,
                () -> CompletableFuture.failedFuture(openFailure),
                zone -> producerStateManager,
                TimestampType.CREATE_TIME,
                new MockTime(0L, 1000L, 0L),
                appended -> { });

        CompletableFuture<PartitionResponse> write = writer.write(idempotentRecords(), NO_ZONE);

        assertThrows(ExecutionException.class, () -> write.get(5, TimeUnit.SECONDS));
        // The prepared append is registered in producer state before storage is touched, so a
        // failure on the way to storage must unwind it rather than leaving it pending.
        verify(producerStateManager).abortAppend(null, openFailure);
        assertEquals(0, writer.ownedWritePayloadCount());
    }

    @Test
    void anAppendThatFailsOnHandoffAbortsTheProducerStateAndFreesTheSequencer() throws Exception {
        ProducerStateManager producerStateManager = mock(ProducerStateManager.class);
        when(producerStateManager.prepareAppend(anyList()))
                .thenReturn(completedFuture(new ProducerStateManager.Ready(null)));
        Log log = mock(Log.class);
        IllegalStateException handoffFailure = new IllegalStateException("append rejected");
        when(log.append(eq(1), any()))
                .thenThrow(handoffFailure)
                .thenReturn(completedFuture(header(0L, 1, 1000L)));
        PartitionWriter writer = new PartitionWriter(
                TP,
                () -> completedFuture(log),
                zone -> producerStateManager,
                TimestampType.CREATE_TIME,
                new MockTime(0L, 1000L, 0L),
                appended -> { });

        CompletableFuture<PartitionResponse> failed = writer.write(idempotentRecords(), NO_ZONE);

        ExecutionException thrown = assertThrows(ExecutionException.class, () -> failed.get(5, TimeUnit.SECONDS));
        assertSame(handoffFailure, thrown.getCause());
        // The log opened, so the prepared append is already registered when the handoff throws.
        // Leaving it pending would reject every later sequence number for this producer.
        verify(producerStateManager).abortAppend(null, handoffFailure);
        assertEquals(0, writer.ownedWritePayloadCount());

        // The submission future has to complete on this path too, otherwise the sequencer never
        // starts the next queued write and the partition stalls behind one failed append.
        PartitionResponse next = writer
                .write(records(new SimpleRecord("a".getBytes(StandardCharsets.UTF_8))), NO_ZONE)
                .get(5, TimeUnit.SECONDS);
        assertEquals(Errors.NONE, next.error);
    }

    @Test
    void aSuccessfulAppendIsReportedAsTheNewLogTail() throws Exception {
        Log log = mock(Log.class);
        LogEntryHeader header = header(7L, 2, 1000L);
        when(log.append(eq(2), any())).thenReturn(completedFuture(header));
        AtomicReference<LogOffset> appended = new AtomicReference<>();
        PartitionWriter writer =
                writer(log, TimestampType.CREATE_TIME, new MockTime(0L, 1000L, 0L), appended::set);

        PartitionResponse response = writer
                .write(records(
                        new SimpleRecord(5L, "a".getBytes(StandardCharsets.UTF_8)),
                        new SimpleRecord(6L, "b".getBytes(StandardCharsets.UTF_8))), NO_ZONE)
                .get(5, TimeUnit.SECONDS);

        assertEquals(Errors.NONE, response.error);
        // The reader's cached offset window is widened from this, so it has to describe the whole
        // entry: where it starts, how many records it holds, and when it was written.
        assertNotNull(appended.get());
        assertEquals(7L, appended.get().offset());
        assertEquals(2, appended.get().numberOfRecords());
        assertEquals(1000L, appended.get().timestamp());
    }

    @Test
    void anAppendThatFailedReportsNoNewLogTail() {
        Log log = mock(Log.class);
        when(log.append(eq(1), any()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("append failed")));
        AtomicReference<LogOffset> appended = new AtomicReference<>();
        PartitionWriter writer =
                writer(log, TimestampType.CREATE_TIME, new MockTime(0L, 1000L, 0L), appended::set);

        assertThrows(ExecutionException.class, () -> writer
                .write(records(new SimpleRecord(5L, "a".getBytes(StandardCharsets.UTF_8))), NO_ZONE)
                .get(5, TimeUnit.SECONDS));

        assertNull(appended.get());
    }

    private PartitionWriter writer(Log log, TimestampType timestampType) {
        return writer(log, timestampType, new MockTime(0L, 1000L, 0L));
    }

    private PartitionWriter writer(Log log, TimestampType timestampType, Time time) {
        return writer(log, timestampType, time, appended -> { });
    }

    private PartitionWriter writer(
            Log log,
            TimestampType timestampType,
            Time time,
            Consumer<LogOffset> onAppended) {
        return new PartitionWriter(
                TP,
                () -> completedFuture(log),
                zone -> {
                    throw new IllegalStateException("no producer state expected for " + zone);
                },
                timestampType,
                time,
                onAppended);
    }

    private static MemoryRecords records(SimpleRecord... records) {
        return MemoryRecords.withRecords(Compression.NONE, records);
    }

    private static MemoryRecords idempotentRecords() {
        return MemoryRecords.withIdempotentRecords(
                Compression.NONE,
                12345L,
                (short) 0,
                0,
                new SimpleRecord("a".getBytes(StandardCharsets.UTF_8)));
    }

    private static MemoryRecords copyOf(ByteBuf payload) {
        ByteBuffer copy = ByteBuffer.allocate(payload.readableBytes());
        payload.getBytes(payload.readerIndex(), copy);
        copy.flip();
        return MemoryRecords.readableRecords(copy);
    }

    private static LogEntryHeader header(long offset, int numberOfRecords, long timestamp) {
        LogEntryHeader header = mock(LogEntryHeader.class);
        when(header.offset()).thenReturn(offset);
        when(header.numberOfRecords()).thenReturn(numberOfRecords);
        when(header.timestamp()).thenReturn(timestamp);
        return header;
    }
}
