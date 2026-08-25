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
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.diskless.DisklessClientZone;

import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.buffer.ByteBuf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class UrsaManagedLedgerWriterNonIdempotentTest {

    @Test
    void testNonIdempotentWriteAppendsValidRecords() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        ManagedLedger managedLedger = mock(ManagedLedger.class);

        doAnswer(invocation -> {
            ByteBuf buffer = invocation.getArgument(0);
            int numberOfMessages = invocation.getArgument(1);
            AsyncCallbacks.AddEntryCallback callback = invocation.getArgument(2);

            MemoryRecords appendedRecords = decodeKafkaRecords(buffer);
            RecordBatch batch = appendedRecords.batches().iterator().next();
            assertEquals(2, numberOfMessages);
            assertTrue(batch.isValid());
            assertFalse(batch.hasProducerId());

            callback.addComplete(position(0L, 1L), null, null);
            return null;
        }).when(managedLedger).asyncAddEntry(
            any(ByteBuf.class),
            anyInt(),
            any(AsyncCallbacks.AddEntryCallback.class),
            any());

        UrsaManagedLedgerWriter writer = new UrsaManagedLedgerWriter(newWriterState(tp, managedLedger));
        MemoryRecords records = MemoryRecords.withRecords(
            Compression.NONE,
            new SimpleRecord("a".getBytes(StandardCharsets.UTF_8)),
            new SimpleRecord("b".getBytes(StandardCharsets.UTF_8))
        );

        try {
            Map<TopicIdPartition, PartitionResponse> response =
                    writer.write(Map.of(tp, records), DisklessClientZone.NO_ZONE).get(5, TimeUnit.SECONDS);
            PartitionResponse partitionResponse = response.get(tp);
            assertEquals(Errors.NONE, partitionResponse.error);
            assertEquals(0L, partitionResponse.baseOffset);
        } finally {
            writer.close();
        }
    }

    @Test
    void testNonIdempotentWritesSamePartitionCanSubmitThroughSequencedWrites() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        ManagedLedger managedLedger = mock(ManagedLedger.class);
        CountDownLatch firstAppendSubmitted = new CountDownLatch(1);
        CountDownLatch secondAppendSubmitted = new CountDownLatch(1);
        AtomicReference<AsyncCallbacks.AddEntryCallback> firstCallbackRef = new AtomicReference<>();
        AtomicReference<AsyncCallbacks.AddEntryCallback> secondCallbackRef = new AtomicReference<>();
        AtomicInteger appendCalls = new AtomicInteger(0);

        doAnswer(invocation -> {
            ByteBuf buffer = invocation.getArgument(0);
            AsyncCallbacks.AddEntryCallback callback = invocation.getArgument(2);

            MemoryRecords appendedRecords = decodeKafkaRecords(buffer);
            RecordBatch batch = appendedRecords.batches().iterator().next();
            assertTrue(batch.isValid());
            assertFalse(batch.hasProducerId());

            int appendIndex = appendCalls.getAndIncrement();
            if (appendIndex == 0) {
                firstCallbackRef.set(callback);
                firstAppendSubmitted.countDown();
            } else if (appendIndex == 1) {
                secondCallbackRef.set(callback);
                secondAppendSubmitted.countDown();
            } else {
                fail("Unexpected append index " + appendIndex);
            }
            return null;
        }).when(managedLedger).asyncAddEntry(
            any(ByteBuf.class),
            anyInt(),
            any(AsyncCallbacks.AddEntryCallback.class),
            any());

        UrsaManagedLedgerWriter writer = new UrsaManagedLedgerWriter(newWriterState(tp, managedLedger));
        MemoryRecords records1 = MemoryRecords.withRecords(
            Compression.NONE,
            new SimpleRecord("first".getBytes(StandardCharsets.UTF_8))
        );
        MemoryRecords records2 = MemoryRecords.withRecords(
            Compression.NONE,
            new SimpleRecord("second".getBytes(StandardCharsets.UTF_8))
        );

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            CompletableFuture<Map<TopicIdPartition, PartitionResponse>> firstWrite =
                CompletableFuture.supplyAsync(() -> writer.write(Map.of(tp, records1), DisklessClientZone.NO_ZONE), executor)
                    .thenCompose(future -> future);

            assertTrue(firstAppendSubmitted.await(5, TimeUnit.SECONDS),
                "Timed out waiting for first non-idempotent append submission");

            CompletableFuture<Map<TopicIdPartition, PartitionResponse>> secondWrite =
                CompletableFuture.supplyAsync(() -> writer.write(Map.of(tp, records2), DisklessClientZone.NO_ZONE), executor)
                    .thenCompose(future -> future);

            assertTrue(secondAppendSubmitted.await(5, TimeUnit.SECONDS),
                "Timed out waiting for second non-idempotent append submission");

            firstCallbackRef.get().addComplete(position(0L, 1L), null, null);
            secondCallbackRef.get().addComplete(position(1L, 1L), null, null);

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
    void testNonIdempotentWriteReturnsNotLeaderWhenManagedLedgerIsClosed() throws Exception {
        TopicIdPartition tp = testTopicPartition();
        ManagedLedger managedLedger = mock(ManagedLedger.class);

        doAnswer(invocation -> {
            AsyncCallbacks.AddEntryCallback callback = invocation.getArgument(2);
            callback.addFailed(new ManagedLedgerException("Already closed"), null);
            return null;
        }).when(managedLedger).asyncAddEntry(
            any(ByteBuf.class),
            anyInt(),
            any(AsyncCallbacks.AddEntryCallback.class),
            any());

        UrsaStorageState state = mock(UrsaStorageState.class);
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

        UrsaManagedLedgerWriter writer = new UrsaManagedLedgerWriter(state);
        MemoryRecords records = MemoryRecords.withRecords(
            Compression.NONE,
            new SimpleRecord("a".getBytes(StandardCharsets.UTF_8))
        );

        try {
            Map<TopicIdPartition, PartitionResponse> response =
                    writer.write(Map.of(tp, records), DisklessClientZone.NO_ZONE).get(5, TimeUnit.SECONDS);
            assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, response.get(tp).error);
            verify(state).removePartitionLog(eq(tp), same(partitionLog));
        } finally {
            writer.close();
        }
    }

    private static UrsaStorageState newWriterState(
            TopicIdPartition tp,
            ManagedLedger managedLedger) {
        UrsaStorageState state = mock(UrsaStorageState.class);
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
        return state;
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

    private static MemoryRecords decodeKafkaRecords(ByteBuf buffer) {
        return MemoryRecords.readableRecords(KafkaEntryFormatter.decode(buffer.duplicate()));
    }
}
