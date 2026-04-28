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
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.storage.diskless.ListOffsetsPartitionRequest;
import org.apache.kafka.storage.diskless.ListOffsetsPartitionResponse;

import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.streamnative.ursa.mledger.UrsaPosition;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class UrsaManagedLedgerReaderTest {

    @Test
    void testListOffsetsEarliestUsesManagedLedgerBoundaries() throws Exception {
        TopicIdPartition tp = createTestPartition();

        UrsaStorageState state = mock(UrsaStorageState.class);
        ManagedLedger ledger = mock(ManagedLedger.class);

        Position firstPosition = ursaPosition(/* streamId */ 1L, /* baseOffset */ 5L, /* numMessages */ 1L);
        when(ledger.getFirstPosition()).thenReturn(firstPosition);
        attachReaderPartitionLog(state, tp, ledger);

        UrsaManagedLedgerReader reader = new UrsaManagedLedgerReader(state);

        ListOffsetsPartitionRequest request = new ListOffsetsPartitionRequest(
                tp, ListOffsetsPartitionRequest.EARLIEST_TIMESTAMP, Optional.empty());
        Map<TopicIdPartition, ListOffsetsPartitionResponse> responses =
                reader.listOffsets(Map.of(tp, request)).get();

        assertEquals(1, responses.size());
        ListOffsetsPartitionResponse response = responses.get(tp);
        assertNotNull(response);
        assertEquals(Errors.NONE, response.error());
        assertEquals(5L, response.offset());
        assertEquals(-1L, response.timestamp());
    }

    @Test
    void testListOffsetsLatestUsesManagedLedgerBoundaries() throws Exception {
        TopicIdPartition tp = createTestPartition();

        UrsaStorageState state = mock(UrsaStorageState.class);
        ManagedLedger ledger = mock(ManagedLedger.class);

        Position lastPosition = ursaPosition(/* streamId */ 1L, /* baseOffset */ 10L, /* numMessages */ 5L);
        when(ledger.getLastConfirmedEntry()).thenReturn(lastPosition);
        attachReaderPartitionLog(state, tp, ledger);

        UrsaManagedLedgerReader reader = new UrsaManagedLedgerReader(state);

        ListOffsetsPartitionRequest request = new ListOffsetsPartitionRequest(
                tp, ListOffsetsPartitionRequest.LATEST_TIMESTAMP, Optional.empty());
        Map<TopicIdPartition, ListOffsetsPartitionResponse> responses =
                reader.listOffsets(Map.of(tp, request)).get();

        assertEquals(1, responses.size());
        ListOffsetsPartitionResponse response = responses.get(tp);
        assertNotNull(response);
        assertEquals(Errors.NONE, response.error());
        assertEquals(15L, response.offset());
        assertEquals(-1L, response.timestamp());
    }

    @Test
    void testFetchUsesManagedLedgerHighWatermark() throws Exception {
        TopicIdPartition tp = createTestPartition();

        UrsaStorageState state = mock(UrsaStorageState.class);
        ManagedLedger ledger = mock(ManagedLedger.class);

        Position lastPosition = ursaPosition(/* streamId */ 1L, /* baseOffset */ 10L, /* numMessages */ 5L);
        when(ledger.getLastConfirmedEntry()).thenReturn(lastPosition);
        attachReaderPartitionLog(state, tp, ledger);

        UrsaManagedLedgerReader reader = new UrsaManagedLedgerReader(state);

        FetchParams params = createFetchParams();
        FetchRequest.PartitionData partitionData = new FetchRequest.PartitionData(
                Uuid.ZERO_UUID, /* fetchOffset */ 15L, 0, /* maxBytes */ 1024 * 1024, Optional.empty());
        Map<TopicIdPartition, FetchPartitionData> responses =
                reader.fetch(params, Map.of(tp, partitionData)).get();

        assertEquals(1, responses.size());
        FetchPartitionData response = responses.get(tp);
        assertNotNull(response);
        assertEquals(Errors.NONE, response.error);
        assertEquals(15L, response.highWatermark);
        assertEquals(MemoryRecords.EMPTY, response.records);
    }

    @Test
    void testFetchInvalidatesPartitionLogWhenManagedLedgerIsNotFound() throws Exception {
        TopicIdPartition tp = createTestPartition();

        UrsaStorageState state = mock(UrsaStorageState.class);
        ManagedLedger ledger = mock(ManagedLedger.class);
        when(ledger.getLastConfirmedEntry()).thenThrow(
                new CompletionException(new ManagedLedgerException.ManagedLedgerNotFoundException("missing")));

        UrsaPartitionLog partitionLog = new UrsaPartitionLog(
                tp,
                state,
                new DisklessLogMetrics(),
                CompletableFuture.completedFuture(ledger),
                null,
                0L,
                0,
                null);
        when(state.getOrCreatePartitionLog(tp)).thenReturn(partitionLog);

        UrsaManagedLedgerReader reader = new UrsaManagedLedgerReader(state);

        FetchParams params = createFetchParams();
        FetchRequest.PartitionData partitionData = new FetchRequest.PartitionData(
                Uuid.ZERO_UUID, /* fetchOffset */ 0L, 0, /* maxBytes */ 1024 * 1024, Optional.empty());
        Map<TopicIdPartition, FetchPartitionData> responses =
                reader.fetch(params, Map.of(tp, partitionData)).get();

        FetchPartitionData response = responses.get(tp);
        assertNotNull(response);
        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, response.error);
        verify(state).removePartitionLog(eq(tp), same(partitionLog));
    }

    @Test
    void testFetchReusesNonDurableCursorsFromPoolWithQueueing() throws Exception {
        TopicIdPartition tp = createTestPartition();

        UrsaStorageState state = mock(UrsaStorageState.class);
        ManagedLedger ledger = mock(ManagedLedger.class);

        long streamId = 1L;
        when(ledger.getFirstPosition()).thenReturn(ursaPosition(streamId, /* baseOffset */ 0L, /* numMessages */ 1L));
        when(ledger.getLastConfirmedEntry()).thenReturn(ursaPosition(streamId, /* baseOffset */ 100L, /* numMessages */ 1L));
        attachReaderPartitionLog(state, tp, ledger);

        AtomicInteger resetCalls = new AtomicInteger(0);
        AtomicReference<Position> lastResetPosition = new AtomicReference<>();
        ConcurrentHashMap<String, ManagedCursor> cursorByName = new ConcurrentHashMap<>();
        HashSet<String> cursorNames = new HashSet<>();

        CountDownLatch started4Reads = new CountDownLatch(4);
        CountDownLatch started5Reads = new CountDownLatch(5);
        ArrayDeque<ReadCompletion> readCompletions = new ArrayDeque<>();

        when(ledger.newNonDurableCursor(any(Position.class), anyString())).thenAnswer(invocation -> {
            String cursorName = invocation.getArgument(1);
            synchronized (cursorNames) {
                cursorNames.add(cursorName);
            }
            return cursorByName.computeIfAbsent(cursorName, name -> {
                ManagedCursor cursor = mock(ManagedCursor.class);

                doAnswer(resetInvocation -> {
                    Position position = resetInvocation.getArgument(0);
                    lastResetPosition.set(position);
                    resetCalls.incrementAndGet();
                    AsyncCallbacks.ResetCursorCallback callback = resetInvocation.getArgument(2);
                    callback.resetComplete(null);
                    return null;
                }).when(cursor).asyncResetCursor(any(Position.class), eq(false),
                        any(AsyncCallbacks.ResetCursorCallback.class));

                doAnswer(readInvocation -> {
                    AsyncCallbacks.ReadEntriesCallback callback = readInvocation.getArgument(2);
                    Object ctx = readInvocation.getArgument(3);
                    synchronized (readCompletions) {
                        readCompletions.add(new ReadCompletion(
                                () -> callback.readEntriesComplete(java.util.List.of(), ctx)));
                    }
                    started4Reads.countDown();
                    started5Reads.countDown();
                    return null;
                }).when(cursor).asyncReadEntries(anyInt(), anyLong(),
                        any(AsyncCallbacks.ReadEntriesCallback.class), any(), any(Position.class));

                return cursor;
            });
        });

        UrsaManagedLedgerReader reader = new UrsaManagedLedgerReader(state);
        FetchParams params = createFetchParams();

        long[] fetchOffsets = new long[]{0L, 1L, 2L, 3L, 7L};
        ArrayList<CompletableFuture<Map<TopicIdPartition, FetchPartitionData>>> futures = new ArrayList<>();
        for (long fetchOffset : fetchOffsets) {
            FetchRequest.PartitionData partitionData = new FetchRequest.PartitionData(
                    Uuid.ZERO_UUID, fetchOffset, 0, /* maxBytes */ 1024 * 1024, Optional.empty());
            futures.add(reader.fetch(params, Map.of(tp, partitionData)));
        }

        assertTrue(started4Reads.await(5, TimeUnit.SECONDS));
        verify(ledger, times(4)).newNonDurableCursor(any(Position.class), anyString());

        ReadCompletion firstRead;
        synchronized (readCompletions) {
            firstRead = readCompletions.poll();
        }
        assertNotNull(firstRead);
        firstRead.complete.run();

        assertTrue(started5Reads.await(5, TimeUnit.SECONDS));

        String expectedPrefix = "kafka-fetch-" + tp.topic() + "-partition-" + tp.partition() + "-cursor-";
        synchronized (cursorNames) {
            assertEquals(4, cursorNames.size());
            for (String name : cursorNames) {
                assertTrue(name.startsWith(expectedPrefix));
            }
        }

        Position expectedResetPosition = PositionFactory.create(streamId, /* entryId */ 7L);
        assertEquals(1, resetCalls.get());
        assertEquals(expectedResetPosition, lastResetPosition.get());

        while (true) {
            ReadCompletion completion;
            synchronized (readCompletions) {
                completion = readCompletions.poll();
            }
            if (completion == null) {
                break;
            }
            completion.complete.run();
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0])).get(5, TimeUnit.SECONDS);
        verify(ledger, times(4)).newNonDurableCursor(any(Position.class), anyString());

        for (ManagedCursor cursor : cursorByName.values()) {
            verify(cursor, never()).close();
        }
    }

    @Test
    void testListOffsetsTimestampSearchReturnsPreciseOffset() throws Exception {
        TopicIdPartition tp = createTestPartition();

        UrsaStorageState state = mock(UrsaStorageState.class);
        ManagedLedger ledger = mock(ManagedLedger.class);
        ManagedCursor cursor = mock(ManagedCursor.class);

        Position firstPosition = ursaPosition(/* streamId */ 1L, /* baseOffset */ 10L, /* numMessages */ 5L);
        Position lastPosition = firstPosition;
        when(ledger.getFirstPosition()).thenReturn(firstPosition);
        when(ledger.getLastConfirmedEntry()).thenReturn(lastPosition);
        when(ledger.getName()).thenReturn("test-ledger");
        when(ledger.getNumberOfEntries()).thenReturn(1L);
        when(ledger.newNonDurableCursor(eq(firstPosition), anyString())).thenReturn(cursor);
        attachReaderPartitionLog(state, tp, ledger);

        doAnswer(invocation -> {
            AsyncCallbacks.ReadEntriesCallback callback = invocation.getArgument(2);
            Object ctx = invocation.getArgument(3);
            callback.readEntriesComplete(java.util.List.of(createKafkaRecordsEntry(
                    /* baseOffset */ 10L, new long[]{1000L, 1200L, 1500L, 1800L, 2000L})), ctx);
            return null;
        }).when(cursor).asyncReadEntries(anyInt(), eq(Long.MAX_VALUE), any(AsyncCallbacks.ReadEntriesCallback.class), any(), any(Position.class));

        UrsaManagedLedgerReader reader = new UrsaManagedLedgerReader(state);

        ListOffsetsPartitionRequest request = new ListOffsetsPartitionRequest(tp, /* timestamp */ 1500L, Optional.empty());
        Map<TopicIdPartition, ListOffsetsPartitionResponse> responses = reader.listOffsets(Map.of(tp, request)).get();

        ListOffsetsPartitionResponse response = responses.get(tp);
        assertNotNull(response);
        assertEquals(Errors.NONE, response.error());
        assertEquals(12L, response.offset());
        assertEquals(1500L, response.timestamp());
    }

    @Test
    void testListOffsetsMaxTimestampUsesPublishTimeSemantics() throws Exception {
        TopicIdPartition tp = createTestPartition();

        UrsaStorageState state = mock(UrsaStorageState.class);
        ManagedLedger ledger = mock(ManagedLedger.class);
        ManagedCursor cursor = mock(ManagedCursor.class);

        Position lastPosition = ursaPosition(/* streamId */ 1L, /* baseOffset */ 10L, /* numMessages */ 5L);
        when(ledger.getLastConfirmedEntry()).thenReturn(lastPosition);
        when(ledger.getName()).thenReturn("test-ledger");
        when(ledger.newNonDurableCursor(eq(lastPosition), anyString())).thenReturn(cursor);
        attachReaderPartitionLog(state, tp, ledger);

        doAnswer(invocation -> {
            AsyncCallbacks.ReadEntriesCallback callback = invocation.getArgument(2);
            Object ctx = invocation.getArgument(3);
            callback.readEntriesComplete(java.util.List.of(createMetadataEntry(/* baseOffset */ 10L, /* publishTime */ 2000L, /* numMessagesInBatch */ 5)), ctx);
            return null;
        }).when(cursor).asyncReadEntries(eq(1), eq(Long.MAX_VALUE), any(AsyncCallbacks.ReadEntriesCallback.class), any(), eq(lastPosition));

        UrsaManagedLedgerReader reader = new UrsaManagedLedgerReader(state);

        ListOffsetsPartitionRequest request = new ListOffsetsPartitionRequest(
                tp, ListOffsetsPartitionRequest.MAX_TIMESTAMP, Optional.empty());
        Map<TopicIdPartition, ListOffsetsPartitionResponse> responses = reader.listOffsets(Map.of(tp, request)).get();

        ListOffsetsPartitionResponse response = responses.get(tp);
        assertNotNull(response);
        assertEquals(Errors.NONE, response.error());
        assertEquals(14L, response.offset());
        assertEquals(2000L, response.timestamp());
    }

    private static FetchParams createFetchParams() {
        return new FetchParams(
                -1,
                -1,
                500,
                1,
                1024 * 1024,
                FetchIsolation.HIGH_WATERMARK,
                Optional.empty()
        );
    }

    private static TopicIdPartition createTestPartition() {
        return new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("test-topic", 0));
    }

    private static UrsaPosition ursaPosition(long streamId, long baseOffset, long numMessages) {
        return new UrsaPosition(streamId, baseOffset, numMessages);
    }

    private static org.apache.bookkeeper.mledger.Entry createKafkaRecordsEntry(long baseOffset, long[] timestamps) {
        SimpleRecord[] records = new SimpleRecord[timestamps.length];
        byte[] key = "key".getBytes(StandardCharsets.UTF_8);
        byte[] value = "value".getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < timestamps.length; i++) {
            records[i] = new SimpleRecord(timestamps[i], key, value);
        }

        MemoryRecords memoryRecords = MemoryRecords.withRecords(Compression.NONE, records);
        RecordAnalyzer.RecordAnalysisResult analysisResult = RecordAnalyzer.analyzeAndValidateRecords(
                memoryRecords,
                new TopicPartition("test-topic", 0),
                0
        );

        ByteBuf encoded = KafkaEntryFormatter.encode(memoryRecords, analysisResult);

        org.apache.bookkeeper.mledger.Entry entry = mock(org.apache.bookkeeper.mledger.Entry.class);
        when(entry.getEntryId()).thenReturn(baseOffset);
        when(entry.getDataBuffer()).thenReturn(encoded);
        doAnswer(invocation -> encoded.release()).when(entry).release();
        return entry;
    }

    private static org.apache.bookkeeper.mledger.Entry createMetadataEntry(long baseOffset, long publishTimeMs, int numMessagesInBatch) {
        MessageMetadata metadata = new MessageMetadata();
        metadata.setProducerName("test-producer");
        metadata.setSequenceId(0L);
        metadata.setPublishTime(publishTimeMs);
        metadata.setNumMessagesInBatch(numMessagesInBatch);
        ByteBuf encoded = Commands.serializeMetadataAndPayload(Commands.ChecksumType.None, metadata, Unpooled.EMPTY_BUFFER);

        org.apache.bookkeeper.mledger.Entry entry = mock(org.apache.bookkeeper.mledger.Entry.class);
        when(entry.getEntryId()).thenReturn(baseOffset);
        when(entry.getDataBuffer()).thenReturn(encoded);
        doAnswer(invocation -> encoded.release()).when(entry).release();
        return entry;
    }

    private static void attachReaderPartitionLog(
            UrsaStorageState state,
            TopicIdPartition tp,
            ManagedLedger ledger) {
        UrsaPartitionLog partitionLog = new UrsaPartitionLog(
                tp,
                state,
                new DisklessLogMetrics(),
                CompletableFuture.completedFuture(ledger),
                null,
                0L,
                0,
                null);
        when(state.getOrCreatePartitionLog(tp)).thenReturn(partitionLog);
    }

    private static final class ReadCompletion {
        private final Runnable complete;

        private ReadCompletion(Runnable complete) {
            this.complete = complete;
        }
    }
}
