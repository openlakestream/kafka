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
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.Records;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.storage.diskless.DisklessClientZone;
import org.apache.kafka.storage.diskless.ListOffsetsPartitionRequest;
import org.apache.kafka.storage.diskless.ListOffsetsPartitionResponse;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.lakestream.api.Log;
import io.lakestream.api.LogCursor;
import io.lakestream.api.LogEntry;
import io.lakestream.api.LogEntryHeader;
import io.lakestream.api.LogOffset;
import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.exception.NoSuchStreamException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class UrsaLakestreamReaderTest {

    private static ScheduledThreadPoolExecutor timer;

    @BeforeAll
    static void startTimer() {
        timer = new ScheduledThreadPoolExecutor(1, runnable -> {
            Thread thread = new Thread(runnable, "diskless-timer-test");
            thread.setDaemon(true);
            return thread;
        });
        timer.setRemoveOnCancelPolicy(true);
    }

    @AfterAll
    static void stopTimer() {
        timer.shutdownNow();
    }

    @Test
    void testListOffsetsEarliestUsesLogBoundaries() throws Exception {
        TopicIdPartition tp = createTestPartition();
        UrsaStorageState state = mock(UrsaStorageState.class);
        Log logInstance = mock(Log.class);
        when(logInstance.getFirstOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(5L, 1)));
        when(logInstance.getLastOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(10L, 5)));
        attachReaderPartitionLog(state, tp, logInstance);

        UrsaLakestreamReader reader = new UrsaLakestreamReader(state);
        ListOffsetsPartitionRequest request = new ListOffsetsPartitionRequest(
                tp, ListOffsetsPartitionRequest.EARLIEST_TIMESTAMP, Optional.empty());

        ListOffsetsPartitionResponse response = reader.listOffsets(Map.of(tp, request)).get().get(tp);

        assertNotNull(response);
        assertEquals(Errors.NONE, response.error());
        assertEquals(5L, response.offset());
        assertEquals(-1L, response.timestamp());
    }

    @Test
    void testListOffsetsLatestUsesLogHighWatermark() throws Exception {
        TopicIdPartition tp = createTestPartition();
        UrsaStorageState state = mock(UrsaStorageState.class);
        Log logInstance = mock(Log.class);
        when(logInstance.getFirstOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(5L, 1)));
        when(logInstance.getLastOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(10L, 5)));
        attachReaderPartitionLog(state, tp, logInstance);

        UrsaLakestreamReader reader = new UrsaLakestreamReader(state);
        ListOffsetsPartitionRequest request = new ListOffsetsPartitionRequest(
                tp, ListOffsetsPartitionRequest.LATEST_TIMESTAMP, Optional.empty());

        ListOffsetsPartitionResponse response = reader.listOffsets(Map.of(tp, request)).get().get(tp);

        assertNotNull(response);
        assertEquals(Errors.NONE, response.error());
        assertEquals(15L, response.offset());
        assertEquals(-1L, response.timestamp());
    }

    @Test
    void testFetchAtHighWatermarkReturnsEmptyRecords() throws Exception {
        TopicIdPartition tp = createTestPartition();
        UrsaStorageState state = mock(UrsaStorageState.class);
        Log logInstance = mock(Log.class);
        when(logInstance.getFirstOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(5L, 1)));
        when(logInstance.getLastOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(10L, 5)));
        attachReaderPartitionLog(state, tp, logInstance);

        UrsaLakestreamReader reader = new UrsaLakestreamReader(state);
        FetchRequest.PartitionData partitionData = fetchPartitionData(15L);

        FetchPartitionData response = reader.fetch(
                createFetchParams(0L), Map.of(tp, partitionData)).get().get(tp);

        assertNotNull(response);
        assertEquals(Errors.NONE, response.error);
        assertEquals(15L, response.highWatermark);
        assertEquals(5L, response.logStartOffset);
        assertEquals(MemoryRecords.EMPTY, response.records);
        verify(logInstance, never()).openEphemeralCursor(anyString(), anyLong());
    }

    @Test
    void testFetchBelowLogStartOffsetReturnsOffsetOutOfRange() throws Exception {
        TopicIdPartition tp = createTestPartition();
        UrsaStorageState state = mock(UrsaStorageState.class);
        Log logInstance = mock(Log.class);
        when(logInstance.getFirstOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(5L, 1)));
        when(logInstance.getLastOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(10L, 5)));
        attachReaderPartitionLog(state, tp, logInstance);

        UrsaLakestreamReader reader = new UrsaLakestreamReader(state);

        FetchPartitionData response = reader.fetch(
                createFetchParams(), Map.of(tp, fetchPartitionData(4L))).get().get(tp);

        assertNotNull(response);
        assertEquals(Errors.OFFSET_OUT_OF_RANGE, response.error);
        assertEquals(15L, response.highWatermark);
        assertEquals(5L, response.logStartOffset);
        assertEquals(MemoryRecords.EMPTY, response.records);
        verify(logInstance, never()).openEphemeralCursor(anyString(), anyLong());
    }

    @Test
    void testFetchAboveHighWatermarkReturnsOffsetOutOfRange() throws Exception {
        TopicIdPartition tp = createTestPartition();
        UrsaStorageState state = mock(UrsaStorageState.class);
        Log logInstance = mock(Log.class);
        when(logInstance.getFirstOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(5L, 1)));
        when(logInstance.getLastOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(10L, 5)));
        attachReaderPartitionLog(state, tp, logInstance);

        UrsaLakestreamReader reader = new UrsaLakestreamReader(state);

        FetchPartitionData response = reader.fetch(
                createFetchParams(), Map.of(tp, fetchPartitionData(16L))).get().get(tp);

        assertNotNull(response);
        assertEquals(Errors.OFFSET_OUT_OF_RANGE, response.error);
        assertEquals(15L, response.highWatermark);
        assertEquals(5L, response.logStartOffset);
        assertEquals(MemoryRecords.EMPTY, response.records);
        verify(logInstance, never()).openEphemeralCursor(anyString(), anyLong());
    }

    @Test
    void testFetchCopiesReadOnlyPayloadAndRebasesKafkaRecordOffsets() throws Exception {
        TopicIdPartition tp = createTestPartition();
        UrsaStorageState state = mock(UrsaStorageState.class);
        Log logInstance = mock(Log.class);
        LogCursor cursor = mock(LogCursor.class);
        when(logInstance.getFirstOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(10L, 3)));
        when(logInstance.getLastOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(10L, 3)));
        when(logInstance.openEphemeralCursor(anyString(), eq(10L)))
                .thenReturn(CompletableFuture.completedFuture(cursor));
        LogEntry firstEntry = createKafkaRecordsEntry(10L, new long[]{1000L});
        LogEntry multiBatchEntry = createKafkaRecordsEntry(
                11L, new long[]{1200L}, new long[]{1500L});
        when(cursor.readEntries(anyInt(), anyLong(), isNull(), eq(13L)))
                .thenReturn(CompletableFuture.completedFuture(List.of(firstEntry, multiBatchEntry)));
        attachReaderPartitionLog(state, tp, logInstance);

        UrsaLakestreamReader reader = new UrsaLakestreamReader(state);

        FetchPartitionData response = reader.fetch(
                createFetchParams(), Map.of(tp, fetchPartitionData(10L))).get().get(tp);

        assertNotNull(response);
        assertEquals(Errors.NONE, response.error);
        assertEquals(13L, response.highWatermark);
        assertEquals(10L, response.logStartOffset);
        List<Long> offsets = new ArrayList<>();
        List<Long> timestamps = new ArrayList<>();
        for (Record record : response.records.records()) {
            offsets.add(record.offset());
            timestamps.add(record.timestamp());
        }
        assertEquals(List.of(10L, 11L, 12L), offsets);
        assertEquals(List.of(1000L, 1200L, 1500L), timestamps);
        verify(firstEntry).close();
        verify(multiBatchEntry).close();
    }

    @Test
    void testCancelledFetchStillClosesLateEntries() throws Exception {
        TopicIdPartition tp = createTestPartition();
        UrsaStorageState state = mock(UrsaStorageState.class);
        Log logInstance = mock(Log.class);
        LogCursor cursor = mock(LogCursor.class);
        CompletableFuture<List<LogEntry>> readFuture = new CompletableFuture<>();
        when(logInstance.getFirstOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(0L, 1)));
        when(logInstance.getLastOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(100L, 1)));
        when(logInstance.openEphemeralCursor(anyString(), eq(0L)))
                .thenReturn(CompletableFuture.completedFuture(cursor));
        when(cursor.readEntries(anyInt(), anyLong(), isNull(), eq(101L))).thenReturn(readFuture);
        UrsaPartitionLog partitionLog = attachReaderPartitionLog(state, tp, logInstance);

        try {
            CompletableFuture<FetchPartitionData> fetchFuture = partitionLog.fetch(fetchPartitionData(0L));
            assertTrue(fetchFuture.cancel(false));

            LogEntry lateEntry = createKafkaRecordsEntry(0L, new long[]{1000L});
            readFuture.complete(List.of(lateEntry));

            verify(lateEntry, timeout(5_000)).close();
        } finally {
            partitionLog.close(false);
        }
    }

    @Test
    void testFetchConversionFailureClosesEntireEntryBatch() throws Exception {
        TopicIdPartition tp = createTestPartition();
        UrsaStorageState state = mock(UrsaStorageState.class);
        Log logInstance = mock(Log.class);
        LogCursor cursor = mock(LogCursor.class);
        when(logInstance.getFirstOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(0L, 1)));
        when(logInstance.getLastOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(1L, 1)));
        when(logInstance.openEphemeralCursor(anyString(), eq(0L)))
                .thenReturn(CompletableFuture.completedFuture(cursor));
        LogEntry failingEntry = mock(LogEntry.class);
        when(failingEntry.payload()).thenThrow(new IllegalStateException("payload unavailable"));
        LogEntry unvisitedEntry = createKafkaRecordsEntry(1L, new long[]{1200L});
        when(cursor.readEntries(anyInt(), anyLong(), isNull(), eq(2L)))
                .thenReturn(CompletableFuture.completedFuture(List.of(failingEntry, unvisitedEntry)));
        attachReaderPartitionLog(state, tp, logInstance);

        UrsaLakestreamReader reader = new UrsaLakestreamReader(state);
        FetchPartitionData response = reader.fetch(
                createFetchParams(), Map.of(tp, fetchPartitionData(0L))).get().get(tp);

        assertEquals(Errors.KAFKA_STORAGE_ERROR, response.error);
        verify(failingEntry).close();
        verify(unvisitedEntry).close();
    }

    @Test
    void testFetchInvalidatesPartitionLogWhenLogIsMissing() throws Exception {
        TopicIdPartition tp = createTestPartition();
        UrsaStorageState state = mock(UrsaStorageState.class);
        Log logInstance = mock(Log.class);
        when(logInstance.getFirstOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(0L, 1)));
        when(logInstance.getLastOffset()).thenReturn(
                CompletableFuture.failedFuture(new NoSuchStreamException(
                        StreamIdentifier.of("default", "missing"))));
        UrsaPartitionLog partitionLog = attachReaderPartitionLog(state, tp, logInstance);

        UrsaLakestreamReader reader = new UrsaLakestreamReader(state);
        FetchPartitionData response = reader.fetch(
                createFetchParams(), Map.of(tp, fetchPartitionData(0L))).get().get(tp);

        assertNotNull(response);
        assertEquals(Errors.UNKNOWN_TOPIC_OR_PARTITION, response.error);
        verify(state, atLeastOnce()).removePartitionLog(eq(tp), same(partitionLog));
    }

    @Test
    void testUnrelatedNotFoundFailureDoesNotInvalidatePartitionLog() throws Exception {
        TopicIdPartition tp = createTestPartition();
        UrsaStorageState state = mock(UrsaStorageState.class);
        Log logInstance = mock(Log.class);
        when(logInstance.getFirstOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(0L, 1)));
        when(logInstance.getLastOffset()).thenReturn(
                CompletableFuture.failedFuture(new ObjectNotFoundException("object missing")));
        UrsaPartitionLog partitionLog = attachReaderPartitionLog(state, tp, logInstance);

        FetchPartitionData response = new UrsaLakestreamReader(state).fetch(
                createFetchParams(), Map.of(tp, fetchPartitionData(0L))).get().get(tp);

        assertEquals(Errors.KAFKA_STORAGE_ERROR, response.error);
        verify(state, never()).removePartitionLog(eq(tp), same(partitionLog));
    }

    @Test
    void testSynchronousCursorReadFailureReleasesTheCursorForReuse() throws Exception {
        TopicIdPartition tp = createTestPartition();
        UrsaStorageState state = mock(UrsaStorageState.class);
        Log logInstance = mock(Log.class);
        LogCursor cursor = mock(LogCursor.class);
        when(logInstance.getFirstOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(0L, 1)));
        when(logInstance.getLastOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(10L, 1)));
        when(logInstance.openEphemeralCursor(anyString(), eq(0L)))
                .thenReturn(CompletableFuture.completedFuture(cursor));
        when(cursor.readOffset()).thenReturn(0L);
        when(cursor.readEntries(anyInt(), anyLong(), isNull(), eq(11L)))
                .thenThrow(new RuntimeException("synchronous read failure"))
                .thenReturn(CompletableFuture.completedFuture(List.of()));
        attachReaderPartitionLog(state, tp, logInstance);

        UrsaLakestreamReader reader = new UrsaLakestreamReader(state);

        FetchPartitionData failedResponse = reader.fetch(
                createFetchParams(), Map.of(tp, fetchPartitionData(0L))).get().get(tp);
        FetchPartitionData retryResponse = reader.fetch(
                createFetchParams(), Map.of(tp, fetchPartitionData(0L))).get().get(tp);

        assertEquals(Errors.KAFKA_STORAGE_ERROR, failedResponse.error);
        assertEquals(Errors.NONE, retryResponse.error);
        verify(logInstance, times(1)).openEphemeralCursor(anyString(), eq(0L));
        verify(cursor, times(2)).readEntries(anyInt(), anyLong(), isNull(), eq(11L));
        verify(cursor, never()).seek(anyLong());
    }

    @Test
    void testListOffsetsTimestampSearchUsesKafkaRecordTimestampAndPreciseOffset() throws Exception {
        TopicIdPartition tp = createTestPartition();
        UrsaStorageState state = mock(UrsaStorageState.class);
        Log logInstance = mock(Log.class);
        when(logInstance.getFirstOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(10L, 3)));
        when(logInstance.getLastOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(10L, 3)));
        when(logInstance.binarySearchOffset(eq(10L), eq(10L), any()))
                .thenReturn(CompletableFuture.completedFuture(10L));
        LogEntry entry = createKafkaRecordsEntry(10L, new long[]{1000L, 1200L, 1500L});
        when(logInstance.readEntry(10L)).thenReturn(CompletableFuture.completedFuture(entry));
        attachReaderPartitionLog(state, tp, logInstance);

        UrsaLakestreamReader reader = new UrsaLakestreamReader(state);
        ListOffsetsPartitionRequest request = new ListOffsetsPartitionRequest(tp, 1500L, Optional.empty());

        ListOffsetsPartitionResponse response = reader.listOffsets(Map.of(tp, request)).get().get(tp);

        assertNotNull(response);
        assertEquals(Errors.NONE, response.error());
        assertEquals(12L, response.offset());
        assertEquals(1500L, response.timestamp());
        verify(entry).close();
        verify(logInstance, never()).openEphemeralCursor(anyString(), anyLong());
    }

    @Test
    void testCancelledTimestampSearchStillClosesTheLateEntry() throws Exception {
        TopicIdPartition tp = createTestPartition();
        UrsaStorageState state = mock(UrsaStorageState.class);
        Log logInstance = mock(Log.class);
        CompletableFuture<LogEntry> entryFuture = new CompletableFuture<>();
        when(logInstance.getFirstOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(10L, 1)));
        when(logInstance.getLastOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(10L, 1)));
        when(logInstance.binarySearchOffset(eq(10L), eq(10L), any()))
                .thenReturn(CompletableFuture.completedFuture(10L));
        when(logInstance.readEntry(10L)).thenReturn(entryFuture);
        UrsaPartitionLog partitionLog = attachReaderPartitionLog(state, tp, logInstance);

        CompletableFuture<ListOffsetsPartitionResponse> responseFuture = partitionLog.listOffsets(
                new ListOffsetsPartitionRequest(tp, 1000L, Optional.empty()));
        assertTrue(responseFuture.cancel(false));

        LogEntry lateEntry = createKafkaRecordsEntry(10L, new long[]{1000L});
        entryFuture.complete(lateEntry);

        verify(lateEntry, timeout(5_000)).close();
    }

    @Test
    void testPartitionCloseClosesTheFetchCursorAndLog() throws Exception {
        TopicIdPartition tp = createTestPartition();
        UrsaStorageState state = mock(UrsaStorageState.class);
        Log logInstance = mock(Log.class);
        LogCursor cursor = mock(LogCursor.class);
        when(logInstance.getFirstOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(0L, 1)));
        when(logInstance.getLastOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(0L, 1)));
        when(logInstance.openEphemeralCursor(anyString(), eq(0L)))
                .thenReturn(CompletableFuture.completedFuture(cursor));
        when(cursor.readEntries(anyInt(), anyLong(), isNull(), eq(1L)))
                .thenReturn(CompletableFuture.completedFuture(List.of()));
        UrsaPartitionLog partitionLog = attachReaderPartitionLog(state, tp, logInstance);

        partitionLog.fetch(fetchPartitionData(0L)).get(5, TimeUnit.SECONDS);
        verify(cursor, never()).close();

        partitionLog.close(false).get(5, TimeUnit.SECONDS);

        verify(cursor).close();
        verify(logInstance).closeAsync();
    }

    @Test
    void testTimestampSearchIndexFailureMapsToStorageError() throws Exception {
        TopicIdPartition tp = createTestPartition();
        UrsaStorageState state = mock(UrsaStorageState.class);
        Log logInstance = mock(Log.class);
        when(logInstance.getFirstOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(10L, 5)));
        when(logInstance.getLastOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(10L, 5)));
        when(logInstance.binarySearchOffset(eq(10L), eq(10L), any()))
                .thenThrow(new RuntimeException("synchronous index lookup failure"));
        attachReaderPartitionLog(state, tp, logInstance);

        UrsaLakestreamReader reader = new UrsaLakestreamReader(state);
        ListOffsetsPartitionRequest request = new ListOffsetsPartitionRequest(tp, 1500L, Optional.empty());

        ListOffsetsPartitionResponse response = reader.listOffsets(Map.of(tp, request)).get().get(tp);

        assertEquals(Errors.KAFKA_STORAGE_ERROR, response.error());
        verify(logInstance, never()).readEntry(anyLong());
    }

    @Test
    void testListOffsetsMaxTimestampAnswersFromLastEntryHeader() throws Exception {
        TopicIdPartition tp = createTestPartition();
        UrsaStorageState state = mock(UrsaStorageState.class);
        Log logInstance = mock(Log.class);
        when(logInstance.getFirstOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(10L, 2)));
        when(logInstance.getLastOffset()).thenReturn(
                CompletableFuture.completedFuture(new LogOffset(12L, 3, 3000L)));
        attachReaderPartitionLog(state, tp, logInstance);

        UrsaLakestreamReader reader = new UrsaLakestreamReader(state);
        ListOffsetsPartitionRequest request = new ListOffsetsPartitionRequest(
                tp, ListOffsetsPartitionRequest.MAX_TIMESTAMP, Optional.empty());

        ListOffsetsPartitionResponse response = reader.listOffsets(Map.of(tp, request)).get().get(tp);

        assertNotNull(response);
        assertEquals(Errors.NONE, response.error());
        assertEquals(12L, response.offset());
        assertEquals(3000L, response.timestamp());
        verify(logInstance, never()).getEntryMetadataRange(anyLong(), anyLong());
        verify(logInstance, never()).readEntry(anyLong());
        verify(logInstance, never()).openEphemeralCursor(anyString(), anyLong());
    }

    @Test
    void testMaxTimestampRangeFailureMapsToStorageError() throws Exception {
        TopicIdPartition tp = createTestPartition();
        UrsaStorageState state = mock(UrsaStorageState.class);
        Log logInstance = mock(Log.class);
        when(logInstance.getFirstOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(10L, 5)));
        when(logInstance.getLastOffset()).thenReturn(
                CompletableFuture.failedFuture(new RuntimeException("last offset lookup failure")));
        attachReaderPartitionLog(state, tp, logInstance);

        UrsaLakestreamReader reader = new UrsaLakestreamReader(state);
        ListOffsetsPartitionRequest request = new ListOffsetsPartitionRequest(
                tp, ListOffsetsPartitionRequest.MAX_TIMESTAMP, Optional.empty());

        ListOffsetsPartitionResponse response = reader.listOffsets(Map.of(tp, request)).get().get(tp);

        assertEquals(Errors.KAFKA_STORAGE_ERROR, response.error());
    }

    @Test
    void testRequestSchedulesOneTimeoutForEveryPartitionItWaitsOn() throws Exception {
        UrsaStorageState state = mock(UrsaStorageState.class);
        Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos = new LinkedHashMap<>();
        List<UrsaPartitionLog> partitionLogs = new ArrayList<>();
        for (int partition = 0; partition < 100; partition++) {
            TopicIdPartition tp = new TopicIdPartition(
                    Uuid.randomUuid(), new TopicPartition("many-partitions-topic", partition));
            partitionLogs.add(attachReaderPartitionLog(state, tp, emptyLog()));
            fetchInfos.put(tp, fetchPartitionData(0L));
        }

        UrsaLakestreamReader reader = new UrsaLakestreamReader(state);
        try {
            CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> responses =
                    reader.fetch(createFetchParams(30_000L), fetchInfos);

            assertFalse(responses.isDone(), "An empty caught-up request must wait for an append");
            assertEquals(1, timer.getQueue().size(),
                    "The whole request shares one deadline, however many partitions it holds");
        } finally {
            partitionLogs.forEach(partitionLog -> partitionLog.close(false));
        }
    }

    @Test
    void testRequestIsAnsweredWhenItsSharedDeadlineFires() throws Exception {
        TopicIdPartition first = createTestPartition();
        TopicIdPartition second = createTestPartition();
        UrsaStorageState state = mock(UrsaStorageState.class);
        attachReaderPartitionLog(state, first, emptyLog());
        attachReaderPartitionLog(state, second, emptyLog());

        Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos = new LinkedHashMap<>();
        fetchInfos.put(first, fetchPartitionData(0L));
        fetchInfos.put(second, fetchPartitionData(0L));

        Map<TopicIdPartition, FetchPartitionData> responses =
                new UrsaLakestreamReader(state).fetch(createFetchParams(50L), fetchInfos)
                        .get(5, TimeUnit.SECONDS);

        assertEquals(List.of(first, second), new ArrayList<>(responses.keySet()));
        assertEquals(0, responses.get(first).records.sizeInBytes());
        assertEquals(0, responses.get(second).records.sizeInBytes());
        assertTrue(timer.getQueue().isEmpty(), "The long-poll timeout must be cancelled with the request");
    }

    @Test
    void testFetchPartitionThatCannotBeResolvedFailsOnItsOwn() throws Exception {
        TopicIdPartition healthy = createTestPartition();
        TopicIdPartition deleted = createTestPartition();
        TopicIdPartition closing = createTestPartition();
        UrsaStorageState state = mock(UrsaStorageState.class);
        Log logInstance = mock(Log.class);
        LogCursor cursor = mock(LogCursor.class);
        when(logInstance.getFirstOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(0L, 1)));
        when(logInstance.getLastOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(0L, 1)));
        when(logInstance.openEphemeralCursor(anyString(), eq(0L)))
                .thenReturn(CompletableFuture.completedFuture(cursor));
        when(cursor.readEntries(anyInt(), anyLong(), isNull(), eq(1L))).thenAnswer(
                invocation -> CompletableFuture.completedFuture(
                        List.of(createKafkaRecordsEntry(0L, new long[]{1000L}))));
        attachReaderPartitionLog(state, healthy, logInstance);
        when(state.getOrCreatePartitionLog(deleted))
                .thenThrow(new NotLeaderOrFollowerException("Topic was deleted"));
        when(state.getOrCreatePartitionLog(closing))
                .thenThrow(new IllegalStateException("Ursa storage state is closed"));

        Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos = new LinkedHashMap<>();
        fetchInfos.put(healthy, fetchPartitionData(0L));
        fetchInfos.put(deleted, fetchPartitionData(0L));
        fetchInfos.put(closing, fetchPartitionData(0L));

        Map<TopicIdPartition, FetchPartitionData> responses =
                new UrsaLakestreamReader(state).fetch(createFetchParams(30_000L), fetchInfos)
                        .get(5, TimeUnit.SECONDS);

        assertEquals(List.of(healthy, deleted, closing), new ArrayList<>(responses.keySet()));
        assertEquals(Errors.NONE, responses.get(healthy).error);
        assertEquals(1, countRecords(responses.get(healthy).records));
        assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, responses.get(deleted).error);
        assertEquals(Errors.KAFKA_STORAGE_ERROR, responses.get(closing).error);
        assertTrue(timer.getQueue().isEmpty(), "Long-poll timeouts must be cancelled with the request");
    }

    @Test
    void testListOffsetsPartitionThatCannotBeResolvedFailsOnItsOwn() throws Exception {
        TopicIdPartition healthy = createTestPartition();
        TopicIdPartition deleted = createTestPartition();
        UrsaStorageState state = mock(UrsaStorageState.class);
        Log logInstance = mock(Log.class);
        when(logInstance.getFirstOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(5L, 1)));
        when(logInstance.getLastOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(10L, 5)));
        attachReaderPartitionLog(state, healthy, logInstance);
        when(state.getOrCreatePartitionLog(deleted))
                .thenThrow(new NotLeaderOrFollowerException("Topic was deleted"));

        Map<TopicIdPartition, ListOffsetsPartitionRequest> requests = new LinkedHashMap<>();
        requests.put(healthy, new ListOffsetsPartitionRequest(
                healthy, ListOffsetsPartitionRequest.EARLIEST_TIMESTAMP, Optional.empty()));
        requests.put(deleted, new ListOffsetsPartitionRequest(
                deleted, ListOffsetsPartitionRequest.EARLIEST_TIMESTAMP, Optional.empty()));

        Map<TopicIdPartition, ListOffsetsPartitionResponse> responses =
                new UrsaLakestreamReader(state).listOffsets(requests).get(5, TimeUnit.SECONDS);

        assertEquals(List.of(healthy, deleted), new ArrayList<>(responses.keySet()));
        assertEquals(Errors.NONE, responses.get(healthy).error());
        assertEquals(5L, responses.get(healthy).offset());
        assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, responses.get(deleted).error());
    }

    private static FetchParams createFetchParams() {
        return createFetchParams(500L);
    }

    private static FetchParams createFetchParams(long maxWaitMs) {
        return new FetchParams(
                -1,
                -1,
                maxWaitMs,
                1,
                1024 * 1024,
                FetchIsolation.HIGH_WATERMARK,
                Optional.empty()
        );
    }

    private static FetchRequest.PartitionData fetchPartitionData(long fetchOffset) {
        return new FetchRequest.PartitionData(
                Uuid.ZERO_UUID, fetchOffset, 0, 1024 * 1024, Optional.empty());
    }

    private static TopicIdPartition createTestPartition() {
        return new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("test-topic", 0));
    }

    private static LogOffset logOffset(long offset, int numberOfRecords) {
        return new LogOffset(offset, numberOfRecords, -1L);
    }

    private static LogEntry createKafkaRecordsEntry(long baseOffset, long[]... timestampBatches) {
        byte[] key = "key".getBytes(StandardCharsets.UTF_8);
        byte[] value = "value".getBytes(StandardCharsets.UTF_8);
        List<MemoryRecords> batches = new ArrayList<>(timestampBatches.length);
        int numberOfRecords = 0;
        int payloadSize = 0;
        for (long[] timestamps : timestampBatches) {
            SimpleRecord[] records = new SimpleRecord[timestamps.length];
            for (int i = 0; i < timestamps.length; i++) {
                records[i] = new SimpleRecord(timestamps[i], key, value);
            }
            MemoryRecords batch = MemoryRecords.withRecords(Compression.NONE, records);
            batches.add(batch);
            numberOfRecords = Math.addExact(numberOfRecords, timestamps.length);
            payloadSize = Math.addExact(payloadSize, batch.buffer().remaining());
        }

        ByteBuf payload = Unpooled.buffer(payloadSize);
        for (MemoryRecords batch : batches) {
            payload.writeBytes(batch.buffer().duplicate());
        }

        LogEntry entry = mock(LogEntry.class);
        AtomicBoolean closed = new AtomicBoolean();
        when(entry.offset()).thenReturn(baseOffset);
        when(entry.numberOfRecords()).thenReturn(numberOfRecords);
        when(entry.size()).thenReturn(payload.readableBytes());
        when(entry.payload()).thenReturn(payload.asReadOnly());
        doAnswer(invocation -> {
            if (closed.compareAndSet(false, true)) {
                payload.release();
            }
            return null;
        }).when(entry).close();
        return entry;
    }

    @Test
    void testRequestWithDataOnOnePartitionAnswersWithoutWaiting() throws Exception {
        TopicIdPartition withData = createTestPartition();
        TopicIdPartition caughtUp = createTestPartition();
        UrsaStorageState state = mock(UrsaStorageState.class);

        Log logWithData = mock(Log.class);
        LogCursor cursor = mock(LogCursor.class);
        when(logWithData.getFirstOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(0L, 1)));
        when(logWithData.getLastOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(0L, 1)));
        when(logWithData.openEphemeralCursor(anyString(), eq(0L)))
                .thenReturn(CompletableFuture.completedFuture(cursor));
        when(cursor.readEntries(anyInt(), anyLong(), isNull(), eq(1L))).thenAnswer(
                invocation -> CompletableFuture.completedFuture(
                        List.of(createKafkaRecordsEntry(0L, new long[]{1000L}))));
        attachReaderPartitionLog(state, withData, logWithData);
        attachReaderPartitionLog(state, caughtUp, emptyLog());

        UrsaLakestreamReader reader = new UrsaLakestreamReader(state);
        Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos = new LinkedHashMap<>();
        fetchInfos.put(withData, fetchPartitionData(0L));
        fetchInfos.put(caughtUp, fetchPartitionData(0L));

        // Every mocked storage future is already complete, so the request resolves inline.
        CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> responses =
                reader.fetch(createFetchParams(30_000L), fetchInfos);

        assertTrue(responses.isDone(), "A request that already has records must not wait");
        Map<TopicIdPartition, FetchPartitionData> result = responses.get(5, TimeUnit.SECONDS);
        assertEquals(List.of(withData, caughtUp), new ArrayList<>(result.keySet()));
        assertEquals(1, countRecords(result.get(withData).records));
        assertEquals(0, result.get(caughtUp).records.sizeInBytes());
        assertTrue(timer.getQueue().isEmpty(), "Long-poll timeouts must be cancelled with the request");
    }

    @Test
    void testRequestWaitsUntilAnAppendLandsOnAnyPartition() throws Exception {
        TopicIdPartition idle = createTestPartition();
        TopicIdPartition appended = createTestPartition();
        UrsaStorageState state = mock(UrsaStorageState.class);
        attachReaderPartitionLog(state, idle, emptyLog());
        Log appendedLog = growingLog();
        UrsaPartitionLog appendedPartitionLog = attachReaderPartitionLog(state, appended, appendedLog);

        UrsaLakestreamReader reader = new UrsaLakestreamReader(state);
        Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos = new LinkedHashMap<>();
        fetchInfos.put(idle, fetchPartitionData(0L));
        fetchInfos.put(appended, fetchPartitionData(0L));

        try {
            CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> responses =
                    reader.fetch(createFetchParams(30_000L), fetchInfos);
            assertFalse(responses.isDone(), "An empty caught-up request must wait for an append");
            assertEquals(1, timer.getQueue().size(), "A request schedules one long-poll timeout");

            PartitionResponse write = appendedPartitionLog.write(
                    MemoryRecords.withRecords(
                            Compression.NONE,
                            new SimpleRecord("a".getBytes(StandardCharsets.UTF_8))),
                    DisklessClientZone.NO_ZONE,
                    "test").get(5, TimeUnit.SECONDS);
            assertEquals(Errors.NONE, write.error);

            Map<TopicIdPartition, FetchPartitionData> result = responses.get(5, TimeUnit.SECONDS);
            assertEquals(List.of(idle, appended), new ArrayList<>(result.keySet()));
            assertEquals(0, result.get(idle).records.sizeInBytes());
            assertEquals(1, countRecords(result.get(appended).records));
            assertEquals(1L, result.get(appended).highWatermark);
            assertTrue(timer.getQueue().isEmpty(), "Long-poll timeouts must be cancelled with the request");
        } finally {
            appendedPartitionLog.close(false);
        }
    }

    @Test
    void testAppendDuringTheFirstReadStillWakesTheRequest() throws Exception {
        TopicIdPartition tp = createTestPartition();
        UrsaStorageState state = mock(UrsaStorageState.class);
        CompletableFuture<LogOffset> gatedLastOffset = new CompletableFuture<>();
        AtomicInteger lastOffsetCalls = new AtomicInteger();
        // The first read hangs on its high-watermark lookup, so the append below lands after the
        // waiter was registered but before the read can report an empty log.
        Log logInstance = mock(Log.class);
        AtomicReference<LogOffset> current = logState(logInstance);
        when(logInstance.getLastOffset()).thenAnswer(invocation -> lastOffsetCalls.getAndIncrement() == 0
                ? gatedLastOffset
                : CompletableFuture.completedFuture(current.get()));
        UrsaPartitionLog partitionLog = attachReaderPartitionLog(state, tp, logInstance);

        UrsaLakestreamReader reader = new UrsaLakestreamReader(state);
        try {
            CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> responses =
                    reader.fetch(createFetchParams(30_000L), Map.of(tp, fetchPartitionData(0L)));
            assertFalse(responses.isDone());

            PartitionResponse write = partitionLog.write(
                    MemoryRecords.withRecords(
                            Compression.NONE,
                            new SimpleRecord("a".getBytes(StandardCharsets.UTF_8))),
                    DisklessClientZone.NO_ZONE,
                    "test").get(5, TimeUnit.SECONDS);
            assertEquals(Errors.NONE, write.error);

            // The first read only now reports the log as it was before the append.
            gatedLastOffset.complete(logOffset(-1L, 0));

            // A missed notification would park this request for the full 30s wait instead.
            FetchPartitionData response = responses.get(5, TimeUnit.SECONDS).get(tp);
            assertEquals(Errors.NONE, response.error);
            assertEquals(1, countRecords(response.records));
            assertTrue(timer.getQueue().isEmpty(), "Long-poll timeouts must be cancelled with the request");
        } finally {
            partitionLog.close(false);
        }
    }

    /** A log that stays empty: first and last offset both report "nothing written yet". */
    private static Log emptyLog() {
        Log logInstance = mock(Log.class);
        when(logInstance.getFirstOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(-1L, 0)));
        when(logInstance.getLastOffset()).thenReturn(
                CompletableFuture.completedFuture(logOffset(-1L, 0)));
        return logInstance;
    }

    /** An empty log whose single append publishes one record at offset 0. */
    private static Log growingLog() {
        Log logInstance = mock(Log.class);
        logState(logInstance);
        return logInstance;
    }

    private static AtomicReference<LogOffset> logState(Log logInstance) {
        AtomicReference<LogOffset> current = new AtomicReference<>(logOffset(-1L, 0));
        LogCursor cursor = mock(LogCursor.class);
        when(logInstance.getFirstOffset()).thenAnswer(
                invocation -> CompletableFuture.completedFuture(current.get()));
        when(logInstance.getLastOffset()).thenAnswer(
                invocation -> CompletableFuture.completedFuture(current.get()));
        when(logInstance.openEphemeralCursor(anyString(), eq(0L)))
                .thenReturn(CompletableFuture.completedFuture(cursor));
        when(cursor.readEntries(anyInt(), anyLong(), isNull(), eq(1L))).thenAnswer(
                invocation -> CompletableFuture.completedFuture(
                        List.of(createKafkaRecordsEntry(0L, new long[]{1000L}))));
        when(logInstance.append(anyInt(), any(ByteBuf.class))).thenAnswer(invocation -> {
            current.set(logOffset(0L, 1));
            LogEntryHeader header = mock(LogEntryHeader.class);
            when(header.offset()).thenReturn(0L);
            return CompletableFuture.completedFuture(header);
        });
        return current;
    }

    private static int countRecords(Records records) {
        int count = 0;
        for (Record record : records.records()) {
            assertNotNull(record);
            count++;
        }
        return count;
    }

    private static UrsaPartitionLog attachReaderPartitionLog(
            UrsaStorageState state,
            TopicIdPartition tp,
            Log logInstance) {
        when(state.time()).thenReturn(Time.SYSTEM);
        when(state.timer()).thenReturn(timer);
        when(state.timestampType(tp.topic())).thenReturn(TimestampType.CREATE_TIME);
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

    private static final class ObjectNotFoundException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        private ObjectNotFoundException(String message) {
            super(message);
        }
    }
}
