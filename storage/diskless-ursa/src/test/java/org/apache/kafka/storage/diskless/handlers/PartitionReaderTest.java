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
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.Records;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.storage.diskless.ListOffsetsPartitionRequest;
import org.apache.kafka.storage.diskless.ListOffsetsPartitionResponse;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.lakestream.api.Log;
import io.lakestream.api.LogCursor;
import io.lakestream.api.LogEntry;
import io.lakestream.api.LogOffset;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PartitionReaderTest {

    private static final TopicIdPartition TP =
            new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("test-topic", 0));
    private static final long OFFSET_RANGE_REFRESH_MS = PartitionReader.OFFSET_RANGE_REFRESH_MS;

    @Test
    void fetchAssemblesEntriesAndReusesCursor() throws Exception {
        Log log = mock(Log.class);
        LogCursor cursor = mock(LogCursor.class);
        when(log.getFirstOffset()).thenReturn(completedFuture(new LogOffset(0L, 1, 1L)));
        when(log.getLastOffset()).thenReturn(completedFuture(new LogOffset(2L, 1, 1L)));   // hwm = 3
        when(log.openEphemeralCursor(anyString(), eq(0L))).thenReturn(completedFuture(cursor));
        // The cursor has consumed offsets 0 and 1 by the time the second fetch acquires it.
        when(cursor.readOffset()).thenReturn(2L);
        List<LogEntry> firstBatch = List.of(entry(0L, records("a")), entry(1L, records("b")));
        List<LogEntry> secondBatch = List.of(entry(2L, records("c")));
        when(cursor.readEntries(eq(10), eq(1024L), isNull(), eq(3L)))
                .thenReturn(completedFuture(firstBatch))
                .thenReturn(completedFuture(secondBatch));

        try (PartitionReader reader = new PartitionReader(TP, log, 10, frozenTime())) {
            FetchPartitionData first = reader.fetch(fetchRequest(0L)).get();
            assertEquals(Errors.NONE, first.error);
            assertEquals(3L, first.highWatermark);
            assertEquals(0L, first.logStartOffset);
            assertEquals(2, count(first.records));

            FetchPartitionData second = reader.fetch(fetchRequest(2L)).get();
            assertEquals(Errors.NONE, second.error);
            assertEquals(1, count(second.records));
            verify(log, times(1)).openEphemeralCursor(anyString(), anyLong());   // cursor reused
            verify(cursor, never()).seek(anyLong());                             // position matched
        }
    }

    @Test
    void fetchOutOfRangeReturnsOffsetOutOfRange() throws Exception {
        Log log = mock(Log.class);
        when(log.getFirstOffset()).thenReturn(completedFuture(new LogOffset(5L, 1, 1L)));
        when(log.getLastOffset()).thenReturn(completedFuture(new LogOffset(9L, 1, 1L)));

        try (PartitionReader reader = new PartitionReader(TP, log, 10, frozenTime())) {
            FetchPartitionData data = reader.fetch(fetchRequest(2L)).get();
            assertEquals(Errors.OFFSET_OUT_OF_RANGE, data.error);
            assertEquals(5L, data.logStartOffset);
            assertEquals(10L, data.highWatermark);
            verify(log, never()).openEphemeralCursor(anyString(), anyLong());
        }
    }

    @Test
    void listOffsetsByTimestampUsesBinarySearchThenDecodesOneEntry() throws Exception {
        Log log = mock(Log.class);
        when(log.getFirstOffset()).thenReturn(completedFuture(new LogOffset(0L, 2, 100L)));
        when(log.getLastOffset()).thenReturn(completedFuture(new LogOffset(4L, 2, 300L)));
        when(log.binarySearchOffset(eq(0L), eq(4L), any())).thenReturn(completedFuture(2L));
        LogEntry found = entry(2L, MemoryRecords.withRecords(Compression.NONE,
                new SimpleRecord(150L, "a".getBytes(StandardCharsets.UTF_8)),
                new SimpleRecord(210L, "b".getBytes(StandardCharsets.UTF_8))));
        when(log.readEntry(2L)).thenReturn(completedFuture(found));

        try (PartitionReader reader = new PartitionReader(TP, log, 10, frozenTime())) {
            ListOffsetsPartitionResponse response = reader.listOffsets(listOffsetsRequest(200L)).get();
            assertEquals(Errors.NONE, response.error());
            assertEquals(3L, response.offset());
            assertEquals(210L, response.timestamp());
            verify(log, never()).openEphemeralCursor(anyString(), anyLong());
            verify(found).close();
        }
    }

    @Test
    void maxTimestampAnswersFromLastEntryHeader() throws Exception {
        Log log = mock(Log.class);
        when(log.getFirstOffset()).thenReturn(completedFuture(new LogOffset(0L, 1, 100L)));
        when(log.getLastOffset()).thenReturn(completedFuture(new LogOffset(2L, 1, 300L)));

        try (PartitionReader reader = new PartitionReader(TP, log, 10, frozenTime())) {
            ListOffsetsPartitionResponse response =
                    reader.listOffsets(listOffsetsRequest(ListOffsetsPartitionRequest.MAX_TIMESTAMP)).get();
            assertEquals(Errors.NONE, response.error());
            assertEquals(2L, response.offset());
            assertEquals(300L, response.timestamp());
            // Entries are appended in write-timestamp order, so no index walk is needed.
            verify(log, never()).getEntryMetadataRange(anyLong(), anyLong());
            verify(log, never()).readEntry(anyLong());
            verify(log, never()).openEphemeralCursor(anyString(), anyLong());
        }
    }

    @Test
    void listOffsetsOnAnEmptyLogReportsNoOffsets() throws Exception {
        Log log = mock(Log.class);
        when(log.getFirstOffset()).thenReturn(completedFuture(LogOffset.NOT_FOUND));
        when(log.getLastOffset()).thenReturn(completedFuture(LogOffset.NOT_FOUND));

        try (PartitionReader reader = new PartitionReader(TP, log, 10, frozenTime())) {
            assertEquals(0L, reader.highWatermark().get());
            assertEquals(0L, reader.listOffsets(
                    listOffsetsRequest(ListOffsetsPartitionRequest.EARLIEST_TIMESTAMP)).get().offset());
            assertEquals(0L, reader.listOffsets(
                    listOffsetsRequest(ListOffsetsPartitionRequest.LATEST_TIMESTAMP)).get().offset());
            assertEquals(-1L, reader.listOffsets(listOffsetsRequest(1234L)).get().offset());
            assertEquals(-1L, reader.listOffsets(
                    listOffsetsRequest(ListOffsetsPartitionRequest.MAX_TIMESTAMP)).get().offset());
            assertEquals(-1L, reader.listOffsets(
                    listOffsetsRequest(ListOffsetsPartitionRequest.LATEST_TIERED_TIMESTAMP)).get().offset());
            verify(log, never()).binarySearchOffset(anyLong(), anyLong(), any());
            verify(log, never()).getEntryMetadataRange(anyLong(), anyLong());
        }
    }

    @Test
    void concurrentFetchUsesAThrowAwayCursorAndClosesIt() throws Exception {
        Log log = mock(Log.class);
        LogCursor firstCursor = mock(LogCursor.class);
        LogCursor secondCursor = mock(LogCursor.class);
        when(log.getFirstOffset()).thenReturn(completedFuture(new LogOffset(0L, 1, 1L)));
        when(log.getLastOffset()).thenReturn(completedFuture(new LogOffset(9L, 1, 1L)));
        when(log.openEphemeralCursor(anyString(), anyLong()))
                .thenReturn(completedFuture(firstCursor))
                .thenReturn(completedFuture(secondCursor));
        CompletableFuture<List<LogEntry>> firstRead = new CompletableFuture<>();
        CompletableFuture<List<LogEntry>> secondRead = new CompletableFuture<>();
        when(firstCursor.readEntries(anyInt(), anyLong(), isNull(), anyLong())).thenReturn(firstRead);
        when(secondCursor.readEntries(anyInt(), anyLong(), isNull(), anyLong())).thenReturn(secondRead);

        try (PartitionReader reader = new PartitionReader(TP, log, 10, frozenTime())) {
            CompletableFuture<FetchPartitionData> firstFetch = reader.fetch(fetchRequest(0L));
            // The second fetch must not queue behind the first: it opens its own throw-away cursor.
            CompletableFuture<FetchPartitionData> secondFetch = reader.fetch(fetchRequest(4L));
            verify(log, times(2)).openEphemeralCursor(anyString(), anyLong());

            secondRead.complete(List.of());
            assertEquals(Errors.NONE, secondFetch.get(5, TimeUnit.SECONDS).error);
            firstRead.complete(List.of());
            assertEquals(Errors.NONE, firstFetch.get(5, TimeUnit.SECONDS).error);

            // Only one cursor is cached; the loser of the race is closed right away.
            verify(secondCursor, never()).close();
            verify(firstCursor).close();
        }
        verify(secondCursor).close();
    }

    @Test
    void closeClosesTheCachedCursor() throws Exception {
        Log log = mock(Log.class);
        LogCursor cursor = mock(LogCursor.class);
        when(log.getFirstOffset()).thenReturn(completedFuture(new LogOffset(0L, 1, 1L)));
        when(log.getLastOffset()).thenReturn(completedFuture(new LogOffset(0L, 1, 1L)));
        when(log.openEphemeralCursor(anyString(), eq(0L))).thenReturn(completedFuture(cursor));
        when(cursor.readEntries(anyInt(), anyLong(), isNull(), eq(1L)))
                .thenReturn(completedFuture(List.of()));

        PartitionReader reader = new PartitionReader(TP, log, 10, frozenTime());
        reader.fetch(fetchRequest(0L)).get();
        verify(cursor, never()).close();

        reader.close();
        verify(cursor).close();

        reader.close();
        verify(cursor, times(1)).close();
    }

    @Test
    void fetchClosesEveryEntryWhenAssemblyFails() {
        Log log = mock(Log.class);
        LogCursor cursor = mock(LogCursor.class);
        when(log.getFirstOffset()).thenReturn(completedFuture(new LogOffset(0L, 1, 1L)));
        when(log.getLastOffset()).thenReturn(completedFuture(new LogOffset(1L, 1, 1L)));
        when(log.openEphemeralCursor(anyString(), eq(0L))).thenReturn(completedFuture(cursor));
        LogEntry failing = mock(LogEntry.class);
        when(failing.payload()).thenThrow(new IllegalStateException("payload unavailable"));
        LogEntry unvisited = entry(1L, records("b"));
        when(cursor.readEntries(anyInt(), anyLong(), isNull(), eq(2L)))
                .thenReturn(completedFuture(List.of(failing, unvisited)));

        try (PartitionReader reader = new PartitionReader(TP, log, 10, frozenTime())) {
            ExecutionException error = assertThrows(
                    ExecutionException.class, () -> reader.fetch(fetchRequest(0L)).get());
            assertInstanceOf(IllegalStateException.class, error.getCause());
            verify(failing).close();
            verify(unvisited).close();
        }
    }

    @Test
    void fetchReleasesTheCursorWhenTheReadThrowsSynchronously() throws Exception {
        Log log = mock(Log.class);
        LogCursor cursor = mock(LogCursor.class);
        when(log.getFirstOffset()).thenReturn(completedFuture(new LogOffset(0L, 1, 1L)));
        when(log.getLastOffset()).thenReturn(completedFuture(new LogOffset(9L, 1, 1L)));
        when(log.openEphemeralCursor(anyString(), eq(0L))).thenReturn(completedFuture(cursor));
        when(cursor.readOffset()).thenReturn(0L);
        when(cursor.readEntries(anyInt(), anyLong(), isNull(), eq(10L)))
                .thenThrow(new RuntimeException("synchronous read failure"))
                .thenReturn(completedFuture(List.of()));

        try (PartitionReader reader = new PartitionReader(TP, log, 10, frozenTime())) {
            assertThrows(ExecutionException.class, () -> reader.fetch(fetchRequest(0L)).get());
            // The failed read must not leak the cursor: the retry reuses it instead of opening another.
            assertEquals(Errors.NONE, reader.fetch(fetchRequest(0L)).get().error);
            verify(log, times(1)).openEphemeralCursor(anyString(), anyLong());
        }
    }

    @Test
    void timestampSearchFallsBackToTheFollowingEntryOnce() throws Exception {
        Log log = mock(Log.class);
        when(log.getFirstOffset()).thenReturn(completedFuture(new LogOffset(0L, 2, 100L)));
        when(log.getLastOffset()).thenReturn(completedFuture(new LogOffset(2L, 2, 300L)));
        when(log.binarySearchOffset(eq(0L), eq(2L), any())).thenReturn(completedFuture(0L));
        LogEntry stale = entry(0L, MemoryRecords.withRecords(Compression.NONE,
                new SimpleRecord(100L, "a".getBytes(StandardCharsets.UTF_8)),
                new SimpleRecord(110L, "b".getBytes(StandardCharsets.UTF_8))));
        LogEntry match = entry(2L, MemoryRecords.withRecords(Compression.NONE,
                new SimpleRecord(150L, "c".getBytes(StandardCharsets.UTF_8)),
                new SimpleRecord(260L, "d".getBytes(StandardCharsets.UTF_8))));
        when(log.readEntry(0L)).thenReturn(completedFuture(stale));
        when(log.readEntry(2L)).thenReturn(completedFuture(match));

        try (PartitionReader reader = new PartitionReader(TP, log, 10, frozenTime())) {
            ListOffsetsPartitionResponse response = reader.listOffsets(listOffsetsRequest(120L)).get();
            assertEquals(2L, response.offset());
            assertEquals(150L, response.timestamp());
            verify(stale).close();
            verify(match).close();
            verify(log, times(2)).readEntry(anyLong());
        }
    }

    @Test
    void timestampSearchReportsNoOffsetOnlyAfterTheRangeIsExhausted() throws Exception {
        Log log = mock(Log.class);
        when(log.getFirstOffset()).thenReturn(completedFuture(new LogOffset(0L, 1, 100L)));
        when(log.getLastOffset()).thenReturn(completedFuture(new LogOffset(2L, 1, 300L)));
        when(log.binarySearchOffset(eq(0L), eq(2L), any())).thenReturn(completedFuture(0L));
        LogEntry first = entry(0L, records("a"));
        LogEntry second = entry(1L, records("b"));
        LogEntry third = entry(2L, records("c"));
        when(log.readEntry(0L)).thenReturn(completedFuture(first));
        when(log.readEntry(1L)).thenReturn(completedFuture(second));
        when(log.readEntry(2L)).thenReturn(completedFuture(third));

        try (PartitionReader reader = new PartitionReader(TP, log, 10, frozenTime())) {
            ListOffsetsPartitionResponse response = reader.listOffsets(listOffsetsRequest(Long.MAX_VALUE)).get();
            assertEquals(-1L, response.offset());
            assertEquals(-1L, response.timestamp());
            verify(first).close();
            verify(second).close();
            verify(third).close();
            verify(log, times(3)).readEntry(anyLong());
        }
    }

    @Test
    void timestampSearchFindsAMatchSeveralEntriesPastTheBinarySearch() throws Exception {
        // A header carries the entry's write time and a record its create time, so a write path
        // running behind the clock leaves a run of entries whose headers are already past the
        // target while their records are not. Giving up after the entry that follows the binary
        // search reported no offset for a timestamp the log does hold.
        Log log = mock(Log.class);
        when(log.getFirstOffset()).thenReturn(completedFuture(new LogOffset(0L, 1, 100L)));
        when(log.getLastOffset()).thenReturn(completedFuture(new LogOffset(3L, 1, 400L)));
        when(log.binarySearchOffset(eq(0L), eq(3L), any())).thenReturn(completedFuture(0L));
        LogEntry stale = entry(0L, recordAt(100L));
        LogEntry alsoStale = entry(1L, recordAt(150L));
        LogEntry stillStale = entry(2L, recordAt(200L));
        LogEntry match = entry(3L, recordAt(260L));
        when(log.readEntry(0L)).thenReturn(completedFuture(stale));
        when(log.readEntry(1L)).thenReturn(completedFuture(alsoStale));
        when(log.readEntry(2L)).thenReturn(completedFuture(stillStale));
        when(log.readEntry(3L)).thenReturn(completedFuture(match));

        try (PartitionReader reader = new PartitionReader(TP, log, 10, frozenTime())) {
            ListOffsetsPartitionResponse response = reader.listOffsets(listOffsetsRequest(250L)).get();
            assertEquals(3L, response.offset());
            assertEquals(260L, response.timestamp());
        }
    }

    @Test
    void timestampSearchStopsAtTheScanBoundAndAnswersWithTheFirstEntryPastTheTarget() throws Exception {
        // Every entry after the first was written past the target while its records still lag far
        // behind it, so the exact record sits beyond the bound. The answer is then the earliest
        // entry written at or after the target: at or before the true offset, never past it.
        Log log = mock(Log.class);
        when(log.getFirstOffset()).thenReturn(completedFuture(new LogOffset(0L, 1, 100L)));
        when(log.getLastOffset()).thenReturn(completedFuture(new LogOffset(299L, 1, 6_000L)));
        when(log.binarySearchOffset(eq(0L), eq(299L), any())).thenReturn(completedFuture(0L));
        for (long offset = 0; offset < 300; offset++) {
            long headerTimestamp = offset == 0 ? 100L : 5_000L + offset;
            LogEntry scanned = entry(offset, recordAt(10L), headerTimestamp);
            when(log.readEntry(offset)).thenReturn(completedFuture(scanned));
        }

        try (PartitionReader reader = new PartitionReader(TP, log, 10, frozenTime())) {
            ListOffsetsPartitionResponse response = reader.listOffsets(listOffsetsRequest(1_000L)).get();
            assertEquals(1L, response.offset());
            assertEquals(5_001L, response.timestamp());
            verify(log, times(256)).readEntry(anyLong());
        }
    }

    @Test
    void timestampSearchReportsNoOffsetWhenTheBoundIsReachedWithNothingPastTheTarget() throws Exception {
        // No scanned entry was even written at or after the target, so there is nothing to answer
        // with: reporting an offset here would name one the caller never asked for.
        Log log = mock(Log.class);
        when(log.getFirstOffset()).thenReturn(completedFuture(new LogOffset(0L, 1, 100L)));
        when(log.getLastOffset()).thenReturn(completedFuture(new LogOffset(299L, 1, 400L)));
        when(log.binarySearchOffset(eq(0L), eq(299L), any())).thenReturn(completedFuture(0L));
        for (long offset = 0; offset < 300; offset++) {
            LogEntry scanned = entry(offset, recordAt(10L), 100L);
            when(log.readEntry(offset)).thenReturn(completedFuture(scanned));
        }

        try (PartitionReader reader = new PartitionReader(TP, log, 10, frozenTime())) {
            ListOffsetsPartitionResponse response = reader.listOffsets(listOffsetsRequest(1_000L)).get();
            assertEquals(-1L, response.offset());
            assertEquals(-1L, response.timestamp());
            verify(log, times(256)).readEntry(anyLong());
        }
    }

    @Test
    void timestampSearchStopsInsteadOfRecursingForeverWhenAnEntryNeverAdvances() throws Exception {
        // A decoded entry's next offset is baseOffset + numberOfRecords (see probe()): a zero-record
        // entry would leave the next offset equal to the very offset the scan just read from, making
        // no forward progress at all. KafkaRecordsPayload.readableRecords rejects an actual
        // zero-record payload outright, so this reproduces the same zero-progress shape by having
        // every read hand back one entry whose own base offset never matches the offset requested --
        // it always folds back to the same fixed offset. Before ForwardScan carried its own
        // remaining-entries budget, nothing but reaching endOffsetExclusive stopped the scan, so an
        // entry that never advances the offset would have made driveScan recurse (or loop) forever
        // instead of terminating.
        Log log = mock(Log.class);
        when(log.getFirstOffset()).thenReturn(completedFuture(new LogOffset(0L, 1, 100L)));
        when(log.getLastOffset()).thenReturn(completedFuture(new LogOffset(299L, 1, 400L)));
        when(log.binarySearchOffset(eq(0L), eq(299L), any())).thenReturn(completedFuture(0L));
        // A fresh entry per read: each one is a single-use resource that releases its payload on
        // close(), so replaying one mock across reads would fail with a Netty reference-count error
        // rather than exercising the scan bound this test targets.
        when(log.readEntry(anyLong())).thenAnswer(invocation -> completedFuture(entry(0L, recordAt(10L), 100L)));

        try (PartitionReader reader = new PartitionReader(TP, log, 10, frozenTime())) {
            ListOffsetsPartitionResponse response =
                    reader.listOffsets(listOffsetsRequest(1_000L)).get(10, TimeUnit.SECONDS);
            assertEquals(-1L, response.offset());
            assertEquals(-1L, response.timestamp());
            verify(log, times(256)).readEntry(anyLong());
        }
    }

    @Test
    void highWatermarkIsTheOffsetPastTheLastRecord() throws Exception {
        Log log = mock(Log.class);
        when(log.getFirstOffset()).thenReturn(completedFuture(new LogOffset(0L, 1, 1L)));
        when(log.getLastOffset()).thenReturn(completedFuture(new LogOffset(10L, 5, 1L)));

        try (PartitionReader reader = new PartitionReader(TP, log, 10, frozenTime())) {
            assertEquals(15L, reader.highWatermark().get());
        }
    }

    @Test
    void theOffsetWindowIsReadFromStorageOncePerRefreshInterval() throws Exception {
        Log log = mock(Log.class);
        LogCursor cursor = mock(LogCursor.class);
        when(log.getFirstOffset()).thenReturn(completedFuture(new LogOffset(0L, 1, 1L)));
        when(log.getLastOffset()).thenReturn(completedFuture(new LogOffset(9L, 1, 1L)));   // hwm = 10
        when(log.openEphemeralCursor(anyString(), anyLong())).thenReturn(completedFuture(cursor));
        when(cursor.readOffset()).thenReturn(0L);
        when(cursor.readEntries(anyInt(), anyLong(), isNull(), eq(10L)))
                .thenAnswer(invocation -> completedFuture(List.of(entry(0L, records("a")))));
        MockTime time = new MockTime(0L, 1_000L, 0L);

        try (PartitionReader reader = new PartitionReader(TP, log, 10, time)) {
            reader.fetch(fetchRequest(0L)).get();
            time.sleep(OFFSET_RANGE_REFRESH_MS - 1);
            reader.fetch(fetchRequest(0L)).get();
            // Both fetches were answered from the window the first one read.
            verify(log, times(1)).getFirstOffset();
            verify(log, times(1)).getLastOffset();

            time.sleep(1L);
            reader.fetch(fetchRequest(0L)).get();
            verify(log, times(2)).getFirstOffset();
            verify(log, times(2)).getLastOffset();
        }
    }

    @Test
    void anObservedAppendExtendsTheWindowWithoutReadingStorage() throws Exception {
        Log log = mock(Log.class);
        LogCursor cursor = mock(LogCursor.class);
        when(log.getFirstOffset()).thenReturn(completedFuture(new LogOffset(0L, 1, 1L)));
        when(log.getLastOffset()).thenReturn(completedFuture(new LogOffset(0L, 1, 1L)));   // hwm = 1
        when(log.openEphemeralCursor(anyString(), eq(0L))).thenReturn(completedFuture(cursor));
        when(cursor.readOffset()).thenReturn(1L);
        List<LogEntry> beforeTheAppend = List.of(entry(0L, records("a")));
        List<LogEntry> afterTheAppend = List.of(entry(1L, records("b")));
        when(cursor.readEntries(anyInt(), anyLong(), isNull(), eq(1L)))
                .thenReturn(completedFuture(beforeTheAppend));
        when(cursor.readEntries(anyInt(), anyLong(), isNull(), eq(2L)))
                .thenReturn(completedFuture(afterTheAppend));

        try (PartitionReader reader = new PartitionReader(TP, log, 10, new MockTime(0L, 1_000L, 0L))) {
            assertEquals(1, count(reader.fetch(fetchRequest(0L)).get().records));

            reader.observeAppend(new LogOffset(1L, 1, 42L));

            // The append this broker made is readable at once: storage may not report it for
            // another refresh interval, but the records behind it are already durable.
            FetchPartitionData afterAppend = reader.fetch(fetchRequest(1L)).get();
            assertEquals(Errors.NONE, afterAppend.error);
            assertEquals(2L, afterAppend.highWatermark);
            assertEquals(1, count(afterAppend.records));
            verify(log, times(1)).getFirstOffset();
            verify(log, times(1)).getLastOffset();
        }
    }

    @Test
    void anObservedAppendNeverMovesTheWindowBackwards() throws Exception {
        Log log = mock(Log.class);
        when(log.getFirstOffset()).thenReturn(completedFuture(new LogOffset(0L, 1, 1L)));
        when(log.getLastOffset()).thenReturn(completedFuture(new LogOffset(4L, 1, 500L)));   // hwm = 5

        try (PartitionReader reader = new PartitionReader(TP, log, 10, new MockTime(0L, 1_000L, 0L))) {
            assertEquals(5L, reader.highWatermark().get());

            // An append that storage has already reported leaves the window exactly where it is.
            reader.observeAppend(new LogOffset(2L, 1, 200L));

            assertEquals(5L, reader.highWatermark().get());
            ListOffsetsPartitionResponse maxTimestamp = reader.listOffsets(
                    listOffsetsRequest(ListOffsetsPartitionRequest.MAX_TIMESTAMP)).get();
            assertEquals(4L, maxTimestamp.offset());
            assertEquals(500L, maxTimestamp.timestamp());
        }
    }

    @Test
    void aFetchOutsideTheCachedWindowReadsStorageBeforeReportingOutOfRange() throws Exception {
        Log log = mock(Log.class);
        LogCursor cursor = mock(LogCursor.class);
        when(log.getFirstOffset()).thenReturn(completedFuture(new LogOffset(0L, 1, 1L)));
        when(log.getLastOffset()).thenReturn(completedFuture(new LogOffset(1L, 1, 1L)));   // hwm = 2
        when(log.openEphemeralCursor(anyString(), eq(0L))).thenReturn(completedFuture(cursor));
        when(cursor.readOffset()).thenReturn(0L);
        List<LogEntry> firstRead = List.of(entry(0L, records("a")));
        when(cursor.readEntries(anyInt(), anyLong(), isNull(), eq(2L)))
                .thenReturn(completedFuture(firstRead));

        try (PartitionReader reader = new PartitionReader(TP, log, 10, new MockTime(0L, 1_000L, 0L))) {
            reader.fetch(fetchRequest(0L)).get();
            verify(log, times(1)).getLastOffset();

            FetchPartitionData outOfRange = reader.fetch(fetchRequest(5L)).get();

            // A cached window may simply predate another owner's append, so the verdict has to
            // come from a window read right now.
            assertEquals(Errors.OFFSET_OUT_OF_RANGE, outOfRange.error);
            verify(log, times(2)).getFirstOffset();
            verify(log, times(2)).getLastOffset();
        }
    }

    @Test
    void aFetchAboveTheCachedWindowIsServedFromTheRefreshedWindow() throws Exception {
        Log log = mock(Log.class);
        LogCursor cursor = mock(LogCursor.class);
        when(log.getFirstOffset()).thenReturn(completedFuture(new LogOffset(0L, 1, 1L)));
        // Another owner of this log appended between the two fetches below.
        when(log.getLastOffset())
                .thenReturn(completedFuture(new LogOffset(1L, 1, 1L)))    // hwm = 2
                .thenReturn(completedFuture(new LogOffset(9L, 1, 1L)));   // hwm = 10
        when(log.openEphemeralCursor(anyString(), eq(0L))).thenReturn(completedFuture(cursor));
        when(cursor.readOffset()).thenReturn(0L);
        when(cursor.seek(5L)).thenReturn(completedFuture(null));
        List<LogEntry> insideTheCachedWindow = List.of(entry(0L, records("a")));
        List<LogEntry> pastTheCachedWindow = List.of(entry(5L, records("b")));
        when(cursor.readEntries(anyInt(), anyLong(), isNull(), eq(2L)))
                .thenReturn(completedFuture(insideTheCachedWindow));
        when(cursor.readEntries(anyInt(), anyLong(), isNull(), eq(10L)))
                .thenReturn(completedFuture(pastTheCachedWindow));

        try (PartitionReader reader = new PartitionReader(TP, log, 10, new MockTime(0L, 1_000L, 0L))) {
            reader.fetch(fetchRequest(0L)).get();

            FetchPartitionData beyondTheCachedWindow = reader.fetch(fetchRequest(5L)).get();

            assertEquals(Errors.NONE, beyondTheCachedWindow.error);
            assertEquals(10L, beyondTheCachedWindow.highWatermark);
            assertEquals(1, count(beyondTheCachedWindow.records));
        }
    }

    @Test
    void anEmptyFetchAtTheTailKeepsTheCachedWindowUntilItExpires() throws Exception {
        Log log = mock(Log.class);
        when(log.getFirstOffset()).thenReturn(completedFuture(new LogOffset(0L, 1, 1L)));
        when(log.getLastOffset()).thenReturn(completedFuture(new LogOffset(0L, 1, 1L)));   // hwm = 1
        MockTime time = new MockTime(0L, 1_000L, 0L);

        try (PartitionReader reader = new PartitionReader(TP, log, 10, time)) {
            FetchPartitionData atTheTail = reader.fetch(fetchRequest(1L)).get();
            assertEquals(Errors.NONE, atTheTail.error);
            assertEquals(0, atTheTail.records.sizeInBytes());
            verify(log, times(1)).getLastOffset();

            // A caught-up consumer long-polls for longer than the refresh interval before it reads
            // again, so nothing is gained by making it read the window twice inside one interval.
            reader.fetch(fetchRequest(1L)).get();
            verify(log, times(1)).getFirstOffset();
            verify(log, times(1)).getLastOffset();

            // Its re-read, after the wait, is what picks up another owner's append.
            time.sleep(OFFSET_RANGE_REFRESH_MS);
            reader.fetch(fetchRequest(1L)).get();
            verify(log, times(2)).getFirstOffset();
            verify(log, times(2)).getLastOffset();
        }
    }

    @Test
    void aReadIssuedBeforeTheFetchCannotReportOutOfRange() throws Exception {
        Log log = mock(Log.class);
        LogCursor cursor = mock(LogCursor.class);
        CompletableFuture<LogOffset> readBeforeTheAppend = new CompletableFuture<>();
        when(log.getFirstOffset()).thenReturn(completedFuture(new LogOffset(0L, 1, 1L)));
        when(log.getLastOffset())
                .thenReturn(readBeforeTheAppend)                            // issued at T0
                .thenReturn(completedFuture(new LogOffset(5L, 1, 1L)));      // hwm = 6, after the append
        when(log.openEphemeralCursor(anyString(), eq(5L))).thenReturn(completedFuture(cursor));
        when(cursor.readOffset()).thenReturn(5L);
        List<LogEntry> appendedByAnotherOwner = List.of(entry(5L, records("a")));
        when(cursor.readEntries(anyInt(), anyLong(), isNull(), eq(6L)))
                .thenReturn(completedFuture(appendedByAnotherOwner));

        try (PartitionReader reader = new PartitionReader(TP, log, 10, new MockTime(0L, 1_000L, 0L))) {
            // A read of the window is already in flight, from before another owner appended.
            CompletableFuture<Long> readInFlight = reader.highWatermark();
            assertFalse(readInFlight.isDone());

            // The consumer learned of offset 5 elsewhere and seeks to it. Joining the read above
            // would condemn a valid offset with OFFSET_OUT_OF_RANGE and reset the consumer.
            CompletableFuture<FetchPartitionData> seek = reader.fetch(fetchRequest(5L));
            readBeforeTheAppend.complete(new LogOffset(0L, 1, 1L));          // hwm = 1: the old log

            FetchPartitionData response = seek.get(5, TimeUnit.SECONDS);
            assertEquals(Errors.NONE, response.error);
            assertEquals(6L, response.highWatermark);
            assertEquals(1, count(response.records));
            // The older read still answers the caller that asked for it, with what it read.
            assertEquals(1L, readInFlight.get(5, TimeUnit.SECONDS));
            verify(log, times(2)).getLastOffset();
        }
    }

    @Test
    void anAppendKeepsTheLastEntryOffsetAndTimestampTogether() throws Exception {
        Log log = mock(Log.class);
        when(log.getFirstOffset()).thenReturn(completedFuture(new LogOffset(0L, 1, 1L)));
        when(log.getLastOffset()).thenReturn(completedFuture(new LogOffset(4L, 1, 500L)));   // hwm = 5

        try (PartitionReader reader = new PartitionReader(TP, log, 10, new MockTime(0L, 1_000L, 0L))) {
            assertEquals(5L, reader.highWatermark().get());

            // An entry written under a clock behind the one that stamped entry 4: ahead on offset,
            // behind on write time.
            reader.observeAppend(new LogOffset(9L, 1, 200L));

            // The high watermark still covers the append, so its records are readable...
            assertEquals(10L, reader.highWatermark().get());
            // ...but MAX_TIMESTAMP must name one real entry, not offset 9 beside entry 4's time.
            ListOffsetsPartitionResponse maxTimestamp = reader.listOffsets(
                    listOffsetsRequest(ListOffsetsPartitionRequest.MAX_TIMESTAMP)).get();
            assertEquals(4L, maxTimestamp.offset());
            assertEquals(500L, maxTimestamp.timestamp());
        }
    }

    @Test
    void aTimestampLookupSearchesUpToTheAppendedEntry() throws Exception {
        // observeAppend runs only after Log.append completed, so storage's index already carries
        // the appended entry: it is a valid upper bound for the binary search even though the
        // cached window predates it.
        Log log = mock(Log.class);
        when(log.getFirstOffset()).thenReturn(completedFuture(new LogOffset(0L, 1, 100L)));
        when(log.getLastOffset()).thenReturn(completedFuture(new LogOffset(2L, 1, 300L)));   // hwm = 3
        LogEntry appended = entry(3L, recordAt(400L), 400L);
        when(log.binarySearchOffset(eq(0L), eq(3L), any())).thenReturn(completedFuture(3L));
        when(log.readEntry(3L)).thenReturn(completedFuture(appended));

        try (PartitionReader reader = new PartitionReader(TP, log, 10, new MockTime(0L, 1_000L, 0L))) {
            assertEquals(3L, reader.highWatermark().get());
            reader.observeAppend(new LogOffset(3L, 1, 400L));

            ListOffsetsPartitionResponse response = reader.listOffsets(listOffsetsRequest(350L)).get();

            assertEquals(3L, response.offset());
            assertEquals(400L, response.timestamp());
            verify(log).binarySearchOffset(eq(0L), eq(3L), any());
        }
    }

    @Test
    void aFailedWindowReadDoesNotPoisonTheCache() throws Exception {
        Log log = mock(Log.class);
        LogCursor cursor = mock(LogCursor.class);
        when(log.getFirstOffset()).thenReturn(completedFuture(new LogOffset(0L, 1, 1L)));
        when(log.getLastOffset())
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("index read failed")))
                .thenReturn(completedFuture(new LogOffset(0L, 1, 1L)));   // hwm = 1
        when(log.openEphemeralCursor(anyString(), eq(0L))).thenReturn(completedFuture(cursor));
        when(cursor.readOffset()).thenReturn(0L);
        List<LogEntry> afterTheFailure = List.of(entry(0L, records("a")));
        when(cursor.readEntries(anyInt(), anyLong(), isNull(), eq(1L)))
                .thenReturn(completedFuture(afterTheFailure));

        try (PartitionReader reader = new PartitionReader(TP, log, 10, new MockTime(0L, 1_000L, 0L))) {
            assertThrows(ExecutionException.class, () -> reader.fetch(fetchRequest(0L)).get());

            // A read that failed caches nothing and leaves nothing in flight, so the next request
            // simply reads the window again.
            FetchPartitionData afterFailure = reader.fetch(fetchRequest(0L)).get();
            assertEquals(Errors.NONE, afterFailure.error);
            assertEquals(1, count(afterFailure.records));
            verify(log, times(2)).getFirstOffset();
            verify(log, times(2)).getLastOffset();
        }
    }

    @Test
    void concurrentRequestsShareOneWindowRead() throws Exception {
        Log log = mock(Log.class);
        CompletableFuture<LogOffset> gatedLastOffset = new CompletableFuture<>();
        when(log.getFirstOffset()).thenReturn(completedFuture(LogOffset.NOT_FOUND));
        when(log.getLastOffset()).thenReturn(gatedLastOffset);

        try (PartitionReader reader = new PartitionReader(TP, log, 10, new MockTime(0L, 1_000L, 0L))) {
            CompletableFuture<FetchPartitionData> first = reader.fetch(fetchRequest(0L));
            CompletableFuture<FetchPartitionData> second = reader.fetch(fetchRequest(0L));
            CompletableFuture<Long> third = reader.highWatermark();
            assertFalse(first.isDone());

            // One read in flight answers every request that arrives while it is running.
            verify(log, times(1)).getFirstOffset();
            verify(log, times(1)).getLastOffset();

            gatedLastOffset.complete(LogOffset.NOT_FOUND);

            assertEquals(Errors.NONE, first.get(5, TimeUnit.SECONDS).error);
            assertEquals(0L, second.get(5, TimeUnit.SECONDS).highWatermark);
            assertEquals(0L, third.get(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void listOffsetsAndTheHighWatermarkFollowTheCachedWindow() throws Exception {
        Log log = mock(Log.class);
        when(log.getFirstOffset()).thenReturn(completedFuture(new LogOffset(5L, 1, 1L)));
        when(log.getLastOffset()).thenReturn(completedFuture(new LogOffset(9L, 1, 100L)));   // hwm = 10
        MockTime time = new MockTime(0L, 1_000L, 0L);

        try (PartitionReader reader = new PartitionReader(TP, log, 10, time)) {
            assertEquals(5L, reader.listOffsets(
                    listOffsetsRequest(ListOffsetsPartitionRequest.EARLIEST_TIMESTAMP)).get().offset());
            assertEquals(10L, reader.listOffsets(
                    listOffsetsRequest(ListOffsetsPartitionRequest.LATEST_TIMESTAMP)).get().offset());
            assertEquals(10L, reader.highWatermark().get());
            verify(log, times(1)).getFirstOffset();
            verify(log, times(1)).getLastOffset();

            time.sleep(OFFSET_RANGE_REFRESH_MS);
            assertEquals(10L, reader.highWatermark().get());
            verify(log, times(2)).getFirstOffset();
            verify(log, times(2)).getLastOffset();
        }
    }
    @Test
    void earliestIsAlwaysAnsweredFromAFreshWindowRead() throws Exception {
        Log log = mock(Log.class);
        when(log.getFirstOffset())
                .thenReturn(completedFuture(new LogOffset(0L, 1, 1L)))
                .thenReturn(completedFuture(new LogOffset(5L, 1, 1L)))   // a trim moved the log's start
                .thenReturn(completedFuture(new LogOffset(7L, 1, 1L)));  // and then another one did
        when(log.getLastOffset()).thenReturn(completedFuture(new LogOffset(9L, 1, 100L)));   // hwm = 10

        // The clock never moves, so every request below falls inside one refresh interval.
        try (PartitionReader reader = new PartitionReader(TP, log, 10, frozenTime())) {
            assertEquals(0L, reader.listOffsets(
                    listOffsetsRequest(ListOffsetsPartitionRequest.EARLIEST_TIMESTAMP)).get().offset());
            verify(log, times(1)).getFirstOffset();

            // Retention trimmed the log through another broker, so nothing invalidated the window
            // this reader is holding. EARLIEST still has to report where the log now starts.
            assertEquals(5L, reader.listOffsets(
                    listOffsetsRequest(ListOffsetsPartitionRequest.EARLIEST_TIMESTAMP)).get().offset());
            verify(log, times(2)).getFirstOffset();

            assertEquals(7L, reader.listOffsets(
                    listOffsetsRequest(ListOffsetsPartitionRequest.EARLIEST_LOCAL_TIMESTAMP)).get().offset());
            verify(log, times(3)).getFirstOffset();
        }
    }

    @Test
    void latestIsAnsweredFromTheCachedWindowWithinTheInterval() throws Exception {
        Log log = mock(Log.class);
        when(log.getFirstOffset()).thenReturn(completedFuture(new LogOffset(0L, 1, 1L)));
        when(log.getLastOffset()).thenReturn(completedFuture(new LogOffset(9L, 1, 100L)));   // hwm = 10

        try (PartitionReader reader = new PartitionReader(TP, log, 10, frozenTime())) {
            assertEquals(10L, reader.listOffsets(
                    listOffsetsRequest(ListOffsetsPartitionRequest.LATEST_TIMESTAMP)).get().offset());
            assertEquals(10L, reader.listOffsets(
                    listOffsetsRequest(ListOffsetsPartitionRequest.LATEST_TIMESTAMP)).get().offset());
            ListOffsetsPartitionResponse maxTimestamp = reader.listOffsets(
                    listOffsetsRequest(ListOffsetsPartitionRequest.MAX_TIMESTAMP)).get();
            assertEquals(9L, maxTimestamp.offset());

            // Only the log's start is worth a read per request; the end is allowed to lag by an
            // interval, as it does for a fetch.
            verify(log, times(1)).getFirstOffset();
            verify(log, times(1)).getLastOffset();
        }
    }

    @Test
    void anEarliestRequestJoinsOnlyAWindowReadIssuedAfterIt() throws Exception {
        Log log = mock(Log.class);
        CompletableFuture<LogOffset> readBeforeTheTrim = new CompletableFuture<>();
        CompletableFuture<LogOffset> readAfterTheTrim = new CompletableFuture<>();
        when(log.getFirstOffset())
                .thenReturn(readBeforeTheTrim)                             // issued at T0
                .thenReturn(readAfterTheTrim)                              // the read both requests need
                .thenReturn(completedFuture(new LogOffset(9L, 1, 1L)));    // a third read: never issued
        when(log.getLastOffset()).thenReturn(completedFuture(new LogOffset(9L, 1, 100L)));   // hwm = 10

        try (PartitionReader reader = new PartitionReader(TP, log, 10, frozenTime())) {
            // A read of the window is already in flight, from before the trim.
            CompletableFuture<Long> readInFlight = reader.highWatermark();
            assertFalse(readInFlight.isDone());

            CompletableFuture<ListOffsetsPartitionResponse> first =
                    reader.listOffsets(listOffsetsRequest(ListOffsetsPartitionRequest.EARLIEST_TIMESTAMP));
            CompletableFuture<ListOffsetsPartitionResponse> second =
                    reader.listOffsets(listOffsetsRequest(ListOffsetsPartitionRequest.EARLIEST_TIMESTAMP));
            // Neither may be answered by that read: it started before either request arrived, so
            // what it reports may predate the trim they are asking about.
            assertFalse(first.isDone());
            assertFalse(second.isDone());
            verify(log, times(1)).getFirstOffset();

            readBeforeTheTrim.complete(new LogOffset(0L, 1, 1L));   // the log as it stood before the trim
            // Both requests waited for that one, and now share the single read that follows it.
            verify(log, times(2)).getFirstOffset();
            assertFalse(first.isDone());
            assertFalse(second.isDone());

            readAfterTheTrim.complete(new LogOffset(5L, 1, 1L));
            assertEquals(5L, first.get(5, TimeUnit.SECONDS).offset());
            assertEquals(5L, second.get(5, TimeUnit.SECONDS).offset());
            verify(log, times(2)).getFirstOffset();
            // The older read still answers the caller that asked for it, with what it read.
            assertEquals(10L, readInFlight.get(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void invalidatingTheWindowMakesTheNextRequestReadStorage() throws Exception {
        Log log = mock(Log.class);
        when(log.getFirstOffset()).thenReturn(completedFuture(new LogOffset(5L, 1, 1L)));
        when(log.getLastOffset()).thenReturn(completedFuture(new LogOffset(9L, 1, 100L)));

        try (PartitionReader reader = new PartitionReader(TP, log, 10, new MockTime(0L, 1_000L, 0L))) {
            assertEquals(10L, reader.highWatermark().get());
            verify(log, times(1)).getFirstOffset();

            // A trim moves the first offset, so the window it left behind must not be answered from.
            reader.invalidateRange();

            assertEquals(10L, reader.highWatermark().get());
            verify(log, times(2)).getFirstOffset();
            verify(log, times(2)).getLastOffset();
        }
    }

    @Test
    void aWindowInvalidatedWhileItWasBeingReadIsNeverServed() throws Exception {
        Log log = mock(Log.class);
        CompletableFuture<LogOffset> gatedLastOffset = new CompletableFuture<>();
        when(log.getFirstOffset()).thenReturn(completedFuture(new LogOffset(5L, 1, 1L)));
        when(log.getLastOffset())
                .thenReturn(gatedLastOffset)
                .thenReturn(completedFuture(new LogOffset(9L, 1, 100L)));   // hwm = 10

        try (PartitionReader reader = new PartitionReader(TP, log, 10, new MockTime(0L, 1_000L, 0L))) {
            CompletableFuture<Long> readAcrossTheTrim = reader.highWatermark();
            assertFalse(readAcrossTheTrim.isDone());

            // A trim lands while the read is in flight, so what that read reports describes the
            // log as it was before the trim. It still answers its own caller...
            reader.invalidateRange();
            gatedLastOffset.complete(new LogOffset(9L, 1, 100L));
            assertEquals(10L, readAcrossTheTrim.get(5, TimeUnit.SECONDS));

            // ...but it is not cached: the next request reads storage again, inside the refresh
            // interval, rather than being answered from a window the trim already condemned.
            assertEquals(10L, reader.highWatermark().get(5, TimeUnit.SECONDS));
            verify(log, times(2)).getFirstOffset();
            verify(log, times(2)).getLastOffset();
        }
    }

    /** A clock that never moves, so one test's requests all fall inside a single refresh interval. */
    private static Time frozenTime() {
        return new MockTime(0L, 1_000L, 0L);
    }

    private static FetchRequest.PartitionData fetchRequest(long fetchOffset) {
        return new FetchRequest.PartitionData(TP.topicId(), fetchOffset, -1L, 1024, Optional.empty());
    }

    private static ListOffsetsPartitionRequest listOffsetsRequest(long timestamp) {
        return new ListOffsetsPartitionRequest(TP, timestamp, Optional.empty());
    }

    private static MemoryRecords records(String value) {
        return MemoryRecords.withRecords(Compression.NONE,
                new SimpleRecord(1L, value.getBytes(StandardCharsets.UTF_8)));
    }

    private static MemoryRecords recordAt(long timestamp) {
        return MemoryRecords.withRecords(Compression.NONE,
                new SimpleRecord(timestamp, "v".getBytes(StandardCharsets.UTF_8)));
    }

    private static int count(Records records) {
        int count = 0;
        for (Iterator<Record> it = records.records().iterator(); it.hasNext(); it.next()) {
            count++;
        }
        return count;
    }

    /** Mocks a storage entry whose payload is read-only, exactly as the Lakestream implementation returns it. */
    private static LogEntry entry(long offset, MemoryRecords records) {
        return entry(offset, records, 0L);
    }

    /** As {@link #entry(long, MemoryRecords)}, with the entry's write time in its header. */
    private static LogEntry entry(long offset, MemoryRecords records, long headerTimestamp) {
        int numberOfRecords = count(records);
        ByteBuf payload = Unpooled.buffer(records.buffer().remaining());
        payload.writeBytes(records.buffer().duplicate());

        LogEntry entry = mock(LogEntry.class);
        AtomicBoolean closed = new AtomicBoolean();
        when(entry.offset()).thenReturn(offset);
        when(entry.numberOfRecords()).thenReturn(numberOfRecords);
        when(entry.timestamp()).thenReturn(headerTimestamp);
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
}
