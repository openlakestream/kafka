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
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.storage.diskless.DisklessFutures;
import org.apache.kafka.storage.diskless.ListOffsetsPartitionRequest;
import org.apache.kafka.storage.diskless.ListOffsetsPartitionResponse;
import org.apache.kafka.storage.diskless.LogEntryUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import io.lakestream.api.Log;
import io.lakestream.api.LogCursor;
import io.lakestream.api.LogEntry;
import io.lakestream.api.LogOffset;

/**
 * The read side of one diskless partition: fetch, ListOffsets, and the high watermark.
 *
 * <p>Fetches share a single cached {@link LogCursor}. A sequential consumer keeps finding that cursor
 * exactly where its previous fetch left it, so the common case costs neither a cursor open nor a
 * seek. A concurrent second fetch never queues behind the first: it opens a throw-away cursor, and
 * whichever read releases first back into the empty cache keeps its cursor while the other is closed.
 *
 * <p>ListOffsets never opens a cursor. A timestamp lookup binary searches entry headers for the
 * last entry whose header predates the target and reads forward from there, over a bounded number
 * of entries; MAX_TIMESTAMP is answered from the last entry's header alone.
 *
 * <p>Every request needs the log's offset window, which costs two index reads. That window is
 * therefore cached for {@link #OFFSET_RANGE_REFRESH_MS} rather than read per request, and appends
 * made through this partition's own writer widen it without any read at all. What that buys is a
 * bounded number of index reads per partition; what it costs is stated in
 * {@link #OFFSET_RANGE_REFRESH_MS}. The one request that never takes the cached window is a
 * ListOffsets for EARLIEST: where the log begins is answered from a read issued after the request
 * arrived, so a trim made by another broker cannot go unseen.
 */
final class PartitionReader implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PartitionReader.class);

    private static final long UNKNOWN_TIMESTAMP = -1L;
    private static final long NOT_FOUND_OFFSET = -1L;
    /** How many entries one timestamp lookup may read past its binary search before answering. */
    private static final int MAX_TIMESTAMP_SCAN_ENTRIES = 256;
    /**
     * How long a window read from storage answers requests before it is read again.
     *
     * <p>A Lakestream log has many concurrent writers -- one per zone-aware owner -- so no broker
     * can hold an exact view of where the log ends. This one is deliberately inexact instead: an
     * offset written by <em>another</em> owner becomes visible here within this interval or one
     * fetch wait, whichever is longer. A caught-up consumer long-polls for {@code
     * fetch.max.wait.ms} before re-reading, which is already longer than this interval, so the
     * interval alone is what refreshes it. Appends made through this partition's own writer are
     * reported by {@link #observeAppend(LogOffset)} and are visible immediately.
     *
     * <p>It is a constant rather than a config: 100 ms is short enough to stay under a consumer's
     * poll interval and long enough that a partition being read by many consumers costs storage a
     * bounded number of index reads per second instead of two per fetch.
     *
     * <p>This bounds the staleness of the log's <em>end</em> only. Its start moves only when
     * retention trims the log, and {@link #listOffsets(ListOffsetsPartitionRequest)} reads that
     * fresh rather than waiting out this interval.
     *
     * <p>Package-private so that the tests can assert against this value rather than repeat it.
     */
    static final long OFFSET_RANGE_REFRESH_MS = 100L;
    /** Passed to {@link #refreshedRange(long)} by a caller that any read in flight may answer. */
    private static final long ANY_READ = 0L;

    private final TopicIdPartition topicIdPartition;
    private final Log logInstance;
    private final int maxEntriesPerFetch;
    private final Time time;
    private final String cursorName;
    private final AtomicReference<LogCursor> cachedCursor = new AtomicReference<>();
    /** The last window read from storage, with the time and the generation of that read. */
    private final AtomicReference<CachedRange> cachedRange = new AtomicReference<>();
    /** The read every caller shares while it is in flight, so a burst costs one pair of reads. */
    private final AtomicReference<RangeRead> rangeRefresh = new AtomicReference<>();
    /** Numbers the reads of the window, so a caller can tell one issued before it from a later one. */
    private final AtomicLong rangeReads = new AtomicLong();
    /** The newest entry this partition's own writer appended, or null before the first append. */
    private final AtomicReference<LogOffset> lastAppend = new AtomicReference<>();
    /** Counts drops of the cached window; a window read under an older generation is never served. */
    private final AtomicLong rangeGeneration = new AtomicLong();
    private final AtomicBoolean closed = new AtomicBoolean();

    PartitionReader(TopicIdPartition topicIdPartition, Log logInstance, int maxEntriesPerFetch, Time time) {
        this.topicIdPartition = topicIdPartition;
        this.logInstance = logInstance;
        this.maxEntriesPerFetch = maxEntriesPerFetch;
        this.time = Objects.requireNonNull(time, "time must not be null");
        this.cursorName = "kafka-fetch-" + topicIdPartition.topic() + "-" + topicIdPartition.partition();
    }

    /** The offset just past the last durable record, or 0 for an empty log. */
    CompletableFuture<Long> highWatermark() {
        return range().thenApply(Range::highWatermark);
    }

    /**
     * Widens the cached window to cover one append made through this partition's own writer, so
     * that the records it just made durable are readable without waiting for storage to report
     * them. Every value only moves forward: an append storage has already reported changes nothing.
     */
    void observeAppend(LogOffset appended) {
        if (appended == null || appended.offset() < 0) {
            return;
        }
        lastAppend.updateAndGet(previous ->
                previous == null || appended.offset() > previous.offset() ? appended : previous);
    }

    /**
     * Drops the cached window, so the next request reads it from storage. Called when the window
     * this reader holds cannot describe the log any more: after a retention trim moved the first
     * offset, and when this reader closes.
     */
    void invalidateRange() {
        rangeGeneration.incrementAndGet();
        cachedRange.set(null);
    }

    CompletableFuture<FetchPartitionData> fetch(FetchRequest.PartitionData request) {
        long fetchOffset = request.fetchOffset;
        int maxBytes = request.maxBytes;
        log.debug("Fetching from partition {} at offset {} with maxBytes {}",
                topicIdPartition, fetchOffset, maxBytes);

        // Read before the cache is consulted: any read of the window that starts after this point
        // is one that must already see whatever was appended before this request arrived.
        long readsBeforeRequest = rangeReads.get();
        Range cached = cachedWindow();
        if (cached != null && cached.covers(fetchOffset)) {
            return readWithin(fetchOffset, maxBytes, cached);
        }
        // A window that does not cover the fetch offset may simply predate an append by another
        // owner of this log, and answering OFFSET_OUT_OF_RANGE resets the client's offset. So the
        // verdict must come from a read issued after this request arrived: sharing a read that was
        // already running would let a window fetched before that append condemn a valid offset.
        return refreshedRange(readsBeforeRequest + 1)
                .thenCompose(range -> readWithin(fetchOffset, maxBytes, range));
    }

    private CompletableFuture<FetchPartitionData> readWithin(long fetchOffset, int maxBytes, Range range) {
        if (!range.covers(fetchOffset)) {
            log.debug("fetchOffset {} outside range [{}, {}] for partition {}",
                    fetchOffset, range.start(), range.highWatermark(), topicIdPartition);
            return CompletableFuture.completedFuture(
                    fetchData(Errors.OFFSET_OUT_OF_RANGE, range, MemoryRecords.EMPTY));
        }
        if (fetchOffset == range.highWatermark()) {
            // Caught up. The window is left cached on purpose: this request now long-polls for
            // fetch.max.wait.ms, which is longer than the refresh interval, so its re-read finds
            // the window expired and reads storage anyway. Dropping it here only doubled the index
            // reads of the very consumers that are idle.
            return CompletableFuture.completedFuture(fetchData(Errors.NONE, range, MemoryRecords.EMPTY));
        }
        return readEntries(fetchOffset, range.highWatermark(), maxBytes)
                .thenApply(records -> fetchData(Errors.NONE, range, records));
    }

    CompletableFuture<ListOffsetsPartitionResponse> listOffsets(ListOffsetsPartitionRequest request) {
        long timestamp = request.timestamp();
        log.debug("ListOffsets for partition {} with timestamp {}", topicIdPartition, timestamp);

        if (timestamp == ListOffsetsPartitionRequest.LATEST_TIERED_TIMESTAMP) {
            return CompletableFuture.completedFuture(notFound());
        }
        boolean earliest = timestamp == ListOffsetsPartitionRequest.EARLIEST_TIMESTAMP
                || timestamp == ListOffsetsPartitionRequest.EARLIEST_LOCAL_TIMESTAMP;
        // Where the log begins is answered from a read issued after this request arrived, never
        // from the cached window: a trim is the only thing that moves the start, this broker is
        // not told of a trim another one ran, and a client asking for EARLIEST is usually about to
        // reset to what it is told. That is the same gate a fetch takes before it may report
        // OFFSET_OUT_OF_RANGE, so a read already in flight for a request no older than this one is
        // shared, and a request costs at most one extra pair of index reads. Every other timestamp
        // keeps the cached window: ListOffsets is rare next to fetch, but not so rare that the end
        // of the log is worth two index reads per request as well.
        CompletableFuture<Range> window = earliest ? refreshedRange(rangeReads.get() + 1) : range();
        return window.thenCompose(range -> {
            if (earliest) {
                return CompletableFuture.completedFuture(success(range.start(), UNKNOWN_TIMESTAMP));
            }
            if (timestamp == ListOffsetsPartitionRequest.LATEST_TIMESTAMP) {
                return CompletableFuture.completedFuture(success(range.highWatermark(), UNKNOWN_TIMESTAMP));
            }
            if (range.isEmpty()) {
                return CompletableFuture.completedFuture(notFound());
            }
            if (timestamp == ListOffsetsPartitionRequest.MAX_TIMESTAMP) {
                // Entries are appended in write-timestamp order, so the last one carries the maximum.
                return CompletableFuture.completedFuture(
                        success(range.lastEntryOffset(), range.lastEntryTimestamp()));
            }
            return firstAtOrAfter(range, timestamp);
        });
    }

    /**
     * Drops the cached window and closes the cached cursor. In-flight reads close their own cursor
     * when they release it, and a window read that is still in flight is no longer cached.
     */
    @Override
    public void close() {
        closed.set(true);
        invalidateRange();
        closeQuietly(cachedCursor.getAndSet(null));
    }

    private CompletableFuture<MemoryRecords> readEntries(long fetchOffset, long maxOffsetExclusive, int maxBytes) {
        return acquireCursor(fetchOffset).thenCompose(lease -> {
            CompletableFuture<List<LogEntry>> read;
            try {
                read = lease.cursor().readEntries(maxEntriesPerFetch, maxBytes, null, maxOffsetExclusive);
                if (read == null) {
                    throw new IllegalStateException(
                            "LogCursor.readEntries returned a null future for " + topicIdPartition);
                }
            } catch (Throwable readError) {
                lease.release();
                return CompletableFuture.<MemoryRecords>failedFuture(readError);
            }
            return read.handle((entries, error) -> {
                MemoryRecords records = null;
                Throwable failure = error == null ? null : DisklessFutures.unwrap(error);
                try {
                    if (failure == null) {
                        records = KafkaRecordsPayload.assemble(entries);
                    }
                } catch (Throwable assembleError) {
                    failure = assembleError;
                } finally {
                    failure = LogEntryUtils.closeAll(entries, failure);
                    lease.release();
                }
                if (failure != null) {
                    throw new CompletionException(failure);
                }
                return records;
            });
        });
    }

    /**
     * Leases the cached cursor, seeking it only when it is not already at {@code startOffset}. When the
     * cache is empty — the reader has never read, or another read holds the cursor — a throw-away cursor
     * is opened instead so that reads never queue behind each other.
     */
    private CompletableFuture<CursorLease> acquireCursor(long startOffset) {
        LogCursor cached = cachedCursor.getAndSet(null);
        if (cached == null) {
            return call(() -> logInstance.openEphemeralCursor(cursorName, startOffset), "Log.openEphemeralCursor")
                    .thenApply(cursor -> {
                        if (cursor == null) {
                            throw new IllegalStateException(
                                    "Log.openEphemeralCursor returned a null cursor for " + topicIdPartition);
                        }
                        return new CursorLease(cursor);
                    });
        }
        try {
            if (cached.readOffset() == startOffset) {
                return CompletableFuture.completedFuture(new CursorLease(cached));
            }
            CompletableFuture<Void> positioned = cached.seek(startOffset);
            if (positioned == null) {
                throw new IllegalStateException("LogCursor.seek returned a null future for " + topicIdPartition);
            }
            return positioned.handle((ignored, seekError) -> {
                if (seekError != null) {
                    closeQuietly(cached);
                    throw new CompletionException(DisklessFutures.unwrap(seekError));
                }
                return new CursorLease(cached);
            });
        } catch (Throwable seekError) {
            closeQuietly(cached);
            return CompletableFuture.failedFuture(seekError);
        }
    }

    /**
     * Binary searches entry headers for the last entry whose header predates {@code target}, then
     * reads entries forward from it until one holds a record at or after {@code target}.
     *
     * <p>A header timestamp is the entry's write time and a record timestamp is its create time.
     * The search assumes a record is never newer than the header it was written under, so an entry
     * whose header predates {@code target} holds no answer at all -- which is what the binary
     * search skips. It reports the last such entry, so the scan starts one entry before the first
     * candidate.
     *
     * <p>Past that point the records still lag their headers by however far the write path is
     * behind the clock, so the first record at or after {@code target} can sit any number of
     * entries further on. The scan is therefore bounded at {@link #MAX_TIMESTAMP_SCAN_ENTRIES}
     * entries: exhausting it answers with the base offset and write time of the earliest scanned
     * entry that was written at or after {@code target}, which is at or before the record the
     * caller asked for and never past it, so a consumer seeking there re-reads a little rather than
     * skipping records.
     *
     * <p>The window handed in here may name an entry newer than the last window read from storage,
     * because {@link #observeAppend(LogOffset)} folds this partition's own appends into it. That is
     * safe to search over: {@code observeAppend} is called only once {@link Log#append} has
     * completed, and an append that has completed is one storage's index already carries -- it is
     * what a fresh {@code getLastOffset} would report. The staleness this class introduces is in
     * the cached copy of the window, never in what storage knows when it is asked.
     */
    private CompletableFuture<ListOffsetsPartitionResponse> firstAtOrAfter(Range range, long target) {
        return call(() -> logInstance.binarySearchOffset(
                        range.start(), range.lastEntryOffset(), header -> header.timestamp() < target),
                "Log.binarySearchOffset")
                .thenCompose(found -> scanForward(new ForwardScan(
                        found == null || found < range.start() ? range.start() : found,
                        target,
                        range.highWatermark())));
    }

    private CompletableFuture<ListOffsetsPartitionResponse> scanForward(ForwardScan scan) {
        CompletableFuture<ListOffsetsPartitionResponse> answer = new CompletableFuture<>();
        driveScan(scan, answer);
        return answer;
    }

    /**
     * Drives the forward scan as a loop rather than as a chain of nested completions: a read that
     * is already complete continues this loop, so a storage layer answering synchronously cannot
     * grow the stack by a frame per entry, and one that answers later resumes the loop from its own
     * completion instead of nesting inside it.
     */
    private void driveScan(ForwardScan scan, CompletableFuture<ListOffsetsPartitionResponse> answer) {
        while (scan.hasMoreToRead()) {
            CompletableFuture<LogEntry> read =
                    call(() -> logInstance.readEntry(scan.entryOffset()), "Log.readEntry");
            if (!read.isDone()) {
                read.whenComplete((entry, error) -> {
                    if (error != null) {
                        answer.completeExceptionally(DisklessFutures.unwrap(error));
                    } else if (accept(scan, entry, answer)) {
                        driveScan(scan, answer);
                    }
                });
                return;
            }
            LogEntry entry;
            try {
                entry = read.join();
            } catch (Throwable readError) {
                answer.completeExceptionally(DisklessFutures.unwrap(readError));
                return;
            }
            if (!accept(scan, entry, answer)) {
                return;
            }
        }
        answer.complete(scan.exhaustedAnswer());
    }

    /** Folds one entry into {@code scan}; false once {@code answer} is settled and the scan is over. */
    private boolean accept(
            ForwardScan scan,
            LogEntry entry,
            CompletableFuture<ListOffsetsPartitionResponse> answer) {
        ListOffsetsPartitionResponse match;
        try {
            match = scan.accept(probe(entry, scan.target()));
        } catch (Throwable decodeError) {
            answer.completeExceptionally(DisklessFutures.unwrap(decodeError));
            return false;
        }
        if (match != null) {
            answer.complete(match);
            return false;
        }
        return true;
    }

    /** Decodes one entry, always closing it, and reports the first record at or after {@code target}. */
    private EntryProbe probe(LogEntry entry, long target) {
        if (entry == null) {
            throw new CompletionException(
                    new IllegalStateException("Log.readEntry returned a null entry for " + topicIdPartition));
        }
        ListOffsetsPartitionResponse match = null;
        long baseOffset = NOT_FOUND_OFFSET;
        long headerTimestamp = UNKNOWN_TIMESTAMP;
        long nextOffset = NOT_FOUND_OFFSET;
        Throwable failure = null;
        try {
            baseOffset = entry.offset();
            headerTimestamp = entry.timestamp();
            MemoryRecords records = KafkaRecordsPayload.readableRecords(
                    entry.payload(), baseOffset, entry.numberOfRecords());
            nextOffset = baseOffset + entry.numberOfRecords();
            for (Record record : records.records()) {
                if (record.timestamp() >= target) {
                    match = success(record.offset(), record.timestamp());
                    break;
                }
            }
        } catch (Throwable decodeError) {
            failure = decodeError;
        } finally {
            failure = LogEntryUtils.closeAll(List.of(entry), failure);
        }
        if (failure != null) {
            throw new CompletionException(failure);
        }
        return new EntryProbe(match, baseOffset, headerTimestamp, nextOffset);
    }

    /** The cached window while it is still fresh, otherwise one read from storage. */
    private CompletableFuture<Range> range() {
        Range cached = cachedWindow();
        return cached != null ? CompletableFuture.completedFuture(cached) : refreshedRange(ANY_READ);
    }

    /**
     * The cached window, or {@code null} once it is as old as {@link #OFFSET_RANGE_REFRESH_MS} or
     * was read under a generation that has since been dropped. This is the one place the cache is
     * consulted, so a window read across a drop is refused here rather than being installed and
     * then taken back -- which would leave a moment in which another request could still see it.
     *
     * <p>Timed against the monotonic clock: a wall clock that steps backwards would make a window
     * look young again and leave its staleness unbounded.
     */
    private Range cachedWindow() {
        CachedRange current = cachedRange.get();
        if (current == null
                || current.generation() != rangeGeneration.get()
                || time.hiResClockMs() - current.readAtMs() >= OFFSET_RANGE_REFRESH_MS) {
            return null;
        }
        return widened(current.range());
    }

    /**
     * Reads the window from storage, sharing one read with every caller that asks while it is in
     * flight: a partition read by many consumers at once costs storage one pair of index reads
     * rather than one pair per request.
     *
     * @param minReadSequence the earliest read that may answer this caller, as a value of
     *                        {@link #rangeReads}. {@link #ANY_READ} takes whatever is in flight;
     *                        a caller that must not be answered by a read issued before it asks
     *                        passes the count it saw plus one.
     */
    private CompletableFuture<Range> refreshedRange(long minReadSequence) {
        while (true) {
            RangeRead inFlight = rangeRefresh.get();
            if (inFlight == null) {
                RangeRead read = new RangeRead(rangeReads.incrementAndGet(), new CompletableFuture<>());
                if (rangeRefresh.compareAndSet(null, read)) {
                    startRefresh(read);
                    return read.result().thenApply(this::widened);
                }
                continue;
            }
            if (inFlight.sequence() >= minReadSequence) {
                return inFlight.result().thenApply(this::widened);
            }
            // This read was issued before the caller needed one, so what it reports may predate
            // what the caller is asking about. One read runs at a time, so wait for it -- its
            // failure is its own caller's to report -- and then read again.
            return inFlight.result()
                    .handle((ignored, error) -> null)
                    .thenCompose(ignored -> refreshedRange(minReadSequence));
        }
    }

    private void startRefresh(RangeRead read) {
        long generation = rangeGeneration.get();
        CompletableFuture<Range> window = call(logInstance::getFirstOffset, "Log.getFirstOffset")
                .thenCombine(call(logInstance::getLastOffset, "Log.getLastOffset"), Range::of);
        window.whenComplete((range, error) -> {
            if (error == null) {
                publish(range, generation);
            }
            // Cleared before the waiters run: a caller that asks for the window from inside one of
            // them must be able to start the next read instead of being handed this finished one.
            // A failed read caches nothing, so the next request simply reads again.
            rangeRefresh.compareAndSet(read, null);
            if (error != null) {
                read.result().completeExceptionally(DisklessFutures.unwrap(error));
            } else {
                read.result().complete(range);
            }
        });
    }

    /**
     * Caches one window read under the generation it was issued in. A drop that lands while the
     * read is in flight leaves that generation behind, and {@link #cachedWindow()} refuses the
     * entry -- so an invalidated window is never observable, not even briefly. The check here only
     * saves the work of caching an entry that is already doomed; the refusal is what makes it safe.
     */
    private void publish(Range range, long generation) {
        if (closed.get() || rangeGeneration.get() != generation) {
            return;
        }
        cachedRange.set(new CachedRange(range, time.hiResClockMs(), generation));
    }

    /**
     * One window as storage reported it, widened by any append this partition made since.
     *
     * <p>{@link #lastAppend} is never cleared, so an append that storage has long since reported
     * itself is still offered to every later window. That is harmless because {@link
     * Range#including(LogOffset)} widens only on a strict improvement -- a window already past the
     * append is returned untouched -- and it stays harmless only while storage holds up its end:
     * a non-empty log's last offset never moves backwards. Retention moves a log's <em>first</em>
     * offset, never its last, and an append that completed is one the index carries. Were storage
     * to regress a last offset instead, this retired append would hold the window past the end of
     * the log.
     */
    private Range widened(Range range) {
        LogOffset appended = lastAppend.get();
        return appended == null ? range : range.including(appended);
    }

    private FetchPartitionData fetchData(Errors error, Range range, MemoryRecords records) {
        return new FetchPartitionData(
                error,
                range.highWatermark(),
                range.start(),
                records,
                Optional.empty(),
                OptionalLong.empty(),
                Optional.empty(),
                OptionalInt.empty(),
                false);
    }

    private ListOffsetsPartitionResponse success(long offset, long timestamp) {
        return ListOffsetsPartitionResponse.success(topicIdPartition, offset, timestamp);
    }

    private ListOffsetsPartitionResponse notFound() {
        return ListOffsetsPartitionResponse.success(topicIdPartition, NOT_FOUND_OFFSET, UNKNOWN_TIMESTAMP);
    }

    private void closeQuietly(LogCursor cursor) {
        if (cursor == null) {
            return;
        }
        try {
            cursor.close();
        } catch (Throwable closeError) {
            log.warn("Failed to close the fetch cursor for partition {}", topicIdPartition, closeError);
        }
    }

    private static long highWatermarkOf(LogOffset lastOffset) {
        return lastOffset == null || lastOffset.offset() < 0
                ? 0L
                : lastOffset.offset() + lastOffset.numberOfRecords();
    }

    /** Runs a storage call so that a synchronous throw or a null future becomes a failed future. */
    private static <T> CompletableFuture<T> call(Supplier<CompletableFuture<T>> operation, String description) {
        try {
            CompletableFuture<T> result = operation.get();
            if (result == null) {
                throw new IllegalStateException(description + " returned a null future");
            }
            return result;
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
    }

    /** Exclusive use of one cursor until {@link #release()} caches it again or closes it. */
    private final class CursorLease {

        private final LogCursor cursor;
        private final AtomicBoolean released = new AtomicBoolean();

        private CursorLease(LogCursor cursor) {
            this.cursor = cursor;
        }

        private LogCursor cursor() {
            return cursor;
        }

        private void release() {
            if (!released.compareAndSet(false, true)) {
                return;
            }
            if (!closed.get() && cachedCursor.compareAndSet(null, cursor)) {
                // close() may have emptied the cache between the check and the swap above.
                if (closed.get() && cachedCursor.compareAndSet(cursor, null)) {
                    closeQuietly(cursor);
                }
                return;
            }
            closeQuietly(cursor);
        }
    }

    /** The readable offset window of the log: {@code [start, highWatermark)}, plus the last entry's header. */
    private record Range(long start, long highWatermark, long lastEntryOffset, long lastEntryTimestamp) {

        static Range of(LogOffset first, LogOffset last) {
            long highWatermark = highWatermarkOf(last);
            long start = first == null || first.offset() < 0 ? highWatermark : first.offset();
            return new Range(
                    start,
                    highWatermark,
                    last == null ? NOT_FOUND_OFFSET : last.offset(),
                    last == null ? UNKNOWN_TIMESTAMP : last.timestamp());
        }

        boolean isEmpty() {
            return lastEntryOffset < 0 || start >= highWatermark;
        }

        /** A fetch at this offset is answerable from this window; one outside it is not. */
        boolean covers(long fetchOffset) {
            return fetchOffset >= start && fetchOffset <= highWatermark;
        }

        /**
         * This window widened to hold one append, never narrowed by it: the append is durable, so
         * its records are readable even when this window was read from storage before it landed.
         * A trim is what moves {@code start}, so an append leaves it alone.
         *
         * <p>The high watermark moves on its own, but the last entry's offset and timestamp move
         * only together, as the pair they were read as. MAX_TIMESTAMP answers with that pair, and
         * a pair assembled from two entries -- one entry's offset beside another's timestamp --
         * would name an entry that never existed. The newer entry is the one with the newer write
         * time, since entries are appended in write-timestamp order; equal times fall back to the
         * later offset, so a storage clock too coarse to separate two appends still advances.
         */
        Range including(LogOffset appended) {
            long appendedHighWatermark = highWatermarkOf(appended);
            boolean appendedIsNewer = appended.timestamp() > lastEntryTimestamp
                    || (appended.timestamp() == lastEntryTimestamp && appended.offset() > lastEntryOffset);
            if (appendedHighWatermark <= highWatermark && !appendedIsNewer) {
                return this;
            }
            return new Range(
                    start,
                    Math.max(highWatermark, appendedHighWatermark),
                    appendedIsNewer ? appended.offset() : lastEntryOffset,
                    appendedIsNewer ? appended.timestamp() : lastEntryTimestamp);
        }
    }

    /**
     * One window read from storage, with the monotonic clock reading of that read and the
     * generation it was read under. Both are what {@link #cachedWindow()} checks before serving it.
     */
    private record CachedRange(Range range, long readAtMs, long generation) {
    }

    /** One read of the window in flight, numbered by when it was issued. */
    private record RangeRead(long sequence, CompletableFuture<Range> result) {
    }

    /**
     * The outcome of decoding one entry: a matching record when the entry held one, plus the
     * entry's own header and the offset of the entry that follows it.
     */
    private record EntryProbe(
            ListOffsetsPartitionResponse match,
            long baseOffset,
            long headerTimestamp,
            long nextOffset) {
    }

    /**
     * One timestamp lookup's forward scan: where to read next, how much of its budget is left, and
     * the answer to fall back on when the budget runs out before the record turns up.
     */
    private final class ForwardScan {

        private final long target;
        private final long endOffsetExclusive;
        private long entryOffset;
        private int remaining = MAX_TIMESTAMP_SCAN_ENTRIES;
        private ListOffsetsPartitionResponse firstEntryAtOrAfterTarget;

        private ForwardScan(long entryOffset, long target, long endOffsetExclusive) {
            this.entryOffset = entryOffset;
            this.target = target;
            this.endOffsetExclusive = endOffsetExclusive;
        }

        private long target() {
            return target;
        }

        private long entryOffset() {
            return entryOffset;
        }

        private boolean hasMoreToRead() {
            return remaining > 0 && entryOffset >= 0 && entryOffset < endOffsetExclusive;
        }

        /** Folds one decoded entry in, and reports the record this entry held, if any. */
        private ListOffsetsPartitionResponse accept(EntryProbe probe) {
            remaining--;
            entryOffset = probe.nextOffset();
            if (probe.match() != null) {
                return probe.match();
            }
            if (firstEntryAtOrAfterTarget == null && probe.headerTimestamp() >= target) {
                firstEntryAtOrAfterTarget = success(probe.baseOffset(), probe.headerTimestamp());
            }
            return null;
        }

        /** The answer once the scan stops without having found the record itself. */
        private ListOffsetsPartitionResponse exhaustedAnswer() {
            if (entryOffset < 0 || entryOffset >= endOffsetExclusive) {
                // Every entry that could hold the answer was read: the log has no such record.
                return notFound();
            }
            // The budget ran out first, so answer with the earliest entry written at or after the
            // target rather than reporting an offset the log does hold as missing.
            return firstEntryAtOrAfterTarget != null ? firstEntryAtOrAfterTarget : notFound();
        }
    }
}
