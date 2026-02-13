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
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.storage.diskless.ListOffsetsPartitionRequest;
import org.apache.kafka.storage.diskless.ListOffsetsPartitionResponse;
import org.apache.kafka.storage.diskless.Reader;

import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionBound;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import io.netty.buffer.ByteBuf;
import io.streamnative.ursa.mledger.UrsaPosition;

/**
 * Reader implementation using ManagedLedger for storage.
 * Implements the Reader interface with full read logic.
 */
public class UrsaManagedLedgerReader implements Reader {

    private static final Logger log = LoggerFactory.getLogger(UrsaManagedLedgerReader.class);
    private static final int MAX_ENTRIES_PER_FETCH = 10;
    private static final int FETCH_CURSOR_POOL_SIZE = 4;

    private final UrsaStorageState state;
    private final ConcurrentHashMap<TopicIdPartition, NonDurableCursorPool> fetchCursorPools = new ConcurrentHashMap<>();

    public UrsaManagedLedgerReader(UrsaStorageState state) {
        this.state = state;
    }

    private static final long UNKNOWN_TIMESTAMP = -1L;

    @Override
    public CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> fetch(
            FetchParams params,
            Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos) {

        if (fetchInfos.isEmpty()) {
            return CompletableFuture.completedFuture(Map.of());
        }

        List<CompletableFuture<AbstractMap.SimpleEntry<TopicIdPartition, FetchPartitionData>>> futures =
                fetchInfos.entrySet().stream()
                        .map(entry -> fetchPartition(entry.getKey(), entry.getValue()))
                        .toList();

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]))
                .thenApply(v -> futures.stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue,
                                (oldValue, newValue) -> oldValue,
                                LinkedHashMap::new)));
    }

    private CompletableFuture<AbstractMap.SimpleEntry<TopicIdPartition, FetchPartitionData>> fetchPartition(
            TopicIdPartition tp,
            FetchRequest.PartitionData partitionData) {

        long fetchOffset = partitionData.fetchOffset;
        int maxBytes = partitionData.maxBytes;

        log.debug("Fetching from partition {} at offset {} with maxBytes {} via ManagedLedger", tp, fetchOffset, maxBytes);

        return state.getOrCreateManagedLedger(tp)
                .thenCompose(managedLedger -> getHighWatermark(managedLedger)
                        .thenCompose(highWatermark -> {
                            if (fetchOffset >= highWatermark) {
                                log.debug("fetchOffset {} >= hwm {}, returning empty for partition {}",
                                        fetchOffset, highWatermark, tp);
                                return CompletableFuture.completedFuture(
                                        new AbstractMap.SimpleEntry<>(tp, new FetchPartitionData(
                                                Errors.NONE,
                                                highWatermark,
                                                0,
                                                MemoryRecords.EMPTY,
                                                Optional.empty(),
                                                OptionalLong.empty(),
                                                Optional.empty(),
                                                OptionalInt.empty(),
                                                false
                                        )));
                            }

                            return readRecords(tp, managedLedger, fetchOffset, highWatermark, MAX_ENTRIES_PER_FETCH, maxBytes)
                                    .thenApply(records -> new AbstractMap.SimpleEntry<>(tp, new FetchPartitionData(
                                            Errors.NONE,
                                            highWatermark,
                                            0,
                                            records,
                                            Optional.empty(),
                                            OptionalLong.empty(),
                                            Optional.empty(),
                                            OptionalInt.empty(),
                                            false
                                    )));
                        }))
                .exceptionally(e -> {
                    log.error("Failed to fetch from partition {}", tp, e);
                    Errors error = mapException(e);
                    return new AbstractMap.SimpleEntry<>(tp, createErrorResponse(error));
                });
    }

    private CompletableFuture<Long> getHighWatermark(ManagedLedger managedLedger) {
        try {
            return CompletableFuture.completedFuture(getNextOffsetForUrsa(managedLedger.getLastConfirmedEntry()));
        } catch (Throwable t) {
            return CompletableFuture.failedFuture(t);
        }
    }

    private long getEarliestOffset(ManagedLedger managedLedger) {
        Position firstPosition = managedLedger.getFirstPosition();
        if (isInvalidPosition(firstPosition)) {
            return 0L;
        }
        if (firstPosition.compareTo(PositionFactory.EARLIEST) == 0) {
            return 0L;
        }
        return firstPosition.getEntryId();
    }

    private long getNextOffsetForUrsa(Position position) {
        if (position == null || position.getEntryId() < 0) {
            return 0L;
        }
        if (position instanceof UrsaPosition ursaPosition) {
            return ursaPosition.getEntryId() + Math.max(1, ursaPosition.numMessages());
        }
        return position.getEntryId() + 1;
    }

    private boolean isInvalidPosition(Position position) {
        return position == null || position.getEntryId() < 0;
    }

    private OptionalLong tryGetPublishTime(ByteBuf entryBuffer) {
        try {
            MessageMetadata metadata = Commands.parseMessageMetadata(entryBuffer.duplicate());
            if (metadata.hasPublishTime()) {
                return OptionalLong.of(metadata.getPublishTime());
            }
            return OptionalLong.empty();
        } catch (Exception e) {
            return OptionalLong.empty();
        }
    }

    private long[] findFirstTimestampGe(ByteBuf entryBuffer, long baseOffset, long targetTimestamp) {
        try {
            ByteBuffer kafkaRecords = KafkaEntryFormatter.decode(entryBuffer.duplicate());
            int readableBytes = kafkaRecords.remaining();

            if (readableBytes == 0) {
                return null;
            }

            if (readableBytes >= 8) {
                int pos = kafkaRecords.position();
                kafkaRecords.putLong(pos, baseOffset);
            }

            MemoryRecords records = MemoryRecords.readableRecords(kafkaRecords);

            for (RecordBatch batch : records.batches()) {
                for (Record record : batch) {
                    if (record.timestamp() >= targetTimestamp) {
                        return new long[]{record.timestamp(), record.offset()};
                    }
                }
            }
            return null;
        } catch (Exception e) {
            log.warn("Failed to parse records at offset {} for timestamp search", baseOffset, e);
            return null;
        }
    }

    private CompletableFuture<OptionalLong> readPublishTimeAt(ManagedLedger managedLedger, Position position) {
        ManagedCursor cursor;
        try {
            cursor = managedLedger.newNonDurableCursor(position, "kafka-read-publish-time-" + System.nanoTime());
        } catch (ManagedLedgerException e) {
            return CompletableFuture.failedFuture(e);
        }

        return asyncReadEntries(cursor, 1, Long.MAX_VALUE, position)
                .whenComplete((ignored, error) -> {
                    try {
                        cursor.close();
                    } catch (Exception e) {
                        log.warn("Failed to close publish-time cursor for ledger {}", managedLedger.getName(), e);
                    }
                })
                .thenApply(entries -> {
                    if (entries.isEmpty()) {
                        return OptionalLong.empty();
                    }

                    try {
                        Entry entry = entries.get(0);
                        return tryGetPublishTime(entry.getDataBuffer());
                    } finally {
                        releaseManagedLedgerEntries(entries);
                    }
                });
    }

    private CompletableFuture<MemoryRecords> readRecords(
            TopicIdPartition tp,
            ManagedLedger managedLedger,
            long fetchOffset,
            long maxOffsetExclusive,
            int maxEntries,
            int maxBytes) {

        long ledgerId = managedLedger.getFirstPosition().getLedgerId();
        final Position maxPosition = maxOffsetExclusive > 0
                ? PositionFactory.create(ledgerId, maxOffsetExclusive - 1)
                : PositionFactory.LATEST;

        final Position startPosition = PositionFactory.create(ledgerId, fetchOffset);

        NonDurableCursorPool pool = fetchCursorPools.computeIfAbsent(tp,
                ignored -> new NonDurableCursorPool(managedLedger, fetchCursorNamePrefix(tp), FETCH_CURSOR_POOL_SIZE));

        return pool.acquire(startPosition)
                .thenCompose(lease -> asyncReadEntries(lease.cursor(), maxEntries, maxBytes, maxPosition)
                        .whenComplete((ignored, error) -> lease.close())
                        .thenApply(this::convertManagedLedgerEntriesToMemoryRecords));
    }

    private CompletableFuture<List<Entry>> asyncReadEntries(
            ManagedCursor cursor,
            int maxEntries,
            long maxSizeBytes,
            Position maxPosition) {
        CompletableFuture<List<Entry>> future = new CompletableFuture<>();
        cursor.asyncReadEntries(maxEntries, maxSizeBytes, new AsyncCallbacks.ReadEntriesCallback() {
            @Override
            public void readEntriesComplete(List<Entry> entries, Object ctx) {
                future.complete(entries);
            }

            @Override
            public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }
        }, null, maxPosition);
        return future;
    }

    private MemoryRecords convertManagedLedgerEntriesToMemoryRecords(List<Entry> entries) {
        if (entries.isEmpty()) {
            return MemoryRecords.EMPTY;
        }

        try {
            int totalSize = entries.stream()
                    .mapToInt(e -> e.getDataBuffer().readableBytes())
                    .sum();

            if (totalSize == 0) {
                return MemoryRecords.EMPTY;
            }

            ByteBuffer combined = ByteBuffer.allocate(totalSize);
            for (Entry entry : entries) {
                ByteBuf payload = entry.getDataBuffer();
                if (payload.readableBytes() == 0) {
                    continue;
                }

                long storageOffset = entry.getEntryId();

                ByteBuffer kafkaRecords = KafkaEntryFormatter.decode(payload);
                int readableBytes = kafkaRecords.remaining();
                if (readableBytes == 0) {
                    continue;
                }

                if (readableBytes >= 8) {
                    int pos = kafkaRecords.position();
                    kafkaRecords.putLong(pos, storageOffset);
                }

                combined.put(kafkaRecords);
            }
            combined.flip();
            return MemoryRecords.readableRecords(combined);
        } finally {
            releaseManagedLedgerEntries(entries);
        }
    }

    private void releaseManagedLedgerEntries(List<Entry> entries) {
        for (Entry entry : entries) {
            if (entry != null) {
                try {
                    entry.release();
                } catch (Exception e) {
                    log.warn("Failed to release managed-ledger entry at offset {}", entry.getEntryId(), e);
                }
            }
        }
    }

    @Override
    public CompletableFuture<Map<TopicIdPartition, ListOffsetsPartitionResponse>> listOffsets(
            Map<TopicIdPartition, ListOffsetsPartitionRequest> requests) {

        if (requests.isEmpty()) {
            return CompletableFuture.completedFuture(Map.of());
        }

        List<CompletableFuture<AbstractMap.SimpleEntry<TopicIdPartition, ListOffsetsPartitionResponse>>> futures =
                requests.entrySet().stream()
                        .map(entry -> listOffsetsForPartition(entry.getKey(), entry.getValue()))
                        .toList();

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]))
                .thenApply(v -> futures.stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue,
                                (oldValue, newValue) -> oldValue,
                                LinkedHashMap::new)));
    }

    private CompletableFuture<AbstractMap.SimpleEntry<TopicIdPartition, ListOffsetsPartitionResponse>> listOffsetsForPartition(
            TopicIdPartition tp,
            ListOffsetsPartitionRequest request) {

        long timestamp = request.timestamp();
        log.debug("ListOffsets for partition {} with timestamp {}", tp, timestamp);

        if (timestamp == ListOffsetsPartitionRequest.LATEST_TIERED_TIMESTAMP) {
            return CompletableFuture.completedFuture(
                    new AbstractMap.SimpleEntry<>(tp, ListOffsetsPartitionResponse.success(tp, -1L, -1L)));
        }

        return state.getOrCreateManagedLedger(tp)
                .thenCompose(managedLedger -> {
                    if (timestamp == ListOffsetsPartitionRequest.EARLIEST_TIMESTAMP ||
                            timestamp == ListOffsetsPartitionRequest.EARLIEST_LOCAL_TIMESTAMP) {
                        return handleEarliestTimestamp(tp, managedLedger);
                    }

                    if (timestamp == ListOffsetsPartitionRequest.LATEST_TIMESTAMP) {
                        return handleLatestTimestamp(tp, managedLedger);
                    }

                    if (timestamp == ListOffsetsPartitionRequest.MAX_TIMESTAMP) {
                        return handleMaxTimestamp(tp, managedLedger);
                    }

                    return handleTimestampSearch(tp, managedLedger, timestamp);
                })
                .exceptionally(e -> {
                    log.error("Failed to list offsets for partition {}", tp, e);
                    Errors error = mapException(e);
                    return new AbstractMap.SimpleEntry<>(tp,
                            ListOffsetsPartitionResponse.error(tp, error));
                });
    }

    private CompletableFuture<AbstractMap.SimpleEntry<TopicIdPartition, ListOffsetsPartitionResponse>> handleEarliestTimestamp(
            TopicIdPartition tp, ManagedLedger managedLedger) {

        long offset = getEarliestOffset(managedLedger);
        log.debug("EARLIEST for partition {}: offset={}", tp, offset);
        return CompletableFuture.completedFuture(
                new AbstractMap.SimpleEntry<>(tp, ListOffsetsPartitionResponse.success(tp, offset, UNKNOWN_TIMESTAMP)));
    }

    private CompletableFuture<AbstractMap.SimpleEntry<TopicIdPartition, ListOffsetsPartitionResponse>> handleLatestTimestamp(
            TopicIdPartition tp, ManagedLedger managedLedger) {

        return getHighWatermark(managedLedger).thenApply(highWatermark -> {
            log.debug("LATEST for partition {}: hwm={}", tp, highWatermark);
            return new AbstractMap.SimpleEntry<>(tp,
                    ListOffsetsPartitionResponse.success(tp, highWatermark, UNKNOWN_TIMESTAMP));
        });
    }

    private CompletableFuture<AbstractMap.SimpleEntry<TopicIdPartition, ListOffsetsPartitionResponse>> handleMaxTimestamp(
            TopicIdPartition tp, ManagedLedger managedLedger) {

        Position lastPosition = managedLedger.getLastConfirmedEntry();
        if (isInvalidPosition(lastPosition)) {
            return CompletableFuture.completedFuture(
                    new AbstractMap.SimpleEntry<>(tp, ListOffsetsPartitionResponse.success(tp, -1L, -1L)));
        }

        return readPublishTimeAt(managedLedger, lastPosition)
                .thenApply(publishTimeOpt -> {
                    long ts = publishTimeOpt.orElse(UNKNOWN_TIMESTAMP);
                    // MAX_TIMESTAMP should return the offset of the message with max timestamp.
                    // With UrsaPosition, the last confirmed entry can contain multiple messages, so return the last
                    // message offset in that entry.
                    long lastOffset = getNextOffsetForUrsa(lastPosition) - 1;
                    if (lastOffset < 0) {
                        lastOffset = -1L;
                    }
                    return new AbstractMap.SimpleEntry<>(tp, ListOffsetsPartitionResponse.success(tp, lastOffset, ts));
                });
    }

    private CompletableFuture<AbstractMap.SimpleEntry<TopicIdPartition, ListOffsetsPartitionResponse>> handleTimestampSearch(
            TopicIdPartition tp, ManagedLedger managedLedger, long targetTimestamp) {

        Position lac = managedLedger.getLastConfirmedEntry();
        if (isInvalidPosition(lac) || managedLedger.getNumberOfEntries() == 0) {
            return listOffsetsNotFound(tp);
        }

        Position firstPosition = getFirstPositionForTimestampSearch(managedLedger);
        if (isInvalidPosition(firstPosition)) {
            return listOffsetsNotFound(tp);
        }

        return findStartPositionByPublishTimeAtOrAfter(managedLedger, firstPosition, lac, targetTimestamp)
                .thenCompose(startPosition -> {
                    if (startPosition == null || isInvalidPosition(startPosition) || startPosition.compareTo(lac) > 0) {
                        return listOffsetsNotFound(tp);
                    }

                    return scanForFirstTimestampAtOrAfter(managedLedger, startPosition, lac, targetTimestamp)
                            .thenApply(result -> {
                                if (result == null) {
                                    return new AbstractMap.SimpleEntry<>(tp, ListOffsetsPartitionResponse.success(tp, -1L, -1L));
                                }
                                return new AbstractMap.SimpleEntry<>(tp, ListOffsetsPartitionResponse.success(tp, result[1], result[0]));
                            });
                });
    }

    private CompletableFuture<Position> findStartPositionByPublishTimeAtOrAfter(
            ManagedLedger managedLedger,
            Position startPosition,
            Position lac,
            long targetTimestamp) {
        long entryCount = managedLedger.getNumberOfEntries();
        if (entryCount <= 0) {
            return CompletableFuture.completedFuture(null);
        }

        long maxIndex = entryCount - 1;
        FindFirstEntryByPublishTimeAtOrAfter op = new FindFirstEntryByPublishTimeAtOrAfter(
                managedLedger, startPosition, lac, targetTimestamp, maxIndex);
        return op.find();
    }

    private final class FindFirstEntryByPublishTimeAtOrAfter {
        private final ManagedLedger managedLedger;
        private final Position startPosition;
        private final Position lac;
        private final long targetTimestamp;
        private long low;
        private long high;

        private FindFirstEntryByPublishTimeAtOrAfter(
                ManagedLedger managedLedger,
                Position startPosition,
                Position lac,
                long targetTimestamp,
                long maxIndex) {
            this.managedLedger = managedLedger;
            this.startPosition = startPosition;
            this.lac = lac;
            this.targetTimestamp = targetTimestamp;
            this.low = 0;
            this.high = maxIndex;
        }

        private CompletableFuture<Position> find() {
            if (startPosition.compareTo(lac) > 0) {
                return CompletableFuture.completedFuture(null);
            }

            return readPublishTimeAt(managedLedger, startPosition)
                    .thenCompose(startPublishTimeOpt -> {
                        long startPublishTime = startPublishTimeOpt.orElse(UNKNOWN_TIMESTAMP);
                        if (startPublishTime == UNKNOWN_TIMESTAMP) {
                            return CompletableFuture.completedFuture(startPosition);
                        }
                        if (startPublishTime >= targetTimestamp) {
                            return CompletableFuture.completedFuture(startPosition);
                        }

                        Position endPosition = positionAtIndex(high);
                        if (endPosition == null || isInvalidPosition(endPosition) || endPosition.compareTo(lac) > 0) {
                            return CompletableFuture.completedFuture(startPosition);
                        }

                        return readPublishTimeAt(managedLedger, endPosition).thenCompose(endPublishTimeOpt -> {
                            long endPublishTime = endPublishTimeOpt.orElse(UNKNOWN_TIMESTAMP);
                            if (endPublishTime == UNKNOWN_TIMESTAMP) {
                                return CompletableFuture.completedFuture(startPosition);
                            }
                            if (endPublishTime < targetTimestamp) {
                                // publishTime cannot be used to safely rule out matching record timestamps,
                                // so fall back to scanning from the beginning.
                                return CompletableFuture.completedFuture(startPosition);
                            }
                            // Invariant: publishTime(low) < targetTimestamp <= publishTime(high)
                            return search();
                        });
                    });
        }

        private CompletableFuture<Position> search() {
            if (high <= low + 1) {
                return CompletableFuture.completedFuture(positionAtIndex(high));
            }

            long mid = low + (high - low) / 2;
            Position midPosition = positionAtIndex(mid);
            if (midPosition == null || isInvalidPosition(midPosition) || midPosition.compareTo(lac) > 0) {
                return CompletableFuture.completedFuture(startPosition);
            }

            return readPublishTimeAt(managedLedger, midPosition).thenCompose(midPublishTimeOpt -> {
                long midPublishTime = midPublishTimeOpt.orElse(UNKNOWN_TIMESTAMP);
                if (midPublishTime == UNKNOWN_TIMESTAMP) {
                    return CompletableFuture.completedFuture(startPosition);
                }

                if (midPublishTime >= targetTimestamp) {
                    high = mid;
                } else {
                    low = mid;
                }
                return search();
            });
        }

        private Position positionAtIndex(long index) {
            if (index <= 0) {
                return startPosition;
            }
            return managedLedger.getPositionAfterN(startPosition, index, PositionBound.startExcluded);
        }
    }

    private CompletableFuture<long[]> scanForFirstTimestampAtOrAfter(
            ManagedLedger managedLedger,
            Position startPosition,
            Position lac,
            long targetTimestamp) {
        ManagedCursor cursor;
        try {
            cursor = managedLedger.newNonDurableCursor(startPosition, "kafka-scan-for-timestamp-" + System.nanoTime());
        } catch (ManagedLedgerException e) {
            return CompletableFuture.failedFuture(e);
        }

        return scanCursorForFirstTimestampAtOrAfter(cursor, lac, targetTimestamp)
                .whenComplete((ignored, error) -> {
                    try {
                        cursor.close();
                    } catch (Exception e) {
                        log.warn("Failed to close timestamp-scan cursor for ledger {}", managedLedger.getName(), e);
                    }
                });
    }

    private CompletableFuture<long[]> scanCursorForFirstTimestampAtOrAfter(
            ManagedCursor cursor,
            Position lac,
            long targetTimestamp) {
        return asyncReadEntries(cursor, 1, Long.MAX_VALUE, lac)
                .thenCompose(entries -> {
                    if (entries.isEmpty()) {
                        return CompletableFuture.completedFuture(null);
                    }

                    try {
                        for (Entry entry : entries) {
                            long baseOffset = entry.getEntryId();
                            long[] found = findFirstTimestampGe(entry.getDataBuffer(), baseOffset, targetTimestamp);
                            if (found != null) {
                                return CompletableFuture.completedFuture(found);
                            }
                        }
                    } finally {
                        releaseManagedLedgerEntries(entries);
                    }

                    return scanCursorForFirstTimestampAtOrAfter(cursor, lac, targetTimestamp);
                });
    }

    private CompletableFuture<AbstractMap.SimpleEntry<TopicIdPartition, ListOffsetsPartitionResponse>> listOffsetsNotFound(
            TopicIdPartition tp) {
        return CompletableFuture.completedFuture(
                new AbstractMap.SimpleEntry<>(tp, ListOffsetsPartitionResponse.success(tp, -1L, -1L)));
    }

    private Position getFirstPositionForTimestampSearch(ManagedLedger managedLedger) {
        Position firstPosition = managedLedger.getFirstPosition();
        if (isInvalidPosition(firstPosition)) {
            return managedLedger.getNextValidPosition(firstPosition);
        }
        return firstPosition;
    }

    private Errors mapException(Throwable e) {
        Throwable cause = unwrapCompletionException(e);
        String className = cause.getClass().getSimpleName();

        if (className.contains("StreamNotFound") || className.contains("NotFound")) {
            return Errors.UNKNOWN_TOPIC_OR_PARTITION;
        }
        if (className.contains("OffsetOutOfRange")) {
            return Errors.OFFSET_OUT_OF_RANGE;
        }

        return Errors.KAFKA_STORAGE_ERROR;
    }

    private Throwable unwrapCompletionException(Throwable e) {
        Throwable cause = e;
        while (cause.getCause() != null && cause instanceof java.util.concurrent.CompletionException) {
            cause = cause.getCause();
        }
        return cause;
    }

    private FetchPartitionData createErrorResponse(Errors error) {
        return new FetchPartitionData(
                error,
                -1,
                -1,
                MemoryRecords.EMPTY,
                Optional.empty(),
                OptionalLong.empty(),
                Optional.empty(),
                OptionalInt.empty(),
                false
        );
    }

    @Override
    public void close() throws IOException {
        for (NonDurableCursorPool pool : fetchCursorPools.values()) {
            try {
                pool.close();
            } catch (Exception e) {
                log.warn("Failed to close fetch cursor pool", e);
            }
        }
        fetchCursorPools.clear();
    }

    @Override
    public void cleanupPartition(TopicIdPartition tp) {
        if (tp == null) {
            return;
        }
        NonDurableCursorPool pool = fetchCursorPools.remove(tp);
        if (pool != null) {
            try {
                pool.close();
            } catch (Exception e) {
                log.warn("Failed to close fetch cursor pool for partition {}", tp, e);
            }
        }
    }

    private static String fetchCursorNamePrefix(TopicIdPartition tp) {
        return "kafka-fetch-" + tp.topic() + "-partition-" + tp.partition() + "-cursor";
    }
}
