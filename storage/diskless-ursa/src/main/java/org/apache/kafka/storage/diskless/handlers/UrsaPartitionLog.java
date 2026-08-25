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
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.storage.diskless.ListOffsetsPartitionRequest;
import org.apache.kafka.storage.diskless.ListOffsetsPartitionResponse;
import org.apache.kafka.storage.diskless.handlers.RecordAnalyzer.RecordAnalysisResult;
import org.apache.kafka.storage.diskless.idempotent.ProducerStateManager;

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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import io.netty.buffer.ByteBuf;
import io.oxia.client.api.AsyncOxiaClient;
import io.streamnative.ursa.mledger.UrsaPosition;

final class UrsaPartitionLog {

    private static final Logger log = LoggerFactory.getLogger(UrsaPartitionLog.class);
    private static final long UNKNOWN_TIMESTAMP = -1L;
    private static final int MAX_ENTRIES_PER_FETCH = 10;
    private static final int FETCH_CURSOR_POOL_SIZE = 4;

    private final TopicIdPartition topicIdPartition;
    private final UrsaStorageState state;
    private final Supplier<AsyncOxiaClient> oxiaClientSupplier;
    private final long producerStateSnapshotIntervalMs;
    private final int producerStateSnapshotRecordThreshold;
    private final ScheduledExecutorService producerStateScheduler;
    private final DisklessLogMetrics logMetrics;
    private final CompletableFuture<ManagedLedger> initFuture;
    private final PartitionWriteSequencer writeSequencer;
    private volatile NonDurableCursorPool fetchCursorPool;
    private volatile boolean closed;
    private final ConcurrentHashMap<String, ProducerStateManager> producerStateManagers = new ConcurrentHashMap<>();

    UrsaPartitionLog(TopicIdPartition topicIdPartition,
                     UrsaStorageState state,
                     DisklessLogMetrics logMetrics,
                     CompletableFuture<ManagedLedger> managedLedgerFuture,
                     Supplier<AsyncOxiaClient> oxiaClientSupplier,
                     long producerStateSnapshotIntervalMs,
                     int producerStateSnapshotRecordThreshold,
                     ScheduledExecutorService producerStateScheduler) {
        this.topicIdPartition = topicIdPartition;
        this.state = state;
        this.logMetrics = logMetrics;
        this.oxiaClientSupplier = oxiaClientSupplier;
        this.producerStateSnapshotIntervalMs = producerStateSnapshotIntervalMs;
        this.producerStateSnapshotRecordThreshold = producerStateSnapshotRecordThreshold;
        this.producerStateScheduler = producerStateScheduler;
        this.writeSequencer = new PartitionWriteSequencer(topicIdPartition.toString());
        this.closed = false;
        this.initFuture = createInitFuture(managedLedgerFuture);
    }

    ProducerStateManager getOrCreateProducerStateManager(String zone) {
        return producerStateManagers.computeIfAbsent(zone, zoneId -> new ProducerStateManager(
                topicIdPartition,
                oxiaClientSupplier,
                () -> initFuture,
                zoneId,
                producerStateSnapshotIntervalMs,
                producerStateSnapshotRecordThreshold,
                producerStateScheduler));
    }

    void installProducerStateManager(String zone, ProducerStateManager producerStateManager) {
        producerStateManagers.put(zone, producerStateManager);
    }

    CompletableFuture<PartitionResponse> write(MemoryRecords records, String zone, String writerName) {
        log.debug("Writing {} bytes to partition {} via {}", records.sizeInBytes(), topicIdPartition, writerName);

        if (records.sizeInBytes() == 0) {
            return CompletableFuture.completedFuture(new PartitionResponse(Errors.NONE));
        }

        boolean hasProducerId = false;
        for (RecordBatch batch : records.batches()) {
            if (batch.isTransactional()) {
                log.warn("Transactional produce rejected for partition {}", topicIdPartition);
                return CompletableFuture.completedFuture(new PartitionResponse(Errors.INVALID_REQUEST));
            }
            if (batch.hasProducerId()) {
                hasProducerId = true;
            }
        }

        RecordAnalysisResult analysisResult;
        try {
            analysisResult = RecordAnalyzer.analyzeAndValidateRecords(
                    records,
                    new TopicPartition(topicIdPartition.topic(), topicIdPartition.partition()),
                    0
            );
        } catch (Exception e) {
            log.warn("Record validation failed for partition {}: {}", topicIdPartition, e.getMessage());
            return CompletableFuture.completedFuture(new PartitionResponse(Errors.INVALID_RECORD));
        }

        if (analysisResult.validBytes() <= 0) {
            return CompletableFuture.completedFuture(new PartitionResponse(Errors.NONE));
        }

        final boolean idempotent = hasProducerId;
        return writeSequencer.submit(() -> writeValidated(records, analysisResult, idempotent, zone));
    }

    private PartitionWriteSequencer.WriteTask<PartitionResponse> writeValidated(
            MemoryRecords records,
            RecordAnalysisResult analysisResult,
            boolean idempotent,
            String zone) {
        CompletableFuture<Void> submissionFuture = new CompletableFuture<>();
        CompletableFuture<PartitionResponse> result = idempotent
                ? appendIdempotentRecords(records, analysisResult, zone, submissionFuture)
                : appendNonIdempotentRecords(records, analysisResult, submissionFuture);
        CompletableFuture<PartitionResponse> mappedResult = result.exceptionally(this::writeErrorResponse);
        return new PartitionWriteSequencer.WriteTask<>(submissionFuture, mappedResult);
    }

    void invalidate() {
        // TODO: Fail queued writes fast after invalidation instead of letting already-enqueued requests
        //  drain through the stale sequencer and independently discover the closed/fenced ledger.
        state.removePartitionLog(topicIdPartition, this);
        close(false);
    }

    CompletableFuture<FetchPartitionData> fetch(
            FetchRequest.PartitionData partitionData) {
        long fetchOffset = partitionData.fetchOffset;
        int maxBytes = partitionData.maxBytes;

        log.debug("Fetching from partition {} at offset {} with maxBytes {}",
                topicIdPartition, fetchOffset, maxBytes);

        return initialized().thenCompose(managedLedger -> getHighWatermark(managedLedger)
                        .thenCompose(highWatermark -> {
                            if (fetchOffset >= highWatermark) {
                                log.debug("fetchOffset {} >= hwm {}, returning empty for partition {}",
                                        fetchOffset, highWatermark, topicIdPartition);
                                return CompletableFuture.completedFuture(new FetchPartitionData(
                                        Errors.NONE,
                                        highWatermark,
                                        0,
                                        MemoryRecords.EMPTY,
                                        Optional.empty(),
                                        OptionalLong.empty(),
                                        Optional.empty(),
                                        OptionalInt.empty(),
                                        false
                                ));
                            }

                            return readRecords(managedLedger, fetchOffset, highWatermark, MAX_ENTRIES_PER_FETCH, maxBytes)
                                    .thenApply(records -> new FetchPartitionData(
                                            Errors.NONE,
                                            highWatermark,
                                            0,
                                            records,
                                            Optional.empty(),
                                            OptionalLong.empty(),
                                            Optional.empty(),
                                            OptionalInt.empty(),
                                            false
                                    ));
                        }))
                .exceptionally(error -> createFetchErrorResponse(mapException(error)));
    }

    CompletableFuture<ListOffsetsPartitionResponse> listOffsets(ListOffsetsPartitionRequest request) {
        long timestamp = request.timestamp();
        log.debug("ListOffsets for partition {} with timestamp {}", topicIdPartition, timestamp);

        if (timestamp == ListOffsetsPartitionRequest.LATEST_TIERED_TIMESTAMP) {
            return CompletableFuture.completedFuture(ListOffsetsPartitionResponse.success(topicIdPartition, -1L, -1L));
        }

        return initialized().thenCompose(managedLedger -> {
            if (timestamp == ListOffsetsPartitionRequest.EARLIEST_TIMESTAMP
                    || timestamp == ListOffsetsPartitionRequest.EARLIEST_LOCAL_TIMESTAMP) {
                return handleEarliestTimestamp(managedLedger);
            }
            if (timestamp == ListOffsetsPartitionRequest.LATEST_TIMESTAMP) {
                return handleLatestTimestamp(managedLedger);
            }
            if (timestamp == ListOffsetsPartitionRequest.MAX_TIMESTAMP) {
                return handleMaxTimestamp(managedLedger);
            }
            return handleTimestampSearch(managedLedger, timestamp);
        }).exceptionally(error -> ListOffsetsPartitionResponse.error(topicIdPartition, mapException(error)));
    }

    CompletableFuture<PartitionResponse> appendIdempotentRecords(
            MemoryRecords records,
            RecordAnalysisResult analysisResult,
            String zone,
            CompletableFuture<Void> submissionFuture) {
        ProducerStateManager producerStateManager = getOrCreateProducerStateManager(zone);
        List<ProducerStateManager.AppendBatch> appendBatches = buildAppendBatches(records);
        if (appendBatches.isEmpty()) {
            submissionFuture.complete(null);
            return CompletableFuture.completedFuture(new PartitionResponse(Errors.NONE));
        }

        return producerStateManager.prepareAppend(appendBatches)
                .thenCompose(prepareResult -> {
                    if (prepareResult instanceof ProducerStateManager.InvalidEpoch invalidEpoch) {
                        submissionFuture.complete(null);
                        return CompletableFuture.completedFuture(
                                new PartitionResponse(Errors.INVALID_PRODUCER_EPOCH, invalidEpoch.message()));
                    }
                    if (prepareResult instanceof ProducerStateManager.OutOfOrderSequence outOfOrderSequence) {
                        submissionFuture.complete(null);
                        return CompletableFuture.completedFuture(
                                new PartitionResponse(Errors.OUT_OF_ORDER_SEQUENCE_NUMBER, outOfOrderSequence.message()));
                    }
                    if (prepareResult instanceof ProducerStateManager.Duplicate duplicate) {
                        submissionFuture.complete(null);
                        return duplicate.appendResultFuture()
                                .thenApply(appendResult ->
                                        new PartitionResponse(Errors.NONE, appendResult.baseOffset(), appendResult.timestamp(), 0L))
                                .exceptionally(this::writeErrorResponse);
                    }
                    if (prepareResult instanceof ProducerStateManager.Ready ready) {
                        return appendPreparedBatches(
                                records, analysisResult, producerStateManager, ready.pendingAppend(), submissionFuture);
                    }
                    submissionFuture.complete(null);
                    return CompletableFuture.failedFuture(
                            new IllegalStateException("Unexpected prepare result: " + prepareResult));
                })
                .whenComplete((response, error) -> {
                    if (error != null) {
                        submissionFuture.complete(null);
                    }
                });
    }

    private CompletableFuture<PartitionResponse> appendPreparedBatches(
            MemoryRecords records,
            RecordAnalysisResult analysisResult,
            ProducerStateManager producerStateManager,
            ProducerStateManager.PendingAppend pendingAppend,
            CompletableFuture<Void> submissionFuture) {
        CompletableFuture<PartitionResponse> result = new CompletableFuture<>();

        initialized().whenComplete((managedLedger, managedLedgerError) -> {
            try {
                if (managedLedgerError != null) {
                    producerStateManager.abortAppend(pendingAppend, managedLedgerError);
                    submissionFuture.complete(null);
                    result.completeExceptionally(managedLedgerError);
                    return;
                }

                ByteBuf data = KafkaEntryFormatter.encode(records, analysisResult);
                int dataSize = data.readableBytes();

                log.debug("Appending {} records ({} bytes) to managed ledger {} for partition {}, analysisResult: {}",
                        analysisResult.recordCount(), dataSize, managedLedger.getName(), topicIdPartition, analysisResult);

                CompletableFuture<Position> addFuture;
                try {
                    addFuture = asyncAddEntry(managedLedger, data, analysisResult.recordCount());
                } catch (Throwable appendInitError) {
                    data.release();
                    producerStateManager.abortAppend(pendingAppend, appendInitError);
                    submissionFuture.complete(null);
                    result.completeExceptionally(appendInitError);
                    return;
                }

                submissionFuture.complete(null);

                addFuture.whenComplete((position, appendError) -> {
                    try {
                        if (appendError != null) {
                            producerStateManager.abortAppend(pendingAppend, appendError);
                            result.completeExceptionally(appendError);
                            return;
                        }

                        long appendTimestamp = state.time().milliseconds();
                        ProducerStateManager.AppendResult appendResult =
                                producerStateManager.completeAppend(pendingAppend, position.getEntryId(), appendTimestamp);
                        result.complete(new PartitionResponse(
                                Errors.NONE,
                                appendResult.baseOffset(),
                                appendResult.timestamp(),
                                0L));
                    } catch (Throwable completeError) {
                        producerStateManager.abortAppend(pendingAppend, completeError);
                        result.completeExceptionally(completeError);
                    } finally {
                        data.release();
                    }
                });
            } catch (Throwable callbackError) {
                producerStateManager.abortAppend(pendingAppend, callbackError);
                submissionFuture.complete(null);
                result.completeExceptionally(callbackError);
            }
        });

        return result;
    }

    CompletableFuture<PartitionResponse> appendNonIdempotentRecords(
            MemoryRecords records,
            RecordAnalysisResult analysisResult,
            CompletableFuture<Void> submissionFuture) {
        CompletableFuture<PartitionResponse> result = new CompletableFuture<>();

        initialized().whenComplete((managedLedger, managedLedgerError) -> {
            try {
                if (managedLedgerError != null) {
                    submissionFuture.complete(null);
                    result.completeExceptionally(managedLedgerError);
                    return;
                }

                ByteBuf data = KafkaEntryFormatter.encode(records, analysisResult);
                int dataSize = data.readableBytes();

                log.debug("Appending {} records ({} bytes) to managed ledger {} for partition {}, analysisResult: {}",
                        analysisResult.recordCount(), dataSize, managedLedger.getName(), topicIdPartition, analysisResult);

                CompletableFuture<Position> addFuture;
                try {
                    addFuture = asyncAddEntry(managedLedger, data, analysisResult.recordCount());
                } catch (Throwable appendInitError) {
                    data.release();
                    submissionFuture.complete(null);
                    result.completeExceptionally(appendInitError);
                    return;
                }

                submissionFuture.complete(null);

                addFuture.whenComplete((position, appendError) -> {
                    try {
                        if (appendError != null) {
                            result.completeExceptionally(appendError);
                            return;
                        }

                        long appendTimestamp = state.time().milliseconds();
                        result.complete(new PartitionResponse(
                                Errors.NONE,
                                position.getEntryId(),
                                appendTimestamp,
                                0L));
                    } finally {
                        data.release();
                    }
                });
            } catch (Throwable callbackError) {
                submissionFuture.complete(null);
                result.completeExceptionally(callbackError);
            }
        });

        return result;
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
            ManagedLedger managedLedger,
            long fetchOffset,
            long maxOffsetExclusive,
            int maxEntries,
            int maxBytes) {
        long ledgerId = managedLedger.getFirstPosition().getLedgerId();
        Position maxPosition = maxOffsetExclusive > 0
                ? PositionFactory.create(ledgerId, maxOffsetExclusive - 1)
                : PositionFactory.LATEST;
        Position startPosition = PositionFactory.create(ledgerId, fetchOffset);
        if (fetchCursorPool == null) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException("Fetch cursor pool is not initialized for " + topicIdPartition));
        }

        return fetchCursorPool.acquire(startPosition)
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
            int totalSize = entries.stream().mapToInt(entry -> entry.getDataBuffer().readableBytes()).sum();
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
            if (entry == null) {
                continue;
            }
            try {
                entry.release();
            } catch (Exception e) {
                log.warn("Failed to release managed-ledger entry at offset {}", entry.getEntryId(), e);
            }
        }
    }

    private CompletableFuture<ListOffsetsPartitionResponse> handleEarliestTimestamp(ManagedLedger managedLedger) {
        long offset = getEarliestOffset(managedLedger);
        log.debug("EARLIEST for partition {}: offset={}", topicIdPartition, offset);
        return CompletableFuture.completedFuture(
                ListOffsetsPartitionResponse.success(topicIdPartition, offset, UNKNOWN_TIMESTAMP));
    }

    private CompletableFuture<ListOffsetsPartitionResponse> handleLatestTimestamp(ManagedLedger managedLedger) {
        return getHighWatermark(managedLedger).thenApply(highWatermark -> {
            log.debug("LATEST for partition {}: hwm={}", topicIdPartition, highWatermark);
            return ListOffsetsPartitionResponse.success(topicIdPartition, highWatermark, UNKNOWN_TIMESTAMP);
        });
    }

    private CompletableFuture<ListOffsetsPartitionResponse> handleMaxTimestamp(ManagedLedger managedLedger) {
        Position lastPosition = managedLedger.getLastConfirmedEntry();
        if (isInvalidPosition(lastPosition)) {
            return CompletableFuture.completedFuture(ListOffsetsPartitionResponse.success(topicIdPartition, -1L, -1L));
        }

        return readPublishTimeAt(managedLedger, lastPosition)
                .thenApply(publishTimeOpt -> {
                    long ts = publishTimeOpt.orElse(UNKNOWN_TIMESTAMP);
                    long lastOffset = getNextOffsetForUrsa(lastPosition) - 1;
                    if (lastOffset < 0) {
                        lastOffset = -1L;
                    }
                    return ListOffsetsPartitionResponse.success(topicIdPartition, lastOffset, ts);
                });
    }

    private CompletableFuture<ListOffsetsPartitionResponse> handleTimestampSearch(
            ManagedLedger managedLedger,
            long targetTimestamp) {
        Position lac = managedLedger.getLastConfirmedEntry();
        if (isInvalidPosition(lac) || managedLedger.getNumberOfEntries() == 0) {
            return listOffsetsNotFound();
        }

        Position firstPosition = getFirstPositionForTimestampSearch(managedLedger);
        if (isInvalidPosition(firstPosition)) {
            return listOffsetsNotFound();
        }

        return findStartPositionByPublishTimeAtOrAfter(managedLedger, firstPosition, lac, targetTimestamp)
                .thenCompose(startPosition -> {
                    if (startPosition == null || isInvalidPosition(startPosition) || startPosition.compareTo(lac) > 0) {
                        return listOffsetsNotFound();
                    }

                    return scanForFirstTimestampAtOrAfter(managedLedger, startPosition, lac, targetTimestamp)
                            .thenApply(result -> result == null
                                    ? ListOffsetsPartitionResponse.success(topicIdPartition, -1L, -1L)
                                    : ListOffsetsPartitionResponse.success(topicIdPartition, result[1], result[0]));
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
        return new FindFirstEntryByPublishTimeAtOrAfter(managedLedger, startPosition, lac, targetTimestamp, maxIndex).find();
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
                            long[] found = findFirstTimestampGe(entry.getDataBuffer(), entry.getEntryId(), targetTimestamp);
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

    private CompletableFuture<ListOffsetsPartitionResponse> listOffsetsNotFound() {
        return CompletableFuture.completedFuture(ListOffsetsPartitionResponse.success(topicIdPartition, -1L, -1L));
    }

    private Position getFirstPositionForTimestampSearch(ManagedLedger managedLedger) {
        Position firstPosition = managedLedger.getFirstPosition();
        if (isInvalidPosition(firstPosition)) {
            return managedLedger.getNextValidPosition(firstPosition);
        }
        return firstPosition;
    }

    private Errors mapException(Throwable error) {
        Throwable cause = unwrapCompletionException(error);
        if (cause instanceof NotLeaderOrFollowerException) {
            invalidate();
            return Errors.NOT_LEADER_OR_FOLLOWER;
        }
        if (cause instanceof ManagedLedgerException managedLedgerException && isClosed(managedLedgerException)) {
            invalidate();
            return Errors.NOT_LEADER_OR_FOLLOWER;
        }
        if (isNotFound(cause)) {
            invalidate();
            return Errors.UNKNOWN_TOPIC_OR_PARTITION;
        }
        return Errors.KAFKA_STORAGE_ERROR;
    }

    private PartitionResponse writeErrorResponse(Throwable error) {
        Throwable cause = unwrapCompletionException(error);
        if (cause instanceof NotLeaderOrFollowerException) {
            invalidate();
            log.info("Partition log is no longer local owner for partition {}", topicIdPartition, cause);
            return new PartitionResponse(Errors.NOT_LEADER_OR_FOLLOWER);
        }
        if (cause instanceof ManagedLedgerException managedLedgerException && isClosed(managedLedgerException)) {
            invalidate();
            log.info("Managed ledger is already closed for partition {}", topicIdPartition, cause);
            return new PartitionResponse(Errors.NOT_LEADER_OR_FOLLOWER);
        }

        log.error("Failed to write to partition {}", topicIdPartition, error);
        return new PartitionResponse(Errors.KAFKA_STORAGE_ERROR);
    }

    private Throwable unwrapCompletionException(Throwable error) {
        Throwable cause = error;
        while (cause.getCause() != null && cause instanceof CompletionException) {
            cause = cause.getCause();
        }
        return cause;
    }

    static boolean isClosed(ManagedLedgerException exception) {
        String message = exception.getMessage();
        return "Already closed".equals(message)
                || exception instanceof ManagedLedgerException.ManagedLedgerAlreadyClosedException
                || exception instanceof ManagedLedgerException.ManagedLedgerFencedException;
    }

    private static boolean isNotFound(Throwable error) {
        return error instanceof ManagedLedgerException.ManagedLedgerNotFoundException
                || error instanceof ManagedLedgerException.MetadataNotFoundException;
    }

    private FetchPartitionData createFetchErrorResponse(Errors error) {
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

    boolean cleanupNonOwnedProducerStates(Set<String> ownedZones, boolean deletePartition) {
        boolean cleaned = false;
        for (String zone : snapshotProducerStateZones()) {
            if (ownedZones.contains(zone)) {
                continue;
            }
            cleaned = cleanupProducerState(zone, deletePartition) || cleaned;
        }
        return cleaned;
    }

    boolean cleanup(boolean deletePartition) {
        boolean cleaned = false;
        for (String zone : snapshotProducerStateZones()) {
            cleaned = cleanupProducerState(zone, deletePartition) || cleaned;
        }
        return cleaned;
    }

    Set<String> snapshotProducerStateZones() {
        return new LinkedHashSet<>(producerStateManagers.keySet());
    }

    void close() {
        close(false);
    }

    void close(boolean deletePartition) {
        closed = true;
        cleanupWriteState();
        cleanupGlobalState();
        closeManagedLedger();
        cleanup(deletePartition);
    }

    void cleanupWriteState() {
        writeSequencer.reset();
    }

    void cleanupReadState() {
        closeFetchCursorPool();
    }

    boolean cleanupGlobalState() {
        boolean cleaned = logMetrics.remove(topicIdPartition);
        return closeFetchCursorPool() || cleaned;
    }

    private boolean cleanupProducerState(String zone, boolean deletePartition) {
        ProducerStateManager producerStateManager = producerStateManagers.remove(zone);
        if (producerStateManager == null) {
            return false;
        }

        producerStateManager.cleanup(deletePartition).exceptionally(error -> {
            log.warn("Failed to cleanup producer state manager for partition {} and zone {}",
                    topicIdPartition, zone, error);
            return null;
        });
        return true;
    }

    private CompletableFuture<Position> asyncAddEntry(ManagedLedger managedLedger, ByteBuf data, int numberOfMessages) {
        CompletableFuture<Position> future = new CompletableFuture<>();
        managedLedger.asyncAddEntry(data, numberOfMessages, new AsyncCallbacks.AddEntryCallback() {
            @Override
            public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                future.complete(position);
            }

            @Override
            public void addFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }
        }, null);
        return future;
    }

    private CompletableFuture<ManagedLedger> createInitFuture(CompletableFuture<ManagedLedger> managedLedgerFuture) {
        CompletableFuture<ManagedLedger> initFuture = new CompletableFuture<>();
        managedLedgerFuture.whenComplete((ledger, error) -> {
            if (error != null) {
                if (closed) {
                    initFuture.completeExceptionally(new NotLeaderOrFollowerException("Partition log already closed"));
                } else {
                    log.warn("Failed to open ManagedLedger for partition {}, evicting from cache",
                            topicIdPartition, error);
                    cleanupWriteState();
                    cleanupReadState();
                    cleanup(false);
                    state.removePartitionLog(topicIdPartition, this);
                    initFuture.completeExceptionally(error);
                }
                return;
            }

            if (closed) {
                closeManagedLedgerQuietly(ledger);
                initFuture.completeExceptionally(new NotLeaderOrFollowerException("Partition log already closed"));
                return;
            }

            UrsaPartitionLog activePartitionLog = state.partitionLog(topicIdPartition);
            if (activePartitionLog != null && activePartitionLog != this) {
                closeManagedLedgerQuietly(ledger);
                initFuture.completeExceptionally(new NotLeaderOrFollowerException("Partition log already replaced"));
                return;
            }

            try {
                initGlobalState(ledger);
                initFuture.complete(ledger);
            } catch (Throwable activationError) {
                closeManagedLedgerQuietly(ledger);
                initFuture.completeExceptionally(activationError);
            }
        });
        return initFuture;
    }

    private CompletableFuture<ManagedLedger> initialized() {
        return initFuture;
    }

    private void initGlobalState(ManagedLedger ledger) {
        boolean created = false;
        synchronized (this) {
            if (fetchCursorPool == null) {
                fetchCursorPool = new NonDurableCursorPool(
                        ledger,
                        fetchCursorNamePrefix(topicIdPartition),
                        FETCH_CURSOR_POOL_SIZE);
                created = true;
            }
        }
        if (created) {
            logMetrics.register(topicIdPartition, ledger);
        }
    }

    private void closeManagedLedger() {
        initialized().whenComplete((ledger, error) -> {
            if (ledger == null) {
                return;
            }
            closeManagedLedgerQuietly(ledger);
        });
    }

    private void closeManagedLedgerQuietly(ManagedLedger ledger) {
        try {
            ledger.close();
        } catch (Exception e) {
            log.warn("Failed to close ManagedLedger for partition {}", topicIdPartition, e);
        }
    }

    private boolean closeFetchCursorPool() {
        NonDurableCursorPool pool;
        synchronized (this) {
            pool = fetchCursorPool;
            fetchCursorPool = null;
        }
        if (pool == null) {
            return false;
        }
        try {
            pool.close();
        } catch (Exception e) {
            log.warn("Failed to close fetch cursor pool for partition {}", topicIdPartition, e);
        }
        return true;
    }

    private static List<ProducerStateManager.AppendBatch> buildAppendBatches(MemoryRecords records) {
        List<ProducerStateManager.AppendBatch> appendBatches = new ArrayList<>();
        for (RecordBatch batch : records.batches()) {
            if (!batch.hasProducerId()) {
                continue;
            }
            int recordCount = batch.countOrNull() != null ? batch.countOrNull() : 0;
            appendBatches.add(new ProducerStateManager.AppendBatch(
                    batch.producerId(),
                    batch.producerEpoch(),
                    batch.baseSequence(),
                    batch.lastSequence(),
                    recordCount,
                    batch.maxTimestamp()
            ));
        }
        return appendBatches;
    }

    private static String fetchCursorNamePrefix(TopicIdPartition tp) {
        return "kafka-fetch-" + tp.topic() + "-partition-" + tp.partition() + "-cursor";
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
                        if (startPublishTime == UNKNOWN_TIMESTAMP || startPublishTime >= targetTimestamp) {
                            return CompletableFuture.completedFuture(startPosition);
                        }

                        Position endPosition = positionAtIndex(high);
                        if (endPosition == null || isInvalidPosition(endPosition) || endPosition.compareTo(lac) > 0) {
                            return CompletableFuture.completedFuture(startPosition);
                        }

                        return readPublishTimeAt(managedLedger, endPosition).thenCompose(endPublishTimeOpt -> {
                            long endPublishTime = endPublishTimeOpt.orElse(UNKNOWN_TIMESTAMP);
                            if (endPublishTime == UNKNOWN_TIMESTAMP || endPublishTime < targetTimestamp) {
                                return CompletableFuture.completedFuture(startPosition);
                            }
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
}
