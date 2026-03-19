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
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.storage.diskless.Writer;
import org.apache.kafka.storage.diskless.handlers.RecordAnalyzer.RecordAnalysisResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Abstract base class for Ursa storage writers.
 * Consolidates common logic for both StorageApi and ManagedLedger implementations.
 */
public abstract class AbstractUrsaStorageWriter implements Writer {

    private static final Logger log = LoggerFactory.getLogger(AbstractUrsaStorageWriter.class);
    private static final CompletableFuture<Void> COMPLETED = CompletableFuture.completedFuture(null);
    private static final Executor SEQUENCED_WRITE_EXECUTOR = ForkJoinPool.commonPool();

    protected final UrsaStorageState state;
    private final ConcurrentHashMap<TopicIdPartition, CompletableFuture<Void>> partitionWriteTails =
            new ConcurrentHashMap<>();

    protected AbstractUrsaStorageWriter(UrsaStorageState state) {
        this.state = state;
    }

    /**
     * Returns the name of this writer implementation for logging purposes.
     */
    protected abstract String writerName();

    /**
     * Performs the actual append for idempotent writes.
     * Implementations should encode records, append to storage, and update caches.
     *
     * @param tp the partition to append to
     * @param records the records to append
     * @param analysisResult the analysis result for the records
     * @return a handle containing submission and completion futures
     */
    protected abstract SequencedWrite<PartitionResponse> performIdempotentAppend(
            TopicIdPartition tp,
            MemoryRecords records,
            RecordAnalysisResult analysisResult);

    /**
     * Performs the actual append for non-idempotent writes.
     * Implementations should encode records and append to storage after validation has completed.
     *
     * @param tp the partition to append to
     * @param records the records to append
     * @param analysisResult the analysis result for the records
     * @return a handle containing submission and completion futures
     */
    protected abstract SequencedWrite<PartitionResponse> performNonIdempotentAppend(
            TopicIdPartition tp,
            MemoryRecords records,
            RecordAnalysisResult analysisResult);

    @Override
    public CompletableFuture<Map<TopicIdPartition, PartitionResponse>> write(
            Map<TopicIdPartition, MemoryRecords> entriesPerPartition) {

        log.debug("Writing to {} partitions via {}", entriesPerPartition.size(), writerName());

        if (entriesPerPartition.isEmpty()) {
            return CompletableFuture.completedFuture(Map.of());
        }

        List<CompletableFuture<AbstractMap.SimpleEntry<TopicIdPartition, PartitionResponse>>> futures =
                entriesPerPartition.entrySet().stream()
                        .map(entry -> writePartition(entry.getKey(), entry.getValue()))
                        .toList();

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]))
                .thenApply(v -> {
                    Map<TopicIdPartition, PartitionResponse> result = futures.stream()
                            .map(CompletableFuture::join)
                            .collect(Collectors.toMap(
                                    Map.Entry::getKey,
                                    Map.Entry::getValue,
                                    (oldValue, newValue) -> oldValue,
                                    LinkedHashMap::new));
                    log.debug("Completed writing to {} partitions via {}", result.size(), writerName());
                    return result;
                });
    }

    private CompletableFuture<AbstractMap.SimpleEntry<TopicIdPartition, PartitionResponse>> writePartition(
            TopicIdPartition tp,
            MemoryRecords records) {
        log.debug("Writing {} bytes to partition {} via {}", records.sizeInBytes(), tp, writerName());

        if (records.sizeInBytes() == 0) {
            return CompletableFuture.completedFuture(
                    new AbstractMap.SimpleEntry<>(tp, new PartitionResponse(Errors.NONE)));
        }

        boolean hasProducerId = false;
        for (RecordBatch batch : records.batches()) {
            if (batch.isTransactional()) {
                log.warn("Transactional produce rejected for partition {}", tp);
                return CompletableFuture.completedFuture(
                        new AbstractMap.SimpleEntry<>(tp, new PartitionResponse(Errors.INVALID_REQUEST)));
            }
            if (batch.hasProducerId()) {
                hasProducerId = true;
            }
        }

        if (hasProducerId) {
            return enqueuePartitionWrite(tp, () -> writeIdempotent(tp, records))
                    .thenApply(response -> new AbstractMap.SimpleEntry<>(tp, response));
        }

        // Analyze and validate records once, before non-idempotent write path
        RecordAnalysisResult analysisResult;
        try {
            analysisResult = RecordAnalyzer.analyzeAndValidateRecords(
                    records,
                    new TopicPartition(tp.topic(), tp.partition()),
                    0
            );
        } catch (Exception e) {
            log.warn("Record validation failed for partition {}: {}", tp, e.getMessage());
            return CompletableFuture.completedFuture(
                    new AbstractMap.SimpleEntry<>(tp, new PartitionResponse(Errors.INVALID_RECORD)));
        }

        if (analysisResult.validBytes() <= 0) {
            return CompletableFuture.completedFuture(
                    new AbstractMap.SimpleEntry<>(tp, new PartitionResponse(Errors.NONE)));
        }

        return enqueuePartitionWrite(tp, () -> writeNonIdempotent(tp, records, analysisResult))
                .thenApply(response -> new AbstractMap.SimpleEntry<>(tp, response));
    }

    private <T> CompletableFuture<T> enqueuePartitionWrite(
            TopicIdPartition tp,
            Supplier<SequencedWrite<T>> task) {

        CompletableFuture<Void> newTail = new CompletableFuture<>();
        CompletableFuture<Void> previousTail;
        for (;;) {
            previousTail = partitionWriteTails.get(tp);
            if (previousTail == null) {
                if (partitionWriteTails.putIfAbsent(tp, newTail) == null) {
                    previousTail = COMPLETED;
                    break;
                }
            } else if (partitionWriteTails.replace(tp, previousTail, newTail)) {
                break;
            }
        }

        CompletableFuture<T> result = new CompletableFuture<>();

        previousTail.whenCompleteAsync((ignored, previousError) -> {
            SequencedWrite<T> write;
            try {
                write = task.get();
            } catch (Throwable t) {
                result.completeExceptionally(t);
                completeTailAndMaybeCleanup(tp, newTail);
                return;
            }

            CompletableFuture<Void> submittedFuture = write.submittedFuture();
            if (submittedFuture == null) {
                submittedFuture = COMPLETED;
            }
            submittedFuture.whenComplete((submittedValue, submittedError) ->
                completeTailAndMaybeCleanup(tp, newTail));

            CompletableFuture<T> taskFuture = write.resultFuture();
            if (taskFuture == null) {
                result.completeExceptionally(new IllegalStateException("Missing write result future for " + tp));
                return;
            }

            taskFuture.whenComplete((value, error) -> {
                if (error != null) {
                    result.completeExceptionally(error);
                } else {
                    result.complete(value);
                }
            });
        }, SEQUENCED_WRITE_EXECUTOR);

        return result;
    }

    private SequencedWrite<PartitionResponse> writeIdempotent(TopicIdPartition tp, MemoryRecords records) {
        RecordAnalysisResult analysisResult;
        try {
            analysisResult = RecordAnalyzer.analyzeAndValidateRecords(
                records,
                new TopicPartition(tp.topic(), tp.partition()),
                0
            );
        } catch (Exception e) {
            log.warn("Record validation failed for partition {}: {}", tp, e.getMessage());
            return new SequencedWrite<>(
                COMPLETED,
                CompletableFuture.completedFuture(new PartitionResponse(Errors.INVALID_RECORD)));
        }

        if (analysisResult.validBytes() <= 0) {
            return new SequencedWrite<>(
                COMPLETED,
                CompletableFuture.completedFuture(new PartitionResponse(Errors.NONE)));
        }

        SequencedWrite<PartitionResponse> write = performIdempotentAppend(tp, records, analysisResult);
        CompletableFuture<PartitionResponse> mappedResult = write.resultFuture()
            .exceptionally(e -> {
                log.error("Failed to write to partition {}", tp, e);
                return new PartitionResponse(Errors.KAFKA_STORAGE_ERROR);
            });
        return new SequencedWrite<>(write.submittedFuture(), mappedResult);
    }

    private SequencedWrite<PartitionResponse> writeNonIdempotent(
            TopicIdPartition tp,
            MemoryRecords records,
            RecordAnalysisResult analysisResult) {
        SequencedWrite<PartitionResponse> write = performNonIdempotentAppend(tp, records, analysisResult);
        CompletableFuture<PartitionResponse> mappedResult = write.resultFuture()
            .exceptionally(e -> {
                log.error("Failed to write to partition {}", tp, e);
                return new PartitionResponse(Errors.KAFKA_STORAGE_ERROR);
            });
        return new SequencedWrite<>(write.submittedFuture(), mappedResult);
    }

    @Override
    public void close() throws IOException {
        partitionWriteTails.clear();
    }

    @Override
    public void cleanupPartition(TopicIdPartition tp) {
        if (tp == null) {
            return;
        }

        partitionWriteTails.remove(tp);
    }

    @Override
    public Set<TopicIdPartition> snapshotPartitionsWithLocalState() {
        return new LinkedHashSet<>(partitionWriteTails.keySet());
    }

    private void completeTailAndMaybeCleanup(TopicIdPartition tp, CompletableFuture<Void> newTail) {
        newTail.complete(null);
        partitionWriteTails.remove(tp, newTail);
    }

    protected static final class SequencedWrite<T> {
        private final CompletableFuture<Void> submittedFuture;
        private final CompletableFuture<T> resultFuture;

        SequencedWrite(CompletableFuture<Void> submittedFuture, CompletableFuture<T> resultFuture) {
            this.submittedFuture = submittedFuture;
            this.resultFuture = resultFuture;
        }

        CompletableFuture<Void> submittedFuture() {
            return submittedFuture;
        }

        CompletableFuture<T> resultFuture() {
            return resultFuture;
        }
    }
}
