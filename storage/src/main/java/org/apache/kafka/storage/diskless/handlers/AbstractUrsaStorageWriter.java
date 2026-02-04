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
import org.apache.kafka.storage.diskless.idempotent.ProducerStateStore;
import org.apache.kafka.storage.diskless.idempotent.ProducerStateStore.SequenceValidationRequest;
import org.apache.kafka.storage.diskless.idempotent.ProducerStateStore.StateUpdateRequest;
import org.apache.kafka.storage.diskless.idempotent.ProducerStateStore.ValidationResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Abstract base class for Ursa storage writers.
 * Consolidates common logic for both StorageApi and ManagedLedger implementations.
 */
public abstract class AbstractUrsaStorageWriter implements Writer {

    private static final Logger log = LoggerFactory.getLogger(AbstractUrsaStorageWriter.class);
    private static final CompletableFuture<Void> COMPLETED = CompletableFuture.completedFuture(null);

    protected final UrsaStorageState state;
    private final ConcurrentHashMap<TopicIdPartition, CompletableFuture<Void>> partitionWriteTails =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<TopicIdPartition, NonIdempotentPartitionAppendPipeline> nonIdempotentPipelines =
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
     * @return a future containing the partition response
     */
    protected abstract CompletableFuture<PartitionResponse> performIdempotentAppend(
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

        // Analyze and validate records once, before any write path
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

        if (hasProducerId) {
            return enqueuePartitionWrite(tp, () -> writeIdempotent(tp, records, analysisResult))
                    .thenApply(response -> new AbstractMap.SimpleEntry<>(tp, response));
        }

        return appendToNonIdempotentPipeline(tp, records, analysisResult)
                .thenApply(response -> new AbstractMap.SimpleEntry<>(tp, response));
    }

    private CompletableFuture<PartitionResponse> appendToNonIdempotentPipeline(
            TopicIdPartition tp,
            MemoryRecords records,
            RecordAnalysisResult analysisResult) {
        while (true) {
            NonIdempotentPartitionAppendPipeline pipeline = getOrCreateNonIdempotentPipeline(tp);
            var result = pipeline.tryAppend(records, analysisResult);
            if (result.isPresent()) {
                return result.get();
            }
            nonIdempotentPipelines.remove(tp, pipeline);
        }
    }

    private <T> CompletableFuture<T> enqueuePartitionWrite(
            TopicIdPartition tp,
            Supplier<CompletableFuture<T>> task) {

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

        previousTail.whenComplete((ignored, previousError) -> {
            CompletableFuture<T> taskFuture;
            try {
                taskFuture = task.get();
            } catch (Throwable t) {
                result.completeExceptionally(t);
                completeTailAndMaybeCleanup(tp, newTail);
                return;
            }

            taskFuture.whenComplete((value, error) -> {
                if (error != null) {
                    result.completeExceptionally(error);
                } else {
                    result.complete(value);
                }
                completeTailAndMaybeCleanup(tp, newTail);
            });
        });

        return result;
    }

    private CompletableFuture<PartitionResponse> writeIdempotent(
            TopicIdPartition tp,
            MemoryRecords records,
            RecordAnalysisResult analysisResult) {

        return validateBeforeWrite(tp, records)
                .thenCompose(validationError -> {
                    if (validationError != null) {
                        return CompletableFuture.completedFuture(validationError);
                    }
                    return performIdempotentAppend(tp, records, analysisResult);
                })
                .exceptionally(e -> {
                    log.error("Failed to write to partition {}", tp, e);
                    return new PartitionResponse(Errors.KAFKA_STORAGE_ERROR);
                });
    }

    private NonIdempotentPartitionAppendPipeline getOrCreateNonIdempotentPipeline(TopicIdPartition tp) {
        return nonIdempotentPipelines.computeIfAbsent(tp, key -> {
            NonIdempotentPartitionAppendPipeline[] holder = new NonIdempotentPartitionAppendPipeline[1];
            holder[0] = new NonIdempotentPartitionAppendPipeline(
                    key,
                    state,
                    state.config().getNonIdempotentMaxInFlightAppendsPerPartition(),
                    state.config().getNonIdempotentMaxInFlightBytesPerPartition(),
                    () -> nonIdempotentPipelines.remove(key, holder[0])
            );
            return holder[0];
        });
    }

    protected CompletableFuture<PartitionResponse> validateBeforeWrite(
            TopicIdPartition tp,
            MemoryRecords records) {

        ProducerStateStore store = state.producerStateStore();
        List<SequenceValidationRequest> requests = new ArrayList<>();

        for (RecordBatch batch : records.batches()) {
            if (!batch.hasProducerId()) {
                continue;
            }

            requests.add(SequenceValidationRequest.builder()
                .topicPartition(tp)
                .producerId(batch.producerId())
                .producerEpoch(batch.producerEpoch())
                .baseSequence(batch.baseSequence())
                .lastSequence(batch.lastSequence())
                .build());
        }

        if (requests.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        return store.validateAll(requests).thenApply(validationResult -> {
            if (validationResult instanceof ValidationResult.Success) {
                return null;
            } else if (validationResult instanceof ValidationResult.Duplicate dup) {
                return new PartitionResponse(Errors.NONE, dup.cachedOffset(), dup.cachedTimestamp(), 0L);
            } else if (validationResult instanceof ValidationResult.InvalidEpoch inv) {
                log.warn("Invalid producer epoch: {}", inv.message());
                return new PartitionResponse(Errors.INVALID_PRODUCER_EPOCH, inv.message());
            } else if (validationResult instanceof ValidationResult.OutOfOrderSequence oos) {
                log.warn("Out of order sequence: {}", oos.message());
                return new PartitionResponse(Errors.OUT_OF_ORDER_SEQUENCE_NUMBER, oos.message());
            } else {
                throw new IllegalStateException("Unknown validation result: " + validationResult);
            }
        });
    }

    protected CompletableFuture<Void> updateStateAfterWrite(
            TopicIdPartition tp,
            MemoryRecords records,
            long baseOffset,
            long logAppendTime) {

        ProducerStateStore store = state.producerStateStore();
        CompletableFuture<Void> result = CompletableFuture.completedFuture(null);

        long currentOffset = baseOffset;
        for (RecordBatch batch : records.batches()) {
            if (!batch.hasProducerId()) {
                int batchRecordCount = batch.countOrNull() != null ? batch.countOrNull() : 0;
                currentOffset += batchRecordCount;
                continue;
            }

            final long batchBaseOffset = currentOffset;
            int batchRecordCount = batch.countOrNull() != null ? batch.countOrNull() : 0;
            currentOffset += batchRecordCount;

            StateUpdateRequest request = StateUpdateRequest.builder()
                .topicPartition(tp)
                .producerId(batch.producerId())
                .producerEpoch(batch.producerEpoch())
                .baseSequence(batch.baseSequence())
                .lastSequence(batch.lastSequence())
                .assignedOffset(batchBaseOffset)
                .timestamp(logAppendTime)
                .build();

            result = result.thenCompose(ignored -> store.updateAfterWrite(request));
        }

        return result;
    }

    @Override
    public void close() throws IOException {
        partitionWriteTails.clear();
        List<NonIdempotentPartitionAppendPipeline> pipelinesToClose = new ArrayList<>(nonIdempotentPipelines.values());
        for (NonIdempotentPartitionAppendPipeline pipeline : pipelinesToClose) {
            pipeline.close();
        }
        nonIdempotentPipelines.clear();
    }

    private void completeTailAndMaybeCleanup(TopicIdPartition tp, CompletableFuture<Void> newTail) {
        newTail.complete(null);
        partitionWriteTails.remove(tp, newTail);
    }
}
