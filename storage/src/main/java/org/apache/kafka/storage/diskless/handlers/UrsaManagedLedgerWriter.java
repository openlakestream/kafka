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
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.storage.diskless.handlers.RecordAnalyzer.RecordAnalysisResult;
import org.apache.kafka.storage.diskless.idempotent.ProducerStateManager;

import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import io.netty.buffer.ByteBuf;

/**
 * Writer implementation using ManagedLedger for storage.
 */
public class UrsaManagedLedgerWriter extends AbstractUrsaStorageWriter {

    private static final Logger log = LoggerFactory.getLogger(UrsaManagedLedgerWriter.class);

    public UrsaManagedLedgerWriter(UrsaStorageState state) {
        super(state);
    }

    @Override
    protected String writerName() {
        return "ManagedLedger";
    }

    @Override
    protected SequencedWrite<PartitionResponse> performIdempotentAppend(
            TopicIdPartition tp,
            MemoryRecords records,
            RecordAnalysisResult analysisResult) {

        ProducerStateManager producerStateManager = state.getOrCreateProducerStateManager(tp);
        CompletableFuture<Void> submissionFuture = new CompletableFuture<>();
        List<ProducerStateManager.AppendBatch> appendBatches = buildAppendBatches(records);
        if (appendBatches.isEmpty()) {
            submissionFuture.complete(null);
            return new SequencedWrite<>(submissionFuture,
                CompletableFuture.completedFuture(new PartitionResponse(Errors.NONE)));
        }

        CompletableFuture<PartitionResponse> resultFuture = producerStateManager.prepareAppend(appendBatches)
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
                        .exceptionally(error -> {
                            log.warn("Duplicate request future failed for partition {}", tp, error);
                            return new PartitionResponse(Errors.KAFKA_STORAGE_ERROR);
                        });
                }
                if (prepareResult instanceof ProducerStateManager.Ready ready) {
                    return appendPreparedBatches(tp, records, analysisResult, producerStateManager, ready.pendingAppend(),
                        submissionFuture);
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

        return new SequencedWrite<>(submissionFuture, resultFuture);
    }

    @Override
    protected SequencedWrite<PartitionResponse> performNonIdempotentAppend(
            TopicIdPartition tp,
            MemoryRecords records,
            RecordAnalysisResult analysisResult) {
        CompletableFuture<Void> submissionFuture = new CompletableFuture<>();
        CompletableFuture<PartitionResponse> resultFuture =
            appendNonIdempotentRecords(tp, records, analysisResult, submissionFuture);
        return new SequencedWrite<>(submissionFuture, resultFuture);
    }

    private CompletableFuture<PartitionResponse> appendPreparedBatches(
            TopicIdPartition tp,
            MemoryRecords records,
            RecordAnalysisResult analysisResult,
            ProducerStateManager producerStateManager,
            ProducerStateManager.PendingAppend pendingAppend,
            CompletableFuture<Void> submissionFuture) {
        CompletableFuture<PartitionResponse> result = new CompletableFuture<>();

        state.getOrCreateManagedLedger(tp).whenComplete((managedLedger, managedLedgerError) -> {
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
                    analysisResult.recordCount(), dataSize, managedLedger.getName(), tp, analysisResult);

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

    private CompletableFuture<PartitionResponse> appendNonIdempotentRecords(
            TopicIdPartition tp,
            MemoryRecords records,
            RecordAnalysisResult analysisResult,
            CompletableFuture<Void> submissionFuture) {
        CompletableFuture<PartitionResponse> result = new CompletableFuture<>();

        state.getOrCreateManagedLedger(tp).whenComplete((managedLedger, managedLedgerError) -> {
            try {
                if (managedLedgerError != null) {
                    submissionFuture.complete(null);
                    result.completeExceptionally(managedLedgerError);
                    return;
                }

                ByteBuf data = KafkaEntryFormatter.encode(records, analysisResult);
                int dataSize = data.readableBytes();

                log.debug("Appending {} records ({} bytes) to managed ledger {} for partition {}, analysisResult: {}",
                    analysisResult.recordCount(), dataSize, managedLedger.getName(), tp, analysisResult);

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
}
