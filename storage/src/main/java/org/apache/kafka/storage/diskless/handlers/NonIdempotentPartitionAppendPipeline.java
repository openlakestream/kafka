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
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.storage.diskless.handlers.RecordAnalyzer.RecordAnalysisResult;

import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import io.netty.buffer.ByteBuf;

/**
 * Per-partition append pipeliner for non-idempotent produce requests.
 *
 * <p>This class allows multiple in-flight {@code append()} calls against the same ManagedLedger, relying on
 * ManagedLedger's guarantee that append responses are completed in the order of invocation. This reduces tail
 * latency amplification caused by broker-side strict serialization when using periodic flush.
 *
 * <p>Idempotent/transactional semantics are intentionally handled outside of this class.
 *
 * <p>Thread-safety: All state modifications are protected by synchronizing on {@code this}. The pipeline
 * transitions to a closed state atomically when it becomes idle, allowing the caller to remove it from
 * the cache and create a new pipeline for subsequent requests.
 *
 * <p>Lifecycle: When the pipeline becomes idle (no pending or in-flight requests), it atomically marks
 * itself as closed and invokes the onClose callback. The callback is invoked AFTER closed is set to true,
 * so any concurrent tryAppend() calls will see closed=true and return empty, avoiding the race condition
 * that would occur if the callback were invoked before setting closed.
 */
final class NonIdempotentPartitionAppendPipeline {

    private static final Logger log = LoggerFactory.getLogger(NonIdempotentPartitionAppendPipeline.class);

    private final TopicIdPartition tp;
    private final UrsaStorageState state;
    private final int maxInflightAppends;
    private final long maxInflightBytes;
    private final Consumer<NonIdempotentPartitionAppendPipeline> onClose;

    private final CompletableFuture<ManagedLedger> managedLedgerFuture;
    private final ArrayDeque<Request> pending = new ArrayDeque<>();
    private int inflight = 0;
    private long inflightBytes = 0L;

    // Indicates this pipeline is closed and should be replaced.
    // Once set to true, never goes back to false.
    private boolean closed = false;

    /**
     * Creates a new pipeline.
     *
     * @param tp the partition this pipeline is for
     * @param state shared storage state
     * @param maxInflightAppends maximum number of concurrent append operations
     * @param maxInflightBytes maximum bytes in concurrent append operations
     * @param onClose callback invoked with this pipeline instance when it becomes idle and closes itself;
     *                used to remove the pipeline from the cache
     */
    NonIdempotentPartitionAppendPipeline(
            TopicIdPartition tp,
            UrsaStorageState state,
            int maxInflightAppends,
            long maxInflightBytes,
            Consumer<NonIdempotentPartitionAppendPipeline> onClose) {
        this.tp = tp;
        this.state = state;
        this.maxInflightAppends = Math.max(1, maxInflightAppends);
        this.maxInflightBytes = maxInflightBytes;
        this.onClose = onClose;
        this.managedLedgerFuture = state.getOrCreateManagedLedger(tp);
    }

    /**
     * Attempts to append records to this pipeline.
     *
     * @return Optional containing the future if the request was accepted,
     *         or empty if this pipeline is closed and the caller should retry with a new pipeline.
     */
    Optional<CompletableFuture<PartitionResponse>> tryAppend(MemoryRecords records, RecordAnalysisResult analysisResult) {
        Request request = new Request(records, analysisResult.recordCount(), records.sizeInBytes(), analysisResult);

        List<Request> toStart;
        synchronized (this) {
            if (closed) {
                return Optional.empty();
            }

            pending.add(request);
            toStart = drainStartableLocked();
        }

        for (Request startable : toStart) {
            startAppend(startable);
        }

        return Optional.of(request.future);
    }

    /**
     * Returns true if this pipeline is closed and should be removed from the cache.
     */
    boolean isClosed() {
        synchronized (this) {
            return closed;
        }
    }

    /**
     * Forcefully closes this pipeline, failing all pending requests.
     * Used during shutdown.
     */
    void close() {
        List<Request> toFail;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            toFail = new ArrayList<>(pending);
            pending.clear();
        }

        for (Request request : toFail) {
            request.future.complete(new PartitionResponse(Errors.KAFKA_STORAGE_ERROR));
        }
    }

    private List<Request> drainStartableLocked() {
        if (pending.isEmpty() || inflight >= maxInflightAppends) {
            return List.of();
        }

        int capacity = maxInflightAppends - inflight;
        List<Request> result = new ArrayList<>(Math.min(capacity, pending.size()));
        while (capacity > 0 && !pending.isEmpty()) {
            Request next = pending.peek();
            if (next == null) {
                break;
            }

            if (maxInflightBytes > 0L) {
                boolean exceedsLimit = inflightBytes + next.sizeInBytes > maxInflightBytes;
                if (exceedsLimit && inflightBytes > 0L) {
                    break;
                }
            }

            Request request = pending.poll();
            if (request == null) {
                break;
            }
            inflight++;
            inflightBytes += request.sizeInBytes;
            result.add(request);
            capacity--;
        }
        return result;
    }

    private void startAppend(Request request) {
        CompletableFuture<PartitionResponse> appendFuture = managedLedgerFuture
                .thenCompose(managedLedger -> appendToManagedLedger(managedLedger, request))
                .exceptionally(e -> {
                    log.error("Failed to write to partition {}", tp, e);
                    return new PartitionResponse(Errors.KAFKA_STORAGE_ERROR);
                });

        appendFuture.whenComplete((response, error) -> {
            if (error != null) {
                request.future.complete(new PartitionResponse(Errors.KAFKA_STORAGE_ERROR));
            } else {
                request.future.complete(response);
            }
            onRequestFinished(request.sizeInBytes);
        });
    }

    /**
     * Appends to ManagedLedger with inline async callback handling.
     */
    private CompletableFuture<PartitionResponse> appendToManagedLedger(ManagedLedger managedLedger, Request request) {
        ByteBuf data = KafkaEntryFormatter.encode(request.records, request.analysisResult);
        int dataSize = data.readableBytes();

        log.debug("Appending {} records ({} bytes) to managed ledger {} for partition {}, analysisResult: {}",
                request.analysisResult.recordCount(), dataSize, managedLedger.getName(), tp, request.analysisResult);

        CompletableFuture<Position> addFuture = new CompletableFuture<>();
        try {
            managedLedger.asyncAddEntry(data, request.analysisResult.recordCount(), new AsyncCallbacks.AddEntryCallback() {
                @Override
                public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                    addFuture.complete(position);
                }

                @Override
                public void addFailed(ManagedLedgerException exception, Object ctx) {
                    addFuture.completeExceptionally(exception);
                }
            }, null);
        } catch (Throwable t) {
            data.release();
            return CompletableFuture.failedFuture(t);
        }

        return addFuture.whenComplete((ignored, error) -> data.release())
                .thenApply(position -> {
                    long logAppendTime = state.time().milliseconds();
                    return new PartitionResponse(Errors.NONE, position.getEntryId(), logAppendTime, 0L);
                });
    }

    private void onRequestFinished(long sizeInBytes) {
        List<Request> toStart;
        boolean shouldInvokeOnClose = false;

        synchronized (this) {
            inflight--;
            if (inflight < 0) {
                inflight = 0;
            }
            inflightBytes -= sizeInBytes;
            if (inflightBytes < 0L) {
                inflightBytes = 0L;
            }

            if (!closed && !pending.isEmpty()) {
                toStart = drainStartableLocked();
            } else {
                toStart = List.of();
            }

            // Atomically check if idle and mark as closed.
            // This prevents the race condition where:
            // 1. Thread A determines idle=true and exits the lock
            // 2. Thread B adds a new request via tryAppend()
            // 3. Thread A removes the pipeline from the map
            // 4. Thread C creates a new pipeline for the same partition
            // By setting closed=true inside the lock, tryAppend() will return empty
            // and the caller will create a new pipeline.
            if (inflight == 0 && pending.isEmpty() && !closed) {
                closed = true;
                shouldInvokeOnClose = true;
            }
        }

        for (Request request : toStart) {
            startAppend(request);
        }

        // Invoke onClose callback AFTER releasing the lock and AFTER closed is set to true.
        // This is safe because:
        // - closed is already true, so any concurrent tryAppend() will return empty
        // - The callback just removes this pipeline from the cache
        // - New requests will create a new pipeline via computeIfAbsent
        if (shouldInvokeOnClose) {
            onClose.accept(this);
        }
    }

    private static final class Request {
        private final MemoryRecords records;
        private final int recordCount;
        private final long sizeInBytes;
        private final RecordAnalysisResult analysisResult;
        private final CompletableFuture<PartitionResponse> future = new CompletableFuture<>();

        private Request(MemoryRecords records, int recordCount, long sizeInBytes, RecordAnalysisResult analysisResult) {
            this.records = records;
            this.recordCount = recordCount;
            this.sizeInBytes = sizeInBytes;
            this.analysisResult = analysisResult;
        }
    }
}
