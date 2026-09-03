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
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.diskless.handlers.RecordAnalyzer.RecordAnalysisResult;
import org.apache.kafka.storage.diskless.idempotent.ProducerStateManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import io.lakestream.api.Log;
import io.lakestream.api.LogEntryHeader;
import io.lakestream.api.LogOffset;
import io.netty.buffer.ByteBuf;

/**
 * The write side of one diskless partition: validation, ordering, payload ownership, the append
 * itself, and the notification that wakes long-polling fetches.
 *
 * <p>A request is validated exactly once, on the calling thread: {@link RecordAnalyzer} runs the
 * single CRC pass and every later check is header-only. A {@code LogAppendTime} topic stamps the
 * request records with the broker clock before they are copied, so the bytes that reach storage
 * already carry the timestamp the produce response reports; a {@code CreateTime} topic reports
 * {@link RecordBatch#NO_TIMESTAMP} and leaves the records untouched.
 *
 * <p>The copied payload is owned by this writer from the moment it is created until exactly one
 * release: the append callback releases it on success or failure, and {@link #close()} releases
 * every payload that has not yet been handed to storage. Appends are serialized per partition by
 * {@link PartitionWriteSequencer}; only submission is ordered, completions may interleave.
 *
 * <p>Each entry that lands is reported to {@code onAppended} before any long-polling fetch is
 * woken, which is what lets {@link PartitionReader} answer from a cached offset window without
 * ever missing this broker's own appends.
 */
final class PartitionWriter {

    private static final Logger log = LoggerFactory.getLogger(PartitionWriter.class);

    private final TopicIdPartition topicIdPartition;
    private final Supplier<CompletableFuture<Log>> logSupplier;
    private final Function<String, ProducerStateManager> producerStateForZone;
    private final Time time;
    /** Where every entry that lands is reported, so the read side can see it without a read. */
    private final Consumer<LogOffset> onAppended;
    private final PartitionWriteSequencer writeSequencer;
    private final Set<OwnedWritePayload> ownedWritePayloads = ConcurrentHashMap.newKeySet();
    private final Object waitersLock = new Object();
    private List<CompletableFuture<Void>> waiters = new ArrayList<>();
    /** The topic's effective {@code message.timestamp.type}, refreshed by config updates. */
    private volatile TimestampType timestampType;
    private volatile boolean closed;

    PartitionWriter(TopicIdPartition topicIdPartition,
                    Supplier<CompletableFuture<Log>> logSupplier,
                    Function<String, ProducerStateManager> producerStateForZone,
                    TimestampType timestampType,
                    Time time,
                    Consumer<LogOffset> onAppended) {
        this.topicIdPartition = Objects.requireNonNull(topicIdPartition, "topicIdPartition must not be null");
        this.logSupplier = Objects.requireNonNull(logSupplier, "logSupplier must not be null");
        this.producerStateForZone =
                Objects.requireNonNull(producerStateForZone, "producerStateForZone must not be null");
        this.timestampType = Objects.requireNonNull(timestampType, "timestampType must not be null");
        this.time = Objects.requireNonNull(time, "time must not be null");
        this.onAppended = Objects.requireNonNull(onAppended, "onAppended must not be null");
        this.writeSequencer = new PartitionWriteSequencer(topicIdPartition.toString());
    }

    /**
     * Adopts a new {@code message.timestamp.type} for this partition's topic. Resolving it per
     * append meant rebuilding the topic's configuration on the produce path; it is resolved once
     * here instead, and the metadata layer pushes every later change through this method.
     */
    void applyTimestampType(TimestampType updated) {
        if (updated != null) {
            timestampType = updated;
        }
    }

    /**
     * Appends one produce request. The returned future fails for storage and ownership errors so the
     * caller can map them to a response and, where needed, retire this partition log.
     */
    CompletableFuture<PartitionResponse> write(MemoryRecords records, String zone) {
        if (closed) {
            return CompletableFuture.completedFuture(notLeaderResponse());
        }
        if (records.sizeInBytes() == 0) {
            return CompletableFuture.completedFuture(new PartitionResponse(Errors.NONE));
        }

        PreparedWrite preparedWrite = prepareWrite(records);
        if (preparedWrite.errorResponse() != null) {
            return CompletableFuture.completedFuture(preparedWrite.errorResponse());
        }

        OwnedWritePayload payload = preparedWrite.payload();
        ownedWritePayloads.add(payload);
        if (closed) {
            payload.release();
            return CompletableFuture.completedFuture(notLeaderResponse());
        }

        try {
            return writeSequencer.submit(() -> writeValidated(preparedWrite, zone));
        } catch (Throwable submitError) {
            payload.release();
            return CompletableFuture.failedFuture(submitError);
        }
    }

    /**
     * Completes once an append lands after this call, or when this writer closes. Never fails, so a
     * long-polling fetch can always re-evaluate itself. The deadline belongs to the request, which
     * completes this waiter when its own timeout fires.
     */
    CompletableFuture<Void> awaitAppend() {
        CompletableFuture<Void> waiter = new CompletableFuture<>();
        synchronized (waitersLock) {
            if (closed) {
                waiter.complete(null);
                return waiter;
            }
            // Waiters whose request already ended drop out here, so an idle partition that never
            // appends keeps at most as many waiters as it has fetches in flight.
            waiters.removeIf(CompletableFuture::isDone);
            waiters.add(waiter);
        }
        return waiter;
    }

    /** Releases every payload that has not reached storage and wakes every long-poll waiter. */
    void close() {
        List<CompletableFuture<Void>> toComplete;
        synchronized (waitersLock) {
            closed = true;
            toComplete = waiters;
            waiters = new ArrayList<>();
        }
        writeSequencer.reset();
        ownedWritePayloads.forEach(OwnedWritePayload::cancelBeforeAppend);
        toComplete.forEach(waiter -> waiter.complete(null));
    }

    int ownedWritePayloadCount() {
        return ownedWritePayloads.size();
    }

    private PreparedWrite prepareWrite(MemoryRecords records) {
        boolean hasProducerId = false;
        for (RecordBatch batch : records.batches()) {
            if (batch.isTransactional()) {
                log.warn("Transactional produce rejected for partition {}", topicIdPartition);
                return PreparedWrite.error(new PartitionResponse(Errors.INVALID_REQUEST));
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
            KafkaRecordsPayload.validateForAppend(records, analysisResult.recordCount());
        } catch (Exception validationError) {
            log.warn("Record validation failed for partition {}: {}",
                    topicIdPartition, validationError.getMessage());
            return PreparedWrite.error(new PartitionResponse(Errors.INVALID_RECORD));
        }

        final boolean idempotent = hasProducerId;
        try {
            long appendTimestamp = RecordBatch.NO_TIMESTAMP;
            if (timestampType == TimestampType.LOG_APPEND_TIME) {
                appendTimestamp = time.milliseconds();
                KafkaRecordsPayload.setLogAppendTime(records, appendTimestamp);
            }
            List<ProducerStateManager.AppendBatch> appendBatches =
                    idempotent ? buildAppendBatches(records) : List.of();
            OwnedWritePayload payload = new OwnedWritePayload(KafkaRecordsPayload.copyForAppend(records));
            return new PreparedWrite(payload, analysisResult, appendBatches, idempotent, appendTimestamp, null);
        } catch (Throwable prepareError) {
            log.error("Failed to copy records for partition {}", topicIdPartition, prepareError);
            return PreparedWrite.error(new PartitionResponse(Errors.KAFKA_STORAGE_ERROR));
        }
    }

    private PartitionWriteSequencer.WriteTask<PartitionResponse> writeValidated(
            PreparedWrite preparedWrite,
            String zone) {
        if (closed) {
            preparedWrite.payload().release();
            return closedWriteTask();
        }

        CompletableFuture<Void> submissionFuture = new CompletableFuture<>();
        CompletableFuture<PartitionResponse> result;
        try {
            result = preparedWrite.idempotent()
                    ? appendIdempotentRecords(preparedWrite, zone, submissionFuture)
                    : append(preparedWrite, submissionFuture, nonIdempotentListener());
        } catch (Throwable writeError) {
            preparedWrite.payload().release();
            submissionFuture.complete(null);
            result = CompletableFuture.failedFuture(writeError);
        }
        return new PartitionWriteSequencer.WriteTask<>(submissionFuture, result);
    }

    private CompletableFuture<PartitionResponse> appendIdempotentRecords(
            PreparedWrite preparedWrite,
            String zone,
            CompletableFuture<Void> submissionFuture) {
        OwnedWritePayload payload = preparedWrite.payload();
        if (closed) {
            payload.release();
            submissionFuture.complete(null);
            return CompletableFuture.completedFuture(notLeaderResponse());
        }

        ProducerStateManager producerStateManager = producerStateForZone.apply(zone);
        if (preparedWrite.appendBatches().isEmpty()) {
            payload.release();
            submissionFuture.complete(null);
            return CompletableFuture.completedFuture(new PartitionResponse(Errors.NONE));
        }

        return producerStateManager.prepareAppend(preparedWrite.appendBatches())
                .thenCompose(prepareResult -> {
                    if (closed) {
                        NotLeaderOrFollowerException ownershipError = ownershipLostException();
                        if (prepareResult instanceof ProducerStateManager.Ready ready) {
                            producerStateManager.abortAppend(ready.pendingAppend(), ownershipError);
                        }
                        payload.release();
                        submissionFuture.complete(null);
                        return CompletableFuture.completedFuture(notLeaderResponse());
                    }

                    if (prepareResult instanceof ProducerStateManager.InvalidEpoch invalidEpoch) {
                        payload.release();
                        submissionFuture.complete(null);
                        return CompletableFuture.completedFuture(
                                new PartitionResponse(Errors.INVALID_PRODUCER_EPOCH, invalidEpoch.message()));
                    }
                    if (prepareResult instanceof ProducerStateManager.OutOfOrderSequence outOfOrderSequence) {
                        payload.release();
                        submissionFuture.complete(null);
                        return CompletableFuture.completedFuture(
                                new PartitionResponse(
                                        Errors.OUT_OF_ORDER_SEQUENCE_NUMBER,
                                        outOfOrderSequence.message()));
                    }
                    if (prepareResult instanceof ProducerStateManager.Duplicate duplicate) {
                        payload.release();
                        submissionFuture.complete(null);
                        return duplicate.appendResultFuture()
                                .thenApply(appendResult -> new PartitionResponse(
                                        Errors.NONE,
                                        appendResult.baseOffset(),
                                        duplicateLogAppendTime(preparedWrite.appendTimestamp(), appendResult),
                                        0L));
                    }
                    if (prepareResult instanceof ProducerStateManager.Ready ready) {
                        return append(
                                preparedWrite,
                                submissionFuture,
                                idempotentListener(producerStateManager, ready.pendingAppend()));
                    }
                    payload.release();
                    submissionFuture.complete(null);
                    return CompletableFuture.failedFuture(
                            new IllegalStateException("Unexpected prepare result: " + prepareResult));
                })
                .whenComplete((response, error) -> {
                    if (error != null) {
                        payload.release();
                        submissionFuture.complete(null);
                    }
                });
    }

    /**
     * Opens the log and starts one append on it. Every exit releases the payload and completes
     * {@code submissionFuture}, which is what lets the next queued write start.
     */
    private CompletableFuture<PartitionResponse> append(
            PreparedWrite preparedWrite,
            CompletableFuture<Void> submissionFuture,
            AppendListener listener) {
        OwnedWritePayload payload = preparedWrite.payload();
        CompletableFuture<PartitionResponse> result = new CompletableFuture<>();

        logSupplier.get().whenComplete((logInstance, logError) -> {
            Throwable startError;
            try {
                startError = startAppend(preparedWrite, submissionFuture, result, listener, logInstance, logError);
            } catch (Throwable unexpected) {
                startError = unexpected;
            }
            if (startError != null) {
                // The payload never reached storage. Unwinding lives here, in one place, so that
                // no exit from startAppend can leave a registered listener, an unreleased payload
                // or a blocked sequencer behind. Releasing a payload that close() already
                // reclaimed is a no-op.
                listener.abort(startError);
                payload.release();
                submissionFuture.complete(null);
                result.completeExceptionally(startError);
            }
        });

        return result;
    }

    /**
     * Hands the payload to storage exactly once and reports the entry back through
     * {@code listener}. Completing {@code submissionFuture} is what lets the next queued write
     * start, so it happens as soon as the append is under way.
     *
     * @return {@code null} once storage owns the payload, otherwise the failure that stopped it
     */
    private Throwable startAppend(
            PreparedWrite preparedWrite,
            CompletableFuture<Void> submissionFuture,
            CompletableFuture<PartitionResponse> result,
            AppendListener listener,
            Log logInstance,
            Throwable logError) {
        if (logError != null) {
            return logError;
        }
        if (closed) {
            return ownershipLostException();
        }
        OwnedWritePayload payload = preparedWrite.payload();
        if (!payload.beginAppend()) {
            // close() reclaimed this payload while the log was still opening.
            return ownershipLostException();
        }
        ByteBuf data = payload.buffer();

        CompletableFuture<LogEntryHeader> appendFuture;
        try {
            log.debug("Appending {} records ({} bytes) to log {} for partition {}, analysisResult: {}",
                    preparedWrite.analysisResult().recordCount(), data.readableBytes(), logInstance.id(),
                    topicIdPartition, preparedWrite.analysisResult());
            appendFuture = logInstance.append(preparedWrite.analysisResult().recordCount(), data);
            if (appendFuture == null) {
                throw new IllegalStateException("Log.append returned null future");
            }
        } catch (Throwable appendInitError) {
            return appendInitError;
        }

        submissionFuture.complete(null);
        appendFuture.whenComplete((entryHeader, appendError) -> {
            try {
                if (appendError != null) {
                    listener.abort(appendError);
                    result.completeExceptionally(appendError);
                    return;
                }
                if (entryHeader == null) {
                    IllegalStateException missingHeader =
                            new IllegalStateException("Log.append returned a null entry header");
                    listener.abort(missingHeader);
                    result.completeExceptionally(missingHeader);
                    return;
                }
                // The records are durable now, so a long-polling fetch can see them. The read
                // side is told where the log now ends before that fetch is woken, so it answers
                // from this append rather than from an offset window read before it landed. The
                // record count comes from this request rather than from the header, because it is
                // exactly what was handed to storage.
                //
                // This runs before the ack and the wake-up because both must already see the new
                // tail, which is why it sits inside a try whose catch unwinds producer state for
                // an append that is already durable: onAppended must not throw.
                onAppended.accept(new LogOffset(
                        entryHeader.offset(),
                        preparedWrite.analysisResult().recordCount(),
                        entryHeader.timestamp()));
                notifyAppended();
                result.complete(listener.completed(entryHeader, preparedWrite.appendTimestamp()));
            } catch (Throwable completeError) {
                listener.abort(completeError);
                result.completeExceptionally(completeError);
            } finally {
                payload.release();
            }
        });
        return null;
    }

    private AppendListener nonIdempotentListener() {
        return new AppendListener() {
            @Override
            public void abort(Throwable cause) {
                // No producer state to unwind.
            }

            @Override
            public PartitionResponse completed(LogEntryHeader entryHeader, long appendTimestamp) {
                return new PartitionResponse(Errors.NONE, entryHeader.offset(), appendTimestamp, 0L);
            }
        };
    }

    private AppendListener idempotentListener(
            ProducerStateManager producerStateManager,
            ProducerStateManager.PendingAppend pendingAppend) {
        return new AppendListener() {
            @Override
            public void abort(Throwable cause) {
                producerStateManager.abortAppend(pendingAppend, cause);
            }

            @Override
            public PartitionResponse completed(LogEntryHeader entryHeader, long appendTimestamp) {
                // A LogAppendTime topic stores the stamp it wrote into the records, so a later
                // duplicate reports exactly what the original append reported. A CreateTime topic
                // has no such stamp and keeps a wall-clock bookkeeping timestamp instead.
                long storedTimestamp = appendTimestamp == RecordBatch.NO_TIMESTAMP
                        ? time.milliseconds()
                        : appendTimestamp;
                ProducerStateManager.AppendResult appendResult = producerStateManager.completeAppend(
                        pendingAppend, entryHeader.offset(), storedTimestamp);
                return new PartitionResponse(Errors.NONE, appendResult.baseOffset(), appendTimestamp, 0L);
            }
        };
    }

    private void notifyAppended() {
        List<CompletableFuture<Void>> toComplete;
        synchronized (waitersLock) {
            if (waiters.isEmpty()) {
                return;
            }
            toComplete = waiters;
            waiters = new ArrayList<>();
        }
        toComplete.forEach(waiter -> waiter.complete(null));
    }

    /**
     * A duplicate reports the timestamp of the append it duplicates, as the classic log does: for a
     * LogAppendTime topic that is the stamp written into the original records. A CreateTime topic
     * reports no log-append time at all, so its bookkeeping timestamp stays internal.
     */
    private static long duplicateLogAppendTime(
            long appendTimestamp,
            ProducerStateManager.AppendResult appendResult) {
        return appendTimestamp == RecordBatch.NO_TIMESTAMP ? RecordBatch.NO_TIMESTAMP : appendResult.timestamp();
    }

    private PartitionWriteSequencer.WriteTask<PartitionResponse> closedWriteTask() {
        return new PartitionWriteSequencer.WriteTask<>(
                CompletableFuture.completedFuture(null),
                CompletableFuture.completedFuture(notLeaderResponse()));
    }

    private static PartitionResponse notLeaderResponse() {
        return new PartitionResponse(Errors.NOT_LEADER_OR_FOLLOWER);
    }

    private NotLeaderOrFollowerException ownershipLostException() {
        return new NotLeaderOrFollowerException("Partition log is closed for " + topicIdPartition);
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

    /** Producer-state bookkeeping for one append: unwound on any failure, settled once the entry lands. */
    private interface AppendListener {
        void abort(Throwable cause);

        PartitionResponse completed(LogEntryHeader entryHeader, long appendTimestamp);
    }

    private enum WritePayloadState {
        PENDING,
        APPENDING,
        RELEASED
    }

    /** One validated request: either an error response, or everything the append needs. */
    private record PreparedWrite(
            OwnedWritePayload payload,
            RecordAnalysisResult analysisResult,
            List<ProducerStateManager.AppendBatch> appendBatches,
            boolean idempotent,
            long appendTimestamp,
            PartitionResponse errorResponse) {

        static PreparedWrite error(PartitionResponse errorResponse) {
            return new PreparedWrite(null, null, List.of(), false, RecordBatch.NO_TIMESTAMP, errorResponse);
        }
    }

    private final class OwnedWritePayload {
        private final ByteBuf data;
        private final AtomicReference<WritePayloadState> payloadState =
                new AtomicReference<>(WritePayloadState.PENDING);

        private OwnedWritePayload(ByteBuf data) {
            this.data = data;
        }

        private boolean beginAppend() {
            return payloadState.compareAndSet(WritePayloadState.PENDING, WritePayloadState.APPENDING);
        }

        private ByteBuf buffer() {
            if (payloadState.get() != WritePayloadState.APPENDING) {
                throw new IllegalStateException("Write payload is not owned by an append");
            }
            return data;
        }

        private void cancelBeforeAppend() {
            if (payloadState.compareAndSet(WritePayloadState.PENDING, WritePayloadState.RELEASED)) {
                releaseBuffer();
            }
        }

        private void release() {
            WritePayloadState previous = payloadState.getAndSet(WritePayloadState.RELEASED);
            if (previous != WritePayloadState.RELEASED) {
                releaseBuffer();
            }
        }

        private void releaseBuffer() {
            try {
                data.release();
            } finally {
                ownedWritePayloads.remove(this);
            }
        }
    }
}
