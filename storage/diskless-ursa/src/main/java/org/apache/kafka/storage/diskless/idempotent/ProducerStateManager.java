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
package org.apache.kafka.storage.diskless.idempotent;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.storage.diskless.DisklessClientZone;
import org.apache.kafka.storage.diskless.DisklessFutures;
import org.apache.kafka.storage.diskless.LogEntryUtils;
import org.apache.kafka.storage.diskless.handlers.KafkaRecordsPayload;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import io.lakestream.api.Log;
import io.lakestream.api.LogCursor;
import io.lakestream.api.LogEntry;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.exceptions.UnexpectedVersionIdException;
import io.oxia.client.api.options.DeleteOption;
import io.oxia.client.api.options.PutOption;

/**
 * Per-partition producer state manager for diskless idempotent produce.
 */
public class ProducerStateManager implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ProducerStateManager.class);

    public static final long DEFAULT_SNAPSHOT_INTERVAL_MS = 30_000L;
    public static final int DEFAULT_SNAPSHOT_RECORD_THRESHOLD = 10_000;

    private static final long DEFAULT_NEXT_OFFSET = 0L;
    private static final int REPLAY_MAX_ENTRIES_PER_READ = 100;
    private static final long REPLAY_MAX_BYTES_PER_READ = 1024L * 1024L;
    private static final long MAX_SNAPSHOT_VALUE_BYTES = 4L * 1024L * 1024L;
    private static final int SNAPSHOT_CLAIM_MAX_ATTEMPTS = 3;
    private static final long CURSOR_CLOSE_RETRY_DELAY_MS = 100L;

    public enum RecoveryState {
        NOT_STARTED,
        IN_PROGRESS,
        DONE
    }

    public record AppendBatch(
            long producerId,
            short producerEpoch,
            int firstSequence,
            int lastSequence,
            int recordCount,
            long timestamp) {
        public AppendBatch {
            if (recordCount < 0) {
                throw new IllegalArgumentException("recordCount must be >= 0");
            }
        }
    }

    public record AppendResult(long baseOffset, long timestamp) {
    }

    public sealed interface PrepareResult permits Ready, Duplicate, InvalidEpoch, OutOfOrderSequence {
    }

    public record Ready(PendingAppend pendingAppend) implements PrepareResult {
    }

    public record Duplicate(CompletableFuture<AppendResult> appendResultFuture) implements PrepareResult {
    }

    public record InvalidEpoch(short currentEpoch, short requestEpoch, String message) implements PrepareResult {
    }

    public record OutOfOrderSequence(int expectedSequence, int actualSequence, String message) implements PrepareResult {
    }

    public record ProducerState(
            long producerId,
            short producerEpoch,
            int lastSequence,
            long lastOffset,
            long lastTimestamp,
            int retainedBatchCount) {
    }

    public static final class PendingAppend {
        private final List<PendingBatch> pendingBatches;
        private final int totalRecordCount;

        private PendingAppend(List<PendingBatch> pendingBatches, int totalRecordCount) {
            this.pendingBatches = List.copyOf(pendingBatches);
            this.totalRecordCount = totalRecordCount;
        }
    }

    private record PendingBatch(
            ProducerStateEntry producerStateEntry,
            AppendBatch appendBatch,
            BatchMetadata batchMetadata) {
    }

    private record PendingPrepareRequest(
            List<AppendBatch> batches,
            CompletableFuture<PrepareResult> future) {
    }

    private record SnapshotPayload(byte[] bytes, long lastOffset, int producerCount, long recordGeneration) {
    }

    private enum ReplayEntryResult {
        CONTINUE,
        MANAGER_CLOSED
    }

    private static final class ProducerShadowState {
        private short producerEpoch;
        private int lastSequence;

        ProducerShadowState(short producerEpoch, int lastSequence) {
            this.producerEpoch = producerEpoch;
            this.lastSequence = lastSequence;
        }

        short producerEpoch() {
            return producerEpoch;
        }

        int expectedSequence() {
            return lastSequence == RecordBatch.NO_SEQUENCE ? 0 : nextSequence(lastSequence);
        }

        void reset(short producerEpoch) {
            this.producerEpoch = producerEpoch;
            this.lastSequence = RecordBatch.NO_SEQUENCE;
        }

        void onBatch(short producerEpoch, int lastSequence) {
            this.producerEpoch = producerEpoch;
            this.lastSequence = lastSequence;
        }
    }

    private final TopicIdPartition topicIdPartition;
    private final Supplier<AsyncOxiaClient> oxiaClientSupplier;
    private final Supplier<CompletableFuture<Log>> logSupplier;
    private final String zone;
    private final long snapshotIntervalMs;
    private final int snapshotRecordThreshold;
    private final ScheduledExecutorService scheduler;
    private final boolean ownsScheduler;
    private volatile ScheduledFuture<?> periodicSnapshotTask;

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final ConcurrentHashMap<LogCursor, CompletableFuture<Void>> replayCursorCloses =
            new ConcurrentHashMap<>();
    private final Map<Long, ProducerStateEntry> producers = new HashMap<>();
    private final List<PendingPrepareRequest> pendingPrepareRequests = new ArrayList<>();
    private CompletableFuture<Void> snapshotWriteTail = CompletableFuture.completedFuture(null);
    private CompletableFuture<Void> inFlightSnapshotWrite;
    private CompletableFuture<Void> pendingSnapshotWrite;
    private CompletableFuture<Void> asyncObservedSnapshotWrite;
    private CompletableFuture<Void> recoveryDrain = CompletableFuture.completedFuture(null);
    private String pendingSnapshotReason;
    private Long ownedSnapshotVersionId;
    private boolean snapshotOwnershipLost;
    private boolean deleteSnapshotOnCleanup;

    private RecoveryState recoveryState = RecoveryState.NOT_STARTED;
    private boolean recoverySkippedDueToExcessiveReplay = false;
    private final Set<Long> bypassedProducerIdsAfterSkippedRecovery = new HashSet<>();
    private long nextOffsetToRecover = DEFAULT_NEXT_OFFSET;
    private long recordsSinceSnapshot = 0L;
    private long snapshotRecordGeneration = 0L;

    public ProducerStateManager(
            TopicIdPartition topicIdPartition,
            Supplier<AsyncOxiaClient> oxiaClientSupplier,
            Supplier<CompletableFuture<Log>> logSupplier) {
        this(topicIdPartition, oxiaClientSupplier, logSupplier,
            DisklessClientZone.NO_ZONE,
            DEFAULT_SNAPSHOT_INTERVAL_MS, DEFAULT_SNAPSHOT_RECORD_THRESHOLD,
            new ScheduledThreadPoolExecutor(1, runnable -> {
                Thread thread = new Thread(runnable, "producer-state-manager-snapshot");
                thread.setDaemon(true);
                return thread;
            }), true);
    }

    public ProducerStateManager(
            TopicIdPartition topicIdPartition,
            Supplier<AsyncOxiaClient> oxiaClientSupplier,
            Supplier<CompletableFuture<Log>> logSupplier,
            String zone,
            long snapshotIntervalMs,
            int snapshotRecordThreshold,
            ScheduledExecutorService scheduler) {
        this(topicIdPartition, oxiaClientSupplier, logSupplier,
            zone,
            snapshotIntervalMs, snapshotRecordThreshold, scheduler, false);
    }

    private ProducerStateManager(
            TopicIdPartition topicIdPartition,
            Supplier<AsyncOxiaClient> oxiaClientSupplier,
            Supplier<CompletableFuture<Log>> logSupplier,
            String zone,
            long snapshotIntervalMs,
            int snapshotRecordThreshold,
            ScheduledExecutorService scheduler,
            boolean ownsScheduler) {
        this.topicIdPartition = Objects.requireNonNull(topicIdPartition, "topicIdPartition must not be null");
        this.oxiaClientSupplier = Objects.requireNonNull(oxiaClientSupplier, "oxiaClientSupplier must not be null");
        this.logSupplier = Objects.requireNonNull(logSupplier, "logSupplier must not be null");
        this.zone = DisklessClientZone.normalize(zone);
        this.snapshotIntervalMs = snapshotIntervalMs;
        this.snapshotRecordThreshold = snapshotRecordThreshold;
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler must not be null");
        this.ownsScheduler = ownsScheduler;
        this.periodicSnapshotTask = null;
    }

    public CompletableFuture<PrepareResult> prepareAppend(List<AppendBatch> batches) {
        if (batches == null || batches.isEmpty()) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("batches must not be empty"));
        }

        synchronized (this) {
            if (closed.get()) {
                return CompletableFuture.failedFuture(new IllegalStateException("ProducerStateManager is already closed"));
            }
            if (snapshotOwnershipLost) {
                return CompletableFuture.failedFuture(snapshotOwnershipLostException());
            }

            if (recoveryState == RecoveryState.DONE) {
                return CompletableFuture.completedFuture(prepareAppendInternal(batches));
            }

            CompletableFuture<PrepareResult> future = new CompletableFuture<>();
            pendingPrepareRequests.add(new PendingPrepareRequest(List.copyOf(batches), future));
            if (recoveryState == RecoveryState.NOT_STARTED) {
                recoveryState = RecoveryState.IN_PROGRESS;
                startRecovery();
            }
            return future;
        }
    }

    public AppendResult completeAppend(PendingAppend pendingAppend, long appendBaseOffset, long appendTimestamp) {
        Objects.requireNonNull(pendingAppend, "pendingAppend must not be null");

        boolean triggerSnapshot;
        AppendResult appendResult = null;
        synchronized (this) {
            long currentOffset = appendBaseOffset;
            for (PendingBatch pendingBatch : pendingAppend.pendingBatches) {
                BatchMetadata metadata = pendingBatch.batchMetadata();
                pendingBatch.producerStateEntry().updateBatchTimestamp(metadata.baseOffsetFuture(), appendTimestamp);
                metadata.baseOffsetFuture().complete(currentOffset);
                if (appendResult == null) {
                    appendResult = new AppendResult(currentOffset, appendTimestamp);
                }
                currentOffset += pendingBatch.appendBatch().recordCount();
            }
            recordsSinceSnapshot += pendingAppend.totalRecordCount;
            snapshotRecordGeneration += pendingAppend.totalRecordCount;
            triggerSnapshot = snapshotRecordThreshold > 0 && recordsSinceSnapshot >= snapshotRecordThreshold;
        }

        if (triggerSnapshot) {
            takeSnapshotAsync("threshold");
        }

        if (appendResult == null) {
            return new AppendResult(appendBaseOffset, appendTimestamp);
        }
        return appendResult;
    }

    public void abortAppend(PendingAppend pendingAppend, Throwable cause) {
        if (pendingAppend == null) {
            return;
        }
        Throwable rollbackError = cause != null ? cause : new RuntimeException("Append aborted");
        synchronized (this) {
            rollbackPendingBatches(pendingAppend.pendingBatches, rollbackError);
        }
    }

    public CompletableFuture<Void> takeSnapshot(String reason) {
        synchronized (this) {
            if (closed.get()) {
                return CompletableFuture.completedFuture(null);
            }
            return takeSnapshotLocked(reason);
        }
    }

    private CompletableFuture<Void> takeSnapshotLocked(String reason) {
        assert Thread.holdsLock(this) : "takeSnapshotLocked must be called under manager lock";
        if (inFlightSnapshotWrite != null) {
            if (pendingSnapshotWrite == null) {
                pendingSnapshotWrite = new CompletableFuture<>();
                snapshotWriteTail = pendingSnapshotWrite;
            }
            pendingSnapshotReason = reason;
            return pendingSnapshotWrite;
        }

        CompletableFuture<Void> snapshotFuture = new CompletableFuture<>();
        snapshotWriteTail = snapshotFuture;
        startSnapshotWriteLocked(reason, snapshotFuture);
        return snapshotFuture;
    }

    private void startSnapshotWriteLocked(String reason, CompletableFuture<Void> result) {
        assert Thread.holdsLock(this) : "startSnapshotWriteLocked must be called under manager lock";
        SnapshotPayload payload = buildSnapshotPayloadForWriteLocked(result);
        if (payload == null) {
            return;
        }
        startPersistSnapshotLocked(payload, reason, result);
    }

    private SnapshotPayload buildSnapshotPayloadForWriteLocked(CompletableFuture<Void> result) {
        assert Thread.holdsLock(this) : "buildSnapshotPayloadForWriteLocked must be called under manager lock";
        SnapshotPayload payload;
        try {
            payload = buildSnapshotPayloadLocked();
        } catch (IOException e) {
            log.warn("[{}] Failed to serialize producer state snapshot", topicIdPartition, e);
            result.completeExceptionally(e);
            return null;
        }
        if (payload == null) {
            result.complete(null);
            return null;
        }
        return payload;
    }

    private void startPersistSnapshotLocked(
            SnapshotPayload payload,
            String reason,
            CompletableFuture<Void> result
    ) {
        assert Thread.holdsLock(this) : "startPersistSnapshotLocked must be called under manager lock";
        AsyncOxiaClient client = oxiaClientSupplier.get();
        if (client == null) {
            result.complete(null);
            return;
        }
        if (snapshotOwnershipLost || ownedSnapshotVersionId == null) {
            result.completeExceptionally(new IllegalStateException(
                    "Producer snapshot ownership is not held for " + topicIdPartition));
            return;
        }

        long expectedVersionId = ownedSnapshotVersionId;
        CompletableFuture<Void> writeFuture;
        try {
            writeFuture = persistSnapshot(client, payload, reason, expectedVersionId);
        } catch (Throwable error) {
            loseSnapshotOwnership(expectedVersionId);
            result.completeExceptionally(error);
            return;
        }
        if (writeFuture == null) {
            loseSnapshotOwnership(expectedVersionId);
            result.completeExceptionally(new IllegalStateException(
                    "Snapshot persistence returned null future for " + topicIdPartition));
            return;
        }

        inFlightSnapshotWrite = result;
        writeFuture.whenComplete((ignored, error) -> completeSnapshotWrite(result, error));
    }

    private void completeSnapshotWrite(CompletableFuture<Void> completedWrite, Throwable error) {
        synchronized (this) {
            if (inFlightSnapshotWrite != completedWrite) {
                return;
            }
            inFlightSnapshotWrite = null;
            if (pendingSnapshotWrite != null) {
                CompletableFuture<Void> nextWrite = pendingSnapshotWrite;
                String nextReason = pendingSnapshotReason;
                pendingSnapshotWrite = null;
                pendingSnapshotReason = null;
                startSnapshotWriteLocked(nextReason, nextWrite);
            }
        }
        if (error == null) {
            completedWrite.complete(null);
        } else {
            completedWrite.completeExceptionally(DisklessFutures.unwrap(error));
        }
    }

    private CompletableFuture<Void> persistSnapshot(
            AsyncOxiaClient client,
            SnapshotPayload payload,
            String reason,
            long expectedVersionId
    ) {
        String topicId = topicIdPartition.topicId().toString();
        String snapshotKey = snapshotKey();
        Set<PutOption> putOptions = snapshotPutOptions(
                topicId, PutOption.IfVersionIdEquals(expectedVersionId));

        return client.get(ProducerStateSnapshotKeys.deletedTopicMarkerKey(topicId))
            .thenCompose(deletionFence -> {
                if (deletionFence != null) {
                    loseSnapshotOwnership(expectedVersionId);
                    return CompletableFuture.completedFuture(false);
                }
                return client.put(snapshotKey, payload.bytes(), putOptions)
                    .handle((putResult, putError) -> {
                        if (putError != null) {
                            loseSnapshotOwnership(expectedVersionId);
                            throw new CompletionException(DisklessFutures.unwrap(putError));
                        }
                        return requireVersionId(putResult);
                    })
                    .thenApply(writtenVersionId -> {
                        updateSnapshotOwnership(expectedVersionId, writtenVersionId);
                        return true;
                    });
            })
            .thenAccept(persisted -> {
                if (persisted) {
                    synchronized (this) {
                        recordsSinceSnapshot = Math.max(0L,
                                snapshotRecordGeneration - payload.recordGeneration());
                    }
                }
                if (persisted && log.isDebugEnabled()) {
                    log.debug("[{}] Snapshot stored at offset {} ({} producers, reason: {})",
                        topicIdPartition, payload.lastOffset(), payload.producerCount(), reason);
                }
            })
            .whenComplete((ignored, error) -> {
                if (error != null) {
                    log.warn("[{}] Failed to persist producer state snapshot", topicIdPartition, error);
                }
            });
    }

    private CompletableFuture<Boolean> deleteOwnedSnapshotVersion(
            AsyncOxiaClient client,
            String snapshotKey,
            long versionId
    ) {
        return client.delete(snapshotKey, Set.of(DeleteOption.IfVersionIdEquals(versionId)));
    }

    private Set<PutOption> snapshotPutOptions(String topicId, PutOption ownershipCondition) {
        return Set.of(
                ownershipCondition,
                PutOption.SecondaryIndex(
                        ProducerStateSnapshotKeys.topicIndexName(),
                        ProducerStateSnapshotKeys.topicIndexKey(topicId)));
    }

    private static long requireVersionId(PutResult putResult) {
        if (putResult == null || putResult.version() == null) {
            throw new IllegalStateException("Oxia put result did not include a record version");
        }
        return putResult.version().versionId();
    }

    private synchronized void updateSnapshotOwnership(long expectedVersionId, long writtenVersionId) {
        if (ownedSnapshotVersionId == null || ownedSnapshotVersionId != expectedVersionId) {
            markSnapshotOwnershipLostLocked();
            throw new IllegalStateException("Producer snapshot ownership changed while writing " + topicIdPartition);
        }
        ownedSnapshotVersionId = writtenVersionId;
    }

    private synchronized void loseSnapshotOwnership(long expectedVersionId) {
        if (ownedSnapshotVersionId == null || ownedSnapshotVersionId == expectedVersionId) {
            markSnapshotOwnershipLostLocked();
        }
    }

    private void markSnapshotOwnershipLostLocked() {
        assert Thread.holdsLock(this) : "markSnapshotOwnershipLostLocked must be called under manager lock";
        ownedSnapshotVersionId = null;
        snapshotOwnershipLost = true;
        cancelPeriodicSnapshotTaskLocked();
    }

    private void cancelPeriodicSnapshotTaskLocked() {
        assert Thread.holdsLock(this) : "cancelPeriodicSnapshotTaskLocked must be called under manager lock";
        if (periodicSnapshotTask != null) {
            periodicSnapshotTask.cancel(false);
            periodicSnapshotTask = null;
        }
    }

    public CompletableFuture<Void> cleanup(boolean deleteSnapshot) {
        List<PendingPrepareRequest> requestsToFail;
        CompletableFuture<Void> pendingRecovery;
        CompletableFuture<Void> pendingSnapshotWrites;
        synchronized (this) {
            deleteSnapshotOnCleanup |= deleteSnapshot;
            if (closed.compareAndSet(false, true)) {
                cancelPeriodicSnapshotTaskLocked();
            }
            requestsToFail = new ArrayList<>(pendingPrepareRequests);
            pendingPrepareRequests.clear();
            pendingRecovery = recoveryDrain;
            pendingSnapshotWrites = snapshotWriteTail;
        }
        requestsToFail.forEach(request -> request.future().completeExceptionally(
            new IllegalStateException("ProducerStateManager is closed")));

        CompletableFuture<Void> cleanupFuture = pendingRecovery.handle((ignored, error) -> null)
            .thenCompose(ignored -> {
                if (deleteSnapshot) {
                    return pendingSnapshotWrites.handle((writeIgnored, writeError) -> null)
                            .thenCompose(writeIgnored -> deleteSnapshotFromOxia());
                }
                synchronized (this) {
                    return takeSnapshotLocked("close");
                }
            });
        return cleanupFuture.whenComplete((ignored, error) -> {
            synchronized (this) {
                producers.clear();
                recoveryState = RecoveryState.NOT_STARTED;
                bypassedProducerIdsAfterSkippedRecovery.clear();
                nextOffsetToRecover = DEFAULT_NEXT_OFFSET;
                recordsSinceSnapshot = 0L;
                snapshotRecordGeneration = 0L;
                ownedSnapshotVersionId = null;
            }
            if (ownsScheduler && scheduler != null) {
                scheduler.shutdown();
            }
        });
    }

    public synchronized Optional<ProducerState> producerState(long producerId) {
        ProducerStateEntry entry = producers.get(producerId);
        if (entry == null || entry.isEmpty()) {
            return Optional.empty();
        }
        BatchMetadata lastBatch = entry.batchMetadata().getLast();
        if (!lastBatch.baseOffsetFuture().isDone() || lastBatch.baseOffsetFuture().isCompletedExceptionally()) {
            return Optional.empty();
        }
        long lastOffset = lastBatch.baseOffsetFuture().getNow(-1L);
        if (lastOffset < 0) {
            return Optional.empty();
        }
        return Optional.of(new ProducerState(
            producerId,
            entry.producerEpoch(),
            entry.lastSequence(),
            lastOffset,
            entry.lastTimestamp(),
            entry.batchMetadata().size()
        ));
    }

    @Override
    public void close() {
        cleanup(false).whenComplete((ignored, error) -> {
            if (error != null) {
                log.warn("[{}] Failed to cleanup producer state manager while closing", topicIdPartition, error);
            }
        });
    }

    private void startRecovery() {
        CompletableFuture<Void> recoveryComplete = new CompletableFuture<>();
        synchronized (this) {
            recoveryDrain = recoveryComplete;
        }
        CompletableFuture<Void> recoveryAttempt;
        try {
            recoveryAttempt = recover();
        } catch (Throwable error) {
            recoveryAttempt = CompletableFuture.failedFuture(error);
        }
        recoveryAttempt.whenComplete((ignored, recoveryError) ->
                completeRecovery(recoveryComplete, recoveryError));
    }

    private void completeRecovery(CompletableFuture<Void> recoveryComplete, Throwable recoveryError) {
        try {
            List<PendingPrepareRequest> requestsToReplay;
            boolean managerClosed;
            synchronized (this) {
                requestsToReplay = new ArrayList<>(pendingPrepareRequests);
                pendingPrepareRequests.clear();
                managerClosed = closed.get();
                recoveryState = recoveryError == null && !managerClosed
                        ? RecoveryState.DONE
                        : RecoveryState.NOT_STARTED;
                if (recoveryError != null || managerClosed) {
                    clearRecoveredStateLocked();
                }
            }

            if (recoveryError != null || managerClosed) {
                failRecoveryRequests(requestsToReplay, recoveryError, managerClosed);
            } else {
                ensurePeriodicSnapshotTaskStarted();
                requestsToReplay.forEach(this::completeRecoveredPrepareRequest);
            }
        } finally {
            recoveryComplete.complete(null);
        }
    }

    private void failRecoveryRequests(
            List<PendingPrepareRequest> requests,
            Throwable recoveryError,
            boolean managerClosed
    ) {
        Throwable failure = recoveryError == null
                ? replayClosedException()
                : DisklessFutures.unwrap(recoveryError);
        if (recoveryError != null && !managerClosed) {
            log.warn("[{}] Failed to recover producer state", topicIdPartition, failure);
        }
        requests.forEach(request -> request.future().completeExceptionally(failure));
    }

    private void completeRecoveredPrepareRequest(PendingPrepareRequest request) {
        try {
            PrepareResult result;
            synchronized (this) {
                if (closed.get()) {
                    throw replayClosedException();
                }
                if (snapshotOwnershipLost) {
                    throw snapshotOwnershipLostException();
                }
                result = prepareAppendInternal(request.batches());
            }
            request.future().complete(result);
        } catch (Throwable error) {
            request.future().completeExceptionally(error);
        }
    }

    private CompletableFuture<Void> recover() {
        return loadSnapshot()
            .thenCompose(ignored -> replayFromLog());
    }

    private CompletableFuture<Void> loadSnapshot() {
        clearRecoveredStateLocked();

        AsyncOxiaClient client = oxiaClientSupplier.get();
        if (client == null) {
            return CompletableFuture.completedFuture(null);
        }

        synchronized (this) {
            if (snapshotOwnershipLost) {
                return CompletableFuture.failedFuture(new IllegalStateException(
                        "Producer snapshot ownership was lost for " + topicIdPartition));
            }
            ownedSnapshotVersionId = null;
        }

        String topicId = topicIdPartition.topicId().toString();
        String snapshotKey = snapshotKey();
        return client.get(ProducerStateSnapshotKeys.deletedTopicMarkerKey(topicId))
                .thenCompose(deletionFence -> {
                    if (deletionFence != null) {
                        synchronized (this) {
                            markSnapshotOwnershipLostLocked();
                        }
                        return CompletableFuture.<Void>failedFuture(deletedTopicException());
                    }
                    return readAndClaimSnapshot(
                            client, snapshotKey, topicId, SNAPSHOT_CLAIM_MAX_ATTEMPTS);
                });
    }

    private IllegalStateException deletedTopicException() {
        return new IllegalStateException(
                "Producer state was permanently deleted for topic " + topicIdPartition.topicId());
    }

    private CompletableFuture<Void> readAndClaimSnapshot(
            AsyncOxiaClient client,
            String snapshotKey,
            String topicId,
            int attemptsRemaining
    ) {
        clearRecoveredStateLocked();
        if (closed.get()) {
            return CompletableFuture.failedFuture(replayClosedException());
        }
        return client.get(snapshotKey)
                .thenCompose(snapshot -> claimSnapshotOwnership(
                        client,
                        snapshotKey,
                        topicId,
                        snapshot,
                        attemptsRemaining));
    }

    private CompletableFuture<Void> claimSnapshotOwnership(
            AsyncOxiaClient client,
            String snapshotKey,
            String topicId,
            GetResult snapshot,
            int attemptsRemaining
    ) {
        if (closed.get()) {
            return CompletableFuture.failedFuture(replayClosedException());
        }

        byte[] claimPayload;
        PutOption ownershipCondition;
        try {
            if (snapshot == null) {
                claimPayload = ProducerStateSerDes.emptySnapshot();
                ownershipCondition = PutOption.IfRecordDoesNotExist;
            } else {
                if (snapshot.version() == null) {
                    throw new IllegalStateException("Oxia snapshot read did not include a record version");
                }
                claimPayload = restoreSnapshotForClaim(snapshot);
                ownershipCondition = PutOption.IfVersionIdEquals(snapshot.version().versionId());
            }
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }

        return client.put(snapshotKey, claimPayload, snapshotPutOptions(topicId, ownershipCondition))
                .handle((putResult, putError) -> handleSnapshotClaimResult(
                        client,
                        snapshotKey,
                        topicId,
                        attemptsRemaining,
                        putResult,
                        putError))
                .thenCompose(result -> result);
    }

    private CompletableFuture<Void> handleSnapshotClaimResult(
            AsyncOxiaClient client,
            String snapshotKey,
            String topicId,
            int attemptsRemaining,
            PutResult putResult,
            Throwable putError
    ) {
        if (putError != null) {
            Throwable failure = DisklessFutures.unwrap(putError);
            if (isDefinitiveClaimConflict(failure)) {
                if (attemptsRemaining > 1) {
                    return readAndClaimSnapshot(
                            client,
                            snapshotKey,
                            topicId,
                            attemptsRemaining - 1);
                }
                return CompletableFuture.failedFuture(failure);
            }
            synchronized (this) {
                markSnapshotOwnershipLostLocked();
            }
            return CompletableFuture.failedFuture(failure);
        }

        long claimedVersionId;
        try {
            claimedVersionId = requireVersionId(putResult);
        } catch (Throwable error) {
            synchronized (this) {
                markSnapshotOwnershipLostLocked();
            }
            return CompletableFuture.failedFuture(error);
        }
        return finishSnapshotClaim(client, snapshotKey, claimedVersionId);
    }

    private CompletableFuture<Void> finishSnapshotClaim(
            AsyncOxiaClient client,
            String snapshotKey,
            long claimedVersionId
    ) {
        synchronized (this) {
            if (snapshotOwnershipLost || ownedSnapshotVersionId != null) {
                return deleteOwnedSnapshotVersion(client, snapshotKey, claimedVersionId)
                        .thenCompose(ignored -> CompletableFuture.failedFuture(
                                new IllegalStateException(
                                        "Producer snapshot ownership changed while claiming "
                                                + topicIdPartition)));
            }
            ownedSnapshotVersionId = claimedVersionId;
        }
        if (closed.get()) {
            boolean deleteClaim;
            synchronized (this) {
                deleteClaim = deleteSnapshotOnCleanup;
            }
            if (deleteClaim) {
                loseSnapshotOwnership(claimedVersionId);
                return deleteOwnedSnapshotVersion(client, snapshotKey, claimedVersionId)
                        .thenCompose(ignored -> CompletableFuture.failedFuture(replayClosedException()));
            }
            return CompletableFuture.failedFuture(replayClosedException());
        }
        return CompletableFuture.completedFuture(null);
    }

    private static boolean isDefinitiveClaimConflict(Throwable error) {
        return error instanceof UnexpectedVersionIdException || error instanceof KeyAlreadyExistsException;
    }

    private byte[] restoreSnapshotForClaim(GetResult snapshot) throws IOException {
        byte[] value = snapshot.value();
        if (value == null || value.length == 0) {
            clearRecoveredStateLocked();
            return ProducerStateSerDes.emptySnapshot();
        }
        try {
            Map<Long, ProducerStateEntry> snapshotProducers = ProducerStateSerDes.deserialize(value);
            synchronized (this) {
                restoreFromSnapshotLocked(snapshotProducers);
            }
            return value;
        } catch (Exception parseError) {
            log.warn("[{}] Replacing corrupt producer snapshot while claiming ownership",
                    topicIdPartition, parseError);
            clearRecoveredStateLocked();
            return ProducerStateSerDes.emptySnapshot();
        }
    }

    private CompletableFuture<Void> replayFromLog() {
        return logSupplier.get().thenCompose(this::replayLogEntries);
    }

    private CompletableFuture<Void> replayLogEntries(Log logInstance) {
        return logInstance.getLastOffset().thenCompose(lastOffset -> {
            long endOffset = lastOffset.offset() < 0 ? 0L
                    : lastOffset.offset() + Math.max(1, lastOffset.numberOfRecords());

            long startOffset;
            synchronized (this) {
                startOffset = nextOffsetToRecover;
            }
            if (startOffset >= endOffset) {
                return CompletableFuture.completedFuture(null);
            }

            boolean hasNoSnapshot = startOffset == DEFAULT_NEXT_OFFSET;
            if (shouldSkipReplay(hasNoSnapshot, endOffset - startOffset)) {
                return CompletableFuture.completedFuture(null);
            }

            long cursorStartOffset = startOffset == DEFAULT_NEXT_OFFSET ? 0L : startOffset;
            return openReplayCursorAndReplay(logInstance, cursorStartOffset, endOffset);
        });
    }

    private boolean shouldSkipReplay(boolean hasNoSnapshot, long messagesToRecover) {
        if (hasNoSnapshot && snapshotRecordThreshold > 0 && messagesToRecover > snapshotRecordThreshold) {
            synchronized (this) {
                recoverySkippedDueToExcessiveReplay = true;
                bypassedProducerIdsAfterSkippedRecovery.clear();
            }
            log.warn("[{}] Skipping producer-state replay without snapshot because messages to recover {} "
                    + "exceed snapshot threshold {}", topicIdPartition, messagesToRecover, snapshotRecordThreshold);
            return true;
        }
        return false;
    }

    private CompletableFuture<Void> openReplayCursorAndReplay(
            Log logInstance, long cursorStartOffset, long endOffset) {
        String cursorName = "producer-state-replay-" + topicIdPartition.topic() + "-"
            + topicIdPartition.partition() + "-" + cursorStartOffset;

        return logInstance.openEphemeralCursor(cursorName, cursorStartOffset)
            .thenCompose(cursor -> {
                CompletableFuture<Void> replayFuture = new CompletableFuture<>();
                CompletableFuture<Void> result = new CompletableFuture<>();
                AtomicInteger replayEntries = new AtomicInteger();
                replayFuture.whenComplete((__, error) -> {
                    closeReplayCursor(cursor).whenComplete((closeIgnored, closeError) -> {
                        if (error != null) {
                            log.warn("[{}] Failed to replay producer state from log", topicIdPartition, error);
                            result.completeExceptionally(error);
                        } else if (closeError != null) {
                            result.completeExceptionally(closeError);
                        } else {
                            if (replayEntries.get() > 0) {
                                takeSnapshotAsync("replay");
                            }
                            log.info("[{}] Finished replaying producer state from log, replayed {} entries",
                                topicIdPartition, replayEntries.get());
                            result.complete(null);
                        }
                    });
                });
                scheduleNextReplayRead(cursor, endOffset, replayEntries, replayFuture);
                return result;
            });
    }

    private CompletableFuture<Void> closeReplayCursor(LogCursor cursor) {
        CompletableFuture<Void> closeDrain = new CompletableFuture<>();
        CompletableFuture<Void> existing = replayCursorCloses.putIfAbsent(cursor, closeDrain);
        if (existing != null) {
            return existing;
        }
        closeDrain.whenComplete((ignored, error) -> replayCursorCloses.remove(cursor, closeDrain));
        scheduleReplayCursorClose(cursor, closeDrain, false);
        return closeDrain;
    }

    private void scheduleReplayCursorClose(
            LogCursor cursor,
            CompletableFuture<Void> closeDrain,
            boolean delayed) {
        if (closeDrain.isDone()) {
            return;
        }
        Runnable closeTask = () -> {
            try {
                cursor.close();
                closeDrain.complete(null);
            } catch (Throwable closeError) {
                log.warn("[{}] Failed to close replay cursor; retaining it for retry",
                        topicIdPartition, closeError);
                scheduleReplayCursorClose(cursor, closeDrain, true);
            }
        };
        try {
            if (delayed) {
                scheduler.schedule(closeTask, CURSOR_CLOSE_RETRY_DELAY_MS, TimeUnit.MILLISECONDS);
            } else {
                scheduler.execute(closeTask);
            }
        } catch (RuntimeException scheduleError) {
            log.warn("[{}] Failed to schedule replay cursor close; retaining it for retry",
                    topicIdPartition, scheduleError);
            CompletableFuture.delayedExecutor(
                    CURSOR_CLOSE_RETRY_DELAY_MS,
                    TimeUnit.MILLISECONDS).execute(closeTask);
        }
    }

    private void asyncReplayEntries(LogCursor cursor,
                                    long endOffset,
                                    final AtomicInteger replayedEntries,
                                    CompletableFuture<Void> replayFuture) {
        if (replayFuture.isDone()) {
            return;
        }
        if (closed.get()) {
            replayFuture.completeExceptionally(replayClosedException());
            return;
        }
        long nextOffset;
        synchronized (this) {
            nextOffset = nextOffsetToRecover;
        }
        if (nextOffset >= endOffset) {
            replayFuture.complete(null);
            return;
        }
        CompletableFuture<List<LogEntry>> readFuture;
        try {
            readFuture = cursor.readEntries(
                    REPLAY_MAX_ENTRIES_PER_READ, REPLAY_MAX_BYTES_PER_READ, null, endOffset);
            if (readFuture == null) {
                throw new IllegalStateException("LogCursor.readEntries returned null future");
            }
        } catch (Throwable readError) {
            replayFuture.completeExceptionally(readError);
            return;
        }
        readFuture.whenComplete((entries, readError) -> {
            if (readError != null) {
                Throwable closeError = closeLogEntries(entries, null);
                if (closeError != null && closeError != readError) {
                    readError.addSuppressed(closeError);
                }
                if (!replayFuture.isDone()) {
                    replayFuture.completeExceptionally(readError);
                } else if (closeError != null) {
                    log.warn("[{}] Failed to close replay entries after a late failed read",
                            topicIdPartition, closeError);
                }
                return;
            }
            processReplayBatch(entries, cursor, endOffset, replayedEntries, replayFuture);
        });
    }

    private void processReplayBatch(List<LogEntry> entries,
                                    LogCursor cursor,
                                    long endOffset,
                                    AtomicInteger replayedEntries,
                                    CompletableFuture<Void> replayFuture) {
        boolean replayComplete = false;
        Throwable processingError = null;
        if (!replayFuture.isDone()) {
            if (closed.get()) {
                processingError = replayClosedException();
            } else if (entries == null || entries.isEmpty()) {
                replayComplete = true;
            } else {
                for (LogEntry entry : entries) {
                    try {
                        if (closed.get()) {
                            processingError = replayClosedException();
                            break;
                        }
                        if (replayEntry(entry) == ReplayEntryResult.MANAGER_CLOSED) {
                            processingError = replayClosedException();
                            break;
                        }
                        replayedEntries.incrementAndGet();
                    } catch (Throwable entryError) {
                        log.error("[{}] Failed to replay entry at offset {}",
                                topicIdPartition, replayEntryOffset(entry, entryError), entryError);
                        processingError = entryError;
                        break;
                    }
                }
            }
        }

        processingError = closeLogEntries(entries, processingError);
        if (replayFuture.isDone()) {
            if (processingError != null) {
                log.warn("[{}] Failed to finish a replay batch after replay completion",
                        topicIdPartition, processingError);
            }
            return;
        }
        if (processingError != null) {
            replayFuture.completeExceptionally(processingError);
        } else if (replayComplete) {
            replayFuture.complete(null);
        } else {
            scheduleNextReplayRead(cursor, endOffset, replayedEntries, replayFuture);
        }
    }

    private static Object replayEntryOffset(LogEntry entry, Throwable entryError) {
        if (entry == null) {
            return "null";
        }
        try {
            return entry.offset();
        } catch (Throwable offsetError) {
            if (entryError != offsetError) {
                entryError.addSuppressed(offsetError);
            }
            return "unknown";
        }
    }

    private static Throwable closeLogEntries(List<LogEntry> entries, Throwable precedingError) {
        return LogEntryUtils.closeAll(entries, precedingError);
    }

    private void scheduleNextReplayRead(LogCursor cursor,
                                        long endOffset,
                                        AtomicInteger replayedEntries,
                                        CompletableFuture<Void> replayFuture) {
        if (replayFuture.isDone()) {
            return;
        }
        if (closed.get()) {
            replayFuture.completeExceptionally(replayClosedException());
            return;
        }
        try {
            scheduler.execute(() -> asyncReplayEntries(
                cursor, endOffset, replayedEntries, replayFuture));
        } catch (RuntimeException scheduleError) {
            replayFuture.completeExceptionally(scheduleError);
        }
    }

    private ReplayEntryResult replayEntry(LogEntry entry) {
        if (closed.get()) {
            return ReplayEntryResult.MANAGER_CLOSED;
        }
        long baseOffset = entry.offset();
        long nextOffset = Math.addExact(baseOffset, entry.numberOfRecords());
        try {
            MemoryRecords records = KafkaRecordsPayload.readableRecords(
                    entry.payload(), baseOffset, entry.numberOfRecords());
            // Header-only: individual records are never decoded during replay.
            for (RecordBatch batch : records.batches()) {
                if (batch.hasProducerId()) {
                    synchronized (this) {
                        if (closed.get()) {
                            return ReplayEntryResult.MANAGER_CLOSED;
                        }
                        appendRecoveredBatchLocked(
                            batch.producerId(),
                            batch.producerEpoch(),
                            batch.baseSequence(),
                            batch.lastSequence(),
                            batch.baseOffset(),
                            batch.maxTimestamp()
                        );
                    }
                }
            }
        } catch (RuntimeException malformed) {
            // A single unreadable entry must not abort recovery for the whole partition.
            log.warn("[{}] Skipping malformed entry at offset {} during producer-state replay",
                    topicIdPartition, baseOffset, malformed);
        } finally {
            synchronized (this) {
                if (!closed.get()) {
                    nextOffsetToRecover = Math.max(nextOffsetToRecover, nextOffset);
                }
            }
        }
        return ReplayEntryResult.CONTINUE;
    }

    // Visible for testing: how far producer-state replay has advanced.
    synchronized long nextOffsetToRecoverForTesting() {
        return nextOffsetToRecover;
    }

    private IllegalStateException replayClosedException() {
        return new IllegalStateException("ProducerStateManager is closed during replay for " + topicIdPartition);
    }

    private PrepareResult prepareAppendInternal(List<AppendBatch> batches) {
        Map<Long, ProducerShadowState> producerShadowStateMap = new HashMap<>();
        Map<Long, ProducerStateEntry> transientProducerStateEntries = new HashMap<>();
        List<PendingBatch> pendingBatches = new ArrayList<>();
        AtomicReference<BatchMetadata> firstDuplicateRef = new AtomicReference<>();

        for (AppendBatch batch : batches) {
            PrepareResult validationResult = validateAndPrepareSingleBatch(
                batch, producerShadowStateMap, transientProducerStateEntries, pendingBatches, firstDuplicateRef);
            if (validationResult != null) {
                return validationResult;
            }
        }

        if (!pendingBatches.isEmpty()) {
            int totalRecords = pendingBatches.stream().mapToInt(batch -> batch.appendBatch().recordCount()).sum();
            return new Ready(new PendingAppend(pendingBatches, totalRecords));
        }

        BatchMetadata firstDuplicate = firstDuplicateRef.get();
        if (firstDuplicate != null) {
            final BatchMetadata duplicateBatchMetadata = firstDuplicate;
            CompletableFuture<AppendResult> duplicateFuture = duplicateBatchMetadata.baseOffsetFuture()
                .thenApply(offset -> new AppendResult(offset, duplicateBatchMetadata.timestamp()));
            return new Duplicate(duplicateFuture);
        }

        return new OutOfOrderSequence(0, 0, "No producer batches found for " + topicIdPartition);
    }

    private PrepareResult validateAndPrepareSingleBatch(
        AppendBatch batch,
        Map<Long, ProducerShadowState> producerShadowStateMap,
        Map<Long, ProducerStateEntry> transientProducerStateEntries,
        List<PendingBatch> pendingBatches,
        AtomicReference<BatchMetadata> firstDuplicateRef) {
        ProducerStateEntry producerStateEntry = producers.get(batch.producerId());
        boolean hasExistingProducerState = producerStateEntry != null;
        if (producerStateEntry == null) {
            producerStateEntry = transientProducerStateEntries.computeIfAbsent(
                batch.producerId(),
                ignored -> new ProducerStateEntry(batch.producerEpoch(), batch.timestamp())
            );
        }

        final ProducerStateEntry baseStateEntry = producerStateEntry;
        ProducerShadowState shadowState = producerShadowStateMap.computeIfAbsent(
            batch.producerId(),
            ignored -> new ProducerShadowState(baseStateEntry.producerEpoch(), baseStateEntry.lastSequence())
        );

        Optional<BatchMetadata> duplicateBatch = producerStateEntry.findDuplicate(
            batch.producerEpoch(), batch.firstSequence(), batch.lastSequence());
        if (duplicateBatch.isPresent()) {
            return handleDuplicateBatch(
                batch, shadowState, duplicateBatch.get(), pendingBatches, firstDuplicateRef);
        }

        BatchMetadata firstDuplicate = firstDuplicateRef.get();
        if (firstDuplicate != null) {
            int expected = shadowState.expectedSequence();
            return new OutOfOrderSequence(expected, batch.firstSequence(),
                "Mixed duplicate and new batches for producer " + batch.producerId()
                    + " on " + topicIdPartition);
        }

        if (batch.producerEpoch() < shadowState.producerEpoch()) {
            rollbackPendingBatches(pendingBatches, new IllegalStateException("invalid producer epoch"));
            String message = "Producer " + batch.producerId() + " epoch " + batch.producerEpoch()
                + " is smaller than current epoch " + shadowState.producerEpoch()
                + " on " + topicIdPartition;
            return new InvalidEpoch(shadowState.producerEpoch(), batch.producerEpoch(), message);
        }

        if (batch.producerEpoch() > shadowState.producerEpoch()) {
            if (batch.firstSequence() != 0) {
                rollbackPendingBatches(pendingBatches, new IllegalStateException("first sequence after epoch bump must be 0"));
                String message = "First sequence after epoch bump for producer " + batch.producerId()
                    + " on " + topicIdPartition + " must be 0, got " + batch.firstSequence();
                return new OutOfOrderSequence(0, batch.firstSequence(), message);
            }
            shadowState.reset(batch.producerEpoch());
        }

        int expectedSequence = shadowState.expectedSequence();
        if (batch.firstSequence() != expectedSequence
                && !consumeOneTimeSequenceValidationBypassIfEnabled(batch.producerId(), hasExistingProducerState)) {
            rollbackPendingBatches(pendingBatches, new IllegalStateException("out of order sequence"));
            String message = "Out of order sequence for producer " + batch.producerId() + " on "
                + topicIdPartition + ": expected " + expectedSequence + ", got " + batch.firstSequence();
            return new OutOfOrderSequence(expectedSequence, batch.firstSequence(), message);
        }

        final ProducerStateEntry validatedStateEntry = producerStateEntry;
        producerStateEntry = producers.computeIfAbsent(
            batch.producerId(),
            ignored -> validatedStateEntry
        );
        BatchMetadata batchMetadata = new BatchMetadata(
            batch.firstSequence(),
            batch.lastSequence(),
            new CompletableFuture<>(),
            batch.timestamp()
        );
        producerStateEntry.appendBatch(batch.producerEpoch(), batchMetadata);
        pendingBatches.add(new PendingBatch(producerStateEntry, batch, batchMetadata));
        shadowState.onBatch(batch.producerEpoch(), batch.lastSequence());
        return null;
    }

    private boolean consumeOneTimeSequenceValidationBypassIfEnabled(long producerId, boolean hasExistingProducerState) {
        if (!recoverySkippedDueToExcessiveReplay) {
            return false;
        }
        if (hasExistingProducerState) {
            return false;
        }
        return bypassedProducerIdsAfterSkippedRecovery.add(producerId);
    }

    private PrepareResult handleDuplicateBatch(
        AppendBatch batch,
        ProducerShadowState shadowState,
        BatchMetadata duplicateBatch,
        List<PendingBatch> pendingBatches,
        AtomicReference<BatchMetadata> firstDuplicateRef) {
        if (!pendingBatches.isEmpty()) {
            rollbackPendingBatches(pendingBatches, new IllegalStateException("mixed duplicate and new batches"));
            int expected = shadowState.expectedSequence();
            return new OutOfOrderSequence(expected, batch.firstSequence(),
                "Mixed duplicate and new batches for producer " + batch.producerId()
                    + " on " + topicIdPartition);
        }
        firstDuplicateRef.compareAndSet(null, duplicateBatch);
        shadowState.onBatch(batch.producerEpoch(), batch.lastSequence());
        return null;
    }

    private void rollbackPendingBatches(List<PendingBatch> pendingBatches, Throwable cause) {
        assert Thread.holdsLock(this) : "rollbackPendingBatches must be called under manager lock";
        for (PendingBatch pendingBatch : pendingBatches) {
            BatchMetadata metadata = pendingBatch.batchMetadata();
            ProducerStateEntry stateEntry = pendingBatch.producerStateEntry();
            long producerId = pendingBatch.appendBatch().producerId();
            if (pendingBatch.producerStateEntry().removeBatch(
                pendingBatch.appendBatch().producerEpoch(),
                pendingBatch.appendBatch().firstSequence(),
                pendingBatch.appendBatch().lastSequence(),
                metadata)) {
                metadata.baseOffsetFuture().completeExceptionally(cause);
            }
            if (stateEntry.isEmpty() && producers.get(producerId) == stateEntry) {
                producers.remove(producerId);
            }
        }
    }

    private SnapshotPayload buildSnapshotPayloadLocked() throws IOException {
        if (producers.isEmpty()) {
            return null;
        }

        ProducerStateSerDes.SerializationResult serializationResult =
            ProducerStateSerDes.serialize(producers, MAX_SNAPSHOT_VALUE_BYTES);
        if (serializationResult == null) {
            return null;
        }

        return new SnapshotPayload(
            serializationResult.bytes(),
            serializationResult.lastOffset(),
            serializationResult.producerCount(),
            snapshotRecordGeneration);
    }

    private void restoreFromSnapshotLocked(Map<Long, ProducerStateEntry> snapshotProducers) {
        clearRecoveredStateLocked();
        producers.putAll(snapshotProducers);
        for (ProducerStateEntry stateEntry : snapshotProducers.values()) {
            for (BatchMetadata batchMetadata : stateEntry.batchMetadata()) {
                long baseOffset = batchMetadata.baseOffsetFuture().getNow(-1L);
                if (baseOffset < 0) {
                    continue;
                }
                nextOffsetToRecover = Math.max(nextOffsetToRecover, baseOffset + batchMetadata.numMessages());
            }
        }
    }

    private void appendRecoveredBatchLocked(
            long producerId,
            short producerEpoch,
            int firstSequence,
            int lastSequence,
            long baseOffset,
            long timestamp) {
        ProducerStateEntry stateEntry = producers.computeIfAbsent(
            producerId,
            ignored -> new ProducerStateEntry(producerEpoch, timestamp)
        );
        CompletableFuture<Long> offsetFuture = CompletableFuture.completedFuture(baseOffset);
        stateEntry.appendBatch(producerEpoch, new BatchMetadata(firstSequence, lastSequence, offsetFuture, timestamp));
    }

    private CompletableFuture<Void> deleteSnapshotFromOxia() {
        AsyncOxiaClient client = oxiaClientSupplier.get();
        if (client == null) {
            return CompletableFuture.completedFuture(null);
        }
        Long ownedVersionId;
        synchronized (this) {
            ownedVersionId = ownedSnapshotVersionId;
        }
        if (ownedVersionId == null) {
            return CompletableFuture.completedFuture(null);
        }
        return deleteOwnedSnapshotVersion(client, snapshotKey(), ownedVersionId)
            .handle((ignored, error) -> {
                if (error != null
                        && !(DisklessFutures.unwrap(error) instanceof UnexpectedVersionIdException)) {
                    log.warn("[{}] Failed to delete producer snapshot", topicIdPartition, error);
                }
                return null;
            });
    }

    private void takeSnapshotAsync(String reason) {
        CompletableFuture<Void> snapshotFuture;
        synchronized (this) {
            if (closed.get() || snapshotOwnershipLost) {
                return;
            }
            snapshotFuture = takeSnapshotLocked(reason);
            if (asyncObservedSnapshotWrite == snapshotFuture) {
                return;
            }
            asyncObservedSnapshotWrite = snapshotFuture;
        }
        snapshotFuture.whenComplete((ignored, error) -> {
            synchronized (this) {
                if (asyncObservedSnapshotWrite == snapshotFuture) {
                    asyncObservedSnapshotWrite = null;
                }
            }
            if (error != null) {
                log.warn("[{}] Snapshot attempt failed ({})", topicIdPartition, reason, error);
            }
        });
    }

    private void ensurePeriodicSnapshotTaskStarted() {
        synchronized (this) {
            if (snapshotIntervalMs <= 0
                    || periodicSnapshotTask != null
                    || closed.get()
                    || snapshotOwnershipLost) {
                return;
            }
            periodicSnapshotTask = scheduler.scheduleAtFixedRate(
                () -> takeSnapshotAsync("periodic"),
                snapshotIntervalMs,
                snapshotIntervalMs,
                TimeUnit.MILLISECONDS);
        }
    }

    private NotLeaderOrFollowerException snapshotOwnershipLostException() {
        return new NotLeaderOrFollowerException(
                "Producer snapshot ownership was lost for " + topicIdPartition);
    }

    private String snapshotKey() {
        return ProducerStateSnapshotKeys.snapshotKey(topicIdPartition.topicId().toString(),
                topicIdPartition.partition(), zone);
    }

    private synchronized void clearRecoveredStateLocked() {
        producers.clear();
        bypassedProducerIdsAfterSkippedRecovery.clear();
        nextOffsetToRecover = DEFAULT_NEXT_OFFSET;
    }

    private static int nextSequence(int lastSequence) {
        return lastSequence == Integer.MAX_VALUE ? 0 : lastSequence + 1;
    }
}
