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
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.storage.diskless.DisklessClientZone;
import org.apache.kafka.storage.diskless.handlers.KafkaEntryFormatter;

import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import io.oxia.client.api.AsyncOxiaClient;
import io.streamnative.ursa.mledger.UrsaPosition;

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

    private record SnapshotPayload(byte[] bytes, long lastOffset, int producerCount) {
    }

    private enum ReplayEntryResult {
        HAS_PRODUCER_BATCH,
        NO_PRODUCER_BATCH,
        DECODE_FAILED
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
    private final Supplier<CompletableFuture<ManagedLedger>> managedLedgerSupplier;
    private final String zone;
    private final long snapshotIntervalMs;
    private final int snapshotRecordThreshold;
    private final ScheduledExecutorService scheduler;
    private final boolean ownsScheduler;
    private volatile ScheduledFuture<?> periodicSnapshotTask;

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Map<Long, ProducerStateEntry> producers = new HashMap<>();
    private final List<PendingPrepareRequest> pendingPrepareRequests = new ArrayList<>();

    private RecoveryState recoveryState = RecoveryState.NOT_STARTED;
    private boolean recoverySkippedDueToExcessiveReplay = false;
    private final Set<Long> bypassedProducerIdsAfterSkippedRecovery = new HashSet<>();
    private long nextOffsetToRecover = DEFAULT_NEXT_OFFSET;
    private long recordsSinceSnapshot = 0L;

    public ProducerStateManager(
            TopicIdPartition topicIdPartition,
            Supplier<AsyncOxiaClient> oxiaClientSupplier,
            Supplier<CompletableFuture<ManagedLedger>> managedLedgerSupplier) {
        this(topicIdPartition, oxiaClientSupplier, managedLedgerSupplier,
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
            Supplier<CompletableFuture<ManagedLedger>> managedLedgerSupplier,
            String zone,
            long snapshotIntervalMs,
            int snapshotRecordThreshold,
            ScheduledExecutorService scheduler) {
        this(topicIdPartition, oxiaClientSupplier, managedLedgerSupplier,
            zone,
            snapshotIntervalMs, snapshotRecordThreshold, scheduler, false);
    }

    private ProducerStateManager(
            TopicIdPartition topicIdPartition,
            Supplier<AsyncOxiaClient> oxiaClientSupplier,
            Supplier<CompletableFuture<ManagedLedger>> managedLedgerSupplier,
            String zone,
            long snapshotIntervalMs,
            int snapshotRecordThreshold,
            ScheduledExecutorService scheduler,
            boolean ownsScheduler) {
        this.topicIdPartition = Objects.requireNonNull(topicIdPartition, "topicIdPartition must not be null");
        this.oxiaClientSupplier = Objects.requireNonNull(oxiaClientSupplier, "oxiaClientSupplier must not be null");
        this.managedLedgerSupplier = Objects.requireNonNull(managedLedgerSupplier, "managedLedgerSupplier must not be null");
        this.zone = DisklessClientZone.normalize(zone);
        this.snapshotIntervalMs = snapshotIntervalMs;
        this.snapshotRecordThreshold = snapshotRecordThreshold;
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler must not be null");
        this.ownsScheduler = ownsScheduler;
        this.periodicSnapshotTask = null;
    }

    public CompletableFuture<PrepareResult> prepareAppend(List<AppendBatch> batches) {
        ensurePeriodicSnapshotTaskStarted();
        if (batches == null || batches.isEmpty()) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("batches must not be empty"));
        }

        synchronized (this) {
            if (closed.get()) {
                return CompletableFuture.failedFuture(new IllegalStateException("ProducerStateManager is already closed"));
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
        if (closed.get()) {
            return CompletableFuture.completedFuture(null);
        }

        SnapshotPayload payload;
        try {
            synchronized (this) {
                payload = buildSnapshotPayloadLocked();
            }
        } catch (IOException e) {
            log.warn("[{}] Failed to serialize producer state snapshot", topicIdPartition, e);
            return CompletableFuture.failedFuture(e);
        }

        if (payload == null) {
            return CompletableFuture.completedFuture(null);
        }

        AsyncOxiaClient client = oxiaClientSupplier.get();
        if (client == null) {
            return CompletableFuture.completedFuture(null);
        }

        return client.put(snapshotKey(), payload.bytes())
            .thenAccept(ignored -> {
                synchronized (this) {
                    recordsSinceSnapshot = 0L;
                }
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Snapshot stored at offset {} ({} producers, reason: {})",
                        topicIdPartition, payload.lastOffset(), payload.producerCount(), reason);
                }
            })
            .exceptionally(error -> {
                log.warn("[{}] Failed to persist producer state snapshot", topicIdPartition, error);
                return null;
            });
    }

    public CompletableFuture<Void> cleanup(boolean deleteSnapshot) {
        if (!deleteSnapshot) {
            takeSnapshotAsync("close");
        }

        if (closed.compareAndSet(false, true)) {
            if (periodicSnapshotTask != null) {
                periodicSnapshotTask.cancel(false);
            }
        }

        List<PendingPrepareRequest> requestsToFail;
        synchronized (this) {
            requestsToFail = new ArrayList<>(pendingPrepareRequests);
            pendingPrepareRequests.clear();
            producers.clear();
            recoveryState = RecoveryState.NOT_STARTED;
            bypassedProducerIdsAfterSkippedRecovery.clear();
            nextOffsetToRecover = DEFAULT_NEXT_OFFSET;
            recordsSinceSnapshot = 0L;
        }
        requestsToFail.forEach(request -> request.future().completeExceptionally(
            new IllegalStateException("ProducerStateManager is closed")));

        CompletableFuture<Void> deleteFuture =
                deleteSnapshot ? deleteSnapshotFromOxia() : CompletableFuture.completedFuture(null);
        if (ownsScheduler && scheduler != null) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        return deleteFuture;
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
        cleanup(false);
    }

    private void startRecovery() {
        recover().whenComplete((ignored, recoveryError) -> {
            List<PendingPrepareRequest> requestsToReplay;
            synchronized (this) {
                requestsToReplay = new ArrayList<>(pendingPrepareRequests);
                pendingPrepareRequests.clear();
                recoveryState = recoveryError == null ? RecoveryState.DONE : RecoveryState.NOT_STARTED;
            }

            if (recoveryError != null) {
                log.warn("[{}] Failed to recover producer state", topicIdPartition, recoveryError);
                requestsToReplay.forEach(request -> request.future().completeExceptionally(recoveryError));
                return;
            }

            for (PendingPrepareRequest request : requestsToReplay) {
                try {
                    PrepareResult result;
                    synchronized (this) {
                        result = prepareAppendInternal(request.batches());
                    }
                    request.future().complete(result);
                } catch (Throwable t) {
                    request.future().completeExceptionally(t);
                }
            }
        });
    }

    private CompletableFuture<Void> recover() {
        return loadSnapshot()
            .thenCompose(ignored -> replayFromManagedLedger())
            .exceptionally(error -> {
                throw new CompletionException(error);
            });
    }

    private CompletableFuture<Void> loadSnapshot() {
        clearRecoveredStateLocked();

        AsyncOxiaClient client = oxiaClientSupplier.get();
        if (client == null) {
            return CompletableFuture.completedFuture(null);
        }

        return loadSnapshotFromKey(client, snapshotKey()).thenAccept(ignored -> { });
    }

    private CompletableFuture<Void> replayFromManagedLedger() {
        return managedLedgerSupplier.get().thenCompose(this::replayManagedLedgerEntries);
    }

    private CompletableFuture<Void> replayManagedLedgerEntries(ManagedLedger managedLedger) {
        Position lastConfirmed = managedLedger.getLastConfirmedEntry();
        long endOffset = getNextOffsetForUrsa(lastConfirmed);

        long startOffset;
        synchronized (this) {
            startOffset = nextOffsetToRecover;
        }
        if (startOffset >= endOffset) {
            return CompletableFuture.completedFuture(null);
        }

        boolean hasNoSnapshot = startOffset == DEFAULT_NEXT_OFFSET;
        long messagesToRecover = endOffset - startOffset;
        if (hasNoSnapshot
                && snapshotRecordThreshold > 0
                && messagesToRecover > snapshotRecordThreshold) {
            synchronized (this) {
                recoverySkippedDueToExcessiveReplay = true;
                bypassedProducerIdsAfterSkippedRecovery.clear();
            }
            log.warn("[{}] Skipping producer-state replay without snapshot because messages to recover {} "
                    + "exceed snapshot threshold {}", topicIdPartition, messagesToRecover, snapshotRecordThreshold);
            return CompletableFuture.completedFuture(null);
        }
        Position startPosition = startOffset == DEFAULT_NEXT_OFFSET
            ? PositionFactory.EARLIEST
            : PositionFactory.create(lastConfirmed.getLedgerId(), startOffset);

        String cursorName = "producer-state-replay-" + topicIdPartition.topic() + "-"
            + topicIdPartition.partition() + "-" + startOffset;
        ManagedCursor cursor;
        try {
            cursor = managedLedger.newNonDurableCursor(startPosition, cursorName);
        } catch (ManagedLedgerException e) {
            throw new CompletionException(e);
        }

        CompletableFuture<Void> replayFuture = new CompletableFuture<>();
        AtomicInteger replayEntries = new AtomicInteger();
        replayFuture.whenComplete((__, error) -> {
            cleanupReplayCursor(managedLedger, cursor.getName());
            if (error != null) {
                log.warn("[{}] Failed to replay producer state from managed ledger", topicIdPartition, error);
            } else {
                if (replayEntries.get() > 0) {
                    takeSnapshotAsync("replay");
                }
                log.info("[{}] Finished replaying producer state from managed ledger, replayed {} entries",
                    topicIdPartition, replayEntries.get());
            }
        });
        scheduleNextReplayRead(cursor, lastConfirmed, endOffset, hasNoSnapshot, replayEntries, replayFuture);
        return replayFuture;
    }

    private void asyncReplayEntries(ManagedCursor cursor,
                                    Position lastConfirmed,
                                    long endOffset,
                                    boolean hasNoSnapshot,
                                    final AtomicInteger replayedEntries,
                                    CompletableFuture<Void> replayFuture) {
        if (replayFuture.isDone()) {
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
        cursor.asyncReadEntries(REPLAY_MAX_ENTRIES_PER_READ, REPLAY_MAX_BYTES_PER_READ,
                new AsyncCallbacks.ReadEntriesCallback() {
                    @Override
                    public void readEntriesComplete(List<Entry> entries, Object ctx) {
                        if (replayFuture.isDone()) {
                            if (entries != null && !entries.isEmpty()) {
                                entries.forEach(ProducerStateManager::safeRelease);
                            }
                            return;
                        }
                        if (entries == null || entries.isEmpty()) {
                            replayFuture.complete(null);
                            return;
                        }
                        for (Entry entry : entries) {
                            try {
                                if (entry.getPosition().compareTo(lastConfirmed) > 0) {
                                    replayFuture.complete(null);
                                    return;
                                }
                                ReplayEntryResult replayEntryResult = replayEntry(entry);
                                if (hasNoSnapshot
                                        && replayedEntries.get() == 0
                                        && replayEntryResult == ReplayEntryResult.NO_PRODUCER_BATCH) {
                                    // Align with KoP behavior: when recovering from scratch and the first entry is
                                    // non-idempotent, stop replay early and rebuild state from subsequent
                                    // new idempotent writes.
                                    replayFuture.complete(null);
                                    return;
                                }
                                replayedEntries.incrementAndGet();
                            } catch (Exception e) {
                                log.error("[{}] Failed to replay entry at offset {}",
                                        topicIdPartition, entry == null ? "null" : entry.getEntryId(), e);
                                replayFuture.completeExceptionally(e);
                                return;
                            } finally {
                                safeRelease(entry);
                            }
                        }
                        scheduleNextReplayRead(cursor, lastConfirmed, endOffset,
                            hasNoSnapshot, replayedEntries, replayFuture);
                    }

                    @Override
                    public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
                        if (!replayFuture.isDone()) {
                            replayFuture.completeExceptionally(exception);
                        }
                    }
                }, null, lastConfirmed);
    }

    private void scheduleNextReplayRead(ManagedCursor cursor,
                                        Position lastConfirmed,
                                        long endOffset,
                                        boolean hasNoSnapshot,
                                        AtomicInteger replayedEntries,
                                        CompletableFuture<Void> replayFuture) {
        if (replayFuture.isDone()) {
            return;
        }
        try {
            scheduler.execute(() -> asyncReplayEntries(
                cursor, lastConfirmed, endOffset, hasNoSnapshot, replayedEntries, replayFuture));
        } catch (RuntimeException scheduleError) {
            replayFuture.completeExceptionally(scheduleError);
        }
    }

    private void cleanupReplayCursor(ManagedLedger managedLedger, String cursorName) {
        managedLedger.asyncDeleteCursor(cursorName, new AsyncCallbacks.DeleteCursorCallback() {
            @Override
            public void deleteCursorComplete(Object ctx) {
                log.debug("[{}] Replay cursor {} deleted", topicIdPartition, cursorName);
            }

            @Override
            public void deleteCursorFailed(ManagedLedgerException exception, Object ctx) {
                log.warn("[{}] Failed to delete replay cursor {}", topicIdPartition, cursorName, exception);
            }
        }, null);
    }

    private ReplayEntryResult replayEntry(Entry entry) {
        long baseOffset = entry.getEntryId();
        long nextOffset = getNextOffsetForUrsa(entry.getPosition());

        try {
            ByteBuffer kafkaRecordsBuffer = KafkaEntryFormatter.decode(entry.getDataBuffer().duplicate());
            if (kafkaRecordsBuffer.remaining() >= Long.BYTES) {
                kafkaRecordsBuffer.putLong(kafkaRecordsBuffer.position(), baseOffset);
            }

            MemoryRecords memoryRecords = MemoryRecords.readableRecords(kafkaRecordsBuffer);
            long batchBaseOffset = baseOffset;
            boolean hasProducerBatch = false;
            for (RecordBatch batch : memoryRecords.batches()) {
                int recordCount = batch.countOrNull() != null ? batch.countOrNull() : 0;
                if (batch.hasProducerId()) {
                    hasProducerBatch = true;
                    synchronized (this) {
                        appendRecoveredBatchLocked(
                            batch.producerId(),
                            batch.producerEpoch(),
                            batch.baseSequence(),
                            batch.lastSequence(),
                            batchBaseOffset,
                            batch.maxTimestamp()
                        );
                    }
                }
                batchBaseOffset += recordCount;
            }
            return hasProducerBatch ? ReplayEntryResult.HAS_PRODUCER_BATCH : ReplayEntryResult.NO_PRODUCER_BATCH;
        } catch (Exception decodeError) {
            log.warn("[{}] Failed to replay entry at offset {}", topicIdPartition, baseOffset, decodeError);
            return ReplayEntryResult.DECODE_FAILED;
        } finally {
            synchronized (this) {
                nextOffsetToRecover = Math.max(nextOffsetToRecover, nextOffset);
            }
        }
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
            serializationResult.producerCount());
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

    private CompletableFuture<Boolean> loadSnapshotFromKey(
            AsyncOxiaClient client,
            String snapshotKey) {
        return client.get(snapshotKey)
            .handle((getResult, error) -> {
                if (error != null) {
                    log.warn("[{}] Failed to load producer snapshot from key {}",
                            topicIdPartition,
                            snapshotKey,
                            error);
                    return false;
                }
                if (getResult == null || getResult.value() == null || getResult.value().length == 0) {
                    return false;
                }
                try {
                    Map<Long, ProducerStateEntry> snapshot = ProducerStateSerDes.deserialize(getResult.value());
                    synchronized (this) {
                        restoreFromSnapshotLocked(snapshot);
                    }
                    return true;
                } catch (Exception parseError) {
                    log.warn("[{}] Ignoring producer snapshot due to parse failure",
                            topicIdPartition,
                            parseError);
                    clearRecoveredStateLocked();
                    return false;
                }
            });
    }

    private CompletableFuture<Void> deleteSnapshotFromOxia() {
        AsyncOxiaClient client = oxiaClientSupplier.get();
        if (client == null) {
            return CompletableFuture.completedFuture(null);
        }
        return client.delete(snapshotKey())
            .handle((ignored, error) -> {
                if (error != null) {
                    log.warn("[{}] Failed to delete producer snapshot", topicIdPartition, error);
                }
                return null;
            });
    }

    private void takeSnapshotAsync(String reason) {
        if (closed.get()) {
            return;
        }
        takeSnapshot(reason).exceptionally(error -> {
            log.warn("[{}] Snapshot attempt failed ({})", topicIdPartition, reason, error);
            return null;
        });
    }

    private void ensurePeriodicSnapshotTaskStarted() {
        synchronized (this) {
            if (snapshotIntervalMs <= 0 || periodicSnapshotTask != null || closed.get()) {
                return;
            }
            periodicSnapshotTask = scheduler.scheduleAtFixedRate(
                () -> takeSnapshotAsync("periodic"),
                snapshotIntervalMs,
                snapshotIntervalMs,
                TimeUnit.MILLISECONDS);
        }
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

    private static void safeRelease(Entry entry) {
        try {
            entry.release();
        } catch (Exception releaseError) {
            log.warn("Failed to release managed-ledger entry {}", entry.getPosition(), releaseError);
        }
    }

    private static long getNextOffsetForUrsa(Position position) {
        if (position == null || position.getEntryId() < 0) {
            return 0L;
        }
        if (position instanceof UrsaPosition ursaPosition) {
            return ursaPosition.getEntryId() + Math.max(1L, ursaPosition.numMessages());
        }
        return position.getEntryId() + 1L;
    }

    private static int nextSequence(int lastSequence) {
        return lastSequence == Integer.MAX_VALUE ? 0 : lastSequence + 1;
    }
}
