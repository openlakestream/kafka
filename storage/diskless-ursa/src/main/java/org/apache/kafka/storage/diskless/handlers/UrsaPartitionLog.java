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
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.storage.diskless.DisklessFutures;
import org.apache.kafka.storage.diskless.ListOffsetsPartitionRequest;
import org.apache.kafka.storage.diskless.ListOffsetsPartitionResponse;
import org.apache.kafka.storage.diskless.idempotent.ProducerStateManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import io.lakestream.api.Log;
import io.lakestream.api.LogOffset;
import io.lakestream.api.exception.LogFencedException;
import io.lakestream.api.exception.NoSuchStreamException;
import io.oxia.client.api.AsyncOxiaClient;

/**
 * One diskless partition, as the broker sees it: the facade over the Lakestream {@link Log} handle
 * and the three components that use it.
 *
 * <p>{@link PartitionWriter} owns produce, {@link PartitionReader} owns fetch and ListOffsets, and
 * {@link PartitionRetention} owns trims. This class owns only what they share: opening the handle,
 * the per-zone {@link ProducerStateManager}s, the log metrics, error mapping, and close.
 *
 * <p>The writer exists from construction because an open that has already failed runs its callback
 * inline, from this constructor; the reader appears only once the handle is open, so a request that
 * arrives before then waits on the init future rather than on a half-built reader.
 */
final class UrsaPartitionLog {

    private static final Logger log = LoggerFactory.getLogger(UrsaPartitionLog.class);
    private static final int MAX_ENTRIES_PER_FETCH = 10;

    private final TopicIdPartition topicIdPartition;
    private final UrsaStorageState state;
    private final Supplier<AsyncOxiaClient> oxiaClientSupplier;
    private final long producerStateSnapshotIntervalMs;
    private final int producerStateSnapshotRecordThreshold;
    private final ScheduledExecutorService producerStateScheduler;
    private final DisklessLogMetrics logMetrics;
    private final CompletableFuture<Log> logFuture;
    private final CompletableFuture<Log> initFuture;
    private final PartitionWriter writer;
    private final PartitionRetention retention;
    private final ConcurrentHashMap<String, ProducerStateManager> producerStateManagers = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
    private volatile PartitionReader reader;

    UrsaPartitionLog(TopicIdPartition topicIdPartition,
                     UrsaStorageState state,
                     DisklessLogMetrics logMetrics,
                     CompletableFuture<Log> logFuture,
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
        this.logFuture = logFuture;
        this.writer = new PartitionWriter(
                topicIdPartition,
                this::initialized,
                this::getOrCreateProducerStateManager,
                state.timestampType(topicIdPartition.topic()),
                state.time(),
                this::observeAppend);
        this.retention = new PartitionRetention(
                topicIdPartition, this::initialized, error -> invalidate(), this::invalidateReaderRange);
        this.initFuture = createInitFuture(logFuture);
    }

    TopicIdPartition topicIdPartition() {
        return topicIdPartition;
    }

    PartitionRetention retention() {
        return retention;
    }

    boolean initializationFailed() {
        return initFuture.isCompletedExceptionally();
    }

    CompletableFuture<PartitionResponse> write(MemoryRecords records, String zone, String writerName) {
        log.debug("Writing {} bytes to partition {} via {}", records.sizeInBytes(), topicIdPartition, writerName);
        return writer.write(records, zone).exceptionally(this::writeErrorResponse);
    }

    CompletableFuture<FetchPartitionData> fetch(FetchRequest.PartitionData partitionData) {
        return initialized()
                .thenCompose(logInstance -> activeReader().fetch(partitionData))
                .exceptionally(error -> createFetchErrorResponse(mapException(error)));
    }

    /**
     * Registers a long-poll waiter for this partition. The reader registers one before its first
     * read so that an append landing during that read wakes the request instead of being missed,
     * and completes it when the request ends -- the deadline is the request's, not this waiter's.
     */
    CompletableFuture<Void> awaitAppend() {
        return writer.awaitAppend();
    }

    /** Adopts a topic configuration change that this partition's writer caches. */
    void applyTimestampType(TimestampType timestampType) {
        writer.applyTimestampType(timestampType);
    }

    CompletableFuture<ListOffsetsPartitionResponse> listOffsets(ListOffsetsPartitionRequest request) {
        return initialized()
                .thenCompose(logInstance -> activeReader().listOffsets(request))
                .exceptionally(error -> ListOffsetsPartitionResponse.error(topicIdPartition, mapException(error)));
    }

    synchronized ProducerStateManager getOrCreateProducerStateManager(String zone) {
        if (closed.get()) {
            throw ownershipLostException();
        }
        return producerStateManagers.computeIfAbsent(zone, zoneId -> new ProducerStateManager(
                topicIdPartition,
                oxiaClientSupplier,
                () -> initFuture,
                zoneId,
                producerStateSnapshotIntervalMs,
                producerStateSnapshotRecordThreshold,
                producerStateScheduler));
    }

    synchronized void installProducerStateManager(String zone, ProducerStateManager producerStateManager) {
        if (closed.get()) {
            throw ownershipLostException();
        }
        producerStateManagers.put(zone, producerStateManager);
    }

    boolean cleanupNonOwnedProducerStates(Set<String> ownedZones, boolean deletePartition) {
        boolean cleaned = false;
        for (String zone : producerStateZones()) {
            if (ownedZones.contains(zone)) {
                continue;
            }
            cleaned = cleanupProducerState(zone, deletePartition).isPresent() || cleaned;
        }
        return cleaned;
    }

    /**
     * Retires this partition log. The returned future completes once the reader, writer, retention
     * and producer state are done with the handle and {@link Log#closeAsync()} has been requested;
     * the handle's own close is retried inside ursa-storage and is deliberately not awaited, so no
     * request or timer thread ever blocks on storage here.
     */
    CompletableFuture<Void> close(boolean deletePartition) {
        PartitionReader retiredReader;
        synchronized (this) {
            if (!closed.compareAndSet(false, true)) {
                return closeFuture;
            }
            retiredReader = reader;
            reader = null;
        }
        writer.close();
        if (retiredReader != null) {
            // The cursor must be gone before the handle it reads through is closed. Metrics are
            // registered together with the reader, so an open that never completed has none.
            retiredReader.close();
            logMetrics.remove(topicIdPartition);
        }
        CompletableFuture<Void> producerState = cleanupProducerStates(deletePartition);
        CompletableFuture<Void> trims = retention.close();
        CompletableFuture.allOf(producerState, trims).whenComplete((ignored, error) ->
                logFuture.whenComplete((logInstance, openError) -> {
                    if (logInstance != null) {
                        try {
                            // Assumes the Ursa LeasedLog overrides closeAsync() with a close that
                            // returns immediately and retries itself. The interface default runs
                            // the blocking close() on the calling thread, which here is whichever
                            // thread completed the producer-state and trim drains.
                            logInstance.closeAsync();
                        } catch (Throwable closeError) {
                            log.warn("Failed to request the close of the log for partition {}",
                                    topicIdPartition, closeError);
                        }
                    }
                    closeFuture.complete(null);
                }));
        return closeFuture;
    }

    /** Retires this partition log and drops it from the state, so the next request reopens it. */
    void invalidate() {
        close(false);
        state.removePartitionLog(topicIdPartition, this);
    }

    int ownedWritePayloadCount() {
        return writer.ownedWritePayloadCount();
    }

    private CompletableFuture<Log> initialized() {
        return initFuture;
    }

    /**
     * Widens the reader's cached offset window to cover an append this broker just made. The
     * reader exists by then: the writer only appends once the init future has completed, which is
     * what installs the reader.
     */
    private void observeAppend(LogOffset appended) {
        PartitionReader currentReader = reader;
        if (currentReader != null) {
            currentReader.observeAppend(appended);
        }
    }

    /** Drops the reader's cached offset window once a trim has moved the log's first offset. */
    private void invalidateReaderRange() {
        PartitionReader currentReader = reader;
        if (currentReader != null) {
            currentReader.invalidateRange();
        }
    }

    /** The reader installed once the log opened; it is dropped as soon as this partition log closes. */
    private PartitionReader activeReader() {
        PartitionReader currentReader = reader;
        if (currentReader == null) {
            throw ownershipLostException();
        }
        return currentReader;
    }

    private NotLeaderOrFollowerException ownershipLostException() {
        return new NotLeaderOrFollowerException("Partition log is closed for " + topicIdPartition);
    }

    private CompletableFuture<Log> createInitFuture(CompletableFuture<Log> opened) {
        CompletableFuture<Log> initialized = new CompletableFuture<>();
        opened.whenComplete((logInstance, error) -> {
            if (error != null) {
                if (closed.get()) {
                    initialized.completeExceptionally(
                            new NotLeaderOrFollowerException("Partition log already closed"));
                    return;
                }
                log.warn("Failed to open Log for partition {}, evicting from cache", topicIdPartition, error);
                // The writer stays open on purpose: this partition log was never leased, so a
                // request that still reaches it must report the open failure rather than a lost
                // leadership. Every in-flight write drains through the failed init future below.
                // Cleanup runs first: the failed init future releases queued writes, and a
                // draining produce must not recreate a ProducerStateManager after the cleanup.
                cleanupProducerStates(false);
                initialized.completeExceptionally(error);
                // An already-failed future invokes this callback inside the map's mapping function,
                // where removing the same key is a recursive update. In that case
                // getOrCreatePartitionLog evicts the failed value right after it is published.
                if (state.partitionLog(topicIdPartition) == this) {
                    state.removePartitionLog(topicIdPartition, this);
                }
                return;
            }

            try {
                install(logInstance, initialized);
            } catch (Throwable initializationError) {
                initialized.completeExceptionally(initializationError);
                close(false);
            }
        });
        return initialized;
    }

    /** Publishes the reader and the log metrics, or leaves neither behind if registration fails. */
    private synchronized void install(Log logInstance, CompletableFuture<Log> initialized) {
        if (closed.get()) {
            throw new NotLeaderOrFollowerException("Partition log already closed");
        }
        UrsaPartitionLog activePartitionLog = state.partitionLog(topicIdPartition);
        if (activePartitionLog != null && activePartitionLog != this) {
            throw new NotLeaderOrFollowerException("Partition log already replaced");
        }
        PartitionReader openedReader = new PartitionReader(
                topicIdPartition, logInstance, MAX_ENTRIES_PER_FETCH, state.time());
        try {
            logMetrics.register(topicIdPartition, logInstance);
        } catch (Throwable metricRegistrationError) {
            logMetrics.remove(topicIdPartition);
            openedReader.close();
            throw metricRegistrationError;
        }
        reader = openedReader;
        initialized.complete(logInstance);
    }

    private CompletableFuture<Void> cleanupProducerStates(boolean deletePartition) {
        List<CompletableFuture<Void>> cleanups = new ArrayList<>();
        for (String zone : producerStateZones()) {
            cleanupProducerState(zone, deletePartition).ifPresent(cleanups::add);
        }
        return CompletableFuture.allOf(cleanups.toArray(new CompletableFuture<?>[0]));
    }

    private Set<String> producerStateZones() {
        return new LinkedHashSet<>(producerStateManagers.keySet());
    }

    /** Empty when the zone has no manager, or when the state is no longer accepting cleanups. */
    private synchronized Optional<CompletableFuture<Void>> cleanupProducerState(
            String zone,
            boolean deletePartition) {
        ProducerStateManager producerStateManager = producerStateManagers.get(zone);
        if (producerStateManager == null) {
            return Optional.empty();
        }
        return state.startProducerStateCleanup(() -> {
            producerStateManagers.remove(zone, producerStateManager);
            return producerStateManager.cleanup(deletePartition);
        }).map(cleanup -> cleanup.<Void>handle((ignored, error) -> {
            if (error != null) {
                log.warn("Failed to cleanup producer state manager for partition {} and zone {}",
                        topicIdPartition, zone, error);
            }
            return null;
        }));
    }

    /**
     * The error a client sees for one storage failure. Classification alone: the failure may have
     * been raised by the partition-log lookup itself, before any handle existed to retire.
     */
    static Errors classify(Throwable error) {
        Throwable cause = DisklessFutures.unwrap(error);
        if (hasCause(cause, NotLeaderOrFollowerException.class)
                || hasCause(cause, LogFencedException.class)) {
            return Errors.NOT_LEADER_OR_FOLLOWER;
        }
        if (hasCause(cause, NoSuchStreamException.class)) {
            return Errors.UNKNOWN_TOPIC_OR_PARTITION;
        }
        return Errors.KAFKA_STORAGE_ERROR;
    }

    private Errors mapException(Throwable error) {
        Errors mapped = classify(error);
        if (mapped == Errors.NOT_LEADER_OR_FOLLOWER || mapped == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
            // The handle behind this partition log is gone; the next request opens a fresh one.
            invalidate();
        }
        return mapped;
    }

    private PartitionResponse writeErrorResponse(Throwable error) {
        Errors mapped = classify(error);
        Throwable cause = DisklessFutures.unwrap(error);
        if (mapped == Errors.NOT_LEADER_OR_FOLLOWER) {
            invalidate();
            log.info("Partition log is no longer local owner for partition {}", topicIdPartition, cause);
        } else if (mapped == Errors.UNKNOWN_TOPIC_OR_PARTITION) {
            invalidate();
            log.debug("Partition log is not provisioned yet for partition {}", topicIdPartition, cause);
        } else {
            log.error("Failed to write to partition {}", topicIdPartition, error);
        }
        return new PartitionResponse(mapped);
    }

    /**
     * Answers one partition of a request whose partition log could not be resolved at all -- the
     * topic was deleted under this broker, or the storage state is closing.
     *
     * <p>Resolution happens partition by partition while the request is being fanned out, so by the
     * time one partition throws the earlier ones may already have been handed to storage. Failing
     * only the partition that could not be resolved is what keeps those earlier records from being
     * reported as failed and produced a second time.
     */
    static Errors unresolved(TopicIdPartition topicIdPartition, String operation, Throwable error) {
        Errors mapped = classify(error);
        log.debug("Diskless {} for partition {} failed with {} before its log was resolved",
                operation, topicIdPartition, mapped, error);
        return mapped;
    }

    static boolean hasCause(Throwable error, Class<? extends Throwable> type) {
        Throwable cause = error;
        while (cause != null) {
            if (type.isInstance(cause)) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    static FetchPartitionData createFetchErrorResponse(Errors error) {
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
}
