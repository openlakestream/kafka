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
import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import io.oxia.client.api.AsyncOxiaClient;

/**
 * Persistent producer state store that uses Oxia for snapshot storage.
 * 
 * <p>This implementation extends {@link InMemoryProducerStateStore} with:
 * <ul>
 *   <li><b>Snapshot Persistence</b>: Periodically saves producer state to Oxia</li>
 *   <li><b>Recovery</b>: Loads snapshot on partition initialization</li>
 *   <li><b>Init Barrier</b>: Blocks validation until partition is initialized</li>
 * </ul>
 * 
 * <h2>Snapshot Strategy</h2>
 * <p>Snapshots are taken:
 * <ul>
 *   <li>Every {@code snapshotIntervalMs} milliseconds (default: 30 seconds)</li>
 *   <li>When record count since last snapshot exceeds {@code snapshotRecordThreshold}</li>
 * </ul>
 * 
 * <h2>Recovery Flow</h2>
 * <ol>
 *   <li>On first access to a partition, load snapshot from Oxia</li>
 *   <li>Mark partition as initialized</li>
 * </ol>
 * 
 * <h2>Oxia Key Format</h2>
 * <pre>producer-state-snapshot/{topicId}-{partition}</pre>
 */
public class UrsaProducerStateStore extends InMemoryProducerStateStore {

    private static final Logger log = LoggerFactory.getLogger(UrsaProducerStateStore.class);

    private static final long DEFAULT_SNAPSHOT_INTERVAL_MS = 30_000;
    private static final int DEFAULT_SNAPSHOT_RECORD_THRESHOLD = 10_000;

    private final Supplier<AsyncOxiaClient> oxiaClientSupplier;
    private final Time time;
    private final long snapshotIntervalMs;
    private final int snapshotRecordThreshold;

    // Tracks initialization state per partition
    private final ConcurrentHashMap<TopicIdPartition, CompletableFuture<Long>> initBarriers = 
        new ConcurrentHashMap<>();

    // Tracks records written since last snapshot per partition
    private final ConcurrentHashMap<TopicIdPartition, AtomicLong> recordsSinceSnapshot = 
        new ConcurrentHashMap<>();

    // Tracks last snapshot offset per partition
    private final ConcurrentHashMap<TopicIdPartition, Long> lastSnapshotOffset = 
        new ConcurrentHashMap<>();

    // Background scheduler for periodic snapshots
    private final ScheduledExecutorService scheduler;
    private final ConcurrentHashMap<TopicIdPartition, ScheduledFuture<?>> snapshotTasks = 
        new ConcurrentHashMap<>();

    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Creates a new UrsaProducerStateStore with default settings.
     * 
     * @param oxiaClientSupplier supplier for the Oxia client (may return null if not available)
     * @param time time source for timestamps
     */
    public UrsaProducerStateStore(Supplier<AsyncOxiaClient> oxiaClientSupplier, Time time) {
        this(oxiaClientSupplier, time, DEFAULT_SNAPSHOT_INTERVAL_MS, DEFAULT_SNAPSHOT_RECORD_THRESHOLD);
    }

    /**
     * Creates a new UrsaProducerStateStore with custom snapshot settings.
     * 
     * @param oxiaClientSupplier supplier for the Oxia client
     * @param time time source for timestamps
     * @param snapshotIntervalMs interval between periodic snapshots
     * @param snapshotRecordThreshold number of records to trigger a snapshot
     */
    public UrsaProducerStateStore(
            Supplier<AsyncOxiaClient> oxiaClientSupplier,
            Time time,
            long snapshotIntervalMs,
            int snapshotRecordThreshold) {
        this.oxiaClientSupplier = oxiaClientSupplier;
        this.time = time;
        this.snapshotIntervalMs = snapshotIntervalMs;
        this.snapshotRecordThreshold = snapshotRecordThreshold;
        this.scheduler = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "producer-state-snapshot-scheduler");
            t.setDaemon(true);
            return t;
        });
        log.info("UrsaProducerStateStore initialized with snapshotIntervalMs={}, snapshotRecordThreshold={}",
            snapshotIntervalMs, snapshotRecordThreshold);
    }

    @Override
    public CompletableFuture<ValidationResult> validateAndUpdate(ValidationRequest request) {
        TopicIdPartition tp = request.topicPartition();

        // Ensure partition is initialized before validation
        return ensureInitialized(tp)
            .thenCompose(snapshotOffset -> {
                // Delegate to parent for actual validation
                return super.validateAndUpdate(request);
            })
            .thenApply(result -> {
                // Track records for snapshot threshold
                if (result instanceof ValidationResult.Success) {
                    recordsSinceSnapshot
                        .computeIfAbsent(tp, k -> new AtomicLong(0))
                        .incrementAndGet();
                    maybeScheduleSnapshot(tp);
                }
                return result;
            });
    }

    @Override
    public CompletableFuture<ValidationResult> validateAll(List<SequenceValidationRequest> requests) {
        if (requests.isEmpty()) {
            return CompletableFuture.completedFuture(new ValidationResult.Success(-1));
        }

        TopicIdPartition tp = requests.get(0).topicPartition();
        return ensureInitialized(tp)
            .thenCompose(snapshotOffset -> super.validateAll(requests));
    }

    @Override
    public CompletableFuture<Void> updateAfterWrite(StateUpdateRequest request) {
        TopicIdPartition tp = request.topicPartition();
        return ensureInitialized(tp)
            .thenCompose(snapshotOffset -> super.updateAfterWrite(request))
            .thenRun(() -> {
                recordsSinceSnapshot
                    .computeIfAbsent(tp, k -> new AtomicLong(0))
                    .incrementAndGet();
                maybeScheduleSnapshot(tp);
            });
    }

    /**
     * Ensures the partition is initialized by loading snapshot if needed.
     * 
     * @param tp the topic-partition
     * @return future completing with the snapshot offset (-1 if no snapshot)
     */
    public CompletableFuture<Long> ensureInitialized(TopicIdPartition tp) {
        return initBarriers.computeIfAbsent(tp, this::initializePartition);
    }

    /**
     * Initializes a partition by loading its snapshot from Oxia.
     */
    private CompletableFuture<Long> initializePartition(TopicIdPartition tp) {
        log.info("Initializing producer state for partition {}", tp);

        AsyncOxiaClient client = oxiaClientSupplier.get();
        if (client == null) {
            log.warn("Oxia client not available, partition {} will start with empty state", tp);
            schedulePeriodicSnapshot(tp);
            return CompletableFuture.completedFuture(-1L);
        }

        String key = generateSnapshotKey(tp);
        return client.get(key)
            .thenApply(result -> {
                long snapshotOffset = -1L;
                if (result != null && result.value() != null) {
                    try {
                        byte[] data = result.value();
                        ProducerStateSnapshot snapshot = ProducerStateSnapshot.fromBytes(data);
                        restoreFromSnapshot(tp, snapshot);
                        snapshotOffset = snapshot.offset();
                        log.info("Restored producer state for partition {} from snapshot at offset {}", 
                            tp, snapshotOffset);
                    } catch (Exception e) {
                        log.error("Failed to deserialize snapshot for partition {}, starting with empty state", 
                            tp, e);
                    }
                } else {
                    log.info("No snapshot found for partition {}, starting with empty state", tp);
                }
                
                schedulePeriodicSnapshot(tp);
                if (snapshotOffset > 0) {
                    lastSnapshotOffset.put(tp, snapshotOffset);
                }
                return snapshotOffset;
            })
            .exceptionally(e -> {
                log.warn("Failed to initialize partition {}: {}", tp, e.getMessage());
                schedulePeriodicSnapshot(tp);
                return -1L;
            });
    }

    /**
     * Restores producer state from a snapshot.
     */
    private void restoreFromSnapshot(TopicIdPartition tp, ProducerStateSnapshot snapshot) {
        Map<Long, ProducerState> states = snapshot.toProducerStates();
        
        // Restore each producer state by simulating validation requests
        for (Map.Entry<Long, ProducerState> entry : states.entrySet()) {
            ProducerState state = entry.getValue();
            
            // Create a validation request to establish the producer state
            // We use lastSequence + 1 as baseSequence for the "next" expected write
            // This is just to prime the state; actual sequence validation starts fresh
            ValidationRequest request = ValidationRequest.builder()
                .topicPartition(tp)
                .producerId(state.producerId())
                .producerEpoch(state.epoch())
                .baseSequence(0)  // Will be overwritten
                .lastSequence(state.lastSequence())
                .assignedOffset(state.lastOffset())
                .timestamp(state.lastTimestamp())
                .build();

            // Directly update the in-memory state (bypassing validation)
            super.validateAndUpdate(request);
        }

        lastSnapshotOffset.put(tp, snapshot.offset());
        recordsSinceSnapshot.put(tp, new AtomicLong(0));
    }

    @Override
    public CompletableFuture<Void> takeSnapshot(TopicIdPartition tp, long offset) {
        AsyncOxiaClient client = oxiaClientSupplier.get();
        if (client == null) {
            log.debug("Oxia client not available, skipping snapshot for partition {}", tp);
            return CompletableFuture.completedFuture(null);
        }

        return getProducerStates(tp)
            .thenCompose(states -> {
                if (states.isEmpty()) {
                    log.debug("No producer states to snapshot for partition {}", tp);
                    return CompletableFuture.completedFuture(null);
                }

                try {
                    ProducerStateSnapshot snapshot = ProducerStateSnapshot.create(
                        offset, 
                        time.milliseconds(), 
                        states
                    );
                    byte[] data = snapshot.toBytes();
                    String key = generateSnapshotKey(tp);

                    return client.put(key, data)
                        .thenAccept(result -> {
                            lastSnapshotOffset.put(tp, offset);
                            recordsSinceSnapshot.computeIfAbsent(tp, k -> new AtomicLong(0)).set(0);
                            log.debug("Saved snapshot for partition {} at offset {} ({} producers, {} bytes)",
                                tp, offset, states.size(), data.length);
                        });
                } catch (Exception e) {
                    log.error("Failed to serialize snapshot for partition {}", tp, e);
                    return CompletableFuture.failedFuture(e);
                }
            })
            .exceptionally(e -> {
                log.error("Failed to take snapshot for partition {}", tp, e);
                return null;
            });
    }

    @Override
    public CompletableFuture<Long> loadSnapshot(TopicIdPartition tp) {
        return ensureInitialized(tp);
    }

    /**
     * Gets all producer states for a partition.
     */
    private CompletableFuture<Map<Long, ProducerState>> getProducerStates(TopicIdPartition tp) {
        return getAllProducerStates(tp);
    }

    @Override
    public CompletableFuture<Void> clearPartition(TopicIdPartition tp) {
        // Cancel any scheduled snapshot task
        ScheduledFuture<?> task = snapshotTasks.remove(tp);
        if (task != null) {
            task.cancel(false);
        }

        // Clear tracking state
        initBarriers.remove(tp);
        recordsSinceSnapshot.remove(tp);
        lastSnapshotOffset.remove(tp);

        // Delete snapshot from Oxia
        AsyncOxiaClient client = oxiaClientSupplier.get();
        if (client != null) {
            String key = generateSnapshotKey(tp);
            client.delete(key)
                .exceptionally(e -> {
                    log.warn("Failed to delete snapshot for partition {}: {}", tp, e.getMessage());
                    return false;
                });
        }

        return super.clearPartition(tp);
    }

    /**
     * Schedules periodic snapshot for a partition.
     */
    private void schedulePeriodicSnapshot(TopicIdPartition tp) {
        if (closed.get()) {
            return;
        }

        ScheduledFuture<?> existingTask = snapshotTasks.get(tp);
        if (existingTask != null) {
            return;  // Already scheduled
        }

        ScheduledFuture<?> task = scheduler.scheduleAtFixedRate(
            () -> {
                if (!closed.get()) {
                    Long currentOffset = lastSnapshotOffset.get(tp);
                    // Use current offset or 0 if not set
                    takeSnapshot(tp, currentOffset != null ? currentOffset : 0);
                }
            },
            snapshotIntervalMs,
            snapshotIntervalMs,
            TimeUnit.MILLISECONDS
        );

        snapshotTasks.put(tp, task);
        log.debug("Scheduled periodic snapshot for partition {} every {}ms", tp, snapshotIntervalMs);
    }

    /**
     * Checks if a snapshot should be taken based on record threshold.
     */
    private void maybeScheduleSnapshot(TopicIdPartition tp) {
        AtomicLong counter = recordsSinceSnapshot.get(tp);
        if (counter != null && counter.get() >= snapshotRecordThreshold) {
            Long currentOffset = lastSnapshotOffset.get(tp);
            if (currentOffset != null) {
                // Trigger async snapshot
                takeSnapshot(tp, currentOffset);
            }
        }
    }

    /**
     * Generates the Oxia key for a partition's snapshot.
     */
    private String generateSnapshotKey(TopicIdPartition tp) {
        return ProducerStateSnapshot.generateSnapshotKey(tp.topicId().toString(), tp.partition());
    }

    /**
     * Updates the last known offset for a partition.
     * Called after records are successfully written.
     */
    public void updateOffset(TopicIdPartition tp, long offset) {
        lastSnapshotOffset.compute(tp, (k, v) -> v == null ? offset : Math.max(v, offset));
    }

    /**
     * Checks if a partition is initialized.
     */
    public boolean isInitialized(TopicIdPartition tp) {
        CompletableFuture<Long> barrier = initBarriers.get(tp);
        return barrier != null && barrier.isDone();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            log.info("Closing UrsaProducerStateStore");

            // Cancel all scheduled tasks
            snapshotTasks.values().forEach(task -> task.cancel(false));
            snapshotTasks.clear();

            // Shutdown scheduler
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }

            // Clear state
            initBarriers.clear();
            recordsSinceSnapshot.clear();
            lastSnapshotOffset.clear();

            super.close();
            log.info("UrsaProducerStateStore closed");
        }
    }
}
