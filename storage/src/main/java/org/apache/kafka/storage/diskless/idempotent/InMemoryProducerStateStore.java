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
import org.apache.kafka.common.record.RecordBatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryProducerStateStore implements ProducerStateStore {

    private static final Logger log = LoggerFactory.getLogger(InMemoryProducerStateStore.class);

    private final ConcurrentHashMap<TopicIdPartition, ConcurrentHashMap<Long, ProducerEntry>> stateByPartition =
        new ConcurrentHashMap<>();

    @Override
    public CompletableFuture<ValidationResult> validateAndUpdate(ValidationRequest request) {
        if (request.producerId() == RecordBatch.NO_PRODUCER_ID) {
            return CompletableFuture.completedFuture(new ValidationResult.Success(request.assignedOffset()));
        }

        ConcurrentHashMap<Long, ProducerEntry> partitionState = stateByPartition
            .computeIfAbsent(request.topicPartition(), k -> new ConcurrentHashMap<>());

        ProducerEntry entry = partitionState.computeIfAbsent(request.producerId(),
            k -> new ProducerEntry(request.producerEpoch()));

        synchronized (entry) {
            ValidationResult validationResult = validateInternal(
                entry, request.topicPartition(), request.producerId(),
                request.producerEpoch(), request.baseSequence(), request.lastSequence()
            );

            if (!(validationResult instanceof ValidationResult.Success)) {
                return CompletableFuture.completedFuture(validationResult);
            }

            updateEntryInternal(entry, request.producerEpoch(), request.baseSequence(),
                request.lastSequence(), request.assignedOffset(), request.timestamp());
        }

        return CompletableFuture.completedFuture(new ValidationResult.Success(request.assignedOffset()));
    }

    @Override
    public CompletableFuture<ValidationResult> validateAll(List<SequenceValidationRequest> requests) {
        if (requests.isEmpty()) {
            return CompletableFuture.completedFuture(new ValidationResult.Success(-1));
        }

        TopicIdPartition tp = requests.get(0).topicPartition();
        for (SequenceValidationRequest request : requests) {
            if (!tp.equals(request.topicPartition())) {
                throw new IllegalArgumentException("validateAll requires requests for a single topic-partition, got both "
                    + tp + " and " + request.topicPartition());
            }
        }

        Map<Long, ProducerRequestState> requestProducerState = new HashMap<>();

        for (SequenceValidationRequest request : requests) {
            if (request.producerId() == RecordBatch.NO_PRODUCER_ID) {
                continue;
            }

            ConcurrentHashMap<Long, ProducerEntry> partitionState = stateByPartition
                .computeIfAbsent(request.topicPartition(), k -> new ConcurrentHashMap<>());

            ProducerEntry entry = partitionState.computeIfAbsent(request.producerId(),
                k -> new ProducerEntry(request.producerEpoch()));

            synchronized (entry) {
                ProducerRequestState requestState = requestProducerState.get(request.producerId());
                if (requestState != null && requestState.epoch == request.producerEpoch()) {
                    int expectedSeq = requestState.expectedSequence;
                    if (request.baseSequence() != expectedSeq) {
                        String msg = String.format(
                            "Out of order sequence for producer %d on %s: expected %d, got %d",
                            request.producerId(), request.topicPartition(), expectedSeq, request.baseSequence()
                        );
                        log.warn(msg);
                        return CompletableFuture.completedFuture(
                            new ValidationResult.OutOfOrderSequence(expectedSeq, request.baseSequence(), msg)
                        );
                    }
                    requestState.expectedSequence = nextSequence(request.lastSequence());
                    continue;
                }

                ValidationResult validationResult = validateInternal(
                    entry,
                    request.topicPartition(),
                    request.producerId(),
                    request.producerEpoch(),
                    request.baseSequence(),
                    request.lastSequence()
                );

                if (!(validationResult instanceof ValidationResult.Success)) {
                    return CompletableFuture.completedFuture(validationResult);
                }

                requestProducerState.put(
                    request.producerId(),
                    new ProducerRequestState(request.producerEpoch(), nextSequence(request.lastSequence()))
                );
            }
        }

        return CompletableFuture.completedFuture(new ValidationResult.Success(-1));
    }

    @Override
    public CompletableFuture<Void> updateAfterWrite(StateUpdateRequest request) {
        if (request.producerId() == RecordBatch.NO_PRODUCER_ID) {
            return CompletableFuture.completedFuture(null);
        }

        ConcurrentHashMap<Long, ProducerEntry> partitionState = stateByPartition
            .computeIfAbsent(request.topicPartition(), k -> new ConcurrentHashMap<>());

        ProducerEntry entry = partitionState.computeIfAbsent(request.producerId(),
            k -> new ProducerEntry(request.producerEpoch()));

        synchronized (entry) {
            updateEntryInternal(entry, request.producerEpoch(), request.baseSequence(),
                request.lastSequence(), request.assignedOffset(), request.timestamp());
        }

        return CompletableFuture.completedFuture(null);
    }

    private ValidationResult validateInternal(
            ProducerEntry entry,
            TopicIdPartition tp,
            long producerId,
            short producerEpoch,
            int baseSequence,
            int lastSequence) {
        if (entry.epoch > producerEpoch) {
            String msg = String.format(
                "Producer %d epoch %d is older than current epoch %d for %s",
                producerId, producerEpoch, entry.epoch, tp
            );
            log.warn(msg);
            return new ValidationResult.InvalidEpoch(entry.epoch, producerEpoch, msg);
        }

        boolean isEpochBump = entry.epoch < producerEpoch;
        boolean isFirstWrite = entry.batches.isEmpty() || isEpochBump;

        if (isFirstWrite) {
            if (baseSequence != 0) {
                String msg = String.format(
                    "First sequence for producer %d on %s must be 0, got %d",
                    producerId, tp, baseSequence
                );
                log.warn(msg);
                return new ValidationResult.OutOfOrderSequence(0, baseSequence, msg);
            }
        } else {
            for (BatchMetadata batch : entry.batches) {
                if (batch.baseSequence == baseSequence && batch.lastSequence == lastSequence) {
                    log.debug("Duplicate batch detected for producer {} on {}, returning cached offset {}",
                        producerId, tp, batch.assignedOffset);
                    return new ValidationResult.Duplicate(batch.assignedOffset, batch.timestamp);
                }
            }

            BatchMetadata lastBatch = entry.batches.getLast();
            int expectedSeq = (lastBatch.lastSequence == Integer.MAX_VALUE) ? 0 : lastBatch.lastSequence + 1;

            if (baseSequence != expectedSeq) {
                String msg = String.format(
                    "Out of order sequence for producer %d on %s: expected %d, got %d",
                    producerId, tp, expectedSeq, baseSequence
                );
                log.warn(msg);
                return new ValidationResult.OutOfOrderSequence(expectedSeq, baseSequence, msg);
            }
        }

        return new ValidationResult.Success(-1);
    }

    private void updateEntryInternal(
            ProducerEntry entry,
            short producerEpoch,
            int baseSequence,
            int lastSequence,
            long assignedOffset,
            long timestamp) {
        if (entry.epoch < producerEpoch) {
            entry.epoch = producerEpoch;
            entry.batches.clear();
            entry.lastTimestamp = timestamp;
        }

        entry.batches.addLast(new BatchMetadata(baseSequence, lastSequence, assignedOffset, timestamp));
        entry.lastTimestamp = timestamp;

        while (entry.batches.size() > NUM_BATCHES_TO_RETAIN) {
            entry.batches.removeFirst();
        }
    }

    @Override
    public CompletableFuture<Optional<ProducerState>> getProducerState(TopicIdPartition tp, long producerId) {
        ConcurrentHashMap<Long, ProducerEntry> partitionState = stateByPartition.get(tp);
        if (partitionState == null) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        ProducerEntry entry = partitionState.get(producerId);
        if (entry == null) {
            return CompletableFuture.completedFuture(Optional.empty());
        }

        synchronized (entry) {
            if (entry.batches.isEmpty()) {
                return CompletableFuture.completedFuture(Optional.empty());
            }
            BatchMetadata lastBatch = entry.batches.getLast();
            return CompletableFuture.completedFuture(Optional.of(new ProducerState(
                producerId,
                entry.epoch,
                lastBatch.lastSequence,
                lastBatch.assignedOffset,
                lastBatch.timestamp,
                entry.batches.size()
            )));
        }
    }

    @Override
    public CompletableFuture<Void> clearPartition(TopicIdPartition tp) {
        stateByPartition.remove(tp);
        log.debug("Cleared producer state for partition {}", tp);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> takeSnapshot(TopicIdPartition tp, long offset) {
        // No-op for in-memory implementation
        // Future UrsaProducerStateStore will persist to S3
        log.debug("Snapshot requested for {} at offset {} (no-op for in-memory store)", tp, offset);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Long> loadSnapshot(TopicIdPartition tp) {
        // No snapshot available for in-memory implementation
        log.debug("Load snapshot requested for {} (not available for in-memory store)", tp);
        return CompletableFuture.completedFuture(-1L);
    }

    @Override
    public CompletableFuture<Integer> removeExpiredProducers(TopicIdPartition tp, long currentTimeMs, long expirationMs) {
        ConcurrentHashMap<Long, ProducerEntry> partitionState = stateByPartition.get(tp);
        if (partitionState == null) {
            return CompletableFuture.completedFuture(0);
        }

        int removed = 0;
        Iterator<Map.Entry<Long, ProducerEntry>> iterator = partitionState.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, ProducerEntry> mapEntry = iterator.next();
            ProducerEntry entry = mapEntry.getValue();
            synchronized (entry) {
                if (entry.lastTimestamp > 0 && (currentTimeMs - entry.lastTimestamp) >= expirationMs) {
                    iterator.remove();
                    removed++;
                    log.debug("Removed expired producer {} from partition {}", mapEntry.getKey(), tp);
                }
            }
        }

        return CompletableFuture.completedFuture(removed);
    }

    private static int nextSequence(int lastSequence) {
        return (lastSequence == Integer.MAX_VALUE) ? 0 : lastSequence + 1;
    }

    @Override
    public void close() {
        stateByPartition.clear();
        log.info("InMemoryProducerStateStore closed");
    }

    public int getProducerCount(TopicIdPartition tp) {
        ConcurrentHashMap<Long, ProducerEntry> partitionState = stateByPartition.get(tp);
        return partitionState != null ? partitionState.size() : 0;
    }

    /**
     * Gets all producer states for a partition. Used by subclasses for snapshotting.
     */
    protected CompletableFuture<Map<Long, ProducerState>> getAllProducerStates(TopicIdPartition tp) {
        ConcurrentHashMap<Long, ProducerEntry> partitionState = stateByPartition.get(tp);
        if (partitionState == null) {
            return CompletableFuture.completedFuture(Map.of());
        }

        ConcurrentHashMap<Long, ProducerState> result = new ConcurrentHashMap<>();
        for (Map.Entry<Long, ProducerEntry> mapEntry : partitionState.entrySet()) {
            long producerId = mapEntry.getKey();
            ProducerEntry entry = mapEntry.getValue();
            synchronized (entry) {
                if (!entry.batches.isEmpty()) {
                    BatchMetadata lastBatch = entry.batches.getLast();
                    result.put(producerId, new ProducerState(
                        producerId,
                        entry.epoch,
                        lastBatch.lastSequence,
                        lastBatch.assignedOffset,
                        lastBatch.timestamp,
                        entry.batches.size()
                    ));
                }
            }
        }
        return CompletableFuture.completedFuture(result);
    }

    private static class ProducerEntry {
        short epoch;
        long lastTimestamp;
        final LinkedList<BatchMetadata> batches = new LinkedList<>();

        ProducerEntry(short epoch) {
            this.epoch = epoch;
            this.lastTimestamp = -1;
        }
    }

    private static class BatchMetadata {
        final int baseSequence;
        final int lastSequence;
        final long assignedOffset;
        final long timestamp;

        BatchMetadata(int baseSequence, int lastSequence, long assignedOffset, long timestamp) {
            this.baseSequence = baseSequence;
            this.lastSequence = lastSequence;
            this.assignedOffset = assignedOffset;
            this.timestamp = timestamp;
        }
    }

    private static final class ProducerRequestState {
        private final short epoch;
        private int expectedSequence;

        private ProducerRequestState(short epoch, int expectedSequence) {
            this.epoch = epoch;
            this.expectedSequence = expectedSequence;
        }
    }
}
