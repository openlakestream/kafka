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

import java.io.Closeable;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Interface for storing and retrieving producer state for idempotent producer support.
 * 
 * <p>This interface is designed to be extensible for different storage backends:
 * <ul>
 *   <li>{@code InMemoryProducerStateStore} - Fast, in-memory implementation (current)</li>
 *   <li>{@code UrsaProducerStateStore} - Persistent storage in Ursa/S3 (future)</li>
 *   <li>{@code DistributedProducerStateStore} - Cross-broker state sync (future)</li>
 * </ul>
 * 
 * <p>The store maintains producer state per partition, tracking:
 * <ul>
 *   <li>Producer epoch - for fencing zombie producers</li>
 *   <li>Sequence numbers - for detecting duplicates and out-of-order</li>
 *   <li>Last N batch metadata - for returning cached offsets on duplicates</li>
 * </ul>
 * 
 * <h2>Future Extension Points:</h2>
 * <ul>
 *   <li><b>Ursa Storage</b>: Implement snapshot persistence to S3 for recovery</li>
 *   <li><b>Cross-broker Sync</b>: Add distributed consensus or leader-based sync</li>
 *   <li><b>Async Operations</b>: All methods return CompletableFuture for async backends</li>
 * </ul>
 */
public interface ProducerStateStore extends Closeable {

    /**
     * Number of recent batches to retain for duplicate detection.
     * Matches Kafka's ProducerStateEntry.NUM_BATCHES_TO_RETAIN.
     */
    int NUM_BATCHES_TO_RETAIN = 5;

    /**
     * Validates a producer batch and updates state if valid.
     * 
     * <p>This method performs the following checks:
     * <ol>
     *   <li>Epoch validation - rejects requests with stale epochs</li>
     *   <li>First write check - first sequence must be 0</li>
     *   <li>Duplicate detection - returns cached offset for exact sequence match</li>
     *   <li>Sequence continuity - ensures no gaps in sequence numbers</li>
     * </ol>
     * 
     * @param request the validation request containing batch metadata
     * @return a future that completes with the validation result
     */
    CompletableFuture<ValidationResult> validateAndUpdate(ValidationRequest request);

    /**
     * Validates a list of producer batches WITHOUT updating state.
     *
     * <p>This is used to validate a single produce request which may contain multiple
     * {@code RecordBatch}es for the same partition. Implementations must validate the
     * batches transitively so that later batches are checked against the state implied
     * by earlier batches in the same list (request-local shadow state).
     *
     * <p>All requests must be for the same topic-partition and must be ordered as they
     * appear in the produce request.
     *
     * @param requests the validation requests in request order
     * @return a future that completes with the first non-success result, or {@link ValidationResult.Success}
     */
    CompletableFuture<ValidationResult> validateAll(List<SequenceValidationRequest> requests);

    /**
     * Updates producer state after a successful storage write.
     * 
     * <p>This method should only be called after:
     * <ol>
     *   <li>{@link #validateAll} returned success</li>
     *   <li>The data was successfully written to storage</li>
     * </ol>
     * 
     * <p>This two-phase approach prevents invalid data from being written to storage.
     * 
     * @param request the update request containing batch metadata with assigned offset
     * @return a future that completes when state is updated
     */
    CompletableFuture<Void> updateAfterWrite(StateUpdateRequest request);

    /**
     * Gets the current producer state for a specific producer in a partition.
     * Useful for debugging and state inspection.
     * 
     * @param tp the topic-partition
     * @param producerId the producer ID
     * @return a future with the producer state, or empty if not found
     */
    CompletableFuture<Optional<ProducerState>> getProducerState(TopicIdPartition tp, long producerId);

    /**
     * Clears all producer state for a partition.
     * Called when a partition is deleted or reassigned.
     * 
     * @param tp the topic-partition to clear
     * @return a future that completes when the state is cleared
     */
    CompletableFuture<Void> clearPartition(TopicIdPartition tp);

    /**
     * Deletes any persistent producer state for a partition.
     * Called when a partition is permanently deleted (for example, during topic deletion).
     *
     * <p>The default implementation delegates to {@link #clearPartition(TopicIdPartition)}.
     *
     * @param tp the topic-partition to delete
     * @return a future that completes when the persistent state is deleted
     */
    default CompletableFuture<Void> deletePartition(TopicIdPartition tp) {
        return clearPartition(tp);
    }

    /**
     * Takes a snapshot of the current state for persistence.
     * Used for recovery after broker restart.
     * 
     * <p>For in-memory implementation, this may be a no-op.
     * For persistent implementations, this triggers state serialization.
     * 
     * @param tp the topic-partition to snapshot
     * @param offset the offset up to which state is valid
     * @return a future that completes when snapshot is taken
     */
    CompletableFuture<Void> takeSnapshot(TopicIdPartition tp, long offset);

    /**
     * Loads state from a snapshot.
     * Called during broker startup for recovery.
     * 
     * @param tp the topic-partition to load
     * @return a future with the loaded offset, or -1 if no snapshot exists
     */
    CompletableFuture<Long> loadSnapshot(TopicIdPartition tp);

    /**
     * Removes expired producers based on the configured expiration time.
     * Called periodically to prevent unbounded memory growth.
     * 
     * @param tp the topic-partition
     * @param currentTimeMs current time in milliseconds
     * @param expirationMs expiration time in milliseconds
     * @return a future with the number of producers removed
     */
    CompletableFuture<Integer> removeExpiredProducers(TopicIdPartition tp, long currentTimeMs, long expirationMs);

    /**
     * Request object for producer state validation (includes offset for state update).
     */
    record ValidationRequest(
        TopicIdPartition topicPartition,
        long producerId,
        short producerEpoch,
        int baseSequence,
        int lastSequence,
        long assignedOffset,
        long timestamp
    ) {
        public static Builder builder() {
            return new Builder();
        }

        public SequenceValidationRequest toSequenceValidationRequest() {
            return new SequenceValidationRequest(topicPartition, producerId, producerEpoch, baseSequence, lastSequence);
        }

        public StateUpdateRequest toStateUpdateRequest() {
            return new StateUpdateRequest(topicPartition, producerId, producerEpoch, baseSequence, lastSequence, assignedOffset, timestamp);
        }

        public static class Builder {
            private TopicIdPartition topicPartition;
            private long producerId;
            private short producerEpoch;
            private int baseSequence;
            private int lastSequence;
            private long assignedOffset;
            private long timestamp;

            public Builder topicPartition(TopicIdPartition tp) {
                this.topicPartition = tp;
                return this;
            }

            public Builder producerId(long id) {
                this.producerId = id;
                return this;
            }

            public Builder producerEpoch(short epoch) {
                this.producerEpoch = epoch;
                return this;
            }

            public Builder baseSequence(int seq) {
                this.baseSequence = seq;
                return this;
            }

            public Builder lastSequence(int seq) {
                this.lastSequence = seq;
                return this;
            }

            public Builder assignedOffset(long offset) {
                this.assignedOffset = offset;
                return this;
            }

            public Builder timestamp(long ts) {
                this.timestamp = ts;
                return this;
            }

            public ValidationRequest build() {
                return new ValidationRequest(
                    topicPartition, producerId, producerEpoch,
                    baseSequence, lastSequence, assignedOffset, timestamp
                );
            }
        }
    }

    /**
     * Request object for sequence validation only (no offset needed).
     * Used for validate-before-write pattern.
     */
    record SequenceValidationRequest(
        TopicIdPartition topicPartition,
        long producerId,
        short producerEpoch,
        int baseSequence,
        int lastSequence
    ) {
        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private TopicIdPartition topicPartition;
            private long producerId;
            private short producerEpoch;
            private int baseSequence;
            private int lastSequence;

            public Builder topicPartition(TopicIdPartition tp) {
                this.topicPartition = tp;
                return this;
            }

            public Builder producerId(long id) {
                this.producerId = id;
                return this;
            }

            public Builder producerEpoch(short epoch) {
                this.producerEpoch = epoch;
                return this;
            }

            public Builder baseSequence(int seq) {
                this.baseSequence = seq;
                return this;
            }

            public Builder lastSequence(int seq) {
                this.lastSequence = seq;
                return this;
            }

            public SequenceValidationRequest build() {
                return new SequenceValidationRequest(
                    topicPartition, producerId, producerEpoch,
                    baseSequence, lastSequence
                );
            }
        }
    }

    /**
     * Request object for state update after successful storage write.
     */
    record StateUpdateRequest(
        TopicIdPartition topicPartition,
        long producerId,
        short producerEpoch,
        int baseSequence,
        int lastSequence,
        long assignedOffset,
        long timestamp
    ) {
        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private TopicIdPartition topicPartition;
            private long producerId;
            private short producerEpoch;
            private int baseSequence;
            private int lastSequence;
            private long assignedOffset;
            private long timestamp;

            public Builder topicPartition(TopicIdPartition tp) {
                this.topicPartition = tp;
                return this;
            }

            public Builder producerId(long id) {
                this.producerId = id;
                return this;
            }

            public Builder producerEpoch(short epoch) {
                this.producerEpoch = epoch;
                return this;
            }

            public Builder baseSequence(int seq) {
                this.baseSequence = seq;
                return this;
            }

            public Builder lastSequence(int seq) {
                this.lastSequence = seq;
                return this;
            }

            public Builder assignedOffset(long offset) {
                this.assignedOffset = offset;
                return this;
            }

            public Builder timestamp(long ts) {
                this.timestamp = ts;
                return this;
            }

            public StateUpdateRequest build() {
                return new StateUpdateRequest(
                    topicPartition, producerId, producerEpoch,
                    baseSequence, lastSequence, assignedOffset, timestamp
                );
            }
        }
    }

    /**
     * Result of producer state validation.
     */
    sealed interface ValidationResult permits 
        ValidationResult.Success, 
        ValidationResult.Duplicate, 
        ValidationResult.InvalidEpoch, 
        ValidationResult.OutOfOrderSequence {

        /**
         * Successful validation - batch can be written.
         */
        record Success(long offset) implements ValidationResult { }

        /**
         * Duplicate batch detected - return cached offset.
         */
        record Duplicate(long cachedOffset, long cachedTimestamp) implements ValidationResult { }

        /**
         * Producer epoch is stale - reject with INVALID_PRODUCER_EPOCH.
         */
        record InvalidEpoch(short currentEpoch, short requestEpoch, String message) implements ValidationResult { }

        /**
         * Sequence number is out of order - reject with OUT_OF_ORDER_SEQUENCE_NUMBER.
         */
        record OutOfOrderSequence(int expectedSequence, int actualSequence, String message) implements ValidationResult { }
    }

    /**
     * Represents the state of a single producer.
     */
    record ProducerState(
        long producerId,
        short epoch,
        int lastSequence,
        long lastOffset,
        long lastTimestamp,
        int batchCount
    ) { }
}
