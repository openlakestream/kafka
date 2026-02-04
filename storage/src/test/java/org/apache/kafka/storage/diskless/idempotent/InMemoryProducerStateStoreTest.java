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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.storage.diskless.idempotent.ProducerStateStore.SequenceValidationRequest;
import org.apache.kafka.storage.diskless.idempotent.ProducerStateStore.StateUpdateRequest;
import org.apache.kafka.storage.diskless.idempotent.ProducerStateStore.ValidationRequest;
import org.apache.kafka.storage.diskless.idempotent.ProducerStateStore.ValidationResult;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InMemoryProducerStateStoreTest {

    private InMemoryProducerStateStore store;
    private TopicIdPartition tp;

    @BeforeEach
    void setUp() {
        store = new InMemoryProducerStateStore();
        tp = new TopicIdPartition(Uuid.randomUuid(), 0, "test-topic");
    }

    @Test
    void testNonIdempotentProducerSkipsValidation() {
        ValidationRequest request = ValidationRequest.builder()
            .topicPartition(tp)
            .producerId(-1L)
            .producerEpoch((short) -1)
            .baseSequence(100)
            .lastSequence(109)
            .assignedOffset(0L)
            .timestamp(System.currentTimeMillis())
            .build();

        ValidationResult result = store.validateAndUpdate(request).join();

        assertInstanceOf(ValidationResult.Success.class, result);
        assertEquals(0L, ((ValidationResult.Success) result).offset());
    }

    @Test
    void testFirstBatchMustStartAtSequenceZero() {
        ValidationRequest request = ValidationRequest.builder()
            .topicPartition(tp)
            .producerId(12345L)
            .producerEpoch((short) 0)
            .baseSequence(5)
            .lastSequence(9)
            .assignedOffset(0L)
            .timestamp(System.currentTimeMillis())
            .build();

        ValidationResult result = store.validateAndUpdate(request).join();

        assertInstanceOf(ValidationResult.OutOfOrderSequence.class, result);
        ValidationResult.OutOfOrderSequence ooo = (ValidationResult.OutOfOrderSequence) result;
        assertEquals(0, ooo.expectedSequence());
        assertEquals(5, ooo.actualSequence());
    }

    @Test
    void testValidSequenceAccepted() {
        long producerId = 12345L;
        short epoch = 0;

        ValidationRequest first = ValidationRequest.builder()
            .topicPartition(tp)
            .producerId(producerId)
            .producerEpoch(epoch)
            .baseSequence(0)
            .lastSequence(9)
            .assignedOffset(0L)
            .timestamp(System.currentTimeMillis())
            .build();

        ValidationResult firstResult = store.validateAndUpdate(first).join();
        assertInstanceOf(ValidationResult.Success.class, firstResult);

        ValidationRequest second = ValidationRequest.builder()
            .topicPartition(tp)
            .producerId(producerId)
            .producerEpoch(epoch)
            .baseSequence(10)
            .lastSequence(19)
            .assignedOffset(10L)
            .timestamp(System.currentTimeMillis())
            .build();

        ValidationResult secondResult = store.validateAndUpdate(second).join();
        assertInstanceOf(ValidationResult.Success.class, secondResult);
    }

    @Test
    void testDuplicateBatchReturnsCache() {
        long producerId = 12345L;
        short epoch = 0;
        long originalTimestamp = 1000L;

        ValidationRequest first = ValidationRequest.builder()
            .topicPartition(tp)
            .producerId(producerId)
            .producerEpoch(epoch)
            .baseSequence(0)
            .lastSequence(9)
            .assignedOffset(100L)
            .timestamp(originalTimestamp)
            .build();

        store.validateAndUpdate(first).join();

        ValidationRequest duplicate = ValidationRequest.builder()
            .topicPartition(tp)
            .producerId(producerId)
            .producerEpoch(epoch)
            .baseSequence(0)
            .lastSequence(9)
            .assignedOffset(200L)
            .timestamp(2000L)
            .build();

        ValidationResult result = store.validateAndUpdate(duplicate).join();

        assertInstanceOf(ValidationResult.Duplicate.class, result);
        ValidationResult.Duplicate dup = (ValidationResult.Duplicate) result;
        assertEquals(100L, dup.cachedOffset());
        assertEquals(originalTimestamp, dup.cachedTimestamp());
    }

    @Test
    void testOutOfOrderSequenceRejected() {
        long producerId = 12345L;
        short epoch = 0;

        ValidationRequest first = ValidationRequest.builder()
            .topicPartition(tp)
            .producerId(producerId)
            .producerEpoch(epoch)
            .baseSequence(0)
            .lastSequence(9)
            .assignedOffset(0L)
            .timestamp(System.currentTimeMillis())
            .build();

        store.validateAndUpdate(first).join();

        ValidationRequest outOfOrder = ValidationRequest.builder()
            .topicPartition(tp)
            .producerId(producerId)
            .producerEpoch(epoch)
            .baseSequence(20)
            .lastSequence(29)
            .assignedOffset(10L)
            .timestamp(System.currentTimeMillis())
            .build();

        ValidationResult result = store.validateAndUpdate(outOfOrder).join();

        assertInstanceOf(ValidationResult.OutOfOrderSequence.class, result);
        ValidationResult.OutOfOrderSequence ooo = (ValidationResult.OutOfOrderSequence) result;
        assertEquals(10, ooo.expectedSequence());
        assertEquals(20, ooo.actualSequence());
    }

    @Test
    void testStaleEpochRejected() {
        long producerId = 12345L;

        ValidationRequest withHigherEpoch = ValidationRequest.builder()
            .topicPartition(tp)
            .producerId(producerId)
            .producerEpoch((short) 5)
            .baseSequence(0)
            .lastSequence(9)
            .assignedOffset(0L)
            .timestamp(System.currentTimeMillis())
            .build();

        store.validateAndUpdate(withHigherEpoch).join();

        ValidationRequest withLowerEpoch = ValidationRequest.builder()
            .topicPartition(tp)
            .producerId(producerId)
            .producerEpoch((short) 3)
            .baseSequence(10)
            .lastSequence(19)
            .assignedOffset(10L)
            .timestamp(System.currentTimeMillis())
            .build();

        ValidationResult result = store.validateAndUpdate(withLowerEpoch).join();

        assertInstanceOf(ValidationResult.InvalidEpoch.class, result);
        ValidationResult.InvalidEpoch invalid = (ValidationResult.InvalidEpoch) result;
        assertEquals((short) 5, invalid.currentEpoch());
        assertEquals((short) 3, invalid.requestEpoch());
    }

    @Test
    void testEpochBumpClearsState() {
        long producerId = 12345L;

        ValidationRequest epoch0 = ValidationRequest.builder()
            .topicPartition(tp)
            .producerId(producerId)
            .producerEpoch((short) 0)
            .baseSequence(0)
            .lastSequence(9)
            .assignedOffset(0L)
            .timestamp(System.currentTimeMillis())
            .build();

        store.validateAndUpdate(epoch0).join();

        ValidationRequest epoch1 = ValidationRequest.builder()
            .topicPartition(tp)
            .producerId(producerId)
            .producerEpoch((short) 1)
            .baseSequence(0)
            .lastSequence(9)
            .assignedOffset(10L)
            .timestamp(System.currentTimeMillis())
            .build();

        ValidationResult result = store.validateAndUpdate(epoch1).join();

        assertInstanceOf(ValidationResult.Success.class, result);
    }

    @Test
    void testSequenceWrapAround() {
        long producerId = 12345L;
        short epoch = 0;

        ValidationRequest maxSeq = ValidationRequest.builder()
            .topicPartition(tp)
            .producerId(producerId)
            .producerEpoch(epoch)
            .baseSequence(0)
            .lastSequence(Integer.MAX_VALUE)
            .assignedOffset(0L)
            .timestamp(System.currentTimeMillis())
            .build();

        store.validateAndUpdate(maxSeq).join();

        ValidationRequest wrapAround = ValidationRequest.builder()
            .topicPartition(tp)
            .producerId(producerId)
            .producerEpoch(epoch)
            .baseSequence(0)
            .lastSequence(9)
            .assignedOffset(100L)
            .timestamp(System.currentTimeMillis())
            .build();

        ValidationResult result = store.validateAndUpdate(wrapAround).join();

        assertInstanceOf(ValidationResult.Success.class, result);
    }

    @Test
    void testGetProducerState() {
        long producerId = 12345L;
        short epoch = 0;

        Optional<ProducerStateStore.ProducerState> empty = store.getProducerState(tp, producerId).join();
        assertTrue(empty.isEmpty());

        ValidationRequest request = ValidationRequest.builder()
            .topicPartition(tp)
            .producerId(producerId)
            .producerEpoch(epoch)
            .baseSequence(0)
            .lastSequence(9)
            .assignedOffset(100L)
            .timestamp(5000L)
            .build();

        store.validateAndUpdate(request).join();

        Optional<ProducerStateStore.ProducerState> stateOpt = store.getProducerState(tp, producerId).join();
        assertTrue(stateOpt.isPresent());

        ProducerStateStore.ProducerState state = stateOpt.get();
        assertEquals(producerId, state.producerId());
        assertEquals(epoch, state.epoch());
        assertEquals(9, state.lastSequence());
        assertEquals(100L, state.lastOffset());
    }

    @Test
    void testClearPartition() {
        long producerId = 12345L;

        ValidationRequest request = ValidationRequest.builder()
            .topicPartition(tp)
            .producerId(producerId)
            .producerEpoch((short) 0)
            .baseSequence(0)
            .lastSequence(9)
            .assignedOffset(0L)
            .timestamp(System.currentTimeMillis())
            .build();

        store.validateAndUpdate(request).join();
        assertEquals(1, store.getProducerCount(tp));

        store.clearPartition(tp).join();

        assertEquals(0, store.getProducerCount(tp));
        assertTrue(store.getProducerState(tp, producerId).join().isEmpty());
    }

    @Test
    void testRemoveExpiredProducers() {
        long producerId = 12345L;
        long oldTimestamp = 1000L;

        ValidationRequest request = ValidationRequest.builder()
            .topicPartition(tp)
            .producerId(producerId)
            .producerEpoch((short) 0)
            .baseSequence(0)
            .lastSequence(9)
            .assignedOffset(0L)
            .timestamp(oldTimestamp)
            .build();

        store.validateAndUpdate(request).join();
        assertEquals(1, store.getProducerCount(tp));

        long currentTime = oldTimestamp + 100000L;
        long expirationMs = 50000L;

        int removed = store.removeExpiredProducers(tp, currentTime, expirationMs).join();
        assertEquals(1, removed);
        assertEquals(0, store.getProducerCount(tp));
    }

    @Test
    void testRetainsOnlyLastFiveBatches() {
        long producerId = 12345L;
        short epoch = 0;

        for (int i = 0; i < 10; i++) {
            ValidationRequest request = ValidationRequest.builder()
                .topicPartition(tp)
                .producerId(producerId)
                .producerEpoch(epoch)
                .baseSequence(i * 10)
                .lastSequence(i * 10 + 9)
                .assignedOffset(i * 10L)
                .timestamp(System.currentTimeMillis())
                .build();

            store.validateAndUpdate(request).join();
        }

        ValidationRequest oldDuplicate = ValidationRequest.builder()
            .topicPartition(tp)
            .producerId(producerId)
            .producerEpoch(epoch)
            .baseSequence(0)
            .lastSequence(9)
            .assignedOffset(999L)
            .timestamp(System.currentTimeMillis())
            .build();

        ValidationResult oldResult = store.validateAndUpdate(oldDuplicate).join();
        assertInstanceOf(ValidationResult.OutOfOrderSequence.class, oldResult);

        ValidationRequest recentDuplicate = ValidationRequest.builder()
            .topicPartition(tp)
            .producerId(producerId)
            .producerEpoch(epoch)
            .baseSequence(90)
            .lastSequence(99)
            .assignedOffset(999L)
            .timestamp(System.currentTimeMillis())
            .build();

        ValidationResult recentResult = store.validateAndUpdate(recentDuplicate).join();
        assertInstanceOf(ValidationResult.Duplicate.class, recentResult);
        assertEquals(90L, ((ValidationResult.Duplicate) recentResult).cachedOffset());
    }

    @Test
    void testMultipleProducersSamePartition() {
        long producer1 = 11111L;
        long producer2 = 22222L;

        ValidationRequest req1 = ValidationRequest.builder()
            .topicPartition(tp)
            .producerId(producer1)
            .producerEpoch((short) 0)
            .baseSequence(0)
            .lastSequence(9)
            .assignedOffset(0L)
            .timestamp(System.currentTimeMillis())
            .build();

        ValidationRequest req2 = ValidationRequest.builder()
            .topicPartition(tp)
            .producerId(producer2)
            .producerEpoch((short) 0)
            .baseSequence(0)
            .lastSequence(9)
            .assignedOffset(10L)
            .timestamp(System.currentTimeMillis())
            .build();

        ValidationResult result1 = store.validateAndUpdate(req1).join();
        ValidationResult result2 = store.validateAndUpdate(req2).join();

        assertInstanceOf(ValidationResult.Success.class, result1);
        assertInstanceOf(ValidationResult.Success.class, result2);
        assertEquals(2, store.getProducerCount(tp));
    }

    @Test
    void testValidateOnlyDoesNotUpdateState() {
        long producerId = 12345L;
        short epoch = 0;

        SequenceValidationRequest request = SequenceValidationRequest.builder()
            .topicPartition(tp)
            .producerId(producerId)
            .producerEpoch(epoch)
            .baseSequence(0)
            .lastSequence(9)
            .build();

        ValidationResult result = store.validateAll(List.of(request)).join();
        assertInstanceOf(ValidationResult.Success.class, result);

        Optional<ProducerStateStore.ProducerState> state = store.getProducerState(tp, producerId).join();
        assertTrue(state.isEmpty());
    }

    @Test
    void testValidateAllHandlesMultipleBatchesOnFirstWrite() {
        long producerId = 12345L;
        short epoch = 0;

        List<SequenceValidationRequest> requests = List.of(
            SequenceValidationRequest.builder()
                .topicPartition(tp)
                .producerId(producerId)
                .producerEpoch(epoch)
                .baseSequence(0)
                .lastSequence(9)
                .build(),
            SequenceValidationRequest.builder()
                .topicPartition(tp)
                .producerId(producerId)
                .producerEpoch(epoch)
                .baseSequence(10)
                .lastSequence(19)
                .build()
        );

        ValidationResult result = store.validateAll(requests).join();
        assertInstanceOf(ValidationResult.Success.class, result);
        assertTrue(store.getProducerState(tp, producerId).join().isEmpty());
    }

    @Test
    void testValidateAllHandlesMultipleBatchesAfterEpochBump() {
        long producerId = 12345L;

        StateUpdateRequest epoch0 = StateUpdateRequest.builder()
            .topicPartition(tp)
            .producerId(producerId)
            .producerEpoch((short) 0)
            .baseSequence(0)
            .lastSequence(9)
            .assignedOffset(0L)
            .timestamp(System.currentTimeMillis())
            .build();
        store.updateAfterWrite(epoch0).join();

        List<SequenceValidationRequest> requests = List.of(
            SequenceValidationRequest.builder()
                .topicPartition(tp)
                .producerId(producerId)
                .producerEpoch((short) 1)
                .baseSequence(0)
                .lastSequence(9)
                .build(),
            SequenceValidationRequest.builder()
                .topicPartition(tp)
                .producerId(producerId)
                .producerEpoch((short) 1)
                .baseSequence(10)
                .lastSequence(19)
                .build()
        );

        ValidationResult result = store.validateAll(requests).join();
        assertInstanceOf(ValidationResult.Success.class, result);
    }

    @Test
    void testValidateThenUpdateAfterWrite() {
        long producerId = 12345L;
        short epoch = 0;

        SequenceValidationRequest validateRequest = SequenceValidationRequest.builder()
            .topicPartition(tp)
            .producerId(producerId)
            .producerEpoch(epoch)
            .baseSequence(0)
            .lastSequence(9)
            .build();

        ValidationResult validateResult = store.validateAll(List.of(validateRequest)).join();
        assertInstanceOf(ValidationResult.Success.class, validateResult);

        assertTrue(store.getProducerState(tp, producerId).join().isEmpty());

        StateUpdateRequest updateRequest = StateUpdateRequest.builder()
            .topicPartition(tp)
            .producerId(producerId)
            .producerEpoch(epoch)
            .baseSequence(0)
            .lastSequence(9)
            .assignedOffset(100L)
            .timestamp(5000L)
            .build();

        store.updateAfterWrite(updateRequest).join();

        Optional<ProducerStateStore.ProducerState> state = store.getProducerState(tp, producerId).join();
        assertTrue(state.isPresent());
        assertEquals(9, state.get().lastSequence());
        assertEquals(100L, state.get().lastOffset());
    }

    @Test
    void testValidateRejectsInvalidEpochBeforeWrite() {
        long producerId = 12345L;

        StateUpdateRequest firstUpdate = StateUpdateRequest.builder()
            .topicPartition(tp)
            .producerId(producerId)
            .producerEpoch((short) 5)
            .baseSequence(0)
            .lastSequence(9)
            .assignedOffset(0L)
            .timestamp(System.currentTimeMillis())
            .build();
        store.updateAfterWrite(firstUpdate).join();

        SequenceValidationRequest staleRequest = SequenceValidationRequest.builder()
            .topicPartition(tp)
            .producerId(producerId)
            .producerEpoch((short) 3)
            .baseSequence(10)
            .lastSequence(19)
            .build();

        ValidationResult result = store.validateAll(List.of(staleRequest)).join();
        assertInstanceOf(ValidationResult.InvalidEpoch.class, result);
    }

    @Test
    void testValidateRejectsOutOfOrderSequenceBeforeWrite() {
        long producerId = 12345L;
        short epoch = 0;

        StateUpdateRequest firstUpdate = StateUpdateRequest.builder()
            .topicPartition(tp)
            .producerId(producerId)
            .producerEpoch(epoch)
            .baseSequence(0)
            .lastSequence(9)
            .assignedOffset(0L)
            .timestamp(System.currentTimeMillis())
            .build();
        store.updateAfterWrite(firstUpdate).join();

        SequenceValidationRequest outOfOrderRequest = SequenceValidationRequest.builder()
            .topicPartition(tp)
            .producerId(producerId)
            .producerEpoch(epoch)
            .baseSequence(20)
            .lastSequence(29)
            .build();

        ValidationResult result = store.validateAll(List.of(outOfOrderRequest)).join();
        assertInstanceOf(ValidationResult.OutOfOrderSequence.class, result);
        assertEquals(10, ((ValidationResult.OutOfOrderSequence) result).expectedSequence());
    }

    @Test
    void testValidateDetectsDuplicateBeforeWrite() {
        long producerId = 12345L;
        short epoch = 0;
        long originalOffset = 100L;
        long originalTimestamp = 5000L;

        StateUpdateRequest firstUpdate = StateUpdateRequest.builder()
            .topicPartition(tp)
            .producerId(producerId)
            .producerEpoch(epoch)
            .baseSequence(0)
            .lastSequence(9)
            .assignedOffset(originalOffset)
            .timestamp(originalTimestamp)
            .build();
        store.updateAfterWrite(firstUpdate).join();

        SequenceValidationRequest duplicateRequest = SequenceValidationRequest.builder()
            .topicPartition(tp)
            .producerId(producerId)
            .producerEpoch(epoch)
            .baseSequence(0)
            .lastSequence(9)
            .build();

        ValidationResult result = store.validateAll(List.of(duplicateRequest)).join();
        assertInstanceOf(ValidationResult.Duplicate.class, result);
        ValidationResult.Duplicate dup = (ValidationResult.Duplicate) result;
        assertEquals(originalOffset, dup.cachedOffset());
        assertEquals(originalTimestamp, dup.cachedTimestamp());
    }

    @Test
    void testTwoPhaseFlowPreventsDataLeakOnInvalidSequence() {
        long producerId = 12345L;
        short epoch = 0;

        StateUpdateRequest firstUpdate = StateUpdateRequest.builder()
            .topicPartition(tp)
            .producerId(producerId)
            .producerEpoch(epoch)
            .baseSequence(0)
            .lastSequence(9)
            .assignedOffset(0L)
            .timestamp(System.currentTimeMillis())
            .build();
        store.updateAfterWrite(firstUpdate).join();

        SequenceValidationRequest invalidRequest = SequenceValidationRequest.builder()
            .topicPartition(tp)
            .producerId(producerId)
            .producerEpoch(epoch)
            .baseSequence(50)
            .lastSequence(59)
            .build();

        ValidationResult validateResult = store.validateAll(List.of(invalidRequest)).join();
        assertInstanceOf(ValidationResult.OutOfOrderSequence.class, validateResult);

        Optional<ProducerStateStore.ProducerState> stateAfter = store.getProducerState(tp, producerId).join();
        assertTrue(stateAfter.isPresent());
        assertEquals(9, stateAfter.get().lastSequence());
        assertEquals(0L, stateAfter.get().lastOffset());
    }
}
