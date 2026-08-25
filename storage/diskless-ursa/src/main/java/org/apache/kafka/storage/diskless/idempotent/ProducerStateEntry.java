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

import org.apache.kafka.common.record.internal.RecordBatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * In-memory producer state for one producer id.
 */
public final class ProducerStateEntry {

    private static final Logger log = LoggerFactory.getLogger(ProducerStateEntry.class);

    public static final int NUM_BATCHES_TO_RETAIN = 5;

    private final Deque<BatchMetadata> batchMetadata = new ArrayDeque<>(NUM_BATCHES_TO_RETAIN);
    private short producerEpoch;
    private long lastTimestamp;

    public ProducerStateEntry(short producerEpoch, long timestamp) {
        this.producerEpoch = producerEpoch;
        this.lastTimestamp = timestamp;
    }

    public Optional<BatchMetadata> findDuplicate(short requestEpoch, int firstSeq, int lastSeq) {
        if (producerEpoch != requestEpoch || batchMetadata.isEmpty()) {
            return Optional.empty();
        }
        return batchMetadata.stream()
            .filter(metadata -> metadata.firstSeq() == firstSeq && metadata.lastSeq() == lastSeq)
            .findFirst();
    }

    public void appendBatch(short requestEpoch, BatchMetadata metadata) {
        maybeUpdateProducerEpoch(requestEpoch);
        while (batchMetadata.size() >= NUM_BATCHES_TO_RETAIN) {
            BatchMetadata removed = batchMetadata.pollFirst();
            if (removed != null && !removed.baseOffsetFuture().isDone()) {
                log.warn("Discarding unresolved in-flight batch metadata {}", removed);
            }
        }
        batchMetadata.addLast(metadata);
        lastTimestamp = Math.max(lastTimestamp, metadata.timestamp());
    }

    public boolean removeBatch(short requestEpoch, int firstSeq, int lastSeq, BatchMetadata expectedMetadata) {
        if (producerEpoch != requestEpoch) {
            return false;
        }
        return batchMetadata.removeIf(batch -> batch.firstSeq() == firstSeq
            && batch.lastSeq() == lastSeq
            && batch.baseOffsetFuture() == expectedMetadata.baseOffsetFuture());
    }

    public short producerEpoch() {
        return producerEpoch;
    }

    public int lastSequence() {
        return batchMetadata.isEmpty() ? RecordBatch.NO_SEQUENCE : batchMetadata.getLast().lastSeq();
    }

    public long lastTimestamp() {
        return lastTimestamp;
    }

    public boolean isEmpty() {
        return batchMetadata.isEmpty();
    }

    public Deque<BatchMetadata> batchMetadata() {
        return batchMetadata;
    }

    public void updateBatchTimestamp(CompletableFuture<Long> baseOffsetFuture, long timestamp) {
        for (BatchMetadata metadata : batchMetadata) {
            if (metadata.baseOffsetFuture() == baseOffsetFuture) {
                metadata.setTimestamp(timestamp);
                lastTimestamp = Math.max(lastTimestamp, timestamp);
                return;
            }
        }
    }

    private void maybeUpdateProducerEpoch(short requestEpoch) {
        if (producerEpoch != requestEpoch) {
            batchMetadata.clear();
            producerEpoch = requestEpoch;
        }
    }
}
