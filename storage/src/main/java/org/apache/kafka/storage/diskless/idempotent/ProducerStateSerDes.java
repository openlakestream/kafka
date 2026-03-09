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

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Serializer/deserializer for producer-state snapshots.
 *
 * <p>This uses JSON format.
 */
public final class ProducerStateSerDes {

    /**
     * Magic byte written at the beginning of every snapshot payload.
     */
    private static final byte MAGIC_V1_JSON = 0x1;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private ProducerStateSerDes() {
    }

    public static SerializationResult serialize(
            Map<Long, ProducerStateEntry> producers,
            long maxSnapshotValueBytes) throws IOException {
        if (producers == null || producers.isEmpty()) {
            return null;
        }

        Map<Long, SnapshotProducer> snapshotProducers = copySnapshotProducers(producers);
        if (snapshotProducers.isEmpty()) {
            return null;
        }

        SerializationResult result = encodeSnapshot(snapshotProducers);
        while (result.bytes().length >= maxSnapshotValueBytes) {
            if (snapshotProducers.size() <= 1) {
                return null;
            }
            pruneOldestHalf(snapshotProducers);
            if (snapshotProducers.isEmpty()) {
                return null;
            }
            result = encodeSnapshot(snapshotProducers);
        }
        return result;
    }

    /**
     * Deserialize a snapshot payload back into producer-state map form.
     */
    public static Map<Long, ProducerStateEntry> deserialize(byte[] bytes) throws IOException {
        if (bytes == null || bytes.length == 0) {
            throw new IOException("Invalid producer state snapshot: payload is empty");
        }
        if (bytes[0] != MAGIC_V1_JSON) {
            throw new IOException("Unsupported producer state snapshot magic " + (bytes[0] & 0xFF)
                + ", expected " + (MAGIC_V1_JSON & 0xFF));
        }

        byte[] payload = Arrays.copyOfRange(bytes, 1, bytes.length);
        SnapshotDocument snapshotProto = OBJECT_MAPPER.readValue(payload, SnapshotDocument.class);
        Map<Long, ProducerStateEntry> producers = new LinkedHashMap<>();
        if (snapshotProto == null || snapshotProto.producers() == null) {
            return producers;
        }
        for (SnapshotProducer producer : snapshotProto.producers()) {
            short producerEpoch = producer.producerEpoch();
            ProducerStateEntry stateEntry = new ProducerStateEntry(producerEpoch, producer.lastTimestamp());
            List<SnapshotBatch> batches = producer.batches();
            if (batches == null) {
                continue;
            }
            for (SnapshotBatch batch : batches) {
                stateEntry.appendBatch(producerEpoch, new BatchMetadata(
                    batch.firstSeq(),
                    batch.lastSeq(),
                    CompletableFuture.completedFuture(batch.baseOffset()),
                    batch.timestamp()
                ));
            }
            producers.put(producer.producerId(), stateEntry);
        }

        return producers;
    }

    public record SerializationResult(byte[] bytes, long lastOffset, int producerCount) {
    }

    private record SnapshotBatch(int firstSeq, int lastSeq, long baseOffset, long timestamp) {
    }

    private record SnapshotProducer(long producerId, short producerEpoch, long lastTimestamp,
                                      List<SnapshotBatch> batches) {
    }

    private record SnapshotDocument(List<SnapshotProducer> producers) {
    }

    private static Map<Long, SnapshotProducer> copySnapshotProducers(Map<Long, ProducerStateEntry> producers) {
        Map<Long, SnapshotProducer> copied = new LinkedHashMap<>();
        for (Map.Entry<Long, ProducerStateEntry> producerEntry : producers.entrySet()) {
            long producerId = producerEntry.getKey();
            ProducerStateEntry stateEntry = producerEntry.getValue();
            Deque<BatchMetadata> batchMetadata = stateEntry.batchMetadata();
            List<SnapshotBatch> batches = new ArrayList<>(batchMetadata.size());
            for (BatchMetadata metadata : batchMetadata) {
                CompletableFuture<Long> baseOffsetFuture = metadata.baseOffsetFuture();
                if (!baseOffsetFuture.isDone() || baseOffsetFuture.isCompletedExceptionally()) {
                    break;
                }
                long baseOffset = baseOffsetFuture.getNow(-1L);
                if (baseOffset < 0) {
                    break;
                }
                batches.add(new SnapshotBatch(
                    metadata.firstSeq(),
                    metadata.lastSeq(),
                    baseOffset,
                    metadata.timestamp()
                ));
            }
            if (!batches.isEmpty()) {
                copied.put(producerId, new SnapshotProducer(
                    producerId,
                    stateEntry.producerEpoch(),
                    stateEntry.lastTimestamp(),
                    batches));
            }
        }
        return copied;
    }

    private static SerializationResult encodeSnapshot(Map<Long, SnapshotProducer> producers) throws IOException {
        long lastOffset = -1L;
        List<SnapshotProducer> snapshotProducers = new ArrayList<>(producers.size());
        for (Map.Entry<Long, SnapshotProducer> group : producers.entrySet()) {
            SnapshotProducer producer = group.getValue();
            snapshotProducers.add(producer);
            for (SnapshotBatch batch : producer.batches()) {
                if (batch.baseOffset() > lastOffset) {
                    lastOffset = batch.baseOffset();
                }
            }
        }
        byte[] snapshotBytes = OBJECT_MAPPER.writeValueAsBytes(new SnapshotDocument(snapshotProducers));
        byte[] payloadWithMagic = new byte[snapshotBytes.length + 1];
        payloadWithMagic[0] = MAGIC_V1_JSON;
        System.arraycopy(snapshotBytes, 0, payloadWithMagic, 1, snapshotBytes.length);
        return new SerializationResult(payloadWithMagic, lastOffset, producers.size());
    }

    private static void pruneOldestHalf(Map<Long, SnapshotProducer> producers) {
        List<Long> sortedByTimestamp = producers.entrySet().stream()
            .sorted(Comparator.comparingLong(entry -> entry.getValue().lastTimestamp()))
            .map(Map.Entry::getKey)
            .toList();

        int retainCount = Math.max(1, producers.size() / 2);
        Map<Long, SnapshotProducer> retained = new LinkedHashMap<>();
        int start = sortedByTimestamp.size() - retainCount;
        for (int i = start; i < sortedByTimestamp.size(); i++) {
            Long producerId = sortedByTimestamp.get(i);
            SnapshotProducer producer = producers.get(producerId);
            if (producer != null) {
                retained.put(producerId, producer);
            }
        }
        producers.clear();
        producers.putAll(retained);
    }
}
