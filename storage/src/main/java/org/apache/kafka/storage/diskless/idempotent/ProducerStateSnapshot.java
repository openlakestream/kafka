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

import org.apache.kafka.server.util.Json;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Snapshot of producer state for a partition, used for persistence and recovery.
 * 
 * <p>This class provides JSON serialization for storing producer state snapshots
 * in external storage (Oxia/S3). The snapshot contains all producer states for a
 * partition at a specific offset, enabling fast recovery without replaying all records.
 * 
 * <p>Snapshot format:
 * <pre>
 * {
 *   "version": 1,
 *   "offset": 12345,
 *   "timestamp": 1234567890123,
 *   "producers": [
 *     {
 *       "producerId": 1001,
 *       "epoch": 5,
 *       "lastSequence": 42,
 *       "lastOffset": 12340,
 *       "lastTimestamp": 1234567890000,
 *       "batches": [
 *         {"baseSequence": 40, "lastSequence": 42, "offset": 12340, "timestamp": 1234567890000}
 *       ]
 *     }
 *   ]
 * }
 * </pre>
 */
public class ProducerStateSnapshot {

    public static final int CURRENT_VERSION = 1;

    /**
     * Root snapshot object for JSON serialization.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class SnapshotData {
        public int version;
        public long offset;
        public long timestamp;
        public List<ProducerData> producers;

        public SnapshotData() {
            this.producers = new ArrayList<>();
        }

        public SnapshotData(int version, long offset, long timestamp, List<ProducerData> producers) {
            this.version = version;
            this.offset = offset;
            this.timestamp = timestamp;
            this.producers = producers != null ? producers : new ArrayList<>();
        }
    }

    /**
     * Producer state data for JSON serialization.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ProducerData {
        public long producerId;
        public short epoch;
        public int lastSequence;
        public long lastOffset;
        public long lastTimestamp;
        public List<BatchData> batches;

        public ProducerData() {
            this.batches = new ArrayList<>();
        }

        public ProducerData(long producerId, short epoch, int lastSequence, 
                           long lastOffset, long lastTimestamp, List<BatchData> batches) {
            this.producerId = producerId;
            this.epoch = epoch;
            this.lastSequence = lastSequence;
            this.lastOffset = lastOffset;
            this.lastTimestamp = lastTimestamp;
            this.batches = batches != null ? batches : new ArrayList<>();
        }
    }

    /**
     * Batch metadata for JSON serialization.
     * Fields are used for JSON serialization/deserialization by Jackson.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    @SuppressWarnings("unused") // Fields used by Jackson for JSON serialization
    public static class BatchData {
        public int baseSequence;
        public int lastSequence;
        public long offset;
        public long timestamp;

        public BatchData() { }

        public BatchData(int baseSequence, int lastSequence, long offset, long timestamp) {
            this.baseSequence = baseSequence;
            this.lastSequence = lastSequence;
            this.offset = offset;
            this.timestamp = timestamp;
        }
    }

    private final SnapshotData data;

    /**
     * Creates a new empty snapshot at the given offset.
     */
    public ProducerStateSnapshot(long offset, long timestamp) {
        this.data = new SnapshotData(CURRENT_VERSION, offset, timestamp, new ArrayList<>());
    }

    /**
     * Creates a snapshot from existing data.
     */
    public ProducerStateSnapshot(SnapshotData data) {
        this.data = data;
    }

    /**
     * Creates a snapshot from producer state map.
     * 
     * @param offset the offset up to which this snapshot is valid
     * @param timestamp the timestamp when snapshot was taken
     * @param producers map of producerId to producer state
     * @return a new snapshot containing all producer states
     */
    public static ProducerStateSnapshot create(
            long offset, 
            long timestamp, 
            Map<Long, ProducerStateStore.ProducerState> producers) {
        List<ProducerData> producerList = new ArrayList<>();
        
        for (Map.Entry<Long, ProducerStateStore.ProducerState> entry : producers.entrySet()) {
            ProducerStateStore.ProducerState state = entry.getValue();
            ProducerData producerData = new ProducerData(
                state.producerId(),
                state.epoch(),
                state.lastSequence(),
                state.lastOffset(),
                state.lastTimestamp(),
                new ArrayList<>()  // Batches are reconstructed during replay
            );
            producerList.add(producerData);
        }
        
        SnapshotData snapshotData = new SnapshotData(CURRENT_VERSION, offset, timestamp, producerList);
        return new ProducerStateSnapshot(snapshotData);
    }

    /**
     * @return the offset up to which this snapshot is valid
     */
    public long offset() {
        return data.offset;
    }

    /**
     * @return the timestamp when this snapshot was taken
     */
    public long timestamp() {
        return data.timestamp;
    }

    /**
     * @return the version of this snapshot format
     */
    public int version() {
        return data.version;
    }

    /**
     * @return list of producer states in this snapshot
     */
    public List<ProducerData> producers() {
        return data.producers;
    }

    /**
     * Converts producer data to ProducerState records.
     * 
     * @return map of producerId to ProducerState
     */
    public Map<Long, ProducerStateStore.ProducerState> toProducerStates() {
        Map<Long, ProducerStateStore.ProducerState> result = new HashMap<>();
        for (ProducerData pd : data.producers) {
            result.put(pd.producerId, new ProducerStateStore.ProducerState(
                pd.producerId,
                pd.epoch,
                pd.lastSequence,
                pd.lastOffset,
                pd.lastTimestamp,
                pd.batches != null ? pd.batches.size() : 0
            ));
        }
        return result;
    }

    /**
     * Serializes this snapshot to JSON bytes.
     * 
     * @return JSON representation as byte array
     * @throws JsonProcessingException if serialization fails
     */
    public byte[] toBytes() throws JsonProcessingException {
        return Json.encodeAsBytes(data);
    }

    /**
     * Serializes this snapshot to JSON string.
     * 
     * @return JSON representation as string
     * @throws JsonProcessingException if serialization fails
     */
    public String toJson() throws JsonProcessingException {
        return Json.encodeAsString(data);
    }

    /**
     * Deserializes a snapshot from JSON bytes.
     * 
     * @param bytes JSON representation
     * @return deserialized snapshot
     * @throws IOException if deserialization fails
     */
    public static ProducerStateSnapshot fromBytes(byte[] bytes) throws IOException {
        SnapshotData snapshotData = Json.parseBytesAs(bytes, SnapshotData.class);
        return new ProducerStateSnapshot(snapshotData);
    }

    /**
     * Deserializes a snapshot from JSON string.
     * 
     * @param json JSON representation
     * @return deserialized snapshot
     * @throws JsonProcessingException if deserialization fails
     */
    public static ProducerStateSnapshot fromJson(String json) throws JsonProcessingException {
        SnapshotData snapshotData = Json.parseStringAs(json, SnapshotData.class);
        return new ProducerStateSnapshot(snapshotData);
    }

    /**
     * Generates the Oxia key for storing this snapshot.
     * 
     * @param topicId the topic UUID
     * @param partition the partition number
     * @return the key string for Oxia storage
     */
    public static String generateSnapshotKey(String topicId, int partition) {
        return "producer-state-snapshot/" + topicId + "-" + partition;
    }

    @Override
    public String toString() {
        return "ProducerStateSnapshot{" +
            "version=" + data.version +
            ", offset=" + data.offset +
            ", timestamp=" + data.timestamp +
            ", producerCount=" + data.producers.size() +
            '}';
    }
}
