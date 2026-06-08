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
package org.apache.kafka.storage.diskless;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

class TestDisklessStorageEngine implements DisklessStorageEngine {
    private final Writer writer;
    private final Reader reader;
    private final DisklessStorageStateOperations state;

    TestDisklessStorageEngine(Writer writer, Reader reader, DisklessStorageStateOperations state) {
        this.writer = writer;
        this.reader = reader;
        this.state = state;
    }

    @Override
    public CompletableFuture<Map<TopicIdPartition, PartitionResponse>> write(
            Map<TopicIdPartition, MemoryRecords> entriesPerPartition,
            String zone) {
        return writer.write(entriesPerPartition, zone);
    }

    @Override
    public CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> fetch(
            FetchParams params,
            Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos) {
        return reader.fetch(params, fetchInfos);
    }

    @Override
    public CompletableFuture<Map<TopicIdPartition, ListOffsetsPartitionResponse>> listOffsets(
            Map<TopicIdPartition, ListOffsetsPartitionRequest> requests) {
        return reader.listOffsets(requests);
    }

    @Override
    public boolean cleanupPartition(TopicIdPartition tp, boolean deletePartition) {
        return state.cleanupPartition(tp, deletePartition);
    }

    @Override
    public void deletePartitionData(TopicIdPartition tp) {
        state.deletePartitionData(tp);
    }

    @Override
    public Set<TopicIdPartition> snapshotTrackedPartitions() {
        return state.snapshotTrackedPartitions();
    }

    @Override
    public boolean cleanupNonOwnedProducerStates(
            TopicIdPartition tp,
            Set<String> retainedZones,
            boolean deleteSnapshot) {
        return state.cleanupNonOwnedProducerStates(tp, retainedZones, deleteSnapshot);
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
        if (writer != null) {
            writer.close();
        }
        if (state != null) {
            state.close();
        }
    }
}
