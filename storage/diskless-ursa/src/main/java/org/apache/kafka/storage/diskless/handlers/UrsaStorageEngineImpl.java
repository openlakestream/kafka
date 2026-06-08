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
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.storage.diskless.DisklessStorageEngine;
import org.apache.kafka.storage.diskless.ListOffsetsPartitionRequest;
import org.apache.kafka.storage.diskless.ListOffsetsPartitionResponse;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class UrsaStorageEngineImpl implements DisklessStorageEngine {

    private final UrsaStorageState state;
    private final UrsaManagedLedgerWriter writer;
    private final UrsaManagedLedgerReader reader;

    public UrsaStorageEngineImpl(
            Time time,
            int brokerId,
            UrsaStorageConfig config,
            BrokerTopicStats brokerTopicStats,
            Map<String, Object> logConfigDefaults,
            Function<String, Map<String, String>> topicConfigSupplier) {
        this.state = new UrsaStorageState(
                time,
                brokerId,
                config,
                brokerTopicStats,
                logConfigDefaults,
                topicConfigSupplier
        );
        this.writer = new UrsaManagedLedgerWriter(state);
        this.reader = new UrsaManagedLedgerReader(state);
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
        reader.close();
        writer.close();
        state.close();
    }
}
