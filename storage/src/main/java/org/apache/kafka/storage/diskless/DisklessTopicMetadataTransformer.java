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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class DisklessTopicMetadataTransformer {

    private static final int INITIAL_LEADER_EPOCH = 0;

    private final DisklessStorageMetadataView metadataView;
    private final DisklessBrokerSelector brokerSelector;

    public DisklessTopicMetadataTransformer(
            DisklessStorageMetadataView metadataView,
            DisklessBrokerSelector brokerSelector
    ) {
        this.metadataView = Objects.requireNonNull(metadataView, "metadataView cannot be null");
        this.brokerSelector = Objects.requireNonNull(brokerSelector, "brokerSelector cannot be null");
    }

    public void transformClusterMetadata(
            ListenerName listenerName,
            Iterable<MetadataResponseTopic> topicMetadata
    ) {
        Objects.requireNonNull(topicMetadata, "topicMetadata cannot be null");

        for (MetadataResponseTopic topic : topicMetadata) {
            if (!metadataView.isDisklessStorageTopic(topic.name())) {
                continue;
            }
            for (var partition : topic.partitions()) {
                int leader = selectLeaderForDisklessPartition(topic.topicId(), partition.partitionIndex());
                partition.setLeaderId(leader);
                List<Integer> replicas = List.of(leader);
                partition.setErrorCode(Errors.NONE.code());
                partition.setReplicaNodes(replicas);
                partition.setIsrNodes(replicas);
                partition.setOfflineReplicas(Collections.emptyList());
                partition.setLeaderEpoch(INITIAL_LEADER_EPOCH);
            }
        }
    }

    public void transformDescribeTopicResponse(
            ListenerName listenerName,
            DescribeTopicPartitionsResponseData responseData
    ) {
        Objects.requireNonNull(responseData, "responseData cannot be null");

        for (var topic : responseData.topics()) {
            if (!metadataView.isDisklessStorageTopic(topic.name())) {
                continue;
            }

            for (var partition : topic.partitions()) {
                int leader = selectLeaderForDisklessPartition(topic.topicId(), partition.partitionIndex());
                partition.setLeaderId(leader);
                List<Integer> replicas = List.of(leader);
                partition.setErrorCode(Errors.NONE.code());
                partition.setReplicaNodes(replicas);
                partition.setIsrNodes(replicas);
                partition.setEligibleLeaderReplicas(Collections.emptyList());
                partition.setLastKnownElr(Collections.emptyList());
                partition.setOfflineReplicas(Collections.emptyList());
                partition.setLeaderEpoch(INITIAL_LEADER_EPOCH);
            }
        }
    }

    private int selectLeaderForDisklessPartition(Uuid topicId, int partitionIndex) {
        return brokerSelector.selectBroker(topicId, partitionIndex)
                .orElseThrow(() ->
                        new RuntimeException("No alive brokers found for diskless partition assignment"));
    }
}
