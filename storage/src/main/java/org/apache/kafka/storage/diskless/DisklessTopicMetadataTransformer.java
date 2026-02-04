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

import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.utils.Utils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.StreamSupport;

public class DisklessTopicMetadataTransformer {

    private static final int INITIAL_LEADER_EPOCH = 0;

    private final DisklessStorageMetadataView metadataView;

    public DisklessTopicMetadataTransformer(DisklessStorageMetadataView metadataView) {
        this.metadataView = Objects.requireNonNull(metadataView, "metadataView cannot be null");
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
                int leader = selectLeaderForDisklessPartition(
                        listenerName, topic.topicId(), partition.partitionIndex());
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
                int leader = selectLeaderForDisklessPartition(
                        listenerName, topic.topicId(), partition.partitionIndex());
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

    private int selectLeaderForDisklessPartition(
            ListenerName listenerName,
            Uuid topicId,
            int partitionIndex
    ) {
        List<Node> brokers = allAliveBrokers(listenerName);

        if (brokers.isEmpty()) {
            throw new RuntimeException("No alive brokers found for diskless partition assignment");
        }

        byte[] input = String.format("%s-%s", topicId, partitionIndex).getBytes(StandardCharsets.UTF_8);
        int hash = Utils.murmur2(input);
        int idx = Math.abs(hash % brokers.size());

        return brokers.get(idx).id();
    }

    private List<Node> allAliveBrokers(ListenerName listenerName) {
        List<Node> result = new ArrayList<>();
        StreamSupport.stream(metadataView.getAliveBrokerNodes(listenerName).spliterator(), false)
                .sorted(Comparator.comparing(Node::id))
                .forEach(result::add);
        return result;
    }
}
