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
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponsePartition;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DisklessTopicMetadataTransformerTest {

    private static final ListenerName LISTENER = ListenerName.forSecurityProtocol(
            org.apache.kafka.common.security.auth.SecurityProtocol.PLAINTEXT);
    private static final ListenerName OWNER_SELECTION_LISTENER = new ListenerName("INTERNAL");

    private DisklessStorageMetadataView metadataView;
    private DisklessBrokerSelector brokerSelector;
    private DisklessTopicMetadataTransformer transformer;

    @BeforeEach
    void setUp() {
        metadataView = mock(DisklessStorageMetadataView.class);
        brokerSelector = new DisklessBrokerSelector(metadataView::getAliveBrokerNodes, OWNER_SELECTION_LISTENER);
        transformer = new DisklessTopicMetadataTransformer(metadataView, brokerSelector);
    }

    @Test
    void testConstructorRejectsNullMetadataView() {
        assertThrows(NullPointerException.class, () -> new DisklessTopicMetadataTransformer(null, brokerSelector));
        assertThrows(NullPointerException.class, () -> new DisklessTopicMetadataTransformer(metadataView, null));
    }

    @Test
    void testTransformClusterMetadataRejectsNull() {
        assertThrows(NullPointerException.class,
                () -> transformer.transformClusterMetadata(LISTENER, null));
    }

    @Test
    void testNonDisklessTopicNotTransformed() {
        Uuid topicId = Uuid.randomUuid();
        MetadataResponseTopic topic = new MetadataResponseTopic()
                .setName("normal-topic")
                .setTopicId(topicId)
                .setPartitions(List.of(
                        new MetadataResponsePartition()
                                .setPartitionIndex(0)
                                .setLeaderId(1)
                                .setReplicaNodes(List.of(1, 2, 3))
                                .setIsrNodes(List.of(1, 2, 3))
                ));

        when(metadataView.isDisklessStorageTopic("normal-topic")).thenReturn(false);

        transformer.transformClusterMetadata(LISTENER, List.of(topic));

        assertEquals(1, topic.partitions().get(0).leaderId());
        assertEquals(List.of(1, 2, 3), topic.partitions().get(0).replicaNodes());
    }

    @Test
    void testDisklessTopicTransformed() {
        Uuid topicId = Uuid.randomUuid();
        MetadataResponseTopic topic = new MetadataResponseTopic()
                .setName("diskless-topic")
                .setTopicId(topicId)
                .setPartitions(List.of(
                        new MetadataResponsePartition()
                                .setPartitionIndex(0)
                                .setLeaderId(99)
                                .setReplicaNodes(List.of(99))
                                .setIsrNodes(List.of(99))
                                .setErrorCode(Errors.LEADER_NOT_AVAILABLE.code())
                ));

        when(metadataView.isDisklessStorageTopic("diskless-topic")).thenReturn(true);
        when(metadataView.getAliveBrokerNodes(OWNER_SELECTION_LISTENER)).thenReturn(List.of(
                new Node(0, "host0", 9092),
                new Node(1, "host1", 9092),
                new Node(2, "host2", 9092)
        ));

        transformer.transformClusterMetadata(LISTENER, List.of(topic));

        MetadataResponsePartition partition = topic.partitions().get(0);
        assertTrue(partition.leaderId() >= 0 && partition.leaderId() <= 2);
        assertEquals(List.of(partition.leaderId()), partition.replicaNodes());
        assertEquals(List.of(partition.leaderId()), partition.isrNodes());
        assertEquals(Errors.NONE.code(), partition.errorCode());
        assertEquals(Collections.emptyList(), partition.offlineReplicas());
    }

    @Test
    void testPartitionsDistributedAcrossBrokers() {
        Uuid topicId = Uuid.randomUuid();
        MetadataResponseTopic topic = new MetadataResponseTopic()
                .setName("diskless-topic")
                .setTopicId(topicId)
                .setPartitions(Arrays.asList(
                        new MetadataResponsePartition().setPartitionIndex(0).setLeaderId(0),
                        new MetadataResponsePartition().setPartitionIndex(1).setLeaderId(0),
                        new MetadataResponsePartition().setPartitionIndex(2).setLeaderId(0),
                        new MetadataResponsePartition().setPartitionIndex(3).setLeaderId(0),
                        new MetadataResponsePartition().setPartitionIndex(4).setLeaderId(0),
                        new MetadataResponsePartition().setPartitionIndex(5).setLeaderId(0)
                ));

        when(metadataView.isDisklessStorageTopic("diskless-topic")).thenReturn(true);
        when(metadataView.getAliveBrokerNodes(OWNER_SELECTION_LISTENER)).thenReturn(List.of(
                new Node(0, "host0", 9092),
                new Node(1, "host1", 9092),
                new Node(2, "host2", 9092)
        ));

        transformer.transformClusterMetadata(LISTENER, List.of(topic));

        Set<Integer> leaders = new HashSet<>();
        for (MetadataResponsePartition partition : topic.partitions()) {
            leaders.add(partition.leaderId());
        }
        assertTrue(leaders.size() > 1, "Partitions should be distributed across multiple brokers");
    }

    @Test
    void testConsistentHashingProducesSameResult() {
        Uuid topicId = Uuid.randomUuid();

        when(metadataView.isDisklessStorageTopic("diskless-topic")).thenReturn(true);
        when(metadataView.getAliveBrokerNodes(OWNER_SELECTION_LISTENER)).thenReturn(List.of(
                new Node(0, "host0", 9092),
                new Node(1, "host1", 9092),
                new Node(2, "host2", 9092)
        ));

        MetadataResponseTopic topic1 = createTopic("diskless-topic", topicId, 3);
        MetadataResponseTopic topic2 = createTopic("diskless-topic", topicId, 3);

        transformer.transformClusterMetadata(LISTENER, List.of(topic1));
        transformer.transformClusterMetadata(LISTENER, List.of(topic2));

        for (int i = 0; i < 3; i++) {
            assertEquals(topic1.partitions().get(i).leaderId(),
                    topic2.partitions().get(i).leaderId(),
                    "Same partition should get same leader");
        }
    }

    @Test
    void testBrokerFailoverChangesLeader() {
        Uuid topicId = Uuid.randomUuid();

        when(metadataView.isDisklessStorageTopic("diskless-topic")).thenReturn(true);

        when(metadataView.getAliveBrokerNodes(OWNER_SELECTION_LISTENER)).thenReturn(List.of(
                new Node(0, "host0", 9092),
                new Node(1, "host1", 9092),
                new Node(2, "host2", 9092)
        ));
        MetadataResponseTopic topicBefore = createTopic("diskless-topic", topicId, 6);
        transformer.transformClusterMetadata(LISTENER, List.of(topicBefore));

        when(metadataView.getAliveBrokerNodes(OWNER_SELECTION_LISTENER)).thenReturn(List.of(
                new Node(1, "host1", 9092),
                new Node(2, "host2", 9092)
        ));
        MetadataResponseTopic topicAfter = createTopic("diskless-topic", topicId, 6);
        transformer.transformClusterMetadata(LISTENER, List.of(topicAfter));

        for (MetadataResponsePartition partition : topicAfter.partitions()) {
            assertNotEquals(0, partition.leaderId(), "Broker 0 should not be leader after failover");
        }
    }

    @Test
    void testThrowsWhenNoBrokersAvailable() {
        Uuid topicId = Uuid.randomUuid();
        MetadataResponseTopic topic = createTopic("diskless-topic", topicId, 1);

        when(metadataView.isDisklessStorageTopic("diskless-topic")).thenReturn(true);
        when(metadataView.getAliveBrokerNodes(OWNER_SELECTION_LISTENER)).thenReturn(Collections.emptyList());

        assertThrows(RuntimeException.class,
                () -> transformer.transformClusterMetadata(LISTENER, List.of(topic)));
    }

    @Test
    void testLeaderSelectionUsesOwnerSelectionListener() {
        Uuid topicId = Uuid.randomUuid();
        MetadataResponseTopic topic = createTopic("diskless-topic", topicId, 1);

        when(metadataView.isDisklessStorageTopic("diskless-topic")).thenReturn(true);
        when(metadataView.getAliveBrokerNodes(LISTENER)).thenReturn(List.of(
                new Node(9, "request-listener-host", 9092)
        ));
        when(metadataView.getAliveBrokerNodes(OWNER_SELECTION_LISTENER)).thenReturn(List.of(
                new Node(0, "host0", 9092),
                new Node(1, "host1", 9092),
                new Node(2, "host2", 9092)
        ));

        transformer.transformClusterMetadata(LISTENER, List.of(topic));

        int leaderId = topic.partitions().get(0).leaderId();
        assertTrue(leaderId >= 0 && leaderId <= 2);
        assertNotEquals(9, leaderId);
    }

    private MetadataResponseTopic createTopic(String name, Uuid topicId, int numPartitions) {
        MetadataResponseTopic topic = new MetadataResponseTopic()
                .setName(name)
                .setTopicId(topicId);
        for (int i = 0; i < numPartitions; i++) {
            topic.partitions().add(new MetadataResponsePartition()
                    .setPartitionIndex(i)
                    .setLeaderId(0)
                    .setReplicaNodes(List.of(0))
                    .setIsrNodes(List.of(0)));
        }
        return topic;
    }
}
