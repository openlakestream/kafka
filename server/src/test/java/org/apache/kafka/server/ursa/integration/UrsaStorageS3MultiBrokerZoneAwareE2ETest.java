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
package org.apache.kafka.server.ursa.integration;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Timeout(value = 600, unit = TimeUnit.SECONDS)
@Tag("integration")
public class UrsaStorageS3MultiBrokerZoneAwareE2ETest extends AbstractUrsaStorageS3MultiBrokerE2ETest {

    @TempDir
    static Path baseDir;

    @BeforeAll
    static void startContainers() throws Exception {
        startCluster(baseDir, true);
    }

    @AfterAll
    static void stopContainers() {
        stopCluster();
    }

    @Nested
    @DisplayName("Rack Topology Tests")
    class RackTopologyTests {

        @Test
        @DisplayName("Zone-aware metadata and request routing follow broker rack topology")
        void testZoneAwareRoutingFollowsBrokerRackTopology() throws Exception {
            String topicName = uniqueTopicName("s3-rack-topology-zone-aware-topic");
            String zoneAMetadataClientId = "metadata-e2e,zone_id=zone-a";
            String zoneBClientId = "client-e2e,zone_id=zone-b";
            int partition = 0;
            int numRecords = 20;
            TopicPartition topicPartition = new TopicPartition(topicName, partition);
            TopicIdPartition topicIdPartition;
            int zoneAOwnerBrokerId;
            int zoneBOwnerBrokerId;

            try (Admin admin = cluster.admin()) {
                createDisklessTopicWithAdmin(admin, topicName, 1, (short) 1);
                waitForPartitionLeadership(admin, topicName, partition, null);

                TopicDescription description = admin.describeTopics(Collections.singleton(topicName))
                        .allTopicNames()
                        .get(60, TimeUnit.SECONDS)
                        .get(topicName);
                topicIdPartition = new TopicIdPartition(description.topicId(), topicPartition);
                zoneAOwnerBrokerId = selectOwnerBroker(
                        description.topicId(),
                        partition,
                        brokerNodesWithRacks(),
                        "zone-a");
                zoneBOwnerBrokerId = selectOwnerBroker(
                        description.topicId(),
                        partition,
                        brokerNodesWithRacks(),
                        "zone-b");
            }

            assertEquals("zone-a", BROKER_RACKS.get(zoneAOwnerBrokerId),
                    "Zone-a client should stay within the zone-a broker subset");
            assertEquals("zone-b", BROKER_RACKS.get(zoneBOwnerBrokerId),
                    "Zone-b client should stay within the zone-b broker subset");
            assertNotEquals(zoneAOwnerBrokerId, zoneBOwnerBrokerId,
                    "Different client zones should resolve to different pseudo leaders in this topology");

            try (Admin zoneAAdmin = createAdminWithClientId(cluster.bootstrapServers(), zoneAMetadataClientId);
                 Admin zoneBAdmin = createAdminWithClientId(cluster.bootstrapServers(), zoneBClientId)) {
                TopicPartitionInfo zoneAPartitionInfo = partitionInfoOrNull(zoneAAdmin, topicName, partition);
                TopicPartitionInfo zoneBPartitionInfo = partitionInfoOrNull(zoneBAdmin, topicName, partition);

                assertNotNull(zoneAPartitionInfo, "Zone-a metadata should include the created partition");
                assertNotNull(zoneBPartitionInfo, "Zone-b metadata should include the created partition");
                assertEquals(zoneAOwnerBrokerId, zoneAPartitionInfo.leader().id(),
                        "Zone-a metadata should point at the zone-a owner");
                assertEquals("zone-a", zoneAPartitionInfo.leader().rack(),
                        "Zone-a metadata should expose the expected broker rack");
                assertEquals(zoneBOwnerBrokerId, zoneBPartitionInfo.leader().id(),
                        "Zone-b metadata should point at the zone-b owner");
                assertEquals("zone-b", zoneBPartitionInfo.leader().rack(),
                        "Zone-b metadata should expose the expected broker rack");
            }

            verifyPartitionLogStateForIoOwners(topicIdPartition, Set.of());

            String zoneABootstrap = brokerBootstrap(zoneAOwnerBrokerId);

            try (Producer<byte[], byte[]> producer =
                         createMultiBrokerProducer(zoneABootstrap, zoneBClientId)) {
                for (int i = 0; i < numRecords; i++) {
                    ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                            topicName, partition,
                            ("key-" + i).getBytes(StandardCharsets.UTF_8),
                            ("value-" + i).getBytes(StandardCharsets.UTF_8));
                    producer.send(record).get(MULTI_BROKER_PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                }
                producer.flush();
            }

            verifyPartitionLogStateForIoOwners(topicIdPartition, Set.of(zoneBOwnerBrokerId));

            try (Consumer<byte[], byte[]> consumer = createMultiBrokerConsumer(
                    zoneABootstrap,
                    "rack-topology-zone-b-group-" + System.currentTimeMillis(),
                    zoneBClientId)) {
                consumer.assign(Collections.singletonList(topicPartition));
                consumer.seekToBeginning(Collections.singletonList(topicPartition));

                List<ConsumerRecord<byte[], byte[]>> records =
                        consumeRecords(consumer, numRecords, Duration.ofSeconds(90));
                assertEquals(numRecords, records.size(),
                        "Zone-b consumer should read all records even when bootstrapping through a zone-a broker");

                for (int i = 0; i < records.size(); i++) {
                    assertEquals("key-" + i, new String(records.get(i).key(), StandardCharsets.UTF_8));
                    assertEquals("value-" + i, new String(records.get(i).value(), StandardCharsets.UTF_8));
                }
            }

            verifyPartitionLogStateForIoOwners(topicIdPartition, Set.of(zoneBOwnerBrokerId));
        }

        @Test
        @DisplayName("Concurrent multi-zone producers and consumers can read and write through their zone-aware owners")
        void testConcurrentMultiZoneProducersAndConsumers() throws Exception {
            String topicName = uniqueTopicName("s3-rack-topology-concurrent-zone-topic");
            int partition = 0;
            int producersPerZone = 2;
            int recordsPerProducer = 15;
            int totalRecords = producersPerZone * 2 * recordsPerProducer;
            TopicPartition topicPartition = new TopicPartition(topicName, partition);
            ZoneOwnerContext zoneOwnerContext = createTopicAndDetermineZoneOwners(topicName, topicPartition, partition);
            String zoneAClientId = "consumer-concurrent,zone_id=zone-a";
            String zoneBClientId = "consumer-concurrent,zone_id=zone-b";
            assertZoneAwareLeadership(
                    topicName,
                    partition,
                    zoneOwnerContext.zoneAOwnerBrokerId,
                    zoneOwnerContext.zoneBOwnerBrokerId,
                    zoneAClientId,
                    zoneBClientId);
            verifyPartitionLogStateForIoOwners(zoneOwnerContext.topicIdPartition, Set.of());

            Set<String> expectedPayloads = expectedConcurrentZonePayloads(producersPerZone, recordsPerProducer);
            ConcurrentZoneTrafficResult trafficResult = runConcurrentMultiZoneTraffic(
                    topicName,
                    topicPartition,
                    partition,
                    producersPerZone,
                    recordsPerProducer,
                    totalRecords,
                    zoneOwnerContext.zoneAOwnerBrokerId,
                    zoneOwnerContext.zoneBOwnerBrokerId,
                    zoneAClientId,
                    zoneBClientId);

            assertEquals(expectedPayloads, trafficResult.zoneAObservedPayloads,
                    "Zone-a consumer should observe every payload written by both zones");
            assertEquals(expectedPayloads, trafficResult.zoneBObservedPayloads,
                    "Zone-b consumer should observe every payload written by both zones");
            verifyPartitionLogStateForIoOwners(
                    zoneOwnerContext.topicIdPartition,
                    Set.of(zoneOwnerContext.zoneAOwnerBrokerId, zoneOwnerContext.zoneBOwnerBrokerId));
        }
    }
}
