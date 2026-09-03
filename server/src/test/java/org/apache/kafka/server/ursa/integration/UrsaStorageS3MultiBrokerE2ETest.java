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
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.metadata.BrokerState;

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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * End-to-end integration tests for Ursa Storage with S3 backend in multi-broker setup
 * without zone-aware routing semantics.
 */
@Timeout(value = 600, unit = TimeUnit.SECONDS)
@Tag("integration")
public class UrsaStorageS3MultiBrokerE2ETest extends AbstractUrsaStorageS3MultiBrokerE2ETest {

    @TempDir
    static Path baseDir;

    @BeforeAll
    static void startContainers() throws Exception {
        startCluster(baseDir, false);
    }

    @AfterAll
    static void stopContainers() {
        stopCluster();
    }

    /**
     * Basic multi-broker tests.
     */
    @Nested
    @DisplayName("Basic Multi-Broker Tests")
    class BasicMultiBrokerTests {

        @Test
        @DisplayName("Multi-broker produce and consume with S3 backend")
        void testMultiBrokerProduceConsumeWithS3Backend() throws Exception {
            String topicName = uniqueTopicName("s3-multi-broker-topic");
            int numRecords = 50;

            try (Admin admin = cluster.admin()) {
                createDisklessTopicWithAdmin(admin, topicName, 1, (short) 1);
                waitForPartitionLeadership(admin, topicName, 0, null);
            }

            try (Producer<byte[], byte[]> producer = createMultiBrokerProducer(cluster.bootstrapServers())) {
                for (int i = 0; i < numRecords; i++) {
                    ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                            topicName, 0,
                            ("key-" + i).getBytes(StandardCharsets.UTF_8),
                            ("value-" + i).getBytes(StandardCharsets.UTF_8));
                    producer.send(record).get(MULTI_BROKER_PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                }
                producer.flush();
            }

            try (Consumer<byte[], byte[]> consumer = createConsumer(cluster.bootstrapServers())) {
                TopicPartition tp = new TopicPartition(topicName, 0);
                consumer.assign(Collections.singletonList(tp));
                consumer.seekToBeginning(Collections.singletonList(tp));

                List<ConsumerRecord<byte[], byte[]>> records = consumeRecords(consumer, numRecords, Duration.ofSeconds(90));
                assertEquals(numRecords, records.size());

                for (int i = 0; i < records.size(); i++) {
                    assertEquals(topicName, records.get(i).topic());
                    assertEquals(0, records.get(i).partition());
                    assertEquals("key-" + i, new String(records.get(i).key(), StandardCharsets.UTF_8));
                    assertEquals("value-" + i, new String(records.get(i).value(), StandardCharsets.UTF_8));
                }
            }
        }

        @Test
        @DisplayName("Multi-partition distributed across brokers")
        void testMultiPartitionDistributedAcrossBrokers() throws Exception {
            String topicName = uniqueTopicName("s3-multi-broker-partition-distribution-topic");
            int numPartitions = 3;
            int recordsPerPartition = 20;

            try (Admin admin = cluster.admin()) {
                createDisklessTopicWithAdmin(admin, topicName, numPartitions, (short) 1);

                for (int p = 0; p < numPartitions; p++) {
                    waitForPartitionLeadership(admin, topicName, p, null);
                }

                TopicDescription desc = admin.describeTopics(Collections.singletonList(topicName))
                        .allTopicNames().get().get(topicName);
                log.info("Diskless topic partition distribution:");
                for (TopicPartitionInfo pInfo : desc.partitions()) {
                    log.info("  Partition {} -> Leader broker {}", pInfo.partition(), pInfo.leader().id());
                }
                assumeTrue(desc.partitions().stream().map(p -> p.leader().id()).distinct().count() > 1,
                        "Partitions are not distributed across brokers; skipping.");
            }

            try (Producer<byte[], byte[]> producer = createMultiBrokerProducer(cluster.bootstrapServers())) {
                for (int partition = 0; partition < numPartitions; partition++) {
                    for (int i = 0; i < recordsPerPartition; i++) {
                        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                                topicName, partition,
                                ("key-" + partition + "-" + i).getBytes(StandardCharsets.UTF_8),
                                ("value-" + partition + "-" + i).getBytes(StandardCharsets.UTF_8));
                        producer.send(record).get(MULTI_BROKER_PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    }
                }
                producer.flush();
            }

            try (Consumer<byte[], byte[]> consumer = createConsumer(cluster.bootstrapServers())) {
                for (int partition = 0; partition < numPartitions; partition++) {
                    TopicPartition tp = new TopicPartition(topicName, partition);
                    consumer.assign(Collections.singletonList(tp));
                    consumer.seekToBeginning(Collections.singletonList(tp));

                    List<ConsumerRecord<byte[], byte[]>> records = consumeRecords(
                            consumer, recordsPerPartition, Duration.ofSeconds(90));
                    assertEquals(recordsPerPartition, records.size());

                    for (int i = 0; i < records.size(); i++) {
                        assertEquals(partition, records.get(i).partition());
                        assertEquals("key-" + partition + "-" + i,
                                new String(records.get(i).key(), StandardCharsets.UTF_8));
                        assertEquals("value-" + partition + "-" + i,
                                new String(records.get(i).value(), StandardCharsets.UTF_8));
                    }
                }
            }
        }
    }

    /**
     * Failover and metadata transformer tests.
     */
    @Nested
    @DisplayName("Failover Tests")
    class FailoverTests {

        @Test
        @DisplayName("Metadata transformer dynamic failover")
        void testMetadataTransformerDynamicFailover() throws Exception {
            String topicName = uniqueTopicName("s3-metadata-transformer-failover-topic");
            int numRecords = 30;

            try (Admin admin = cluster.admin()) {
                createDisklessTopicWithAdmin(admin, topicName, 1, (short) 1);
                waitForPartitionLeadership(admin, topicName, 0, null);
            }

            try (Producer<byte[], byte[]> producer = createMultiBrokerProducer(cluster.bootstrapServers())) {
                for (int i = 0; i < numRecords; i++) {
                    ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                            topicName, 0,
                            ("key-" + i).getBytes(StandardCharsets.UTF_8),
                            ("value-" + i).getBytes(StandardCharsets.UTF_8));
                    producer.send(record).get(MULTI_BROKER_PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                }
                producer.flush();
            }

            int originalLeader;
            try (Admin admin = cluster.admin()) {
                originalLeader = leaderId(admin, topicName, 0);
            }
            log.info("Original leader for partition 0: broker {}", originalLeader);

            log.info("Shutting down leader broker {}...", originalLeader);
            cluster.brokers().get(originalLeader).shutdown();
            cluster.brokers().get(originalLeader).awaitShutdown();
            log.info("Leader broker {} is now down", originalLeader);

            try {
                String survivingBootstrap = getSurvivingBrokersBootstrap(originalLeader);
                log.info("Connecting to surviving brokers: {}", survivingBootstrap);

                try (Consumer<byte[], byte[]> consumer = createConsumer(survivingBootstrap)) {
                    TopicPartition tp = new TopicPartition(topicName, 0);
                    consumer.assign(Collections.singletonList(tp));
                    consumer.seekToBeginning(Collections.singletonList(tp));

                    List<ConsumerRecord<byte[], byte[]>> records = consumeRecords(consumer, numRecords, Duration.ofSeconds(120));

                    assertEquals(numRecords, records.size(),
                            "Should consume all records after leader failover via metadata transformer");

                    for (int i = 0; i < records.size(); i++) {
                        assertEquals("key-" + i, new String(records.get(i).key(), StandardCharsets.UTF_8));
                        assertEquals("value-" + i, new String(records.get(i).value(), StandardCharsets.UTF_8));
                    }
                    log.info("Successfully consumed {} records after leader broker {} went down - "
                            + "metadata transformer failover works!", numRecords, originalLeader);
                }
            } finally {
                log.info("Restarting broker {}...", originalLeader);
                cluster.brokers().get(originalLeader).startup();
                cluster.waitForReadyBrokers();
                waitForAllBrokersRunning();
            }
        }

        @Test
        @DisplayName("Broker liveness owner remap cleans stale diskless state on the still-alive old owner")
        void testBrokerLivenessOwnerRemapCleansOldOwnerState() throws Exception {
            String topicName = uniqueTopicName("s3-owner-remap-cleanup-topic");
            int numPartitions = 12;
            int numRecords = 15;
            OwnerRemapCandidate candidate;
            TopicPartition topicPartition;
            TopicIdPartition topicIdPartition;

            try (Admin admin = cluster.admin()) {
                createDisklessTopicWithAdmin(admin, topicName, numPartitions, (short) 1);
                for (int partition = 0; partition < numPartitions; partition++) {
                    waitForPartitionLeadership(admin, topicName, partition, null);
                }

                TopicDescription description = admin.describeTopics(Collections.singleton(topicName))
                        .allTopicNames()
                        .get(60, TimeUnit.SECONDS)
                        .get(topicName);
                candidate = findOwnerRemapCandidate(description);
                assertNotNull(candidate, "Expected a diskless partition whose owner remaps after a non-owner broker shutdown");
                topicPartition = new TopicPartition(topicName, candidate.partition);
                topicIdPartition = new TopicIdPartition(candidate.topicId, topicPartition);
            }

            produceRecords(cluster.bootstrapServers(), topicName, candidate.partition, numRecords);

            waitForBrokerPartitionLogState(candidate.oldOwnerBrokerId, topicIdPartition, true);
            waitForBrokerPartitionLogState(candidate.newOwnerBrokerId, topicIdPartition, false);

            cluster.brokers().get(candidate.brokerToShutdown).shutdown();
            cluster.brokers().get(candidate.brokerToShutdown).awaitShutdown();

            try {
                String survivingBootstrap = getSurvivingBrokersBootstrap(candidate.brokerToShutdown);
                try (Admin survivingAdmin = cluster.admin(Map.of(
                        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, survivingBootstrap))) {
                    waitForPartitionLeadership(
                            survivingAdmin,
                            topicName,
                            candidate.partition,
                            candidate.newOwnerBrokerId
                    );
                }

                waitForBrokerPartitionLogState(candidate.oldOwnerBrokerId, topicIdPartition, false);

                consumeAndVerifyRecords(
                        brokerBootstrap(candidate.newOwnerBrokerId),
                        topicName,
                        candidate.partition,
                        numRecords
                );
                waitForBrokerPartitionLogState(candidate.newOwnerBrokerId, topicIdPartition, true);
            } finally {
                cluster.brokers().get(candidate.brokerToShutdown).startup();
                cluster.waitForReadyBrokers();
                waitForAllBrokersRunning();
            }
        }

        @Test
        @DisplayName("Restarted former owner regains diskless state only after owner I/O")
        void testRestartedFormerOwnerHasExclusiveStateOnlyAfterOwnerIo() throws Exception {
            String topicName = uniqueTopicName("s3-owner-restart-exclusive-state-topic");
            TopicPartition topicPartition = new TopicPartition(topicName, 0);
            int numRecords = 15;
            TopicIdPartition topicIdPartition;
            int originalOwnerBrokerId;

            List<Integer> allBrokerIds = new ArrayList<>();
            for (int brokerId = 0; brokerId < NUM_BROKERS; brokerId++) {
                allBrokerIds.add(brokerId);
            }

            try (Admin admin = cluster.admin()) {
                createDisklessTopicWithAdmin(admin, topicName, 1, (short) 1);
                waitForPartitionLeadership(admin, topicName, 0, null);

                TopicDescription description = admin.describeTopics(Collections.singleton(topicName))
                        .allTopicNames()
                        .get(60, TimeUnit.SECONDS)
                        .get(topicName);
                topicIdPartition = new TopicIdPartition(description.topicId(), topicPartition);
                originalOwnerBrokerId = leaderId(admin, topicName, 0);
            }

            produceRecords(cluster.bootstrapServers(), topicName, 0, numRecords);
            waitForExclusivePartitionLogState(allBrokerIds, originalOwnerBrokerId, topicIdPartition);
            waitForDisklessLogMetrics(topicName, 0, true);

            cluster.brokers().get(originalOwnerBrokerId).shutdown();
            cluster.brokers().get(originalOwnerBrokerId).awaitShutdown();

            try {
                List<Integer> survivingBrokerIds = new ArrayList<>(allBrokerIds);
                survivingBrokerIds.remove(Integer.valueOf(originalOwnerBrokerId));

                String survivingBootstrap = getSurvivingBrokersBootstrap(originalOwnerBrokerId);
                int failoverOwnerBrokerId;
                try (Admin survivingAdmin = cluster.admin(Map.of(
                        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, survivingBootstrap))) {
                    waitForPartitionLeadership(survivingAdmin, topicName, 0, null);
                    failoverOwnerBrokerId = leaderId(survivingAdmin, topicName, 0);
                }

                assertNotEquals(originalOwnerBrokerId, failoverOwnerBrokerId,
                        "Ownership should move away from the shut down broker");

                waitForNoPartitionLogState(survivingBrokerIds, topicIdPartition);
                consumeAndVerifyRecords(brokerBootstrap(failoverOwnerBrokerId), topicName, 0, numRecords);
                waitForExclusivePartitionLogState(survivingBrokerIds, failoverOwnerBrokerId, topicIdPartition);
                waitForDisklessLogMetrics(topicName, 0, true);

                cluster.brokers().get(originalOwnerBrokerId).startup();
                cluster.waitForReadyBrokers();
                waitForAllBrokersRunning();

                try (Admin admin = cluster.admin()) {
                    waitForPartitionLeadership(admin, topicName, 0, originalOwnerBrokerId);
                }

                waitForNoPartitionLogState(allBrokerIds, topicIdPartition);
                consumeAndVerifyRecords(brokerBootstrap(originalOwnerBrokerId), topicName, 0, numRecords);
                waitForExclusivePartitionLogState(allBrokerIds, originalOwnerBrokerId, topicIdPartition);
                waitForDisklessLogMetrics(topicName, 0, true);
            } finally {
                if (cluster.brokers().get(originalOwnerBrokerId).brokerState() != BrokerState.RUNNING) {
                    cluster.brokers().get(originalOwnerBrokerId).startup();
                    cluster.waitForReadyBrokers();
                    waitForAllBrokersRunning();
                }
            }
        }

        @Test
        @DisplayName("Bootstrapping diskless fetch through a non-owner broker does not retain local state on the non-owner")
        void testDisklessFetchViaNonOwnerBootstrapDoesNotRetainStateOnNonOwner() throws Exception {
            String topicName = uniqueTopicName("s3-multi-broker-non-owner-bootstrap-topic");
            int numRecords = 30;
            TopicPartition topicPartition = new TopicPartition(topicName, 0);
            TopicIdPartition topicIdPartition;
            int leaderId;
            int nonOwnerBrokerId;

            try (Admin admin = cluster.admin()) {
                createDisklessTopicWithAdmin(admin, topicName, 1, (short) 1);
                waitForPartitionLeadership(admin, topicName, 0, null);

                TopicDescription description = admin.describeTopics(Collections.singleton(topicName))
                        .allTopicNames()
                        .get(60, TimeUnit.SECONDS)
                        .get(topicName);
                topicIdPartition = new TopicIdPartition(description.topicId(), topicPartition);
                leaderId = leaderId(admin, topicName, 0);
                nonOwnerBrokerId = (leaderId + 1) % NUM_BROKERS;
            }

            try (Producer<byte[], byte[]> producer = createMultiBrokerProducer(cluster.bootstrapServers())) {
                for (int i = 0; i < numRecords; i++) {
                    ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                            topicName, 0,
                            ("key-" + i).getBytes(StandardCharsets.UTF_8),
                            ("value-" + i).getBytes(StandardCharsets.UTF_8));
                    producer.send(record).get(MULTI_BROKER_PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                }
                producer.flush();
            }

            waitForBrokerPartitionLogState(leaderId, topicIdPartition, true);
            assertFalse(brokerHasPartitionLogState(nonOwnerBrokerId, topicIdPartition),
                    "Non-owner broker should not retain partition log state before fetch");

            String nonOwnerBootstrapServer = brokerBootstrap(nonOwnerBrokerId);
            try (Consumer<byte[], byte[]> consumer = createConsumer(nonOwnerBootstrapServer)) {
                consumer.assign(Collections.singletonList(topicPartition));
                consumer.seekToBeginning(Collections.singletonList(topicPartition));

                List<ConsumerRecord<byte[], byte[]>> records = consumeRecords(consumer, numRecords, Duration.ofSeconds(90));
                assertEquals(numRecords, records.size(),
                        "Should consume all records when bootstrapping through non-owner broker " + nonOwnerBrokerId);

                for (int i = 0; i < records.size(); i++) {
                    assertEquals("key-" + i, new String(records.get(i).key(), StandardCharsets.UTF_8));
                    assertEquals("value-" + i, new String(records.get(i).value(), StandardCharsets.UTF_8));
                }
            }

            assertFalse(brokerHasPartitionLogState(nonOwnerBrokerId, topicIdPartition),
                    "Non-owner broker should still have no partition log state after fetch bootstrap redirect");
        }
    }

    /**
     * Topic configuration tests.
     */
    @Nested
    @DisplayName("Topic Configuration Tests")
    class TopicConfigurationTests {

        @Test
        @DisplayName("Create diskless topic with default replication factor")
        void testCreateDisklessTopicWithDefaultReplicationFactor() throws Exception {
            String topicName = uniqueTopicName("s3-diskless-default-rf-topic");
            int numPartitions = 3;
            int numRecords = 30;

            try (Admin admin = cluster.admin()) {
                Map<String, String> topicConfigs = new HashMap<>();
                topicConfigs.put(TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true");

                NewTopic newTopic = new NewTopic(topicName, numPartitions, (short) -1).configs(topicConfigs);
                admin.createTopics(Collections.singleton(newTopic)).all().get(60, TimeUnit.SECONDS);

                TopicDescription topicDescription = waitForTopicReadyAndReturn(admin, topicName, numPartitions);
                assertNotNull(topicDescription);
                assertEquals(numPartitions, topicDescription.partitions().size());

                for (TopicPartitionInfo partitionInfo : topicDescription.partitions()) {
                    assertEquals(1, partitionInfo.replicas().size(),
                            "Diskless topic with RF=-1 should resolve to RF=1, not default.replication.factor=" + NUM_BROKERS);
                    assertEquals(1, partitionInfo.isr().size(),
                            "Diskless topic with RF=-1 should have 1 ISR");
                }

                log.info("Created diskless topic {} with {} partitions using RF=-1, verified RF=1 (not default {})",
                        topicName, numPartitions, NUM_BROKERS);

                for (int p = 0; p < numPartitions; p++) {
                    waitForPartitionLeadership(admin, topicName, p, null);
                }
            }

            try (Producer<byte[], byte[]> producer = createMultiBrokerProducer(cluster.bootstrapServers())) {
                for (int i = 0; i < numRecords; i++) {
                    int partition = i % numPartitions;
                    ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                            topicName, partition,
                            ("key-" + i).getBytes(StandardCharsets.UTF_8),
                            ("value-" + i).getBytes(StandardCharsets.UTF_8));
                    producer.send(record).get(MULTI_BROKER_PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                }
                producer.flush();
            }

            log.info("Produced {} records to diskless topic with default RF", numRecords);

            try (Consumer<byte[], byte[]> consumer = createConsumer(cluster.bootstrapServers())) {
                List<TopicPartition> partitions = new ArrayList<>();
                for (int p = 0; p < numPartitions; p++) {
                    partitions.add(new TopicPartition(topicName, p));
                }
                consumer.assign(partitions);
                consumer.seekToBeginning(partitions);

                List<ConsumerRecord<byte[], byte[]>> allRecords = consumeRecords(consumer, numRecords, Duration.ofSeconds(90));
                assertEquals(numRecords, allRecords.size(),
                        "Should consume all " + numRecords + " records from diskless topic with default RF");

                log.info("Consumed {} records from diskless topic with default RF", allRecords.size());
            }

            log.info("Diskless topic with RF=-1 test passed - topic created with RF=1 (not default {}) and is fully functional",
                    NUM_BROKERS);
        }

        private TopicDescription waitForTopicReadyAndReturn(Admin admin, String topicName, int partitions)
                throws InterruptedException {
            final TopicDescription[] result = new TopicDescription[1];
            org.apache.kafka.test.TestUtils.waitForCondition(() -> {
                try {
                    TopicDescription desc = admin.describeTopics(Collections.singleton(topicName))
                            .allTopicNames()
                            .get(5, TimeUnit.SECONDS)
                            .get(topicName);
                    if (desc != null && desc.partitions().size() == partitions) {
                        result[0] = desc;
                        return true;
                    }
                    return false;
                } catch (Exception e) {
                    return false;
                }
            }, 60_000L, "Timed out waiting for topic " + topicName + " partitions=" + partitions);
            return result[0];
        }
    }
}
