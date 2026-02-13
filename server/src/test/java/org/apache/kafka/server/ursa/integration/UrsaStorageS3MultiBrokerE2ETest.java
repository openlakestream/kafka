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
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.metadata.BrokerState;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import io.streamnative.oxia.testcontainers.OxiaContainer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * End-to-end integration tests for Ursa Storage with S3 backend in multi-broker setup.
 *
 * <p>Tests are organized into nested classes by functionality for better isolation.
 */
@Timeout(value = 600, unit = TimeUnit.SECONDS)
@Tag("integration")
public class UrsaStorageS3MultiBrokerE2ETest extends UrsaStorageE2ETestBase {

    private static final int NUM_BROKERS = 3;
    private static final String S3_BUCKET = "kafka-ursa-storage-multi-broker";
    private static final int MULTI_BROKER_PRODUCE_TIMEOUT_SECONDS = 120;

    @TempDir
    static Path baseDir;

    private static OxiaContainer oxiaContainer;
    private static LocalStackContainer localStackContainer;
    private static URI s3Endpoint;
    private static KafkaClusterTestKit cluster;

    @BeforeAll
    static void startContainers() throws Exception {
        assumeTrue(Boolean.parseBoolean(System.getProperty("runUrsaS3MultiBrokerE2E", "false")),
                "Skipping multi-broker S3 Ursa E2E by default on 4.2 compatibility branch");
        oxiaContainer = new OxiaContainer(DockerImageName.parse("oxia/oxia:main"));
        oxiaContainer.start();
        log.info("Oxia container started at: {}", oxiaContainer.getServiceAddress());

        localStackContainer = new LocalStackContainer(
                DockerImageName.parse("localstack/localstack:3.6"))
                .withServices(LocalStackContainer.Service.S3);
        localStackContainer.start();

        s3Endpoint = localStackContainer.getEndpointOverride(LocalStackContainer.Service.S3);
        log.info("LocalStack S3 container started at: {}", s3Endpoint);

        createS3BucketWithRetry(s3Endpoint, S3_BUCKET);
        log.info("Created S3 bucket: {}", S3_BUCKET);

        cluster = createClusterWithS3Backend(baseDir);
        cluster.format();
        cluster.startup();
        cluster.waitForReadyBrokers();
        waitForAllBrokersRunning();
    }

    @AfterAll
    static void stopContainers() {
        if (cluster != null) {
            try {
                cluster.close();
            } catch (Exception e) {
                log.warn("Failed to close KafkaClusterTestKit cleanly", e);
            }
        }
        if (localStackContainer != null) {
            localStackContainer.stop();
            log.info("LocalStack container stopped");
        }
        if (oxiaContainer != null) {
            oxiaContainer.stop();
            log.info("Oxia container stopped");
        }
    }

    private static KafkaClusterTestKit createClusterWithS3Backend(Path baseDir) throws Exception {
        String oxiaServiceAddress = oxiaContainer.getServiceAddress();
        String s3Prefix = "ursa-e2e/" + baseDir.getFileName();

        TestKitNodes nodes = new TestKitNodes.Builder()
                .setNumBrokerNodes(NUM_BROKERS)
                .setNumControllerNodes(1)
                .build();

        return new KafkaClusterTestKit.Builder(nodes)
                .setConfigProp(ServerLogConfigs.URSA_STORAGE_ENABLE_CONFIG, "true")
                .setConfigProp(ServerLogConfigs.URSA_STORAGE_OXIA_SERVICE_URL_CONFIG, oxiaServiceAddress)
                .setConfigProp(ServerLogConfigs.URSA_STORAGE_BACKEND_TYPE_CONFIG, "S3")
                .setConfigProp(ServerLogConfigs.URSA_STORAGE_PATH_CONFIG, s3Prefix)
                .setConfigProp(ServerLogConfigs.URSA_STORAGE_S3_ENDPOINT_CONFIG, s3Endpoint.toString())
                .setConfigProp(ServerLogConfigs.URSA_STORAGE_S3_ACCESS_KEY_CONFIG, "test")
                .setConfigProp(ServerLogConfigs.URSA_STORAGE_S3_SECRET_KEY_CONFIG, "test")
                .setConfigProp(ServerLogConfigs.URSA_STORAGE_S3_BUCKET_CONFIG, S3_BUCKET)
                .setConfigProp(ServerLogConfigs.URSA_STORAGE_S3_REGION_CONFIG, localStackContainer.getRegion())
                .setConfigProp("offsets.topic.replication.factor", "1")
                .setConfigProp("transaction.state.log.replication.factor", "1")
                .setConfigProp("transaction.state.log.min.isr", "1")
                .setConfigProp("default.replication.factor", String.valueOf(NUM_BROKERS))
                .build();
    }

    private static void waitForAllBrokersRunning() throws Exception {
        TestUtils.waitForCondition(
                () -> cluster.brokers().values().stream().allMatch(b -> b.brokerState() == BrokerState.RUNNING),
                90_000, "Not all brokers reached RUNNING state");
    }

    private Producer<byte[], byte[]> createMultiBrokerProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, "10");
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "200");
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000");
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "120000");
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, String.valueOf(MAX_REQUEST_SIZE));
        return new org.apache.kafka.clients.producer.KafkaProducer<>(props);
    }

    private void createDisklessTopicWithAdmin(Admin admin, String topicName, int partitions, short replicationFactor)
            throws Exception {
        Map<String, String> topicConfigs = new HashMap<>();
        topicConfigs.put(TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true");

        NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor).configs(topicConfigs);
        admin.createTopics(Collections.singleton(newTopic)).all().get(60, TimeUnit.SECONDS);
    }

    private void waitForPartitionLeadership(Admin admin, String topic, int partition, Integer expectedLeaderId)
            throws Exception {
        TestUtils.waitForCondition(
                () -> {
                    Integer leader = leaderIdOrNull(admin, topic, partition);
                    if (leader == null) return false;
                    return expectedLeaderId == null || expectedLeaderId.equals(leader);
                },
                90_000,
                expectedLeaderId == null
                        ? "Partition leader not elected for " + topic + "-" + partition
                        : "Partition leader for " + topic + "-" + partition + " did not become " + expectedLeaderId);
    }

    private Integer leaderId(Admin admin, String topic, int partition) throws Exception {
        Integer leader = leaderIdOrNull(admin, topic, partition);
        assertNotNull(leader, "Leader should not be null for " + topic + "-" + partition);
        return leader;
    }

    private Integer leaderIdOrNull(Admin admin, String topic, int partition) throws Exception {
        TopicPartitionInfo info = partitionInfoOrNull(admin, topic, partition);
        if (info == null) return null;
        Node leader = info.leader();
        return leader == null ? null : leader.id();
    }

    private TopicPartitionInfo partitionInfoOrNull(Admin admin, String topic, int partition) throws Exception {
        Map<String, TopicDescription> descriptions = admin.describeTopics(Collections.singleton(topic))
                .allTopicNames().get(60, TimeUnit.SECONDS);
        TopicDescription desc = descriptions.get(topic);
        if (desc == null) return null;
        for (TopicPartitionInfo info : desc.partitions()) {
            if (info.partition() == partition) return info;
        }
        return null;
    }

    private String brokerBootstrap(int brokerId) {
        int port = cluster.brokers().get(brokerId)
                .socketServer()
                .boundPort(cluster.nodes().brokerListenerName());
        return "localhost:" + port;
    }

    private String getSurvivingBrokersBootstrap(int brokerToShutdown) {
        List<String> brokerAddresses = new ArrayList<>();
        for (int i = 0; i < NUM_BROKERS; i++) {
            if (i != brokerToShutdown) {
                brokerAddresses.add(brokerBootstrap(i));
            }
        }
        String result = String.join(",", brokerAddresses);
        log.info("Using surviving brokers: {}", result);
        return result;
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

            try (Admin admin = Admin.create(cluster.clientProperties())) {
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

            try (Admin admin = Admin.create(cluster.clientProperties())) {
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

            try (Admin admin = Admin.create(cluster.clientProperties())) {
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
            try (Admin admin = Admin.create(cluster.clientProperties())) {
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
                    log.info("Successfully consumed {} records after leader broker {} went down - " +
                            "metadata transformer failover works!", numRecords, originalLeader);
                }
            } finally {
                log.info("Restarting broker {}...", originalLeader);
                cluster.brokers().get(originalLeader).startup();
                cluster.waitForReadyBrokers();
                waitForAllBrokersRunning();
            }
        }

        @Test
        @DisplayName("Consume from any broker with diskless storage")
        void testConsumeFromAnyBrokerWithDisklessStorage() throws Exception {
            String topicName = uniqueTopicName("s3-multi-broker-cross-broker-consume-topic");
            int numRecords = 30;

            try (Admin admin = Admin.create(cluster.clientProperties())) {
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

            int leaderId;
            try (Admin admin = Admin.create(cluster.clientProperties())) {
                leaderId = leaderId(admin, topicName, 0);
            }

            for (int brokerId = 0; brokerId < NUM_BROKERS; brokerId++) {
                String brokerBootstrapServer = brokerBootstrap(brokerId);
                String brokerRole = (brokerId == leaderId) ? "leader" : "non-leader";
                log.info("Testing consumption from {} broker {} using {}", brokerRole, brokerId, brokerBootstrapServer);

                try (Consumer<byte[], byte[]> consumer = createConsumer(brokerBootstrapServer)) {
                    TopicPartition tp = new TopicPartition(topicName, 0);
                    consumer.assign(Collections.singletonList(tp));
                    consumer.seekToBeginning(Collections.singletonList(tp));

                    List<ConsumerRecord<byte[], byte[]>> records = consumeRecords(consumer, numRecords, Duration.ofSeconds(90));
                    assertEquals(numRecords, records.size(),
                            "Should consume all records from " + brokerRole + " broker " + brokerId);

                    for (int i = 0; i < records.size(); i++) {
                        assertEquals("key-" + i, new String(records.get(i).key(), StandardCharsets.UTF_8));
                        assertEquals("value-" + i, new String(records.get(i).value(), StandardCharsets.UTF_8));
                    }
                    log.info("Successfully consumed {} records from {} broker {}", numRecords, brokerRole, brokerId);
                }
            }
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

            try (Admin admin = Admin.create(cluster.clientProperties())) {
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

        private TopicDescription waitForTopicReadyAndReturn(Admin admin, String topicName, int partitions) throws InterruptedException {
            final TopicDescription[] result = new TopicDescription[1];
            TestUtils.waitForCondition(() -> {
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
