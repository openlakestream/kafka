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
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.metadata.BrokerState;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;
import org.apache.kafka.storage.internals.log.LogMetricNames;
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
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.management.ObjectName;

import io.streamnative.oxia.testcontainers.OxiaContainer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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

    private static final class OwnerRemapCandidate {
        private final int partition;
        private final int oldOwnerBrokerId;
        private final int newOwnerBrokerId;
        private final int brokerToShutdown;
        private final Uuid topicId;

        private OwnerRemapCandidate(
                int partition,
                int oldOwnerBrokerId,
                int newOwnerBrokerId,
                int brokerToShutdown,
                Uuid topicId
        ) {
            this.partition = partition;
            this.oldOwnerBrokerId = oldOwnerBrokerId;
            this.newOwnerBrokerId = newOwnerBrokerId;
            this.brokerToShutdown = brokerToShutdown;
            this.topicId = topicId;
        }
    }

    @BeforeAll
    static void startContainers() throws Exception {
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

        return enableBrokerRequestPipelining(new KafkaClusterTestKit.Builder(nodes))
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
        configureProducerRequestPipelining(props);
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

    private boolean brokerHasManagedLedgerState(int brokerId, TopicIdPartition topicIdPartition) throws Exception {
        Object ursaStorageState = cluster.brokers().get(brokerId)
                .replicaManager()
                .disklessStorageSupport()
                .getUrsaState();
        var snapshotMethod = ursaStorageState.getClass().getMethod("snapshotPartitionsWithLocalState");
        Object snapshot = snapshotMethod.invoke(ursaStorageState);
        if (snapshot instanceof Map<?, ?> managedLedgerMap) {
            @SuppressWarnings("unchecked")
            Map<TopicIdPartition, ?> managedLedgerCache = (Map<TopicIdPartition, ?>) managedLedgerMap;
            return managedLedgerCache.containsKey(topicIdPartition);
        } else if (snapshot instanceof java.util.Collection<?> partitions) {
            return partitions.contains(topicIdPartition);
        } else {
            throw new IllegalStateException(
                    "Unexpected snapshotPartitionsWithLocalState() return type: " + snapshot.getClass());
        }
    }

    private void waitForBrokerManagedLedgerState(
            int brokerId,
            TopicIdPartition topicIdPartition,
            boolean expected
    ) throws InterruptedException {
        String expectation = expected ? "create" : "cleanup";
        TestUtils.waitForCondition(
                () -> {
                    try {
                        return brokerHasManagedLedgerState(brokerId, topicIdPartition) == expected;
                    } catch (Exception e) {
                        return false;
                    }
                },
                30_000L,
                "Broker " + brokerId + " did not " + expectation + " managed ledger state for " + topicIdPartition);
    }

    private void waitForExclusiveManagedLedgerState(
            List<Integer> brokerIds,
            int expectedOwnerBrokerId,
            TopicIdPartition topicIdPartition
    ) throws InterruptedException {
        TestUtils.waitForCondition(
                () -> {
                    try {
                        for (int brokerId : brokerIds) {
                            boolean expected = brokerId == expectedOwnerBrokerId;
                            if (brokerHasManagedLedgerState(brokerId, topicIdPartition) != expected) {
                                return false;
                            }
                        }
                        return true;
                    } catch (Exception e) {
                        return false;
                    }
                },
                30_000L,
                "Expected only broker " + expectedOwnerBrokerId + " to retain managed ledger state for "
                        + topicIdPartition + " across brokers " + brokerIds);
    }

    private void waitForDisklessLogMetrics(String topic, int partition, boolean expectedPresent) throws InterruptedException {
        String expectation = expectedPresent ? "appear" : "be cleaned up";
        TestUtils.waitForCondition(
                () -> disklessLogMetricsPresent(topic, partition) == expectedPresent,
                30_000L,
                "Diskless log metrics for " + topic + "-" + partition + " did not " + expectation);
    }

    private boolean disklessLogMetricsPresent(String topic, int partition) {
        return jmxGaugeLongValue(LogMetricNames.SIZE, topic, partition) != null
                && jmxGaugeLongValue(LogMetricNames.LOG_START_OFFSET, topic, partition) != null
                && jmxGaugeLongValue(LogMetricNames.LOG_END_OFFSET, topic, partition) != null;
    }

    private static Long jmxGaugeLongValue(String metricName, String topic, int partition) {
        String partitionText = Integer.toString(partition);
        for (var entry : KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet()) {
            var name = entry.getKey();
            if (!"kafka.log".equals(name.getGroup()) || !"Log".equals(name.getType())) {
                continue;
            }

            ObjectName objectName = parseObjectName(name.getMBeanName());
            if (objectName == null || !isTargetMetric(objectName, metricName, topic, partitionText)) {
                continue;
            }

            Object metric = entry.getValue();
            if (metric instanceof com.yammer.metrics.core.Gauge<?> gauge) {
                Object value = gauge.value();
                if (value instanceof Number numberValue) {
                    return numberValue.longValue();
                }
            }
        }
        return null;
    }

    private static ObjectName parseObjectName(String mBeanName) {
        try {
            return new ObjectName(mBeanName);
        } catch (Exception ignored) {
            return null;
        }
    }

    private static boolean isTargetMetric(
            ObjectName objectName,
            String metricName,
            String topic,
            String partitionText
    ) {
        return metricName.equals(objectName.getKeyProperty("name"))
                && topic.equals(objectName.getKeyProperty("topic"))
                && partitionText.equals(objectName.getKeyProperty("partition"));
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

    private OwnerRemapCandidate findOwnerRemapCandidate(TopicDescription description) {
        // Pick a partition and a non-owner broker to shut down such that the surviving-broker
        // hash remaps ownership away from the still-alive old owner. This keeps the test focused
        // on "liveness changed, old owner stayed up, and stale local state must be cleaned".
        for (TopicPartitionInfo partitionInfo : description.partitions()) {
            int oldOwnerBrokerId = partitionInfo.leader().id();
            for (int brokerToShutdown = 0; brokerToShutdown < NUM_BROKERS; brokerToShutdown++) {
                if (brokerToShutdown == oldOwnerBrokerId) {
                    continue;
                }

                List<Node> survivingBrokers = new ArrayList<>();
                for (int brokerId = 0; brokerId < NUM_BROKERS; brokerId++) {
                    if (brokerId != brokerToShutdown) {
                        survivingBrokers.add(new Node(brokerId, "host" + brokerId, 9092));
                    }
                }

                int newOwnerBrokerId = selectOwnerBroker(description.topicId(), partitionInfo.partition(), survivingBrokers);
                if (newOwnerBrokerId != oldOwnerBrokerId && newOwnerBrokerId != brokerToShutdown) {
                    return new OwnerRemapCandidate(
                            partitionInfo.partition(),
                            oldOwnerBrokerId,
                            newOwnerBrokerId,
                            brokerToShutdown,
                            description.topicId()
                    );
                }
            }
        }
        return null;
    }

    private int selectOwnerBroker(Uuid topicId, int partition, List<Node> aliveBrokers) {
        List<Integer> brokerIds = new ArrayList<>();
        for (Node broker : aliveBrokers) {
            brokerIds.add(broker.id());
        }
        Collections.sort(brokerIds);
        byte[] input = (topicId + "-" + partition).getBytes(StandardCharsets.UTF_8);
        int idx = Math.floorMod(Utils.murmur2(input), brokerIds.size());
        return brokerIds.get(idx);
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
        @DisplayName("Broker liveness owner remap cleans stale diskless state on the still-alive old owner")
        void testBrokerLivenessOwnerRemapCleansOldOwnerState() throws Exception {
            String topicName = uniqueTopicName("s3-owner-remap-cleanup-topic");
            int numPartitions = 12;
            int numRecords = 15;
            OwnerRemapCandidate candidate;
            TopicPartition topicPartition;
            TopicIdPartition topicIdPartition;

            try (Admin admin = Admin.create(cluster.clientProperties())) {
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

            waitForBrokerManagedLedgerState(candidate.oldOwnerBrokerId, topicIdPartition, true);
            waitForBrokerManagedLedgerState(candidate.newOwnerBrokerId, topicIdPartition, false);

            cluster.brokers().get(candidate.brokerToShutdown).shutdown();
            cluster.brokers().get(candidate.brokerToShutdown).awaitShutdown();

            try {
                String survivingBootstrap = getSurvivingBrokersBootstrap(candidate.brokerToShutdown);
                Properties adminProps = new Properties();
                adminProps.putAll(cluster.clientProperties());
                adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, survivingBootstrap);

                try (Admin survivingAdmin = Admin.create(adminProps)) {
                    waitForPartitionLeadership(
                            survivingAdmin,
                            topicName,
                            candidate.partition,
                            candidate.newOwnerBrokerId
                    );
                }

                waitForBrokerManagedLedgerState(candidate.oldOwnerBrokerId, topicIdPartition, false);

                consumeAndVerifyRecords(
                        brokerBootstrap(candidate.newOwnerBrokerId),
                        topicName,
                        candidate.partition,
                        numRecords
                );
                waitForBrokerManagedLedgerState(candidate.newOwnerBrokerId, topicIdPartition, true);
            } finally {
                cluster.brokers().get(candidate.brokerToShutdown).startup();
                cluster.waitForReadyBrokers();
                waitForAllBrokersRunning();
            }
        }

        @Test
        @DisplayName("Restarted former owner only regains diskless state after it becomes owner again")
        void testRestartedFormerOwnerHasExclusiveStateOnlyWhenItBecomesOwnerAgain() throws Exception {
            String topicName = uniqueTopicName("s3-owner-restart-exclusive-state-topic");
            TopicPartition topicPartition = new TopicPartition(topicName, 0);
            int numRecords = 15;
            TopicIdPartition topicIdPartition;
            int originalOwnerBrokerId;

            List<Integer> allBrokerIds = new ArrayList<>();
            for (int brokerId = 0; brokerId < NUM_BROKERS; brokerId++) {
                allBrokerIds.add(brokerId);
            }

            try (Admin admin = Admin.create(cluster.clientProperties())) {
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
            waitForExclusiveManagedLedgerState(allBrokerIds, originalOwnerBrokerId, topicIdPartition);
            waitForDisklessLogMetrics(topicName, 0, true);

            cluster.brokers().get(originalOwnerBrokerId).shutdown();
            cluster.brokers().get(originalOwnerBrokerId).awaitShutdown();

            try {
                List<Integer> survivingBrokerIds = new ArrayList<>(allBrokerIds);
                survivingBrokerIds.remove(Integer.valueOf(originalOwnerBrokerId));

                String survivingBootstrap = getSurvivingBrokersBootstrap(originalOwnerBrokerId);
                int failoverOwnerBrokerId;
                Properties adminProps = new Properties();
                adminProps.putAll(cluster.clientProperties());
                adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, survivingBootstrap);

                try (Admin survivingAdmin = Admin.create(adminProps)) {
                    waitForPartitionLeadership(survivingAdmin, topicName, 0, null);
                    failoverOwnerBrokerId = leaderId(survivingAdmin, topicName, 0);
                }

                assertNotEquals(originalOwnerBrokerId, failoverOwnerBrokerId,
                        "Ownership should move away from the shut down broker");

                waitForDisklessLogMetrics(topicName, 0, false);
                consumeAndVerifyRecords(brokerBootstrap(failoverOwnerBrokerId), topicName, 0, numRecords);
                waitForExclusiveManagedLedgerState(survivingBrokerIds, failoverOwnerBrokerId, topicIdPartition);
                waitForDisklessLogMetrics(topicName, 0, true);

                cluster.brokers().get(originalOwnerBrokerId).startup();
                cluster.waitForReadyBrokers();
                waitForAllBrokersRunning();

                try (Admin admin = Admin.create(cluster.clientProperties())) {
                    waitForPartitionLeadership(admin, topicName, 0, originalOwnerBrokerId);
                }

                waitForDisklessLogMetrics(topicName, 0, false);
                consumeAndVerifyRecords(brokerBootstrap(originalOwnerBrokerId), topicName, 0, numRecords);
                waitForExclusiveManagedLedgerState(allBrokerIds, originalOwnerBrokerId, topicIdPartition);
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
        @DisplayName("Partition reassignment follower transition cleans stale diskless managed ledger state on the added broker")
        void testPartitionReassignmentFollowerTransitionDoesNotRetainUnexpectedDisklessState() throws Exception {
            String topicName = uniqueTopicName("s3-reassignment-metrics-topic");
            TopicPartition topicPartition = new TopicPartition(topicName, 0);
            int recordsBeforeReassignment = 12;
            int recordsAfterReassignment = 8;

            int originalLeader;
            int targetLeader;
            TopicIdPartition topicIdPartition;
            try (Admin admin = Admin.create(cluster.clientProperties())) {
                createDisklessTopicWithAdmin(admin, topicName, 1, (short) 1);
                waitForPartitionLeadership(admin, topicName, 0, null);

                TopicDescription description = admin.describeTopics(Collections.singleton(topicName))
                        .allTopicNames()
                        .get(60, TimeUnit.SECONDS)
                        .get(topicName);
                topicIdPartition = new TopicIdPartition(description.topicId(), topicPartition);
                originalLeader = leaderId(admin, topicName, 0);
            }

            produceRecords(cluster.bootstrapServers(), topicName, 0, recordsBeforeReassignment);

            targetLeader = (originalLeader + 1) % NUM_BROKERS;
            waitForBrokerManagedLedgerState(originalLeader, topicIdPartition, true);
            waitForBrokerManagedLedgerState(targetLeader, topicIdPartition, false);

            Object ursaStorageState = cluster.brokers().get(targetLeader)
                    .replicaManager()
                    .disklessStorageSupport()
                    .getUrsaState();
            Object managedLedgerFuture = ursaStorageState.getClass()
                    .getMethod("getOrCreateManagedLedger", TopicIdPartition.class)
                    .invoke(ursaStorageState, topicIdPartition);
            ((java.util.concurrent.CompletableFuture<?>) managedLedgerFuture)
                    .get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            waitForBrokerManagedLedgerState(targetLeader, topicIdPartition, true);

            try (Admin admin = Admin.create(cluster.clientProperties())) {
                admin.alterPartitionReassignments(Map.of(
                        topicPartition,
                        Optional.of(new NewPartitionReassignment(List.of(targetLeader)))
                )).all().get(60, TimeUnit.SECONDS);
            }

            waitForBrokerManagedLedgerState(targetLeader, topicIdPartition, false);

            produceRecords(cluster.bootstrapServers(), topicName, 0, recordsAfterReassignment);

            long expectedLogEndOffset = recordsBeforeReassignment + recordsAfterReassignment;

            try (Consumer<byte[], byte[]> consumer = createConsumer(cluster.bootstrapServers())) {
                consumer.assign(Collections.singletonList(topicPartition));
                consumer.seekToBeginning(Collections.singletonList(topicPartition));

                List<ConsumerRecord<byte[], byte[]>> records = consumeRecords(
                        consumer,
                        (int) expectedLogEndOffset,
                        Duration.ofSeconds(90));
                assertEquals(expectedLogEndOffset, records.size(),
                        "Should consume all records across the reassignment boundary");
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

            try (Admin admin = Admin.create(cluster.clientProperties())) {
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

            waitForBrokerManagedLedgerState(leaderId, topicIdPartition, true);
            assertFalse(brokerHasManagedLedgerState(nonOwnerBrokerId, topicIdPartition),
                    "Non-owner broker should not retain managed ledger state before fetch");

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

            assertFalse(brokerHasManagedLedgerState(nonOwnerBrokerId, topicIdPartition),
                    "Non-owner broker should still have no managed ledger state after fetch bootstrap redirect");
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
