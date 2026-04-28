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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.metadata.BrokerState;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;
import org.apache.kafka.storage.internals.log.LogMetricNames;
import org.apache.kafka.test.TestUtils;

import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.management.ObjectName;

import io.streamnative.oxia.testcontainers.OxiaContainer;

import static org.junit.jupiter.api.Assertions.assertNotNull;

abstract class AbstractUrsaStorageS3MultiBrokerE2ETest extends UrsaStorageE2ETestBase {

    protected static final int NUM_BROKERS = 3;
    protected static final String S3_BUCKET = "kafka-ursa-storage-multi-broker";
    protected static final int MULTI_BROKER_PRODUCE_TIMEOUT_SECONDS = 120;
    protected static final Map<Integer, String> BROKER_RACKS = Map.of(
            0, "zone-a",
            1, "zone-b",
            2, "zone-a");

    protected static OxiaContainer oxiaContainer;
    protected static LocalStackContainer localStackContainer;
    protected static URI s3Endpoint;
    protected static KafkaClusterTestKit cluster;

    protected static final class OwnerRemapCandidate {
        protected final int partition;
        protected final int oldOwnerBrokerId;
        protected final int newOwnerBrokerId;
        protected final int brokerToShutdown;
        protected final Uuid topicId;

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

    protected static final class ZoneOwnerContext {
        protected final TopicIdPartition topicIdPartition;
        protected final int zoneAOwnerBrokerId;
        protected final int zoneBOwnerBrokerId;

        private ZoneOwnerContext(
                TopicIdPartition topicIdPartition,
                int zoneAOwnerBrokerId,
                int zoneBOwnerBrokerId
        ) {
            this.topicIdPartition = topicIdPartition;
            this.zoneAOwnerBrokerId = zoneAOwnerBrokerId;
            this.zoneBOwnerBrokerId = zoneBOwnerBrokerId;
        }
    }

    protected static final class ConcurrentZoneTrafficResult {
        protected final Set<String> zoneAObservedPayloads;
        protected final Set<String> zoneBObservedPayloads;

        private ConcurrentZoneTrafficResult(Set<String> zoneAObservedPayloads, Set<String> zoneBObservedPayloads) {
            this.zoneAObservedPayloads = zoneAObservedPayloads;
            this.zoneBObservedPayloads = zoneBObservedPayloads;
        }
    }

    protected static void startCluster(Path baseDir, boolean includeBrokerRacks) throws Exception {
        stopCluster();

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

        cluster = createClusterWithS3Backend(baseDir, includeBrokerRacks);
        cluster.format();
        cluster.startup();
        cluster.waitForReadyBrokers();
        waitForAllBrokersRunning();
    }

    protected static void stopCluster() {
        if (cluster != null) {
            try {
                cluster.close();
            } catch (Exception e) {
                log.warn("Failed to close KafkaClusterTestKit cleanly", e);
            } finally {
                cluster = null;
            }
        }
        if (localStackContainer != null) {
            try {
                localStackContainer.stop();
                log.info("LocalStack container stopped");
            } finally {
                localStackContainer = null;
                s3Endpoint = null;
            }
        }
        if (oxiaContainer != null) {
            try {
                oxiaContainer.stop();
                log.info("Oxia container stopped");
            } finally {
                oxiaContainer = null;
            }
        }
    }

    private static KafkaClusterTestKit createClusterWithS3Backend(Path baseDir, boolean includeBrokerRacks)
            throws Exception {
        String oxiaServiceAddress = oxiaContainer.getServiceAddress();
        String s3Prefix = "ursa-e2e/" + baseDir.getFileName();

        TestKitNodes.Builder nodesBuilder = new TestKitNodes.Builder()
                .setNumBrokerNodes(NUM_BROKERS)
                .setNumControllerNodes(1);
        if (includeBrokerRacks) {
            nodesBuilder.setPerServerProperties(brokerRackProperties());
        }
        TestKitNodes nodes = nodesBuilder.build();

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

    private static Map<Integer, Map<String, String>> brokerRackProperties() {
        Map<Integer, Map<String, String>> result = new HashMap<>();
        BROKER_RACKS.forEach((brokerId, rack) ->
                result.put(brokerId, Map.of(ServerConfigs.BROKER_RACK_CONFIG, rack)));
        return result;
    }

    protected static void waitForAllBrokersRunning() throws Exception {
        TestUtils.waitForCondition(
                () -> cluster.brokers().values().stream().allMatch(b -> b.brokerState() == BrokerState.RUNNING),
                90_000,
                "Not all brokers reached RUNNING state");
    }

    protected Producer<byte[], byte[]> createMultiBrokerProducer(String bootstrapServers) {
        return createMultiBrokerProducer(bootstrapServers, null);
    }

    protected Producer<byte[], byte[]> createMultiBrokerProducer(String bootstrapServers, String clientId) {
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
        if (clientId != null) {
            props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        }
        configureProducerRequestPipelining(props);
        return new org.apache.kafka.clients.producer.KafkaProducer<>(props);
    }

    protected Consumer<byte[], byte[]> createMultiBrokerConsumer(String bootstrapServers, String groupId, String clientId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, String.valueOf(MAX_REQUEST_SIZE));
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        return new KafkaConsumer<>(props);
    }

    protected Admin createAdminWithClientId(String bootstrapServers, String clientId) {
        Properties props = new Properties();
        props.putAll(cluster.clientProperties());
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, clientId);
        return Admin.create(props);
    }

    protected void createDisklessTopicWithAdmin(Admin admin, String topicName, int partitions, short replicationFactor)
            throws Exception {
        Map<String, String> topicConfigs = new HashMap<>();
        topicConfigs.put(TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true");

        NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor).configs(topicConfigs);
        admin.createTopics(Collections.singleton(newTopic)).all().get(60, TimeUnit.SECONDS);
    }

    protected void waitForPartitionLeadership(Admin admin, String topic, int partition, Integer expectedLeaderId)
            throws Exception {
        TestUtils.waitForCondition(
                () -> {
                    Integer leader = leaderIdOrNull(admin, topic, partition);
                    if (leader == null) {
                        return false;
                    }
                    return expectedLeaderId == null || expectedLeaderId.equals(leader);
                },
                90_000,
                expectedLeaderId == null
                        ? "Partition leader not elected for " + topic + "-" + partition
                        : "Partition leader for " + topic + "-" + partition + " did not become " + expectedLeaderId);
    }

    protected Integer leaderId(Admin admin, String topic, int partition) throws Exception {
        Integer leader = leaderIdOrNull(admin, topic, partition);
        assertNotNull(leader, "Leader should not be null for " + topic + "-" + partition);
        return leader;
    }

    protected Integer leaderIdOrNull(Admin admin, String topic, int partition) throws Exception {
        TopicPartitionInfo info = partitionInfoOrNull(admin, topic, partition);
        if (info == null) {
            return null;
        }
        Node leader = info.leader();
        return leader == null ? null : leader.id();
    }

    protected TopicPartitionInfo partitionInfoOrNull(Admin admin, String topic, int partition) throws Exception {
        Map<String, TopicDescription> descriptions = admin.describeTopics(Collections.singleton(topic))
                .allTopicNames()
                .get(60, TimeUnit.SECONDS);
        TopicDescription desc = descriptions.get(topic);
        if (desc == null) {
            return null;
        }
        for (TopicPartitionInfo info : desc.partitions()) {
            if (info.partition() == partition) {
                return info;
            }
        }
        return null;
    }

    protected boolean brokerHasManagedLedgerState(int brokerId, TopicIdPartition topicIdPartition) throws Exception {
        return cluster.brokers().get(brokerId)
                .replicaManager()
                .disklessStorageSupport()
                .snapshotTrackedPartitions()
                .contains(topicIdPartition);
    }

    protected void waitForBrokerManagedLedgerState(
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

    protected void waitForExclusiveManagedLedgerState(
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

    protected void waitForDisklessLogMetrics(String topic, int partition, boolean expectedPresent)
            throws InterruptedException {
        String expectation = expectedPresent ? "appear" : "be cleaned up";
        TestUtils.waitForCondition(
                () -> disklessLogMetricsPresent(topic, partition) == expectedPresent,
                30_000L,
                "Diskless log metrics for " + topic + "-" + partition + " did not " + expectation);
    }

    protected boolean disklessLogMetricsPresent(String topic, int partition) {
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

    protected String brokerBootstrap(int brokerId) {
        int port = cluster.brokers().get(brokerId)
                .socketServer()
                .boundPort(cluster.nodes().brokerListenerName());
        return "localhost:" + port;
    }

    protected String getSurvivingBrokersBootstrap(int brokerToShutdown) {
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

    protected OwnerRemapCandidate findOwnerRemapCandidate(TopicDescription description) {
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

    protected int selectOwnerBroker(Uuid topicId, int partition, List<Node> aliveBrokers) {
        return selectOwnerBroker(topicId, partition, aliveBrokers, null);
    }

    protected int selectOwnerBroker(Uuid topicId, int partition, List<Node> aliveBrokers, String clientZone) {
        List<Node> candidateBrokers = aliveBrokers;
        if (clientZone != null) {
            List<Node> brokersInClientZone = aliveBrokers.stream()
                    .filter(node -> clientZone.equals(node.rack()))
                    .toList();
            if (!brokersInClientZone.isEmpty()) {
                candidateBrokers = brokersInClientZone;
            }
        }

        List<Integer> brokerIds = new ArrayList<>();
        for (Node broker : candidateBrokers) {
            brokerIds.add(broker.id());
        }
        Collections.sort(brokerIds);
        byte[] input = (topicId + "-" + partition).getBytes(StandardCharsets.UTF_8);
        int idx = Math.floorMod(Utils.murmur2(input), brokerIds.size());
        return brokerIds.get(idx);
    }

    protected List<Node> brokerNodesWithRacks() {
        List<Node> nodes = new ArrayList<>();
        for (Map.Entry<Integer, String> entry : BROKER_RACKS.entrySet()) {
            nodes.add(new Node(entry.getKey(), "host" + entry.getKey(), 9092, entry.getValue()));
        }
        nodes.sort(Comparator.comparingInt(Node::id));
        return nodes;
    }

    protected Set<String> consumePayloads(
            String bootstrapServers,
            String groupId,
            String clientId,
            TopicPartition topicPartition,
            int expectedCount,
            Duration timeout
    ) {
        try (Consumer<byte[], byte[]> consumer = createMultiBrokerConsumer(bootstrapServers, groupId, clientId)) {
            consumer.assign(Collections.singletonList(topicPartition));
            consumer.seekToBeginning(Collections.singletonList(topicPartition));

            List<ConsumerRecord<byte[], byte[]>> records = consumeRecords(consumer, expectedCount, timeout);
            Set<String> payloads = new HashSet<>();
            for (ConsumerRecord<byte[], byte[]> record : records) {
                payloads.add(toPayload(
                        new String(record.key(), StandardCharsets.UTF_8),
                        new String(record.value(), StandardCharsets.UTF_8)));
            }
            return payloads;
        }
    }

    protected String toPayload(String key, String value) {
        return key + "=" + value;
    }

    protected ZoneOwnerContext createTopicAndDetermineZoneOwners(
            String topicName,
            TopicPartition topicPartition,
            int partition
    ) throws Exception {
        try (Admin admin = Admin.create(cluster.clientProperties())) {
            createDisklessTopicWithAdmin(admin, topicName, 1, (short) 1);
            waitForPartitionLeadership(admin, topicName, partition, null);

            TopicDescription description = admin.describeTopics(Collections.singleton(topicName))
                    .allTopicNames()
                    .get(60, TimeUnit.SECONDS)
                    .get(topicName);
            return new ZoneOwnerContext(
                    new TopicIdPartition(description.topicId(), topicPartition),
                    selectOwnerBroker(description.topicId(), partition, brokerNodesWithRacks(), "zone-a"),
                    selectOwnerBroker(description.topicId(), partition, brokerNodesWithRacks(), "zone-b"));
        }
    }

    protected void assertZoneAwareLeadership(
            String topicName,
            int partition,
            int zoneAOwnerBrokerId,
            int zoneBOwnerBrokerId,
            String zoneAClientId,
            String zoneBClientId
    ) throws Exception {
        try (Admin zoneAAdmin = createAdminWithClientId(cluster.bootstrapServers(), zoneAClientId);
             Admin zoneBAdmin = createAdminWithClientId(cluster.bootstrapServers(), zoneBClientId)) {
            waitForPartitionLeadership(zoneAAdmin, topicName, partition, zoneAOwnerBrokerId);
            waitForPartitionLeadership(zoneBAdmin, topicName, partition, zoneBOwnerBrokerId);
        }
    }

    protected Set<String> expectedConcurrentZonePayloads(int producersPerZone, int recordsPerProducer) {
        Set<String> expectedPayloads = new HashSet<>();
        for (int producerIndex = 0; producerIndex < producersPerZone; producerIndex++) {
            for (int recordIndex = 0; recordIndex < recordsPerProducer; recordIndex++) {
                expectedPayloads.add(toPayload(
                        "zone-a-p" + producerIndex + "-k" + recordIndex,
                        "zone-a-p" + producerIndex + "-v" + recordIndex));
                expectedPayloads.add(toPayload(
                        "zone-b-p" + producerIndex + "-k" + recordIndex,
                        "zone-b-p" + producerIndex + "-v" + recordIndex));
            }
        }
        return expectedPayloads;
    }

    protected Future<Set<String>> submitZoneConsumer(
            ExecutorService executor,
            CountDownLatch consumersReady,
            CountDownLatch startLatch,
            String bootstrapServers,
            String groupId,
            String clientId,
            TopicPartition topicPartition,
            int expectedCount
    ) {
        return executor.submit(() -> {
            consumersReady.countDown();
            awaitStartLatch(startLatch, "consumer " + clientId);
            return consumePayloads(
                    bootstrapServers,
                    groupId,
                    clientId,
                    topicPartition,
                    expectedCount,
                    Duration.ofSeconds(120));
        });
    }

    protected Future<?> submitZoneProducer(
            ExecutorService executor,
            CountDownLatch startLatch,
            String bootstrapServers,
            String clientId,
            String zonePrefix,
            String topicName,
            int partition,
            int producerIndex,
            int recordsPerProducer
    ) {
        return executor.submit(() -> {
            awaitStartLatch(startLatch, "producer " + clientId);
            try (Producer<byte[], byte[]> producer = createMultiBrokerProducer(bootstrapServers, clientId)) {
                for (int recordIndex = 0; recordIndex < recordsPerProducer; recordIndex++) {
                    producer.send(new ProducerRecord<>(
                            topicName,
                            partition,
                            (zonePrefix + "-p" + producerIndex + "-k" + recordIndex).getBytes(StandardCharsets.UTF_8),
                            (zonePrefix + "-p" + producerIndex + "-v" + recordIndex).getBytes(StandardCharsets.UTF_8)))
                            .get(MULTI_BROKER_PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                }
                producer.flush();
            }
            return null;
        });
    }

    protected ConcurrentZoneTrafficResult runConcurrentMultiZoneTraffic(
            String topicName,
            TopicPartition topicPartition,
            int partition,
            int producersPerZone,
            int recordsPerProducer,
            int totalRecords,
            int zoneAOwnerBrokerId,
            int zoneBOwnerBrokerId,
            String zoneAClientId,
            String zoneBClientId
    ) throws Exception {
        String zoneABootstrap = brokerBootstrap(zoneBOwnerBrokerId);
        String zoneBBootstrap = brokerBootstrap(zoneAOwnerBrokerId);
        ExecutorService executor = Executors.newFixedThreadPool(producersPerZone * 2 + 2);
        CountDownLatch consumersReady = new CountDownLatch(2);
        CountDownLatch startLatch = new CountDownLatch(1);
        List<Future<?>> producerFutures = new ArrayList<>();

        try {
            Future<Set<String>> zoneAConsumerFuture = submitZoneConsumer(
                    executor,
                    consumersReady,
                    startLatch,
                    zoneABootstrap,
                    "zone-a-concurrent-group-" + System.currentTimeMillis(),
                    zoneAClientId,
                    topicPartition,
                    totalRecords);
            Future<Set<String>> zoneBConsumerFuture = submitZoneConsumer(
                    executor,
                    consumersReady,
                    startLatch,
                    zoneBBootstrap,
                    "zone-b-concurrent-group-" + System.currentTimeMillis(),
                    zoneBClientId,
                    topicPartition,
                    totalRecords);

            for (int producerIndex = 0; producerIndex < producersPerZone; producerIndex++) {
                producerFutures.add(submitZoneProducer(
                        executor,
                        startLatch,
                        zoneABootstrap,
                        "producer-zone-a-" + producerIndex + ",zone_id=zone-a",
                        "zone-a",
                        topicName,
                        partition,
                        producerIndex,
                        recordsPerProducer));
                producerFutures.add(submitZoneProducer(
                        executor,
                        startLatch,
                        zoneBBootstrap,
                        "producer-zone-b-" + producerIndex + ",zone_id=zone-b",
                        "zone-b",
                        topicName,
                        partition,
                        producerIndex,
                        recordsPerProducer));
            }

            if (!consumersReady.await(30, TimeUnit.SECONDS)) {
                throw new AssertionError("Timed out waiting for multi-zone consumers to become ready");
            }
            startLatch.countDown();

            for (Future<?> producerFuture : producerFutures) {
                producerFuture.get(120, TimeUnit.SECONDS);
            }

            return new ConcurrentZoneTrafficResult(
                    zoneAConsumerFuture.get(120, TimeUnit.SECONDS),
                    zoneBConsumerFuture.get(120, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
        }
    }

    protected void awaitStartLatch(CountDownLatch startLatch, String actorName) throws InterruptedException {
        if (!startLatch.await(30, TimeUnit.SECONDS)) {
            throw new AssertionError("Timed out waiting to start " + actorName);
        }
    }

    protected void verifyManagedLedgerStateForZoneOwners(
            TopicIdPartition topicIdPartition,
            int zoneAOwnerBrokerId,
            int zoneBOwnerBrokerId
    ) throws Exception {
        waitForBrokerManagedLedgerState(zoneAOwnerBrokerId, topicIdPartition, true);
        waitForBrokerManagedLedgerState(zoneBOwnerBrokerId, topicIdPartition, true);
        for (int brokerId = 0; brokerId < NUM_BROKERS; brokerId++) {
            if (brokerId != zoneAOwnerBrokerId && brokerId != zoneBOwnerBrokerId) {
                waitForBrokerManagedLedgerState(brokerId, topicIdPartition, false);
            }
        }
    }
}
