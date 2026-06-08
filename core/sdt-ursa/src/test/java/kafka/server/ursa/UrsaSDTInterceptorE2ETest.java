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
package kafka.server.ursa;

import kafka.server.ursa.sdt.UrsaSDTInterceptor;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.server.config.ServerLogConfigs;

import org.apache.commons.lang3.RandomStringUtils;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.OxiaClientBuilder;
import io.streamnative.oxia.testcontainers.OxiaContainer;
import io.streamnative.ursa.compaction.OxiaCompactTaskManager;
import io.streamnative.ursa.compaction.task.CompactStreamTask;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("UrsaSDTInterceptor E2E Tests (without Ursa Storage)")
@Timeout(value = 120, unit = TimeUnit.SECONDS)
@Tag("integration")
public class UrsaSDTInterceptorE2ETest {

    private static final int NUM_RECORDS = 100;
    private static final int PRODUCE_TIMEOUT_SECONDS = 60;
    private static final String URSA_SDT_TEST_CLASS_PATH_PROPERTY = "ursa.sdt.test.class.path";

    private static KafkaClusterTestKit cluster;
    private static OxiaContainer oxiaContainer;
    private static Admin admin;
    private static AsyncOxiaClient oxiaClient;
    private static OxiaCompactTaskManager taskManager;

    @BeforeAll
    static void startCluster() throws Exception {
        var ursaSdtClassPath = System.getProperty(URSA_SDT_TEST_CLASS_PATH_PROPERTY);
        assertNotNull(ursaSdtClassPath, URSA_SDT_TEST_CLASS_PATH_PROPERTY + " is not set");
        oxiaContainer = new OxiaContainer(DockerImageName.parse("oxia/oxia:main"));
        oxiaContainer.start();
        cluster = new KafkaClusterTestKit.Builder(
                new TestKitNodes.Builder()
                        .setNumBrokerNodes(2)
                        .setNumControllerNodes(1)
                .build())
                .setConfigProp(ServerLogConfigs.INTERCEPTOR_CLASS_NAME_CONFIG,
                    "kafka.server.ursa.sdt.UrsaSDTInterceptor")
                .setConfigProp(ServerLogConfigs.URSA_STORAGE_CLASS_PATH_CONFIG, ursaSdtClassPath)
                .setConfigProp(ServerLogConfigs.URSA_OXIA_SERVICE_URL_CONFIG,
                    String.format("oxia://%s/default", oxiaContainer.getServiceAddress()))
                .setConfigProp("clusterTailCompactDataVisibilityIntervalInSeconds", "3")
                .setConfigProp("clusterSdtEnabled", "true")
                .setConfigProp("offsets.topic.replication.factor", "1")
                .setConfigProp("transaction.state.log.replication.factor", "1")
                .setConfigProp("transaction.state.log.min.isr", "1")
                .build();
        cluster.format();
        cluster.startup();
        cluster.waitForReadyBrokers();
        admin = Admin.create(cluster.clientProperties());
        oxiaClient = OxiaClientBuilder.create(oxiaContainer.getServiceAddress())
            .namespace("default")
            .asyncClient()
            .get(PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        taskManager = new OxiaCompactTaskManager(oxiaClient);
    }

    @AfterAll
    static void stopCluster() throws Exception {
        try {
            if (admin != null) {
                admin.close();
            }
            if (oxiaClient != null) {
                oxiaClient.close();
            }
            if (cluster != null) {
                cluster.close();
            }
        } finally {
            if (oxiaContainer != null) {
                oxiaContainer.stop();
            }
        }
    }

    @Test
    @DisplayName("Should publish compaction tasks only from current live leader after leader move")
    void testPublishTasksAfterLiveLeaderMove() throws Exception {
        var currentTopic = "test-topic-" + RandomStringUtils.secure().nextAlphabetic(4);
        var topicPartition = new TopicPartition(currentTopic, 0);
        createTopicWithAssignment(currentTopic, Map.of(0, List.of(0, 1)));

        waitForLeader(currentTopic, 0, 0);

        produceRecords(currentTopic, 0, 0, NUM_RECORDS);

        var topicNameInTask = currentTopic + "-partition-0";

        Awaitility.await().atMost(Duration.ofSeconds(60))
            .pollInterval(Duration.ofSeconds(2))
            .untilAsserted(() -> {
                var tasksInTopic = tasksForTopic(topicNameInTask);
                assertEquals(1, tasksInTopic.size());
                var task = tasksInTopic.get(0);
                assertEquals(0, task.getStartOffset());
                assertEquals(99, task.getEndOffset());
                assertEquals("0", task.getProperties().get(UrsaSDTInterceptor.PUBLISHER_BROKER_ID_PROPERTY));
            });

        admin.alterPartitionReassignments(Map.of(
            topicPartition,
            Optional.of(new NewPartitionReassignment(List.of(1, 0)))
        )).all().get(PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        admin.electLeaders(ElectionType.PREFERRED, Set.of(topicPartition))
            .all().get(PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        waitForLeader(currentTopic, 0, 1);

        produceRecords(currentTopic, 0, NUM_RECORDS, NUM_RECORDS);

        Awaitility.await().atMost(Duration.ofSeconds(60))
            .pollInterval(Duration.ofSeconds(2))
            .untilAsserted(() -> {
                var tasksInTopic = tasksForTopic(topicNameInTask);
                assertEquals(2, tasksInTopic.size());
                var task = tasksInTopic.get(1);
                assertEquals(100, task.getStartOffset());
                assertEquals(199, task.getEndOffset());
                assertEquals("1", task.getProperties().get(UrsaSDTInterceptor.PUBLISHER_BROKER_ID_PROPERTY));
            });

        Awaitility.await().atMost(Duration.ofSeconds(15))
            .pollDelay(Duration.ofSeconds(8))
            .untilAsserted(() -> {
                var tasksInTopic = tasksForTopic(topicNameInTask);
                assertEquals(2, tasksInTopic.size());
                assertTrue(tasksInTopic.stream()
                    .filter(task -> task.getStartOffset() >= NUM_RECORDS)
                    .noneMatch(task -> "0".equals(task.getProperties()
                        .get(UrsaSDTInterceptor.PUBLISHER_BROKER_ID_PROPERTY))));
            });
    }

    @Test
    @DisplayName("Should not fail when single-replica leader moves to another live broker")
    void testPublishTasksAfterSingleReplicaLeaderMove() throws Exception {
        var currentTopic = "test-topic-" + RandomStringUtils.secure().nextAlphabetic(4);
        var topicPartition = new TopicPartition(currentTopic, 0);
        createTopicWithAssignment(currentTopic, Map.of(0, List.of(0)));

        waitForLeader(currentTopic, 0, 0);
        waitForReplicas(currentTopic, 0, List.of(0));

        produceRecords(currentTopic, 0, 0, NUM_RECORDS);

        var topicNameInTask = currentTopic + "-partition-0";

        Awaitility.await().atMost(Duration.ofSeconds(60))
            .pollInterval(Duration.ofSeconds(2))
            .untilAsserted(() -> {
                var tasksInTopic = tasksForTopic(topicNameInTask);
                assertEquals(1, tasksInTopic.size());
                var task = tasksInTopic.get(0);
                assertEquals(0, task.getStartOffset());
                assertEquals(99, task.getEndOffset());
                assertEquals("0", task.getProperties().get(UrsaSDTInterceptor.PUBLISHER_BROKER_ID_PROPERTY));
            });

        admin.alterPartitionReassignments(Map.of(
            topicPartition,
            Optional.of(new NewPartitionReassignment(List.of(1)))
        )).all().get(PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        waitForLeader(currentTopic, 0, 1);
        waitForReplicas(currentTopic, 0, List.of(1));

        Awaitility.await().pollDelay(Duration.ofSeconds(8)).atMost(Duration.ofSeconds(15))
            .untilAsserted(() -> {
                var tasksInTopic = tasksForTopic(topicNameInTask);
                assertEquals(1, tasksInTopic.size());
            });

        produceRecords(currentTopic, 0, NUM_RECORDS, NUM_RECORDS);

        Awaitility.await().atMost(Duration.ofSeconds(60))
            .pollInterval(Duration.ofSeconds(2))
            .untilAsserted(() -> {
                var tasksInTopic = tasksForTopic(topicNameInTask);
                assertEquals(2, tasksInTopic.size());
                var task = tasksInTopic.get(1);
                assertEquals(100, task.getStartOffset());
                assertEquals(199, task.getEndOffset());
                assertEquals("1", task.getProperties().get(UrsaSDTInterceptor.PUBLISHER_BROKER_ID_PROPERTY));
            });

        Awaitility.await().pollDelay(Duration.ofSeconds(8)).atMost(Duration.ofSeconds(15))
            .untilAsserted(() -> {
                var tasksInTopic = tasksForTopic(topicNameInTask);
                assertEquals(2, tasksInTopic.size());
                assertTrue(tasksInTopic.stream()
                    .filter(task -> task.getStartOffset() >= NUM_RECORDS)
                    .noneMatch(task -> "0".equals(task.getProperties()
                        .get(UrsaSDTInterceptor.PUBLISHER_BROKER_ID_PROPERTY))));
            });
    }

    @Test
    @DisplayName("Should publish compaction tasks")
    void testPublishTasksWithMultiplePartitions() throws Exception {
        var currentTopic = "test-topic-" + RandomStringUtils.secure().nextAlphabetic(4);
        var partitions = 5;
        createTopicWithDefaults(currentTopic, partitions);
        try (Producer<byte[], byte[]> producer = createProducer()) {
            for (int i = 0; i < 10; i++) {
                for (int j = 0; j < partitions; j++) {
                    ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                        currentTopic,
                        j,
                        ("key-" + i).getBytes(StandardCharsets.UTF_8),
                        ("value-" + i).getBytes(StandardCharsets.UTF_8));
                    producer.send(record).get(PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                }
                producer.flush();
            }
        }

        Awaitility.await().atMost(Duration.ofSeconds(60))
            .pollInterval(Duration.ofSeconds(5))
            .untilAsserted(() -> {
                var tasks = taskManager.getFirstNTasksOfTopic(10).get();
                var topics = tasks.keySet();
                for (int i = 0; i < partitions; i++) {
                    var topicName = currentTopic + "-partition-" + i;
                    assertTrue(topics.contains(topicName), "Compaction task for partition " + i + " is not found");
                    var tasksInTopic = new ArrayList<CompactStreamTask>(tasks.get(topicName));
                    assertEquals(1, tasksInTopic.size(),
                        String.format("There should be only one compaction task for %s, tasks %s", topicName,
                            Arrays.toString(tasksInTopic.toArray())));
                    var task = tasksInTopic.get(0);
                    assertEquals(topicName, task.getTopic());
                    assertEquals(0, task.getStartOffset());
                    assertEquals(9, task.getEndOffset());
                    assertEquals(CompactStreamTask.Type.KAFKA, task.getType());
                }
            });

    }

    @Test
    @DisplayName("Should publish multiple compaction tasks when producing data in batches")
    void testPublishMultipleCompactionTasks() throws Exception {
        var currentTopic = "test-topic-" + RandomStringUtils.secure().nextAlphabetic(4);
        var partitions = 1;
        createTopicWithDefaults(currentTopic, partitions);

        // Produce first batch of records
        try (Producer<byte[], byte[]> producer = createProducer()) {
            for (int i = 0; i < NUM_RECORDS; i++) {
                for (int j = 0; j < partitions; j++) {
                    ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                        currentTopic,
                        j,
                        ("key-" + i).getBytes(StandardCharsets.UTF_8),
                        ("value-" + i).getBytes(StandardCharsets.UTF_8));
                    producer.send(record).get(PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                }
            }
            producer.flush();
        }

        // Wait for first batch of tasks
        Awaitility.await().atMost(Duration.ofSeconds(60))
            .pollInterval(Duration.ofSeconds(3))
            .untilAsserted(() -> {
                var tasks = taskManager.getFirstNTasksOfTopic(10).get();
                assertFalse(tasks.isEmpty());
                var topicName = currentTopic + "-partition-" + 0;
                var tasksInTopic = new ArrayList<>(tasks.get(topicName));
                assertEquals(1, tasksInTopic.size());
                var task = tasksInTopic.get(0);
                assertEquals(topicName, task.getTopic());
                assertEquals(0, task.getStartOffset());
                assertEquals(99, task.getEndOffset());
                assertEquals(CompactStreamTask.Type.KAFKA, task.getType());
            });

        // Produce second batch of records
        try (Producer<byte[], byte[]> producer = createProducer()) {
            for (int i = NUM_RECORDS; i < NUM_RECORDS * 2; i++) {
                for (int j = 0; j < partitions; j++) {
                    ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                        currentTopic,
                        j,
                        ("key-" + i).getBytes(StandardCharsets.UTF_8),
                        ("value-" + i).getBytes(StandardCharsets.UTF_8));
                    producer.send(record).get(PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                }
            }
            producer.flush();
        }

        // Wait for updated tasks with new offsets
        Awaitility.await().atMost(Duration.ofSeconds(60))
            .pollInterval(Duration.ofSeconds(3))
            .untilAsserted(() -> {
                var tasks = taskManager.getFirstNTasksOfTopic(10).get();
                var topicName = currentTopic + "-partition-" + 0;
                var tasksInTopic = new ArrayList<>(tasks.get(topicName));
                assertEquals(2, tasksInTopic.size());
                var task = tasksInTopic.get(tasksInTopic.size() - 1);
                assertEquals(topicName, task.getTopic());
                assertEquals(100, task.getStartOffset());
                assertEquals(199, task.getEndOffset());
                assertEquals(CompactStreamTask.Type.KAFKA, task.getType());
            });
    }


    private Producer<byte[], byte[]> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(props);
    }

    private void produceRecords(String topicName, int partition, int startIndex, int numRecords) throws Exception {
        try (Producer<byte[], byte[]> producer = createProducer()) {
            for (int i = startIndex; i < startIndex + numRecords; i++) {
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                    topicName,
                    partition,
                    ("key-" + i).getBytes(StandardCharsets.UTF_8),
                    ("value-" + i).getBytes(StandardCharsets.UTF_8));
                producer.send(record).get(PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            producer.flush();
        }
    }


    private void createTopicWithDefaults(String topicName, int partitions) throws Exception {
        NewTopic newTopic = new NewTopic(topicName, partitions, (short) 1);
        admin.createTopics(Collections.singleton(newTopic))
                .all().get(PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private void createTopicWithAssignment(String topicName, Map<Integer, List<Integer>> replicasAssignments)
            throws Exception {
        NewTopic newTopic = new NewTopic(topicName, replicasAssignments);
        admin.createTopics(Collections.singleton(newTopic))
                .all().get(PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private void waitForLeader(String topicName, int partition, int expectedLeader) {
        Awaitility.await().atMost(Duration.ofSeconds(60))
            .pollInterval(Duration.ofSeconds(1))
            .untilAsserted(() -> assertEquals(expectedLeader, leader(topicName, partition)));
    }

    private int leader(String topicName, int partition) throws Exception {
        var topicDescription = admin.describeTopics(Collections.singleton(topicName))
            .allTopicNames().get(PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS).get(topicName);
        Node leader = topicDescription.partitions().get(partition).leader();
        assertNotNull(leader);
        return leader.id();
    }

    private void waitForReplicas(String topicName, int partition, List<Integer> expectedReplicas) {
        Awaitility.await().atMost(Duration.ofSeconds(60))
            .pollInterval(Duration.ofSeconds(1))
            .untilAsserted(() -> assertEquals(expectedReplicas, replicas(topicName, partition)));
    }

    private List<Integer> replicas(String topicName, int partition) throws Exception {
        var topicDescription = admin.describeTopics(Collections.singleton(topicName))
            .allTopicNames().get(PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS).get(topicName);
        return topicDescription.partitions().get(partition).replicas().stream()
            .map(Node::id)
            .toList();
    }

    private List<CompactStreamTask> tasksForTopic(String topicName)
            throws Exception {
        var tasks = taskManager.getFirstNTasksOfTopic(10).get(PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        var tasksInTopic = tasks.get(topicName);
        if (tasksInTopic == null) {
            return Collections.emptyList();
        }
        return new ArrayList<>(tasksInTopic);
    }
}
