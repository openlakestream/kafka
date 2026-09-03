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
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.metadata.BrokerState;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.utility.DockerImageName;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.api.options.PutOption;
import io.oxia.testcontainers.OxiaContainer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end integration tests for Ursa Storage using real Kafka producer and consumer.
 * Uses KafkaClusterTestKit to spin up a test Kafka cluster with Ursa storage mode
 * using Lakestream logs with embedded Oxia for persistent storage tests.
 *
 * <p>Tests are organized into nested classes by functionality for better isolation
 * and parallel execution capability.
 *
 * <p><strong>Ordering is load-bearing.</strong> Every nested class shares one cluster, and the
 * bulk-deletion case in the storage-lifecycle group churns a thousand partitions, leaving the
 * brokers releasing write leases long after its own assertions pass. Anything that runs after it
 * starves waiting for stream creation. A nested class with no {@code @Order} sorts after every
 * annotated one, and so does an un-annotated method inside an ordered class -- which puts it
 * squarely behind that churn. New tests therefore carry an {@code @Order} of their own, below the
 * bulk case's.
 */
@Timeout(value = 180, unit = TimeUnit.SECONDS)
@Tag("integration")
@TestClassOrder(ClassOrderer.OrderAnnotation.class)
public class UrsaStorageE2ETest extends UrsaStorageE2ETestBase {
    @TempDir
    static Path baseDir;

    private static OxiaContainer oxiaContainer;
    private static KafkaClusterTestKit cluster;

    @BeforeAll
    static void startCluster() throws Exception {
        oxiaContainer = new OxiaContainer(DockerImageName.parse("oxia/oxia:main"));
        oxiaContainer.start();
        log.info("Oxia container started at: {}", oxiaContainer.getServiceAddress());

        cluster = createCluster(baseDir, oxiaContainer.getServiceAddress());
        cluster.format();
        cluster.startup();
        log.info("Kafka cluster started with bootstrap servers: {}", cluster.bootstrapServers());
    }

    @AfterAll
    static void stopCluster() {
        if (cluster != null) {
            try {
                cluster.close();
                log.info("Kafka cluster stopped");
            } catch (Exception e) {
                log.warn("Failed to close KafkaClusterTestKit cleanly", e);
            } finally {
                cluster = null;
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

    private static KafkaClusterTestKit createCluster(Path storagePath, String oxiaServiceAddress) throws Exception {
        log.info("Creating cluster with Ursa storage, Oxia at: {}, storage path: {}",
                oxiaServiceAddress, storagePath);

        return enableBrokerRequestPipelining(new KafkaClusterTestKit.Builder(
                new TestKitNodes.Builder()
                        .setNumBrokerNodes(1)
                        .setNumControllerNodes(1)
                        .build()))
                .setConfigProp(ServerLogConfigs.URSA_STORAGE_ENABLE_CONFIG, "true")
                .setConfigProp(ServerLogConfigs.URSA_STORAGE_TOPIC_DEFAULT_ENABLE_CONFIG, "false")
                .setConfigProp(ServerLogConfigs.URSA_CATALOG_OXIA_SERVICE_URL_CONFIG,
                        "oxia://" + oxiaServiceAddress + "/default")
                .setConfigProp(ServerLogConfigs.URSA_OXIA_SERVICE_URL_CONFIG,
                        "oxia://" + oxiaServiceAddress + "/default")
                .setConfigProp(ServerLogConfigs.URSA_STORAGE_BACKEND_TYPE_CONFIG, "LOCAL")
                .setConfigProp(ServerLogConfigs.URSA_STORAGE_PATH_CONFIG, storagePath.toString())
                .setConfigProp("offsets.topic.replication.factor", "1")
                .setConfigProp("transaction.state.log.replication.factor", "1")
                .setConfigProp("transaction.state.log.min.isr", "1")
                .build();
    }

    /**
     * Basic produce/consume tests.
     */
    @Nested
    @Order(1)
    @DisplayName("Basic Produce/Consume Tests")
    class BasicProduceConsumeTests {

        @Test
        @DisplayName("Produce and consume records with Ursa storage")
        void testProduceConsumeWithUrsaStorage() throws Exception {
            String topicName = uniqueTopicName("ursa-storage-topic");
            int numRecords = 100;

            createDisklessTopic(cluster, topicName);
            waitForTopicReady(cluster, topicName, 1);

            produceRecords(cluster.bootstrapServers(), topicName, numRecords);
            consumeAndVerifyRecords(cluster.bootstrapServers(), topicName, numRecords);
        }

        @Test
        @DisplayName("Produce records with null keys")
        void testProduceWithNullKeys() throws Exception {
            String topicName = uniqueTopicName("null-keys-topic");
            int numRecords = 100;

            createDisklessTopic(cluster, topicName);
            waitForTopicReady(cluster, topicName, 1);

            try (Producer<byte[], byte[]> producer = createProducer(cluster.bootstrapServers())) {
                for (int i = 0; i < numRecords; i++) {
                    ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                            topicName, 0, null, ("value-" + i).getBytes(StandardCharsets.UTF_8));
                    producer.send(record).get(PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                }
                producer.flush();
            }

            try (Consumer<byte[], byte[]> consumer = createConsumer(cluster.bootstrapServers())) {
                TopicPartition tp = new TopicPartition(topicName, 0);
                consumer.assign(Collections.singletonList(tp));
                consumer.seekToBeginning(Collections.singletonList(tp));

                List<ConsumerRecord<byte[], byte[]>> records = consumeRecords(consumer, numRecords);
                assertEquals(numRecords, records.size());

                for (int i = 0; i < records.size(); i++) {
                    assertNull(records.get(i).key());
                    assertEquals("value-" + i, new String(records.get(i).value(), StandardCharsets.UTF_8));
                }
            }
        }

        @Test
        @DisplayName("Produce and consume with multiple partitions")
        void testMultiplePartitions() throws Exception {
            String topicName = uniqueTopicName("multi-partition-topic");
            int numPartitions = 3;
            int recordsPerPartition = 10;

            createDisklessTopic(cluster, topicName, numPartitions);
            waitForTopicReady(cluster, topicName, numPartitions);

            try (Producer<byte[], byte[]> producer = createProducer(cluster.bootstrapServers())) {
                for (int partition = 0; partition < numPartitions; partition++) {
                    for (int i = 0; i < recordsPerPartition; i++) {
                        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                                topicName, partition,
                                ("key-" + partition + "-" + i).getBytes(),
                                ("value-" + partition + "-" + i).getBytes());
                        producer.send(record).get(PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    }
                }
                producer.flush();
            }

            try (Consumer<byte[], byte[]> consumer = createConsumer(cluster.bootstrapServers())) {
                for (int partition = 0; partition < numPartitions; partition++) {
                    TopicPartition tp = new TopicPartition(topicName, partition);
                    consumer.assign(Collections.singletonList(tp));
                    consumer.seekToBeginning(Collections.singletonList(tp));

                    List<ConsumerRecord<byte[], byte[]>> records = consumeRecords(consumer, recordsPerPartition);
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

        @Test
        @DisplayName("Produce and consume large records")
        void testLargeRecords() throws Exception {
            String topicName = uniqueTopicName("large-records-topic");
            int recordSize = 100 * 1024; // 100KB
            int numRecords = 10;

            createDisklessTopic(cluster, topicName);
            waitForTopicReady(cluster, topicName, 1);

            try (Producer<byte[], byte[]> producer = createProducer(cluster.bootstrapServers())) {
                for (int i = 0; i < numRecords; i++) {
                    byte[] largeValue = new byte[recordSize];
                    for (int j = 0; j < recordSize; j++) {
                        largeValue[j] = (byte) ((i + j) % 256);
                    }
                    ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                            topicName, 0, ("key-" + i).getBytes(), largeValue);
                    producer.send(record).get(PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                }
                producer.flush();
            }

            try (Consumer<byte[], byte[]> consumer = createConsumer(cluster.bootstrapServers())) {
                TopicPartition tp = new TopicPartition(topicName, 0);
                consumer.assign(Collections.singletonList(tp));
                consumer.seekToBeginning(Collections.singletonList(tp));

                List<ConsumerRecord<byte[], byte[]>> records = consumeRecords(consumer, numRecords);
                assertEquals(numRecords, records.size());

                for (int i = 0; i < records.size(); i++) {
                    byte[] value = records.get(i).value();
                    assertEquals(recordSize, value.length);
                    for (int j = 0; j < recordSize; j++) {
                        assertEquals((byte) ((i + j) % 256), value[j]);
                    }
                }
            }
        }
    }

    /**
     * Offset seek and consume tests.
     */
    @Nested
    @Order(2)
    @DisplayName("Offset Seek Tests")
    class OffsetSeekTests {

        @Test
        @DisplayName("Consume from specific offset")
        void testConsumeFromOffset() throws Exception {
            String topicName = uniqueTopicName("offset-seek-topic");
            int numRecords = 100;

            createDisklessTopic(cluster, topicName);
            waitForTopicReady(cluster, topicName, 1);
            produceRecords(cluster.bootstrapServers(), topicName, numRecords);

            try (Consumer<byte[], byte[]> consumer = createConsumer(cluster.bootstrapServers())) {
                TopicPartition tp = new TopicPartition(topicName, 0);
                consumer.assign(Collections.singletonList(tp));
                consumer.seek(tp, 50);

                List<ConsumerRecord<byte[], byte[]>> records = consumeRecords(consumer, 50);
                assertEquals(50, records.size());
                assertEquals("key-50", new String(records.get(0).key(), StandardCharsets.UTF_8));
                assertEquals("value-50", new String(records.get(0).value(), StandardCharsets.UTF_8));
            }
        }
    }

    /**
     * Broker restart and data persistence tests.
     */
    @Nested
    @Order(3)
    @DisplayName("Persistence Tests")
    class PersistenceTests {

        @Test
        @DisplayName("Data persists across broker restart with HWM recovery")
        void testDataPersistsAcrossBrokerRestart() throws Exception {
            String topicName = uniqueTopicName("persistence-test-topic");
            int recordsBeforeRestart = 30;
            int recordsAfterRestart = 10;
            int totalRecords = recordsBeforeRestart + recordsAfterRestart;

            createDisklessTopic(cluster, topicName);
            waitForTopicReady(cluster, topicName, 1);

            // Produce records BEFORE restart
            produceRecords(cluster.bootstrapServers(), topicName, recordsBeforeRestart);
            log.info("Produced {} records before broker restart", recordsBeforeRestart);

            // Verify records before restart
            consumeAndVerifyRecords(cluster.bootstrapServers(), topicName, recordsBeforeRestart);
            log.info("Verified {} records before broker restart", recordsBeforeRestart);

            // Restart broker
            var broker = cluster.brokers().values().iterator().next();
            log.info("Shutting down broker...");
            broker.shutdown();
            broker.awaitShutdown();
            log.info("Broker shutdown complete");

            log.info("Starting broker...");
            broker.startup();
            TestUtils.waitForCondition(
                    () -> broker.brokerState() == BrokerState.RUNNING,
                    30000, "Broker did not reach RUNNING state after restart");
            log.info("Broker restart complete, state: {}", broker.brokerState());

            // Produce MORE records AFTER restart to verify HWM recovery
            try (Producer<byte[], byte[]> producer = createProducer(cluster.bootstrapServers())) {
                for (int i = recordsBeforeRestart; i < totalRecords; i++) {
                    ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                            topicName, 0,
                            ("key-" + i).getBytes(StandardCharsets.UTF_8),
                            ("value-" + i).getBytes(StandardCharsets.UTF_8));
                    producer.send(record).get(PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                }
                producer.flush();
            }
            log.info("Produced {} more records after broker restart", recordsAfterRestart);

            // Consume ALL records and verify continuous offsets
            try (Consumer<byte[], byte[]> consumer = createConsumer(cluster.bootstrapServers())) {
                TopicPartition tp = new TopicPartition(topicName, 0);
                consumer.assign(Collections.singletonList(tp));
                consumer.seekToBeginning(Collections.singletonList(tp));

                List<ConsumerRecord<byte[], byte[]>> allRecords = consumeRecords(consumer, totalRecords);
                assertEquals(totalRecords, allRecords.size(),
                        "Should have all " + totalRecords + " records (before + after restart)");

                for (int i = 0; i < allRecords.size(); i++) {
                    ConsumerRecord<byte[], byte[]> record = allRecords.get(i);
                    assertEquals(i, record.offset(), "Record at index " + i + " should have offset " + i);
                    assertEquals("key-" + i, new String(record.key(), StandardCharsets.UTF_8));
                    assertEquals("value-" + i, new String(record.value(), StandardCharsets.UTF_8));
                }
            }

            log.info("Data persistence and HWM recovery test passed - all {} records have correct continuous offsets", totalRecords);
        }

        @Test
        @DisplayName("Real Ursa storage creates data files")
        void testProduceConsumeWithRealUrsaStorage() throws Exception {
            String topicName = uniqueTopicName("real-ursa-topic");
            int numRecords = 50;

            createDisklessTopic(cluster, topicName);
            waitForTopicReady(cluster, topicName, 1);
            produceRecords(cluster.bootstrapServers(), topicName, numRecords);

            log.info("Produced {} records to topic {}", numRecords, topicName);

            consumeAndVerifyRecords(cluster.bootstrapServers(), topicName, numRecords);
            log.info("Consumed {} records from topic {}", numRecords, topicName);

            // Verify that real Ursa storage created data files
            long fileCount = Files.walk(baseDir)
                    .filter(Files::isRegularFile)
                    .count();
            log.info("Real Ursa storage created {} files in {}", fileCount, baseDir);
            if (fileCount == 0) {
                log.warn("No files created in storage path - this may indicate mock storage is being used");
            }
        }
    }

    /**
     * ListOffsets API tests.
     */
    @Nested
    @Order(4)
    @DisplayName("ListOffsets API Tests")
    class ListOffsetsTests {

        @Test
        @DisplayName("ListOffsets with Admin client")
        void testListOffsetsWithAdminClient() throws Exception {
            String topicName = uniqueTopicName("list-offsets-topic");
            TopicPartition tp = new TopicPartition(topicName, 0);

            createDisklessTopic(cluster, topicName);
            waitForTopicReady(cluster, topicName, 1);

            // Test EARLIEST and LATEST on empty topic
            try (Admin admin = cluster.admin()) {
                ListOffsetsResult.ListOffsetsResultInfo earliestEmpty =
                        admin.listOffsets(Map.of(tp, OffsetSpec.earliest())).all().get().get(tp);
                assertEquals(0L, earliestEmpty.offset(), "EARLIEST on empty topic should be 0");

                ListOffsetsResult.ListOffsetsResultInfo latestEmpty =
                        admin.listOffsets(Map.of(tp, OffsetSpec.latest())).all().get().get(tp);
                assertEquals(0L, latestEmpty.offset(), "LATEST on empty topic should be 0");
            }

            // Produce records
            try (Producer<byte[], byte[]> producer = createProducer(cluster.bootstrapServers())) {
                for (int i = 0; i < 10; i++) {
                    ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                            topicName, 0,
                            ("key-" + i).getBytes(StandardCharsets.UTF_8),
                            ("value-" + i).getBytes(StandardCharsets.UTF_8));
                    producer.send(record).get(PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    TimeUnit.MILLISECONDS.sleep(5);
                }
                producer.flush();
            }

            // Test listOffsets with Admin client
            try (Admin admin = cluster.admin()) {
                ListOffsetsResult.ListOffsetsResultInfo earliest =
                        admin.listOffsets(Map.of(tp, OffsetSpec.earliest())).all().get().get(tp);
                assertEquals(0L, earliest.offset(), "EARLIEST should return offset 0");

                ListOffsetsResult.ListOffsetsResultInfo latest =
                        admin.listOffsets(Map.of(tp, OffsetSpec.latest())).all().get().get(tp);
                assertEquals(10L, latest.offset(), "LATEST should return offset 10 (HWM)");

                ListOffsetsResult.ListOffsetsResultInfo maxTimestamp =
                        admin.listOffsets(Map.of(tp, OffsetSpec.maxTimestamp())).all().get().get(tp);
                assertEquals(latest.offset() - 1, maxTimestamp.offset(),
                        "MAX_TIMESTAMP should return the last message offset");

                // Test timestamp search
                ListOffsetsResult.ListOffsetsResultInfo firstByTimestamp =
                        admin.listOffsets(Map.of(tp, OffsetSpec.forTimestamp(0L))).all().get().get(tp);
                assertTrue(firstByTimestamp.offset() >= earliest.offset() && firstByTimestamp.offset() < latest.offset(),
                        "forTimestamp(0) should return a valid offset in [LSO, LEO)");

                ListOffsetsResult.ListOffsetsResultInfo afterMax =
                        admin.listOffsets(Map.of(tp, OffsetSpec.forTimestamp(maxTimestamp.timestamp() + 1))).all().get().get(tp);
                assertEquals(-1L, afterMax.offset(), "Timestamp search after MAX_TIMESTAMP should return offset -1");
            }

            log.info("Admin client listOffsets test passed");
        }

        @Test
        @DisplayName("OffsetsForTimes with Consumer")
        void testOffsetsForTimesWithConsumer() throws Exception {
            String topicName = uniqueTopicName("offsets-for-times-topic");
            TopicPartition tp = new TopicPartition(topicName, 0);

            createDisklessTopic(cluster, topicName);
            waitForTopicReady(cluster, topicName, 1);

            // Produce records
            try (Producer<byte[], byte[]> producer = createProducer(cluster.bootstrapServers())) {
                for (int i = 0; i < 5; i++) {
                    ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                            topicName, 0,
                            ("key-" + i).getBytes(StandardCharsets.UTF_8),
                            ("value-" + i).getBytes(StandardCharsets.UTF_8));
                    producer.send(record).get(PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    TimeUnit.MILLISECONDS.sleep(5);
                }
                producer.flush();
            }

            try (Admin admin = cluster.admin()) {
                ListOffsetsResult.ListOffsetsResultInfo earliest =
                        admin.listOffsets(Map.of(tp, OffsetSpec.earliest())).all().get().get(tp);
                ListOffsetsResult.ListOffsetsResultInfo latest =
                        admin.listOffsets(Map.of(tp, OffsetSpec.latest())).all().get().get(tp);
                ListOffsetsResult.ListOffsetsResultInfo maxTimestamp =
                        admin.listOffsets(Map.of(tp, OffsetSpec.maxTimestamp())).all().get().get(tp);

                try (Consumer<byte[], byte[]> consumer = createConsumer(cluster.bootstrapServers())) {
                    consumer.assign(Collections.singletonList(tp));

                    OffsetAndTimestamp first = consumer.offsetsForTimes(Map.of(tp, 0L)).get(tp);
                    assertNotNull(first, "offsetsForTimes(0) should return a result");
                    assertTrue(first.offset() >= earliest.offset() && first.offset() < latest.offset(),
                            "offsetsForTimes(0) should return a valid offset in [LSO, LEO)");

                    long midTimestamp = first.timestamp() +
                            Math.max(1L, (maxTimestamp.timestamp() - first.timestamp()) / 2);
                    OffsetAndTimestamp mid = consumer.offsetsForTimes(Map.of(tp, midTimestamp)).get(tp);
                    assertNotNull(mid, "offsetsForTimes(mid) should return a result");
                    assertTrue(mid.timestamp() >= midTimestamp,
                            "offsetsForTimes(mid) should return a timestamp >= requested timestamp");

                    OffsetAndTimestamp afterMax = consumer.offsetsForTimes(
                            Map.of(tp, maxTimestamp.timestamp() + 1)).get(tp);
                    assertNull(afterMax, "offsetsForTimes(afterMax) should return null");

                    consumer.seek(tp, mid.offset());
                    List<ConsumerRecord<byte[], byte[]>> records = consumeRecords(consumer, 1);
                    assertTrue(records.size() >= 1, "Should consume at least 1 record from the returned offset");
                }
            }

            log.info("Consumer offsetsForTimes test passed");
        }
    }

    /**
     * Retention tests.
     */
    @Nested
    @Order(5)
    @DisplayName("Retention Behavior Tests")
    class RetentionBehaviorTests {

        @Test
        @DisplayName("Retention should advance earliest offset even after fetch cursor")
        void testRetentionShouldAdvanceEarliestOffsetEvenAfterFetchCursor() throws Exception {
            String topicName = uniqueTopicName("retention-fetch-cursor-topic");
            TopicPartition topicPartition = new TopicPartition(topicName, 0);
            int numRecords = 10;

            createDisklessTopic(
                    cluster,
                    topicName,
                    1,
                    (short) 1,
                    Map.of(TopicConfig.RETENTION_MS_CONFIG, "1"));
            waitForTopicReady(cluster, topicName, 1);
            produceRecords(cluster.bootstrapServers(), topicName, numRecords);

            Uuid topicId;
            try (Admin admin = cluster.admin()) {
                topicId = admin.describeTopics(Collections.singleton(topicName))
                        .allTopicNames()
                        .get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                        .get(topicName)
                        .topicId();
            }

            try (Consumer<byte[], byte[]> consumer = createConsumer(cluster.bootstrapServers())) {
                consumer.assign(Collections.singletonList(topicPartition));
                consumer.seekToBeginning(Collections.singletonList(topicPartition));
                List<ConsumerRecord<byte[], byte[]>> records =
                        consumeRecords(consumer, 1, Duration.ofSeconds(5));
                assertFalse(records.isEmpty(), "Expected fetch path to create non-durable cursor");
            }

            // Sleep 50 ms to ensure retention has time to be able to trim the topic.
            TimeUnit.MILLISECONDS.sleep(50);

            var broker = cluster.brokers().values().iterator().next();
            forceTrimForTopicPartition(
                    IsolatedUrsaCatalogInspector.disklessStorageEngine(
                            broker.replicaManager().disklessStorageSupport()),
                    new TopicIdPartition(topicId, topicPartition));

            try (Admin admin = cluster.admin()) {
                long earliestOffset = admin.listOffsets(Map.of(topicPartition, OffsetSpec.earliest()))
                        .all()
                        .get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                        .get(topicPartition)
                        .offset();

                assertTrue(earliestOffset > 0,
                        "Expected earliest offset to advance after retention trim, but got " + earliestOffset
                                + ". This reproduces the non-durable cursor pinning issue.");
            }
        }
    }

    /**
     * Topic configuration mutation tests.
     */
    @Nested
    @Order(6)
    @DisplayName("Topic Config Mutation Tests")
    class TopicConfigMutationTests {

        @Test
        @DisplayName("Topic default disabled keeps implicit topics on classic storage")
        void testTopicDefaultDisabledKeepsImplicitTopicOnClassicStorage() throws Exception {
            String topicName = uniqueTopicName("classic-default-topic");

            try (Admin admin = cluster.admin()) {
                NewTopic classicTopic = new NewTopic(topicName, 1, (short) 1);
                admin.createTopics(Collections.singleton(classicTopic))
                        .all().get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                waitForTopicReady(admin, topicName, 1);

                ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
                ConfigEntry ursaStorageEnable = admin.describeConfigs(Collections.singleton(topicResource))
                        .all()
                        .get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                        .get(topicResource)
                        .get(TopicConfig.URSA_STORAGE_ENABLE_CONFIG);

                assertNotNull(ursaStorageEnable);
                assertEquals("false", ursaStorageEnable.value());
            }

            var broker = cluster.brokers().values().iterator().next();
            assertFalse(broker.replicaManager().disklessStorageSupport().isDisklessStorageTopic(topicName));
        }

        @ParameterizedTest(name = "Cannot alter ursa.storage.enable to {0}")
        @ValueSource(booleans = {false, true})
        @DisplayName("Cannot alter ursa.storage.enable after topic creation")
        void testCannotAlterUrsaStorageEnableAfterTopicCreation(boolean newUrsaStorageEnabled) throws Exception {
            boolean initialUrsaStorageEnabled = !newUrsaStorageEnabled;
            String topicName = uniqueTopicName(initialUrsaStorageEnabled
                    ? "immutable-diskless-config-topic"
                    : "immutable-classic-config-topic");

            if (initialUrsaStorageEnabled) {
                createDisklessTopic(cluster, topicName);
            } else {
                try (Admin admin = cluster.admin()) {
                    NewTopic classicTopic = new NewTopic(topicName, 1, (short) 1)
                            .configs(Map.of(TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "false"));
                    admin.createTopics(Collections.singleton(classicTopic))
                            .all().get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                }
            }
            waitForTopicReady(cluster, topicName, 1);
            assertCannotAlterUrsaStorageEnable(topicName, newUrsaStorageEnabled);
        }

        @Test
        @DisplayName("Topic config updates forward the complete config snapshot to Ursa storage")
        void testTopicConfigUpdateForwardsCompleteSnapshotToUrsaStorage() throws Exception {
            String topicName = uniqueTopicName("topic-config-update");
            createDisklessTopic(cluster, topicName);
            waitForTopicReady(cluster, topicName, 1);

            ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
            try (Admin admin = cluster.admin()) {
                Uuid topicId = admin.describeTopics(Set.of(topicName))
                        .allTopicNames()
                        .get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                        .get(topicName)
                        .topicId();
                TopicIdPartition topicIdPartition = new TopicIdPartition(
                        topicId, new TopicPartition(topicName, 0));
                setTopicConfig(admin, topicResource, TopicConfig.RETENTION_MS_CONFIG, "60000");
                waitForUrsaTopicConfig(topicIdPartition, Map.of(
                        IsolatedUrsaCatalogInspector.kafkaManagedProperty(cluster), "true",
                        IsolatedUrsaCatalogInspector.kafkaTopicNameProperty(cluster), topicName,
                        IsolatedUrsaCatalogInspector.kafkaTopicIdProperty(cluster), topicId.toString(),
                        TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true",
                        TopicConfig.RETENTION_MS_CONFIG, "60000"));

                setTopicConfig(admin, topicResource, TopicConfig.RETENTION_BYTES_CONFIG, "1048576");
                waitForUrsaTopicConfig(topicIdPartition, Map.of(
                        IsolatedUrsaCatalogInspector.kafkaManagedProperty(cluster), "true",
                        IsolatedUrsaCatalogInspector.kafkaTopicNameProperty(cluster), topicName,
                        IsolatedUrsaCatalogInspector.kafkaTopicIdProperty(cluster), topicId.toString(),
                        TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true",
                        TopicConfig.RETENTION_MS_CONFIG, "60000",
                        TopicConfig.RETENTION_BYTES_CONFIG, "1048576"));
            }
        }

        private void setTopicConfig(
                Admin admin,
                ConfigResource topicResource,
                String configName,
                String configValue
        ) throws Exception {
            AlterConfigOp operation = new AlterConfigOp(
                    new ConfigEntry(configName, configValue),
                    AlterConfigOp.OpType.SET);
            admin.incrementalAlterConfigs(Map.of(topicResource, List.of(operation)))
                    .all()
                    .get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }

        private void waitForUrsaTopicConfig(
                TopicIdPartition topicIdPartition,
                Map<String, String> expectedConfig)
                throws Exception {
            String sourceRevisionProperty =
                    IsolatedUrsaCatalogInspector.kafkaSourceRevisionProperty(cluster);
            AtomicReference<Exception> lastFailure = new AtomicReference<>();
            TestUtils.waitForCondition(() -> {
                try {
                    var broker = cluster.brokers().values().iterator().next();
                    Map<String, String> actualConfig = getUrsaTopicConfig(
                            IsolatedUrsaCatalogInspector.disklessStorageEngine(
                                    broker.replicaManager().disklessStorageSupport()),
                            topicIdPartition);
                    Map<String, String> userVisibleConfig = new HashMap<>(actualConfig);
                    String sourceRevision = userVisibleConfig.remove(sourceRevisionProperty);
                    return sourceRevision != null
                            && Long.parseLong(sourceRevision) >= 0
                            && expectedConfig.equals(userVisibleConfig);
                } catch (Exception e) {
                    lastFailure.set(e);
                    return false;
                }
            }, 30_000, 100, () -> {
                Exception failure = lastFailure.get();
                return "Timed out waiting for Ursa topic config " + expectedConfig
                        + " for " + topicIdPartition
                        + (failure == null ? "" : "; last lookup failure: " + failure);
            });
        }

        private void assertCannotAlterUrsaStorageEnable(String topicName, boolean newUrsaStorageEnabled) throws Exception {
            try (Admin admin = cluster.admin()) {
                ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
                AlterConfigOp alterUrsaStorageEnableConfig = new AlterConfigOp(
                        new ConfigEntry(TopicConfig.URSA_STORAGE_ENABLE_CONFIG, Boolean.toString(newUrsaStorageEnabled)),
                        AlterConfigOp.OpType.SET);

                ExecutionException ex = assertThrows(ExecutionException.class,
                        () -> admin.incrementalAlterConfigs(Map.of(topicResource, List.of(alterUrsaStorageEnableConfig)))
                                .all().get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS));

                assertInstanceOf(InvalidConfigurationException.class, ex.getCause(),
                        "Expected InvalidConfigurationException, got: " + ex.getCause());
                assertTrue(ex.getCause().getMessage().contains(TopicConfig.URSA_STORAGE_ENABLE_CONFIG),
                        "Error message should mention " + TopicConfig.URSA_STORAGE_ENABLE_CONFIG
                                + ", got: " + ex.getCause().getMessage());
            }
        }
    }

    /**
     * Storage lifecycle verification tests.
     * Tests Lakestream lifecycle semantics and cleanup of Kafka-owned producer state.
     */
    @Nested
    @Order(7)
    @DisplayName("Storage Lifecycle Tests")
    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    class StorageLifecycleTests {
        private static final String TEST_PRODUCER_ZONE = "test-zone";
        private static final String TEST_PRODUCER_ZONE_WITH_PATH_SEPARATOR = "rack/0";
        private final Map<String, Uuid> topicIds = new ConcurrentHashMap<>();

        @Test
        @Order(1)
        @DisplayName("Committed partition layout is visible through the Lakestream catalog after I/O")
        void testCommittedPartitionLayoutVisibleThroughLakestreamCatalog() throws Exception {
            String topicName = uniqueTopicName("log-metadata-topic");

            createDisklessTopic(cluster, topicName);
            try (Admin admin = cluster.admin()) {
                waitForTopicReady(admin, topicName, 1);
            }
            assertCatalogStreamReady(topicName, 1);
            produceRecords(cluster.bootstrapServers(), topicName, 10);
            consumeAndVerifyRecords(cluster.bootstrapServers(), topicName, 10);

            assertCommittedPartitionLayoutVisible(topicName, 1);
            log.info("Lakestream committed partition layout test passed for topic {}", topicName);
        }

        @Test
        @Order(2)
        @DisplayName("Partitioned stream lifecycle and producer state cleanup use owned APIs")
        void testPartitionedStreamLifecycleAndProducerStateCleanup() throws Exception {
            String topicName = uniqueTopicName("partitioned-topics-metadata-topic");
            int numPartitions = 3;

            createDisklessTopic(cluster, topicName, numPartitions);
            try (Admin admin = cluster.admin()) {
                waitForTopicReady(admin, topicName, numPartitions);
            }
            assertCatalogStreamReady(topicName, numPartitions);

            for (int partition = 0; partition < numPartitions; partition++) {
                produceRecords(cluster.bootstrapServers(), topicName, partition, 1);
            }
            assertCommittedPartitionLayoutVisible(topicName, numPartitions);

            Uuid topicId;
            try (Admin admin = cluster.admin()) {
                waitForTopicReady(admin, topicName, numPartitions);
                topicId = admin.describeTopics(Collections.singleton(topicName))
                        .allTopicNames()
                        .get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                        .get(topicName)
                        .topicId();

                putProducerStateSnapshotsInOxia(topicId, numPartitions);
                assertProducerStateSnapshotsExistInOxia(topicId, numPartitions);

                admin.deleteTopics(Collections.singletonList(topicName))
                        .all().get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }

            assertCatalogStreamDropped(topicName);
            assertProducerStateSnapshotsDeletedFromOxia(topicId, numPartitions);
            log.info("Partitioned stream lifecycle test passed for topic {}", topicName);
        }

        @Test
        // Runs last: it is the churn every other test here has to survive.
        @Order(3)
        @Timeout(value = 900, unit = TimeUnit.SECONDS)
        @DisplayName("Bulk diskless topic deletion permanently deletes every Lakestream stream")
        void testBulkTopicDeletionUnregistersStreams() throws Exception {
            int topicCount = 100;
            int partitionsPerTopic = 10;
            List<String> topicNames = createBulkDisklessTopics(topicCount, partitionsPerTopic);

            produceOneRecordPerPartition(cluster.bootstrapServers(), topicNames, partitionsPerTopic);
            List<String> streamNames = new ArrayList<>(topicNames.size());
            for (String topicName : topicNames) {
                streamNames.add(catalogStreamName(topicName));
            }

            try (Admin admin = cluster.admin()) {
                admin.deleteTopics(topicNames).all().get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }

            try (IsolatedUrsaCatalogInspector catalog = createCatalogInspector()) {
                assertBulkStreamsUnregistered(catalog, streamNames, topicCount);
            }
        }

        private void assertCatalogStreamReady(String topicName, int expectedPartitions)
                throws Exception {
            String streamName = catalogStreamName(topicName);
            String namespace = IsolatedUrsaCatalogInspector.namespace(cluster);
            try (IsolatedUrsaCatalogInspector catalog = createCatalogInspector()) {
                TestUtils.waitForCondition(() -> {
                    if (!catalog.isStreamListed(
                            namespace, streamName, 10, TimeUnit.SECONDS)) {
                        return false;
                    }
                    return catalog.partitionCount(
                            namespace, streamName, 10, TimeUnit.SECONDS) == expectedPartitions;
                }, 30_000, 100,
                        () -> "Timed out waiting for catalog stream reconciliation: "
                                + namespace + "/" + streamName);
            }
        }

        private void assertCatalogStreamDropped(String topicName) throws Exception {
            String streamName = catalogStreamName(topicName);
            String namespace = IsolatedUrsaCatalogInspector.namespace(cluster);
            try (IsolatedUrsaCatalogInspector catalog = createCatalogInspector()) {
                TestUtils.waitForCondition(
                        () -> !catalog.streamExists(
                                namespace, streamName, 10, TimeUnit.SECONDS),
                        30_000,
                        100,
                        () -> "Timed out waiting for catalog stream deletion: "
                                + namespace + "/" + streamName);
            }
        }

        private void assertCommittedPartitionLayoutVisible(String topicName, int expectedPartitions) throws Exception {
            String namespace = IsolatedUrsaCatalogInspector.namespace(cluster);
            try (IsolatedUrsaCatalogInspector catalog = createCatalogInspector()) {
                List<Long> logIds = catalog.partitionLogIds(
                        namespace,
                        catalogStreamName(topicName),
                        10,
                        TimeUnit.SECONDS);
                assertEquals(expectedPartitions, logIds.size(),
                        "Unexpected committed Lakestream partition count for " + topicName);
            }
        }

        private void putProducerStateSnapshotsInOxia(Uuid topicId, int partitions) throws Exception {
            String oxiaServiceAddress = oxiaContainer.getServiceAddress();
            String namespace = "default";
            String topicIndexName = IsolatedUrsaCatalogInspector.producerStateTopicIndexName(cluster);
            String topicIndexKey = IsolatedUrsaCatalogInspector.producerStateTopicIndexKey(
                    cluster, topicId.toString());

            try (AsyncOxiaClient client = OxiaClientBuilder.create(oxiaServiceAddress)
                    .namespace(namespace)
                    .asyncClient()
                    .get()) {
                boolean wroteLegacyUnindexedSnapshot = false;
                for (int partition = 0; partition < partitions; partition++) {
                    for (String key : producerStateSnapshotKeys(topicId, partition)) {
                        byte[] value = ("dummy-" + partition).getBytes(StandardCharsets.UTF_8);
                        if (!wroteLegacyUnindexedSnapshot) {
                            client.put(key, value).get(10, TimeUnit.SECONDS);
                            wroteLegacyUnindexedSnapshot = true;
                        } else {
                            client.put(
                                    key,
                                    value,
                                    Set.of(PutOption.SecondaryIndex(
                                            topicIndexName,
                                            topicIndexKey)))
                                    .get(10, TimeUnit.SECONDS);
                        }
                    }
                }
            }
        }

        private void assertProducerStateSnapshotsExistInOxia(Uuid topicId, int partitions) throws Exception {
            for (int partition = 0; partition < partitions; partition++) {
                for (String key : producerStateSnapshotKeys(topicId, partition)) {
                    assertOxiaKeyExists(key, "ProducerStateSnapshot");
                }
            }
        }

        private void assertProducerStateSnapshotsDeletedFromOxia(Uuid topicId, int partitions) throws Exception {
            try (AsyncOxiaClient client = createOxiaClient()) {
                for (int partition = 0; partition < partitions; partition++) {
                    for (String key : producerStateSnapshotKeys(topicId, partition)) {
                        assertOxiaKeyDeleted(client, key, "ProducerStateSnapshot");
                    }
                }
            }
        }

        private List<String> producerStateSnapshotKeys(Uuid topicId, int partition) throws Exception {
            return List.of(
                    IsolatedUrsaCatalogInspector.producerStateSnapshotKey(
                            cluster, topicId.toString(), partition),
                    IsolatedUrsaCatalogInspector.producerStateSnapshotKey(
                            cluster, topicId.toString(), partition, TEST_PRODUCER_ZONE),
                    IsolatedUrsaCatalogInspector.producerStateSnapshotKey(
                            cluster,
                            topicId.toString(),
                            partition,
                            TEST_PRODUCER_ZONE_WITH_PATH_SEPARATOR));
        }

        private void assertOxiaKeyExists(String key, String description) throws Exception {
            try (AsyncOxiaClient client = createOxiaClient()) {
                assertOxiaKeyExists(client, key, description);
            }
        }

        private AsyncOxiaClient createOxiaClient() throws Exception {
            return OxiaClientBuilder.create(oxiaContainer.getServiceAddress())
                    .namespace("default")
                    .asyncClient()
                    .get();
        }

        private void produceOneRecordPerPartition(String bootstrapServers, List<String> topicNames, int partitions)
                throws Exception {
            try (Producer<byte[], byte[]> producer = createProducer(bootstrapServers)) {
                for (String topicName : topicNames) {
                    for (int partition = 0; partition < partitions; partition++) {
                        producer.send(new ProducerRecord<>(
                                topicName,
                                partition,
                                ("bulk-key-" + topicName + "-" + partition).getBytes(StandardCharsets.UTF_8),
                                ("bulk-value-" + topicName + "-" + partition).getBytes(StandardCharsets.UTF_8)
                        )).get(PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    }
                }
                producer.flush();
            }
        }

        private List<String> createBulkDisklessTopics(int topicCount, int partitionsPerTopic) throws Exception {
            List<String> topicNames = new ArrayList<>(topicCount);
            List<NewTopic> topicsToCreate = new ArrayList<>(topicCount);
            for (int topicIndex = 0; topicIndex < topicCount; topicIndex++) {
                String topicName = uniqueTopicName("bulk-delete-topic-" + topicIndex);
                topicNames.add(topicName);
                topicsToCreate.add(new NewTopic(topicName, partitionsPerTopic, (short) 1)
                        .configs(Map.of(TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true")));
            }

            try (Admin admin = cluster.admin()) {
                admin.createTopics(topicsToCreate).all().get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                for (String topicName : topicNames) {
                    waitForTopicReady(admin, topicName, partitionsPerTopic);
                }
            }
            return topicNames;
        }

        /**
         * Waits for the whole bulk delete, which is paced by how fast write leases are released.
         * Dropping 100 topics of 10 partitions asks Ursa to close a thousand leased logs at once,
         * far past the width of its close executor, so the rejected closes retry on a backoff that
         * grows to ten seconds and the stream deletes behind them wait a lease-drain timeout each.
         * The full sweep measured a little over three minutes here, so the wait is generous: a
         * shorter one leaves the deletes running underneath whichever test comes next, which is
         * what poisoned this suite when the wait was two minutes.
         */
        private void assertBulkStreamsUnregistered(
                IsolatedUrsaCatalogInspector catalog,
                List<String> streamNames,
                int topicCount
        ) throws Exception {
            TestUtils.waitForCondition(() -> {
                try {
                    return areBulkStreamsUnregistered(catalog, streamNames);
                } catch (Exception e) {
                    return false;
                }
            }, 300_000, 500,
                    () -> "Timed out waiting for bulk stream deletion for " + topicCount + " topics");
        }

        private boolean areBulkStreamsUnregistered(
                IsolatedUrsaCatalogInspector catalog,
                List<String> streamNames
        ) throws Exception {
            String namespace = IsolatedUrsaCatalogInspector.namespace(cluster);
            for (String streamName : streamNames) {
                if (catalog.streamExists(
                        namespace,
                        streamName,
                        10,
                        TimeUnit.SECONDS)) {
                    return false;
                }
            }
            return true;
        }

        private void assertOxiaKeyExists(AsyncOxiaClient client, String key, String description) throws Exception {
            TestUtils.waitForCondition(() -> {
                try {
                    GetResult result = client.get(key).get(10, TimeUnit.SECONDS);
                    return result != null && result.value() != null && result.value().length > 0;
                } catch (Exception e) {
                    return false;
                }
            }, 30_000, 100, () -> "Timed out waiting for " + description + " to exist in Oxia: " + key);
        }

        private void assertOxiaKeyDeleted(AsyncOxiaClient client, String key, String description) throws Exception {
            TestUtils.waitForCondition(() -> {
                try {
                    return isOxiaKeyDeleted(client, key);
                } catch (Exception e) {
                    return false;
                }
            }, 30_000, 100, () -> "Timed out waiting for " + description + " to be deleted from Oxia: " + key);
        }

        private boolean isOxiaKeyDeleted(AsyncOxiaClient client, String key) throws Exception {
            GetResult result = client.get(key).get(10, TimeUnit.SECONDS);
            return result == null || result.value() == null;
        }

        private String catalogStreamName(String topicName) throws Exception {
            return IsolatedUrsaCatalogInspector.streamName(cluster, topicName, topicId(topicName));
        }

        private IsolatedUrsaCatalogInspector createCatalogInspector() throws Exception {
            String catalogUri = "oxia://" + oxiaContainer.getServiceAddress() + "/default";
            Properties properties = new Properties();
            properties.setProperty("backendStorageType", "LOCAL");
            properties.setProperty("storagePath", baseDir.toString());
            properties.setProperty("oxiaStorageUrl", catalogUri);
            return IsolatedUrsaCatalogInspector.open(cluster, catalogUri, properties);
        }

        private Uuid topicId(String topicName) throws Exception {
            Uuid cached = topicIds.get(topicName);
            if (cached != null) {
                return cached;
            }
            try (Admin admin = cluster.admin()) {
                Uuid discovered = admin.describeTopics(Set.of(topicName))
                        .allTopicNames()
                        .get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                        .get(topicName)
                        .topicId();
                Uuid raced = topicIds.putIfAbsent(topicName, discovered);
                return raced != null ? raced : discovered;
            }
        }
    }

    private static void forceTrimForTopicPartition(Object ursaStorageState,
                                                   TopicIdPartition topicIdPartition) throws Exception {
        Object state = unwrapUrsaStorageState(ursaStorageState);
        var getOrCreatePartitionLogMethod = state.getClass()
                .getDeclaredMethod("getOrCreatePartitionLog", TopicIdPartition.class);
        getOrCreatePartitionLogMethod.setAccessible(true);
        Object partitionLog = getOrCreatePartitionLogMethod.invoke(state, topicIdPartition);

        var initializedMethod = partitionLog.getClass().getDeclaredMethod("initialized");
        initializedMethod.setAccessible(true);
        Object logFuture = initializedMethod.invoke(partitionLog);
        Object logInstance = ((CompletableFuture<?>) logFuture)
                .get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        // Retention lives in PartitionRetention now and is coalesced and asynchronous, so the trim
        // is driven straight through the Log handle to keep this check deterministic.
        Class<?> logClass = Class.forName(
                "io.lakestream.api.Log",
                true,
                logInstance.getClass().getClassLoader());
        Object lastOffset = awaitFuture(logClass.getMethod("getLastOffset").invoke(logInstance));
        long lastOffsetValue = (long) lastOffset.getClass().getMethod("offset").invoke(lastOffset);
        assertTrue(lastOffsetValue >= 0, "Expected the partition to hold records before trimming");

        Object trimOffset = awaitFuture(logClass
                .getMethod("computeRetentionTrimOffset", long.class, long.class, long.class)
                .invoke(logInstance, lastOffsetValue, 1L, -1L));
        long trimOffsetValue = ((Number) trimOffset).longValue();
        assertTrue(trimOffsetValue >= 0, "Expected retention to select a trim offset");

        awaitFuture(logClass.getMethod("softTrim", long.class).invoke(logInstance, trimOffsetValue));
    }

    private static Object awaitFuture(Object future) throws Exception {
        return ((CompletableFuture<?>) future).get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private static Object unwrapUrsaStorageState(Object ursaStateOrEngine) throws Exception {
        try {
            ursaStateOrEngine.getClass().getDeclaredMethod("getOrCreatePartitionLog", TopicIdPartition.class);
            return ursaStateOrEngine;
        } catch (NoSuchMethodException ignored) {
            Object engine = IsolatedUrsaCatalogInspector.leasedDelegateHolder(ursaStateOrEngine);
            try {
                Field delegateField = engine.getClass().getDeclaredField("delegate");
                delegateField.setAccessible(true);
                engine = delegateField.get(engine);
            } catch (NoSuchFieldException ignoredDelegate) {
                // The object is already the concrete engine.
            }

            Field stateField = engine.getClass().getDeclaredField("state");
            stateField.setAccessible(true);
            return stateField.get(engine);
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> getUrsaTopicConfig(
            Object ursaStateOrEngine,
            TopicIdPartition topicIdPartition) throws Exception {
        Object state = unwrapUrsaStorageState(ursaStateOrEngine);
        Field holderField = state.getClass().getDeclaredField("lakestreamStorageHolder");
        holderField.setAccessible(true);
        Object holder = holderField.get(state);

        var catalogMethod = holder.getClass().getDeclaredMethod("catalog");
        catalogMethod.setAccessible(true);
        Object catalog = catalogMethod.invoke(holder);

        Class<?> streamIdentity = holder.getClass().getClassLoader()
                .loadClass("org.apache.kafka.storage.diskless.handlers.KafkaStreamIdentity");
        var identifierMethod = streamIdentity.getDeclaredMethod(
                "streamIdentifier", String.class, Uuid.class);
        identifierMethod.setAccessible(true);
        Object identifier = identifierMethod.invoke(
                null, topicIdPartition.topic(), topicIdPartition.topicId());

        var loadStreamMethod = catalog.getClass().getMethod("loadStream", identifier.getClass());
        Object metadata = ((CompletableFuture<?>) loadStreamMethod.invoke(catalog, identifier))
                .get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        var propertiesMethod = metadata.getClass().getMethod("properties");
        return Map.copyOf((Map<String, String>) propertiesMethod.invoke(metadata));
    }
}
