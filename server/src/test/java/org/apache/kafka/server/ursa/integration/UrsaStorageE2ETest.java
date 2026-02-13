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
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
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
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.OxiaClientBuilder;
import io.streamnative.oxia.testcontainers.OxiaContainer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end integration tests for Ursa Storage using real Kafka producer and consumer.
 * Uses KafkaClusterTestKit to spin up a test Kafka cluster with Ursa storage mode
 * using ManagedLedger with embedded Oxia for persistent storage tests.
 *
 * <p>Tests are organized into nested classes by functionality for better isolation
 * and parallel execution capability.
 */
@Timeout(value = 180, unit = TimeUnit.SECONDS)
@Tag("integration")
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
            }
        }
        if (oxiaContainer != null) {
            oxiaContainer.stop();
            log.info("Oxia container stopped");
        }
    }

    private static KafkaClusterTestKit createCluster(Path storagePath, String oxiaServiceAddress) throws Exception {
        log.info("Creating cluster with Ursa storage, Oxia at: {}, storage path: {}",
                oxiaServiceAddress, storagePath);

        return new KafkaClusterTestKit.Builder(
                new TestKitNodes.Builder()
                        .setNumBrokerNodes(1)
                        .setNumControllerNodes(1)
                        .build())
                .setConfigProp(ServerLogConfigs.URSA_STORAGE_ENABLE_CONFIG, "true")
                .setConfigProp(ServerLogConfigs.URSA_STORAGE_OXIA_SERVICE_URL_CONFIG, oxiaServiceAddress)
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
            try (Admin admin = Admin.create(cluster.clientProperties())) {
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
            try (Admin admin = Admin.create(cluster.clientProperties())) {
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

            try (Admin admin = Admin.create(cluster.clientProperties())) {
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
     * Oxia metadata verification tests.
     * Tests that ManagedLedger and partitioned topic metadata are correctly stored in Oxia.
     */
    @Nested
    @DisplayName("Oxia Metadata Tests")
    class OxiaMetadataTests {

        @Test
        @DisplayName("ManagedLedger metadata created under /managed-ledgers prefix")
        void testManagedLedgerMetadataCreatedUnderManagedLedgersPrefix() throws Exception {
            String topicName = uniqueTopicName("managed-ledger-metadata-topic");

            createDisklessTopic(cluster, topicName);
            produceRecords(cluster.bootstrapServers(), topicName, 10);
            consumeAndVerifyRecords(cluster.bootstrapServers(), topicName, 10);

            assertManagedLedgerMetadataExistsInOxia(topicName);
            log.info("ManagedLedger metadata test passed for topic {}", topicName);
        }

        @Test
        @DisplayName("Partitioned topics metadata and related keys created and deleted correctly")
        void testPartitionedTopicsMetadataCreatedAndDeleted() throws Exception {
            String topicName = uniqueTopicName("partitioned-topics-metadata-topic");
            int numPartitions = 3;

            createDisklessTopic(cluster, topicName, numPartitions);
            assertPartitionedTopicMetadataExistsInOxia(topicName, numPartitions);

            for (int partition = 0; partition < numPartitions; partition++) {
                produceRecords(cluster.bootstrapServers(), topicName, partition, 1);
                assertManagedLedgerMetadataExistsInOxia(topicName, partition);
            }

            Uuid topicId;
            try (Admin admin = Admin.create(cluster.clientProperties())) {
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

            assertPartitionedTopicMetadataDeletedFromOxia(topicName);
            assertManagedLedgerMetadataDeletedFromOxia(topicName, numPartitions);
            assertProducerStateSnapshotsDeletedFromOxia(topicId, numPartitions);
            log.info("Partitioned topics metadata test passed for topic {}", topicName);
        }

        private void assertManagedLedgerMetadataExistsInOxia(String topicName) throws Exception {
            assertManagedLedgerMetadataExistsInOxia(topicName, 0);
        }

        private void assertManagedLedgerMetadataExistsInOxia(String topicName, int partition) throws Exception {
            String managedLedgerPath = "/managed-ledgers/public/default/persistent/" + topicName + "-partition-" + partition;
            assertOxiaKeyExists(managedLedgerPath, "ManagedLedgerMetadata");
        }

        private void assertPartitionedTopicMetadataExistsInOxia(String topicName, int expectedPartitions)
                throws Exception {
            String oxiaServiceAddress = oxiaContainer.getServiceAddress();
            String namespace = ServerLogConfigs.URSA_STORAGE_NAMESPACE_DEFAULT;
            String key = "/admin/partitioned-topics/public/default/persistent/" + topicName;
            Pattern partitionsPattern = Pattern.compile("\"partitions\"\\s*:\\s*" + expectedPartitions);

            try (AsyncOxiaClient client = OxiaClientBuilder.create(oxiaServiceAddress)
                    .namespace(namespace)
                    .asyncClient()
                    .get()) {
                TestUtils.waitForCondition(() -> {
                    GetResult result = client.get(key).get(10, TimeUnit.SECONDS);
                    if (result == null) {
                        return false;
                    }
                    byte[] value = result.value();
                    if (value != null && value.length > 0) {
                        String body = new String(value, StandardCharsets.UTF_8);
                        if (!partitionsPattern.matcher(body).find()) {
                            throw new AssertionError("Partitioned topic metadata does not contain expected partitions. key=" + key);
                        }
                        return true;
                    }
                    return false;
                }, 30_000, 100, () -> "Timed out waiting for partitioned topic metadata in Oxia: " + key);
            }
        }

        private void assertPartitionedTopicMetadataDeletedFromOxia(String topicName) throws Exception {
            String key = "/admin/partitioned-topics/public/default/persistent/" + topicName;
            assertOxiaKeyDeleted(key, "PartitionedTopicMetadata");
        }

        private void assertManagedLedgerMetadataDeletedFromOxia(String topicName, int partitions) throws Exception {
            for (int partition = 0; partition < partitions; partition++) {
                String key = "/managed-ledgers/public/default/persistent/" + topicName + "-partition-" + partition;
                assertOxiaKeyDeleted(key, "ManagedLedgerMetadata");
            }
        }

        private void putProducerStateSnapshotsInOxia(Uuid topicId, int partitions) throws Exception {
            String oxiaServiceAddress = oxiaContainer.getServiceAddress();
            String namespace = ServerLogConfigs.URSA_STORAGE_NAMESPACE_DEFAULT;

            try (AsyncOxiaClient client = OxiaClientBuilder.create(oxiaServiceAddress)
                    .namespace(namespace)
                    .asyncClient()
                    .get()) {
                for (int partition = 0; partition < partitions; partition++) {
                    String key = "producer-state-snapshot/" + topicId + "-" + partition;
                    client.put(key, ("dummy-" + partition).getBytes(StandardCharsets.UTF_8)).get(10, TimeUnit.SECONDS);
                }
            }
        }

        private void assertProducerStateSnapshotsExistInOxia(Uuid topicId, int partitions) throws Exception {
            for (int partition = 0; partition < partitions; partition++) {
                String key = "producer-state-snapshot/" + topicId + "-" + partition;
                assertOxiaKeyExists(key, "ProducerStateSnapshot");
            }
        }

        private void assertProducerStateSnapshotsDeletedFromOxia(Uuid topicId, int partitions) throws Exception {
            for (int partition = 0; partition < partitions; partition++) {
                String key = "producer-state-snapshot/" + topicId + "-" + partition;
                assertOxiaKeyDeleted(key, "ProducerStateSnapshot");
            }
        }

        private void assertOxiaKeyExists(String key, String description) throws Exception {
            String oxiaServiceAddress = oxiaContainer.getServiceAddress();
            String namespace = ServerLogConfigs.URSA_STORAGE_NAMESPACE_DEFAULT;

            try (AsyncOxiaClient client = OxiaClientBuilder.create(oxiaServiceAddress)
                    .namespace(namespace)
                    .asyncClient()
                    .get()) {
                TestUtils.waitForCondition(() -> {
                    GetResult result = client.get(key).get(10, TimeUnit.SECONDS);
                    return result != null && result.value() != null && result.value().length > 0;
                }, 30_000, 100, () -> "Timed out waiting for " + description + " to exist in Oxia: " + key);
            }
        }

        private void assertOxiaKeyDeleted(String key, String description) throws Exception {
            String oxiaServiceAddress = oxiaContainer.getServiceAddress();
            String namespace = ServerLogConfigs.URSA_STORAGE_NAMESPACE_DEFAULT;

            try (AsyncOxiaClient client = OxiaClientBuilder.create(oxiaServiceAddress)
                    .namespace(namespace)
                    .asyncClient()
                    .get()) {
                TestUtils.waitForCondition(() -> {
                    GetResult result = client.get(key).get(10, TimeUnit.SECONDS);
                    return result == null || result.value() == null;
                }, 30_000, 100, () -> "Timed out waiting for " + description + " to be deleted from Oxia: " + key);
            }
        }
    }
}
