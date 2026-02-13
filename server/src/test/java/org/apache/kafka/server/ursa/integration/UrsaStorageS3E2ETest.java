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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.streamnative.oxia.testcontainers.OxiaContainer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end integration tests for Ursa Storage with S3 backend using LocalStack.
 * Uses KafkaClusterTestKit to spin up a test Kafka cluster with Ursa storage mode
 * using ManagedLedger with embedded Oxia and LocalStack S3.
 *
 * <p>Tests are organized into nested classes by functionality for better isolation.
 */
@Timeout(value = 300, unit = TimeUnit.SECONDS)
@Tag("integration")
public class UrsaStorageS3E2ETest extends UrsaStorageE2ETestBase {

    private static final String S3_BUCKET = "kafka-ursa-storage";

    @TempDir
    static Path baseDir;

    private static OxiaContainer oxiaContainer;
    private static LocalStackContainer localStackContainer;
    private static URI s3Endpoint;
    private static KafkaClusterTestKit cluster;

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

        createS3Bucket(s3Endpoint, S3_BUCKET);
        log.info("Created S3 bucket: {}", S3_BUCKET);

        cluster = createClusterWithS3Backend(baseDir);
        cluster.format();
        cluster.startup();
        cluster.waitForReadyBrokers();
        log.info("Kafka cluster started with bootstrap servers: {}", cluster.bootstrapServers());
    }

    @AfterAll
    static void stopContainers() {
        if (cluster != null) {
            try {
                cluster.close();
                log.info("Kafka cluster stopped");
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

    private static KafkaClusterTestKit createClusterWithS3Backend(Path storagePath) throws Exception {
        String oxiaServiceAddress = oxiaContainer.getServiceAddress();
        String s3Prefix = "ursa-e2e/" + storagePath.getFileName();
        log.info("Creating cluster with S3 Ursa storage, Oxia at: {}, S3 endpoint: {}, S3 prefix: {}",
                oxiaServiceAddress, s3Endpoint, s3Prefix);

        return new KafkaClusterTestKit.Builder(
                new TestKitNodes.Builder()
                        .setNumBrokerNodes(1)
                        .setNumControllerNodes(1)
                        .build())
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
                .build();
    }

    /**
     * Basic S3 backend produce/consume tests.
     */
    @Nested
    @DisplayName("Basic S3 Backend Tests")
    class BasicS3Tests {

        @Test
        @DisplayName("Produce and consume with S3 backend")
        void testProduceConsumeWithS3Backend() throws Exception {
            String topicName = uniqueTopicName("s3-ursa-topic");
            int numRecords = 50;

            createDisklessTopic(cluster, topicName);
            produceRecords(cluster.bootstrapServers(), topicName, numRecords);

            log.info("Produced {} records to topic {} with S3 backend", numRecords, topicName);

            consumeAndVerifyRecords(cluster.bootstrapServers(), topicName, numRecords);
            log.info("Consumed {} records from topic {} with S3 backend", numRecords, topicName);
        }

        @Test
        @DisplayName("Multiple partitions with S3 backend")
        void testMultiplePartitionsWithS3Backend() throws Exception {
            String topicName = uniqueTopicName("s3-multi-partition-topic");
            int numPartitions = 3;
            int recordsPerPartition = 10;

            createDisklessTopic(cluster, topicName, numPartitions);

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

            log.info("Produced {} records per partition to {} partitions with S3 backend",
                    recordsPerPartition, numPartitions);

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
    }

    /**
     * S3 backend persistence and broker restart tests.
     */
    @Nested
    @DisplayName("S3 Persistence Tests")
    class S3PersistenceTests {

        @Test
        @DisplayName("Data persists across broker restart with S3")
        void testDataPersistsAcrossBrokerRestartWithS3() throws Exception {
            String topicName = uniqueTopicName("s3-persistence-test-topic");
            int recordsBeforeRestart = 30;
            int recordsAfterRestart = 10;
            int totalRecords = recordsBeforeRestart + recordsAfterRestart;

            createDisklessTopic(cluster, topicName);

            // Produce records BEFORE restart
            produceRecords(cluster.bootstrapServers(), topicName, recordsBeforeRestart);
            log.info("Produced {} records before broker restart with S3 backend", recordsBeforeRestart);

            // Verify records before restart
            consumeAndVerifyRecords(cluster.bootstrapServers(), topicName, recordsBeforeRestart);
            log.info("Verified {} records before broker restart", recordsBeforeRestart);

            // Restart broker
            var broker = cluster.brokers().values().iterator().next();
            log.info("Shutting down broker with S3 backend...");
            broker.shutdown();
            broker.awaitShutdown();
            log.info("Broker shutdown complete");

            log.info("Starting broker with S3 backend...");
            broker.startup();
            TestUtils.waitForCondition(
                    () -> broker.brokerState() == BrokerState.RUNNING,
                    30000, "Broker did not reach RUNNING state after restart");
            log.info("Broker restart complete, state: {}", broker.brokerState());

            // Produce MORE records AFTER restart
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
            log.info("Produced {} more records after broker restart with S3 backend", recordsAfterRestart);

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

            log.info("S3 data persistence and HWM recovery test passed - all {} records have correct continuous offsets", totalRecords);
        }
    }

    /**
     * Idempotent producer tests with S3 backend.
     */
    @Nested
    @DisplayName("Idempotent Producer Tests")
    class IdempotentProducerTests {

        @Test
        @DisplayName("Idempotent producer with S3 backend")
        void testIdempotentProducerWithS3Backend() throws Exception {
            String topicName = uniqueTopicName("s3-idempotent-producer-topic");
            int numRecords = 30;

            createDisklessTopic(cluster, topicName);

            try (Producer<byte[], byte[]> producer = createIdempotentProducer(cluster.bootstrapServers())) {
                for (int i = 0; i < numRecords; i++) {
                    ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                            topicName, 0,
                            ("key-" + i).getBytes(StandardCharsets.UTF_8),
                            ("value-" + i).getBytes(StandardCharsets.UTF_8));
                    producer.send(record).get(PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                }
                producer.flush();
            }

            log.info("Produced {} records with idempotent producer to topic {}", numRecords, topicName);

            try (Consumer<byte[], byte[]> consumer = createConsumer(cluster.bootstrapServers())) {
                TopicPartition tp = new TopicPartition(topicName, 0);
                consumer.assign(Collections.singletonList(tp));
                consumer.seekToBeginning(Collections.singletonList(tp));

                List<ConsumerRecord<byte[], byte[]>> records = consumeRecords(consumer, numRecords);
                assertEquals(numRecords, records.size(),
                        "Should consume all " + numRecords + " records from idempotent producer");

                for (int i = 0; i < records.size(); i++) {
                    assertEquals(i, records.get(i).offset(), "Record at index " + i + " should have offset " + i);
                    assertEquals("key-" + i, new String(records.get(i).key(), StandardCharsets.UTF_8));
                    assertEquals("value-" + i, new String(records.get(i).value(), StandardCharsets.UTF_8));
                }
            }

            log.info("Idempotent producer test passed - all {} records produced and consumed correctly", numRecords);
        }

        @Test
        @DisplayName("Multiple idempotent producers same partition")
        void testMultipleIdempotentProducersSamePartition() throws Exception {
            String topicName = uniqueTopicName("s3-multi-idempotent-producer-topic");
            int recordsPerProducer = 10;
            int numProducers = 3;

            createDisklessTopic(cluster, topicName);

            for (int p = 0; p < numProducers; p++) {
                try (Producer<byte[], byte[]> producer = createIdempotentProducer(cluster.bootstrapServers())) {
                    for (int i = 0; i < recordsPerProducer; i++) {
                        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                                topicName, 0,
                                ("producer-" + p + "-key-" + i).getBytes(StandardCharsets.UTF_8),
                                ("producer-" + p + "-value-" + i).getBytes(StandardCharsets.UTF_8));
                        producer.send(record).get(PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    }
                    producer.flush();
                }
                log.info("Producer {} sent {} records", p, recordsPerProducer);
            }

            int totalRecords = numProducers * recordsPerProducer;

            try (Consumer<byte[], byte[]> consumer = createConsumer(cluster.bootstrapServers())) {
                TopicPartition tp = new TopicPartition(topicName, 0);
                consumer.assign(Collections.singletonList(tp));
                consumer.seekToBeginning(Collections.singletonList(tp));

                List<ConsumerRecord<byte[], byte[]>> records = consumeRecords(consumer, totalRecords);
                assertEquals(totalRecords, records.size(),
                        "Should have all " + totalRecords + " records from " + numProducers + " producers");

                for (int i = 0; i < records.size(); i++) {
                    assertEquals(i, records.get(i).offset(), "Record offsets should be sequential");
                }
            }

            log.info("Multiple idempotent producers test passed - {} total records from {} producers",
                    totalRecords, numProducers);
        }
    }

    /**
     * Transactional producer tests with S3 backend.
     */
    @Nested
    @DisplayName("Transactional Producer Tests")
    class TransactionalProducerTests {

        @Test
        @DisplayName("Transactional producer rejected")
        void testTransactionalProducerRejected() throws Exception {
            String topicName = uniqueTopicName("s3-transactional-rejected-topic");

            createDisklessTopic(cluster, topicName);

            try (Producer<byte[], byte[]> producer = createTransactionalProducer(cluster.bootstrapServers())) {
                producer.initTransactions();
                producer.beginTransaction();

                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                        topicName, 0,
                        "tx-key".getBytes(StandardCharsets.UTF_8),
                        "tx-value".getBytes(StandardCharsets.UTF_8));

                try {
                    producer.send(record).get(PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    producer.commitTransaction();
                    throw new AssertionError("Transactional produce should have been rejected");
                } catch (Exception e) {
                    producer.abortTransaction();
                    log.info("Transactional produce correctly rejected: {}", e.getMessage());
                }
            }

            try (Consumer<byte[], byte[]> consumer = createConsumer(cluster.bootstrapServers())) {
                TopicPartition tp = new TopicPartition(topicName, 0);
                consumer.assign(Collections.singletonList(tp));
                consumer.seekToBeginning(Collections.singletonList(tp));

                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(5));
                assertEquals(0, records.count(),
                        "No records should be in topic since transactional produce was rejected");
            }

            log.info("Transactional producer rejection test passed");
        }
    }

    /**
     * Offset commit tests with S3 backend.
     */
    @Nested
    @DisplayName("Offset Commit Tests")
    class OffsetCommitTests {

        @Test
        @DisplayName("Offset commit with S3 backend")
        void testOffsetCommitWithS3Backend() throws Exception {
            String topicName = uniqueTopicName("s3-offset-commit-topic");
            String groupId = "test-offset-commit-group-" + System.currentTimeMillis();
            int numRecords = 20;

            createDisklessTopic(cluster, topicName);
            produceRecords(cluster.bootstrapServers(), topicName, numRecords);

            log.info("Produced {} records to topic {}", numRecords, topicName);

            TopicPartition tp = new TopicPartition(topicName, 0);
            int firstBatchSize = 10;

            // First consumer: consume first batch and commit
            try (Consumer<byte[], byte[]> consumer = createConsumerWithGroupId(cluster.bootstrapServers(), groupId)) {
                consumer.assign(Collections.singletonList(tp));
                consumer.seekToBeginning(Collections.singletonList(tp));

                List<ConsumerRecord<byte[], byte[]>> firstBatch = consumeRecords(consumer, firstBatchSize);
                assertEquals(firstBatchSize, firstBatch.size());

                consumer.commitSync();

                Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(Set.of(tp));
                assertNotNull(committed.get(tp), "Committed offset should not be null");
                assertEquals(firstBatchSize, committed.get(tp).offset(),
                        "Committed offset should be " + firstBatchSize);

                log.info("First consumer committed offset {} for partition {}", committed.get(tp).offset(), tp);
            }

            // Second consumer: verify committed offset and consume remaining
            try (Consumer<byte[], byte[]> consumer2 = createConsumerWithGroupId(cluster.bootstrapServers(), groupId)) {
                consumer2.assign(Collections.singletonList(tp));

                Map<TopicPartition, OffsetAndMetadata> previouslyCommitted = consumer2.committed(Set.of(tp));
                assertNotNull(previouslyCommitted.get(tp), "Previously committed offset should be retrievable");
                assertEquals(firstBatchSize, previouslyCommitted.get(tp).offset(),
                        "New consumer should see the committed offset from first consumer");

                log.info("Second consumer verified committed offset: {}", previouslyCommitted.get(tp).offset());

                long position = consumer2.position(tp);
                assertEquals(firstBatchSize, position, "Position should start from committed offset");

                List<ConsumerRecord<byte[], byte[]>> remainingRecords = consumeRecords(consumer2, numRecords - firstBatchSize);
                assertEquals(numRecords - firstBatchSize, remainingRecords.size(),
                        "Should consume remaining records starting from committed offset");

                for (int i = 0; i < remainingRecords.size(); i++) {
                    int expectedIndex = firstBatchSize + i;
                    assertEquals(expectedIndex, remainingRecords.get(i).offset(),
                            "Record offset should be " + expectedIndex);
                    assertEquals("key-" + expectedIndex, new String(remainingRecords.get(i).key(), StandardCharsets.UTF_8));
                    assertEquals("value-" + expectedIndex, new String(remainingRecords.get(i).value(), StandardCharsets.UTF_8));
                }

                consumer2.commitSync();

                Map<TopicPartition, OffsetAndMetadata> finalCommitted = consumer2.committed(Set.of(tp));
                assertEquals(numRecords, finalCommitted.get(tp).offset(),
                        "Final committed offset should be " + numRecords);

                log.info("Second consumer committed final offset: {}", finalCommitted.get(tp).offset());
            }

            log.info("Offset commit test passed - offsets are correctly committed and retrieved");
        }
    }

    /**
     * Producer state persistence tests - crash recovery, concurrent producers, and recovery benchmarks.
     */
    @Nested
    @DisplayName("Producer State Tests")
    class ProducerStateTests {

        @Test
        @DisplayName("Multiple restarts maintain producer state integrity")
        void testMultipleRestarts() throws Exception {
            String topicName = uniqueTopicName("multiple-restarts-topic");
            int recordsPerRound = 5;
            int numRestarts = 3;

            createDisklessTopic(cluster, topicName);
            String bootstrapServers = cluster.bootstrapServers();
            var broker = cluster.brokers().values().iterator().next();

            int totalRecords = 0;
            for (int round = 0; round < numRestarts; round++) {
                try (Producer<byte[], byte[]> producer = createIdempotentProducer(bootstrapServers)) {
                    for (int i = 0; i < recordsPerRound; i++) {
                        int recordNum = totalRecords + i;
                        producer.send(new ProducerRecord<>(topicName, 0,
                                ("key-" + recordNum).getBytes(), ("value-" + recordNum).getBytes()))
                                .get(PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    }
                    producer.flush();
                }
                totalRecords += recordsPerRound;
                log.info("Round {}: Produced {} records, total: {}", round + 1, recordsPerRound, totalRecords);

                broker.shutdown();
                broker.awaitShutdown();
                broker.startup();
                TestUtils.waitForCondition(
                        () -> broker.brokerState() == BrokerState.RUNNING,
                        30000, "Broker did not reach RUNNING state");
                log.info("Round {}: Broker restarted", round + 1);
            }

            try (Consumer<byte[], byte[]> consumer = createConsumer(bootstrapServers)) {
                TopicPartition tp = new TopicPartition(topicName, 0);
                consumer.assign(Collections.singletonList(tp));
                consumer.seekToBeginning(Collections.singletonList(tp));

                List<ConsumerRecord<byte[], byte[]>> records = consumeRecords(consumer, totalRecords);
                assertEquals(totalRecords, records.size());

                for (int i = 0; i < records.size(); i++) {
                    assertEquals(i, records.get(i).offset(), "Offset should be sequential");
                }
            }

            log.info("Multiple restarts test passed - {} total records with sequential offsets", totalRecords);
        }

        @Test
        @DisplayName("Concurrent idempotent producers work correctly")
        void testConcurrentIdempotentProducers() throws Exception {
            String topicName = uniqueTopicName("concurrent-producers-topic");
            int numProducers = 5;
            int recordsPerProducer = 20;

            createDisklessTopic(cluster, topicName);
            String bootstrapServers = cluster.bootstrapServers();

            ExecutorService executor = Executors.newFixedThreadPool(numProducers);
            AtomicInteger successCount = new AtomicInteger(0);
            List<Future<?>> futures = new ArrayList<>();

            for (int p = 0; p < numProducers; p++) {
                final int producerNum = p;
                futures.add(executor.submit(() -> {
                    try (Producer<byte[], byte[]> producer = createIdempotentProducer(bootstrapServers)) {
                        for (int i = 0; i < recordsPerProducer; i++) {
                            producer.send(new ProducerRecord<>(topicName, 0,
                                    ("p" + producerNum + "-k" + i).getBytes(),
                                    ("p" + producerNum + "-v" + i).getBytes()))
                                    .get(PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                            successCount.incrementAndGet();
                        }
                        producer.flush();
                    } catch (Exception e) {
                        log.error("Producer {} failed", producerNum, e);
                    }
                }));
            }

            for (Future<?> f : futures) {
                f.get(60, TimeUnit.SECONDS);
            }
            executor.shutdown();

            int expectedTotal = numProducers * recordsPerProducer;
            assertEquals(expectedTotal, successCount.get(), "All sends should succeed");

            try (Consumer<byte[], byte[]> consumer = createConsumer(bootstrapServers)) {
                TopicPartition tp = new TopicPartition(topicName, 0);
                consumer.assign(Collections.singletonList(tp));
                consumer.seekToBeginning(Collections.singletonList(tp));

                List<ConsumerRecord<byte[], byte[]>> records = consumeRecords(consumer, expectedTotal);
                assertEquals(expectedTotal, records.size());

                for (int i = 0; i < records.size(); i++) {
                    assertEquals(i, records.get(i).offset());
                }
            }

            log.info("Concurrent idempotent producers test passed - {} total records", expectedTotal);
        }

        @Test
        @DisplayName("Recovery time benchmark")
        void testRecoveryTimeBenchmark() throws Exception {
            String topicName = uniqueTopicName("recovery-benchmark-topic");
            int numRecords = 500;

            createDisklessTopic(cluster, topicName);
            String bootstrapServers = cluster.bootstrapServers();

            try (Producer<byte[], byte[]> producer = createIdempotentProducer(bootstrapServers)) {
                for (int i = 0; i < numRecords; i++) {
                    producer.send(new ProducerRecord<>(topicName, 0,
                            ("key-" + i).getBytes(), ("value-" + i).getBytes()))
                            .get(PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                }
                producer.flush();
            }

            log.info("Produced {} records, now measuring recovery time", numRecords);

            var broker = cluster.brokers().values().iterator().next();
            broker.shutdown();
            broker.awaitShutdown();

            long startTime = System.nanoTime();
            broker.startup();
            TestUtils.waitForCondition(
                    () -> broker.brokerState() == BrokerState.RUNNING,
                    60000, "Broker did not reach RUNNING state");

            try (Consumer<byte[], byte[]> consumer = createConsumer(bootstrapServers)) {
                TopicPartition tp = new TopicPartition(topicName, 0);
                consumer.assign(Collections.singletonList(tp));
                consumer.seekToBeginning(Collections.singletonList(tp));
                consumeRecords(consumer, 1);
            }
            long endTime = System.nanoTime();

            double recoveryTimeMs = (endTime - startTime) / 1_000_000.0;
            log.info("Recovery time for {} records: {} ms", numRecords, recoveryTimeMs);

            assertTrue(recoveryTimeMs < 60000, "Recovery should complete within 60 seconds");
        }

        @Test
        @DisplayName("Multiple producer sessions maintain sequential offsets")
        void testMultipleProducerSessionsMaintainSequentialOffsets() throws Exception {
            String topicName = uniqueTopicName("multi-session-offsets-topic");
            int numSessions = 5;
            int recordsPerSession = 10;

            createDisklessTopic(cluster, topicName);
            String bootstrapServers = cluster.bootstrapServers();
            TopicPartition tp = new TopicPartition(topicName, 0);

            for (int session = 0; session < numSessions; session++) {
                try (Producer<byte[], byte[]> producer = createIdempotentProducer(bootstrapServers)) {
                    for (int i = 0; i < recordsPerSession; i++) {
                        producer.send(new ProducerRecord<>(topicName, 0,
                                ("s" + session + "-key-" + i).getBytes(),
                                ("s" + session + "-value-" + i).getBytes()))
                                .get(PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    }
                    producer.flush();
                }
                log.info("Session {} completed with {} records", session, recordsPerSession);
            }

            int totalRecords = numSessions * recordsPerSession;
            try (Consumer<byte[], byte[]> consumer = createConsumer(bootstrapServers)) {
                consumer.assign(Collections.singletonList(tp));
                consumer.seekToBeginning(Collections.singletonList(tp));

                List<ConsumerRecord<byte[], byte[]>> records = consumeRecords(consumer, totalRecords);
                assertEquals(totalRecords, records.size());

                for (int i = 0; i < records.size(); i++) {
                    assertEquals(i, records.get(i).offset(), "Offset " + i + " should match");
                }
            }

            log.info("Multiple producer sessions test passed - {} sequential offsets verified", totalRecords);
        }
    }
}
