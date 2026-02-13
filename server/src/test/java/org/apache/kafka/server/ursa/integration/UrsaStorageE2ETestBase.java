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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.test.TestUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Base class for Ursa Storage E2E integration tests.
 * Provides common utilities for creating producers, consumers, topics, and consuming records.
 */
public abstract class UrsaStorageE2ETestBase {

    protected static final Logger log = LoggerFactory.getLogger(UrsaStorageE2ETestBase.class);

    // Common timeout constants
    protected static final int DEFAULT_TIMEOUT_SECONDS = 30;
    protected static final int PRODUCE_TIMEOUT_SECONDS = 60;
    protected static final int CONSUME_TIMEOUT_MS = 30_000;
    protected static final int TOPIC_READY_TIMEOUT_MS = 30_000;

    // Common configuration constants
    protected static final int MAX_REQUEST_SIZE = 10485760; // 10MB

    /**
     * Creates a standard Kafka producer with byte array serializers.
     */
    protected Producer<byte[], byte[]> createProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, String.valueOf(MAX_REQUEST_SIZE));
        return new KafkaProducer<>(props);
    }

    /**
     * Creates an idempotent Kafka producer.
     */
    protected Producer<byte[], byte[]> createIdempotentProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, String.valueOf(MAX_REQUEST_SIZE));
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.RETRIES_CONFIG, "3");
        return new KafkaProducer<>(props);
    }

    /**
     * Creates a transactional Kafka producer.
     */
    protected Producer<byte[], byte[]> createTransactionalProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, String.valueOf(MAX_REQUEST_SIZE));
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "test-tx-" + System.currentTimeMillis());
        props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "10000");
        return new KafkaProducer<>(props);
    }

    /**
     * Creates a standard Kafka consumer with byte array deserializers.
     */
    protected Consumer<byte[], byte[]> createConsumer(String bootstrapServers) {
        return createConsumerWithGroupId(bootstrapServers, "test-group-" + System.currentTimeMillis());
    }

    /**
     * Creates a Kafka consumer with a specific group ID.
     */
    protected Consumer<byte[], byte[]> createConsumerWithGroupId(String bootstrapServers, String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, String.valueOf(MAX_REQUEST_SIZE));
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return new KafkaConsumer<>(props);
    }

    /**
     * Consumes records from a consumer until the expected count is reached or timeout.
     */
    protected List<ConsumerRecord<byte[], byte[]>> consumeRecords(
            Consumer<byte[], byte[]> consumer, int expectedCount) {
        return consumeRecords(consumer, expectedCount, Duration.ofMillis(CONSUME_TIMEOUT_MS));
    }

    /**
     * Consumes records from a consumer until the expected count is reached or timeout.
     */
    protected List<ConsumerRecord<byte[], byte[]>> consumeRecords(
            Consumer<byte[], byte[]> consumer, int expectedCount, Duration timeout) {
        List<ConsumerRecord<byte[], byte[]>> allRecords = new ArrayList<>();
        long deadline = System.currentTimeMillis() + timeout.toMillis();

        while (allRecords.size() < expectedCount && System.currentTimeMillis() < deadline) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<byte[], byte[]> record : records) {
                allRecords.add(record);
            }
        }

        return allRecords;
    }

    /**
     * Creates a topic with Ursa storage enabled.
     */
    protected void createDisklessTopic(KafkaClusterTestKit cluster, String topicName) throws Exception {
        createDisklessTopic(cluster, topicName, 1, (short) 1, Map.of());
    }

    /**
     * Creates a topic with Ursa storage enabled and specified partitions.
     */
    protected void createDisklessTopic(KafkaClusterTestKit cluster, String topicName, int partitions) throws Exception {
        createDisklessTopic(cluster, topicName, partitions, (short) 1, Map.of());
    }

    /**
     * Creates a topic with Ursa storage enabled and additional configs.
     */
    protected void createDisklessTopic(
            KafkaClusterTestKit cluster,
            String topicName,
            int partitions,
            short replicationFactor,
            Map<String, String> additionalConfigs) throws Exception {
        try (Admin admin = Admin.create(cluster.clientProperties())) {
            Map<String, String> topicConfigs = new HashMap<>();
            topicConfigs.put(TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true");
            topicConfigs.putAll(additionalConfigs);

            NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor)
                    .configs(topicConfigs);
            admin.createTopics(Collections.singleton(newTopic)).all().get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
    }

    /**
     * Waits for a topic to be ready with the expected number of partitions.
     */
    protected void waitForTopicReady(KafkaClusterTestKit cluster, String topicName, int partitions) throws Exception {
        try (Admin admin = Admin.create(cluster.clientProperties())) {
            waitForTopicReady(admin, topicName, partitions);
        }
    }

    /**
     * Waits for a topic to be ready with the expected number of partitions.
     */
    protected static void waitForTopicReady(Admin admin, String topicName, int partitions) throws InterruptedException {
        TestUtils.waitForCondition(() -> {
            try {
                TopicDescription desc = admin.describeTopics(Collections.singleton(topicName))
                        .allTopicNames()
                        .get(5, TimeUnit.SECONDS)
                        .get(topicName);
                return desc != null && desc.partitions().size() == partitions;
            } catch (Exception e) {
                return false;
            }
        }, TOPIC_READY_TIMEOUT_MS, "Timed out waiting for topic " + topicName + " partitions=" + partitions);
    }

    /**
     * Produces records to a topic.
     */
    protected void produceRecords(String bootstrapServers, String topicName, int numRecords) throws Exception {
        produceRecords(bootstrapServers, topicName, 0, numRecords);
    }

    /**
     * Produces records to a specific partition.
     */
    protected void produceRecords(String bootstrapServers, String topicName, int partition, int numRecords) throws Exception {
        try (Producer<byte[], byte[]> producer = createProducer(bootstrapServers)) {
            for (int i = 0; i < numRecords; i++) {
                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                        topicName,
                        partition,
                        ("key-" + i).getBytes(StandardCharsets.UTF_8),
                        ("value-" + i).getBytes(StandardCharsets.UTF_8)
                );
                producer.send(record).get(PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            producer.flush();
        }
    }

    /**
     * Consumes and verifies records from a topic.
     */
    protected void consumeAndVerifyRecords(String bootstrapServers, String topicName, int expectedCount) throws Exception {
        consumeAndVerifyRecords(bootstrapServers, topicName, 0, expectedCount);
    }

    /**
     * Consumes and verifies records from a specific partition.
     */
    protected void consumeAndVerifyRecords(String bootstrapServers, String topicName, int partition, int expectedCount) throws Exception {
        try (Consumer<byte[], byte[]> consumer = createConsumer(bootstrapServers)) {
            TopicPartition tp = new TopicPartition(topicName, partition);
            consumer.assign(Collections.singletonList(tp));
            consumer.seekToBeginning(Collections.singletonList(tp));

            List<ConsumerRecord<byte[], byte[]>> records = consumeRecords(consumer, expectedCount);
            if (records.size() != expectedCount) {
                throw new AssertionError("Expected " + expectedCount + " records but got " + records.size());
            }

            for (int i = 0; i < records.size(); i++) {
                ConsumerRecord<byte[], byte[]> record = records.get(i);
                String expectedKey = "key-" + i;
                String expectedValue = "value-" + i;
                String actualKey = new String(record.key(), StandardCharsets.UTF_8);
                String actualValue = new String(record.value(), StandardCharsets.UTF_8);
                if (!expectedKey.equals(actualKey) || !expectedValue.equals(actualValue)) {
                    throw new AssertionError("Record mismatch at index " + i +
                            ": expected key=" + expectedKey + ", value=" + expectedValue +
                            " but got key=" + actualKey + ", value=" + actualValue);
                }
            }
        }
    }

    /**
     * Generates a unique topic name with the given prefix.
     */
    protected static String uniqueTopicName(String prefix) {
        return prefix + "-" + UUID.randomUUID();
    }

    /**
     * Creates an S3 bucket using HTTP PUT request.
     * This avoids AWS SDK imports which are disallowed by checkstyle.
     */
    protected static void createS3Bucket(URI s3Endpoint, String bucketName) throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(s3Endpoint + "/" + bucketName))
                .PUT(HttpRequest.BodyPublishers.noBody())
                .header("Content-Type", "application/xml")
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new RuntimeException("Failed to create S3 bucket: " + bucketName +
                    ", status: " + response.statusCode() + ", body: " + response.body());
        }
    }

    /**
     * Creates an S3 bucket with retry logic.
     */
    protected static void createS3BucketWithRetry(URI s3Endpoint, String bucketName) throws Exception {
        final Exception[] lastError = new Exception[1];
        try {
            TestUtils.waitForCondition(() -> {
                try {
                    createS3Bucket(s3Endpoint, bucketName);
                    return true;
                } catch (Exception e) {
                    lastError[0] = e;
                    return false;
                }
            }, 60_000, 1_000, () -> "Failed to create S3 bucket: " + bucketName);
        } catch (AssertionError e) {
            throw new RuntimeException("Failed to create S3 bucket: " + bucketName, lastError[0]);
        }
    }

    /**
     * Closes a resource quietly, logging any exceptions.
     */
    protected static void closeQuietly(AutoCloseable closeable, String name) {
        if (closeable != null) {
            try {
                closeable.close();
                log.info("Closed {}", name);
            } catch (Exception e) {
                log.warn("Failed to close {}", name, e);
            }
        }
    }
}
