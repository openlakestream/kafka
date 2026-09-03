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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.server.config.ServerLogConfigs;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import io.oxia.testcontainers.OxiaContainer;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * E2E test for external Ursa lakehouse compaction with compacted-index verification.
 *
 * <p>This test:
 * <ol>
 *   <li>Writes Kafka records to a topic with Ursa diskless storage enabled</li>
 *   <li>Starts an external compactor container to compact WAL data to Parquet</li>
 *   <li>Verifies entry indexes show FileType.PARQUET after compaction</li>
 *   <li>Verifies Parquet files exist on disk</li>
 * </ol>
 */
@Timeout(value = 600, unit = TimeUnit.SECONDS)
@Tag("integration")
public class UrsaLakehouseCompactionE2ETest extends UrsaStorageE2ETestBase {

    private static final int NUM_RECORDS = 100;
    private static final String S3_BUCKET = "kafka-ursa-storage";
    private static final String COMPACTOR_IMAGE_ENV = "URSA_COMPACTOR_IMAGE";

    private static Network network;
    private static OxiaContainer oxiaContainer;
    private static LocalStackContainer localStackContainer;
    private static URI s3Endpoint;
    private static KafkaClusterTestKit cluster;
    private static String s3Prefix;
    private static Properties compactionStorageConfig;

    @BeforeAll
    static void startContainers() throws Exception {
        String compactorImage = System.getenv(COMPACTOR_IMAGE_ENV);
        Assumptions.assumeTrue(compactorImage != null && !compactorImage.isBlank(),
                "Set " + COMPACTOR_IMAGE_ENV + " to an image containing the standalone Ursa compactor package");

        // Used as S3 key prefix (not a local path) for WAL and compacted data.
        s3Prefix = "ursa-lakehouse-compaction-e2e/" + UUID.randomUUID();

        // Make sure S3A (used by parquet readers/writers) has credentials available.
        System.setProperty("aws.accessKeyId", "test");
        System.setProperty("aws.secretAccessKey", "test");

        network = Network.newNetwork();

        oxiaContainer = new OxiaContainer(DockerImageName.parse("oxia/oxia:main"))
                .withNetwork(network)
                .withNetworkAliases("oxia");
        oxiaContainer.start();
        log.info("Oxia container started at: {}", oxiaContainer.getServiceAddress());

        localStackContainer = new LocalStackContainer(
                DockerImageName.parse("localstack/localstack:3.6"))
                .withServices(LocalStackContainer.Service.S3)
                .withNetwork(network)
                .withNetworkAliases("localstack");
        localStackContainer.start();

        s3Endpoint = localStackContainer.getEndpointOverride(LocalStackContainer.Service.S3);
        log.info("LocalStack S3 container started at: {}", s3Endpoint);

        createS3Bucket(s3Endpoint, S3_BUCKET);
        log.info("Created S3 bucket: {}", S3_BUCKET);

        compactionStorageConfig = createCompactionConfig(
                oxiaContainer.getServiceAddress(),
                s3Endpoint,
                localStackContainer.getRegion(),
                S3_BUCKET,
                s3Prefix);

        cluster = createCluster(
                oxiaContainer.getServiceAddress(), s3Endpoint, localStackContainer, s3Prefix);
        cluster.format();
        cluster.startup();
        cluster.waitForReadyBrokers();
        log.info("Kafka cluster started with bootstrap servers: {}", cluster.bootstrapServers());
    }

    @AfterAll
    static void stopContainers() {
        closeQuietly(cluster, "Kafka cluster");
        closeQuietly(localStackContainer, "LocalStack container");
        closeQuietly(oxiaContainer, "Oxia container");
        closeQuietly(network, "Docker network");
    }

    @Test
    @DisplayName("Write, compact, and verify Parquet files")
    @SuppressWarnings("NPathComplexity")
    public void testWriteCompactAndVerifyParquetFiles() throws Exception {
        String topicName = uniqueTopicName("ursa-lakehouse-compaction-e2e-topic");

        createTopicWithUrsaStorage(topicName);
        Uuid topicId = topicId(topicName);
        writeRecords(cluster.bootstrapServers(), topicName);

        String compactorImage = System.getenv(COMPACTOR_IMAGE_ENV);
        try (GenericContainer<?> compactor = startCompactorContainer(
                network,
                DockerImageName.parse(compactorImage),
                s3Prefix,
                localStackContainer.getRegion(),
                S3_BUCKET)) {
            waitForParquetCompaction(topicName, topicId);
            verifyKafkaConsumerCanReadAllMessages(cluster.bootstrapServers(), topicName);
        }
    }

    private void createTopicWithUrsaStorage(String topicName) throws Exception {
        createDisklessTopic(cluster, topicName);
        log.info("Created topic: {}", topicName);
    }

    private void writeRecords(String bootstrapServers, String topicName) throws Exception {
        try (Producer<byte[], byte[]> producer = createProducer(bootstrapServers)) {
            for (int i = 0; i < NUM_RECORDS; i++) {
                producer.send(new ProducerRecord<>(
                        topicName,
                        0,
                        ("key-" + i).getBytes(StandardCharsets.UTF_8),
                        ("value-" + i).getBytes(StandardCharsets.UTF_8)
                )).get(PRODUCE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            producer.flush();
        }
        log.info("Successfully wrote {} records to topic {}", NUM_RECORDS, topicName);
    }

    private void waitForParquetCompaction(String topicName, Uuid topicId) {
        log.info("Waiting for compaction output to be visible (max 5 minutes)...");
        Awaitility.await()
                .atMost(5, TimeUnit.MINUTES)
                .pollInterval(5, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    verifyReadsUseParquetIndexes(topicName, topicId);
                    verifyParquetFilesExistOnS3();
                });
    }

    private void verifyReadsUseParquetIndexes(String topicName, Uuid topicId) throws Exception {
        TopicIdPartition topicIdPartition = topicIdPartition(topicName, topicId);
        String streamName = IsolatedUrsaCatalogInspector.streamName(cluster, topicName, topicId);
        String namespace = IsolatedUrsaCatalogInspector.namespace(cluster);
        String catalogUri = compactionStorageConfig.getProperty("metadataStoreUrl");

        try (IsolatedUrsaCatalogInspector catalog =
                     IsolatedUrsaCatalogInspector.open(cluster, catalogUri, compactionStorageConfig)) {
            IsolatedUrsaCatalogInspector.LogIndexSummary summary = catalog.readLogIndexSummary(
                    namespace,
                    streamName,
                    topicIdPartition.partition(),
                    30,
                    TimeUnit.SECONDS);

            assertTrue(summary.lastOffset() >= 0,
                    "Expected stream to have data, but last entry was NOT_FOUND");

            assertTrue(summary.indexCount() > 0,
                    "Expected indexes to be present for logId=" + summary.logId());
            assertTrue(summary.parquetIndexCount() > 0,
                    "Expected at least one PARQUET index after compaction, logId=" + summary.logId());

            log.info("Verified PARQUET index usage: {} of {} indexes are PARQUET for logId={}",
                    summary.parquetIndexCount(), summary.indexCount(), summary.logId());
        }
    }

    private Uuid topicId(String topicName) throws Exception {
        try (Admin admin = cluster.admin()) {
            return admin.describeTopics(Set.of(topicName))
                    .allTopicNames()
                    .get(30, TimeUnit.SECONDS)
                    .get(topicName)
                    .topicId();
        }
    }

    private static TopicIdPartition topicIdPartition(String topicName, Uuid topicId) {
        return new TopicIdPartition(topicId, new TopicPartition(topicName, 0));
    }

    private void verifyParquetFilesExistOnS3() throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        URI listUri = URI.create(s3Endpoint + "/" + S3_BUCKET + "?prefix=" + s3Prefix);
        HttpRequest request = HttpRequest.newBuilder()
                .GET()
                .uri(listUri)
                .timeout(Duration.ofSeconds(10))
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertTrue(response.statusCode() == 200,
                "Failed to list S3 objects, status=" + response.statusCode() + ", body=" + response.body());
        assertTrue(response.body().contains(".parquet"),
                "Expected Parquet objects under s3://" + S3_BUCKET + "/" + s3Prefix);
        log.info("Parquet objects verified in S3 bucket={}, prefix={}", S3_BUCKET, s3Prefix);
    }

    private static Properties createCompactionConfig(
            String oxiaServiceAddress,
            URI s3Endpoint,
            String region,
            String bucket,
            String prefix) {
        String oxiaUrl = "oxia://" + oxiaServiceAddress + "/default";

        Properties properties = new Properties();
        properties.setProperty("oxiaStorageUrl", oxiaUrl);
        properties.setProperty("metadataStoreUrl", oxiaUrl);
        properties.setProperty("backendStorageType", "S3");
        properties.setProperty("bucket", bucket);
        properties.setProperty("prefix", prefix);
        properties.setProperty("cloudStorageEndpoint", s3Endpoint.toString());
        properties.setProperty("region", region);
        properties.setProperty("s3AccessKeyId", "test");
        properties.setProperty("s3SecretAccessKey", "test");
        properties.setProperty("entrySerDeType", "KAFKA_BATCHED_RAW_PARQUET");
        properties.setProperty("compactedFileSizeLimit", String.valueOf(10 * 1024));
        properties.setProperty("tailCompactDataVisibilityIntervalInSeconds", "5");
        properties.setProperty("refreshLocalTopicInternalInSeconds", "5");
        properties.setProperty("compactedThreadNum", "2");
        properties.setProperty("publishThreadNum", "2");
        properties.setProperty("commitThreadNum", "2");
        properties.setProperty("writeBufferSize", String.valueOf(256 * 1024));
        properties.setProperty("writeBufferFlushIntervalMs", "100");
        properties.setProperty("metastoreRequestRateLimitPerSecond", "500");
        properties.setProperty("compactionBucket", bucket);
        properties.setProperty("compactionPrefix", prefix);
        return properties;
    }

    private static GenericContainer<?> startCompactorContainer(
            Network network,
            DockerImageName compactorImage,
            String s3Prefix,
            String region,
            String bucket) {
        String oxiaUrl = "oxia://oxia:6648/default";

        GenericContainer<?> compactor = new GenericContainer<>(compactorImage)
                .withNetwork(network)
                .withWorkingDirectory("/opt/ursa")
                .withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint("sh", "-ec"))
                .withEnv("URSA_JAVA_OPTS", "-Xmx512M -XX:+UseZGC")
                .withEnv("OTEL_SDK_DISABLED", "true")
                .withEnv("URSA_METADATA_STORE_URL", oxiaUrl)
                .withEnv("URSA_OXIA_STORAGE_URL", oxiaUrl)
                .withEnv("URSA_BACKEND_STORAGE_TYPE", "S3")
                .withEnv("URSA_BUCKET", bucket)
                .withEnv("URSA_PREFIX", s3Prefix)
                .withEnv("URSA_CLOUD_STORAGE_ENDPOINT", "http://localstack:4566")
                .withEnv("URSA_REGION", region)
                .withEnv("URSA_S3_ACCESS_KEY_ID", "test")
                .withEnv("URSA_S3_SECRET_ACCESS_KEY", "test")
                // V2 lakehouse writer/reader uses these properties for s3a:// path resolution.
                .withEnv("URSA_COMPACTION_BACKEND_STORAGE_TYPE", "S3")
                .withEnv("URSA_COMPACTION_BUCKET", bucket)
                .withEnv("URSA_COMPACTION_PREFIX", s3Prefix)
                .withEnv("URSA_COMPACTION_BUCKET_REGION", region)
                .withEnv("URSA_ENTRY_SERDE_TYPE", "KAFKA_BATCHED_RAW_PARQUET")
                .withEnv("URSA_COMPACTED_FILE_SIZE_LIMIT", String.valueOf(10 * 1024))
                .withEnv("URSA_TAIL_VISIBILITY_INTERVAL_SECONDS", "5")
                .withEnv("URSA_REFRESH_LOG_INTERVAL_SECONDS", "5")
                .withEnv("URSA_INTERNAL_COMPACTION_TASK_PUBLISHER_ENABLED", "true")
                .withEnv("URSA_COMPACTED_THREAD_NUM", "2")
                .withEnv("URSA_PUBLISH_THREAD_NUM", "2")
                .withEnv("URSA_COMMIT_THREAD_NUM", "2")
                .withEnv("URSA_METASTORE_RATE_LIMIT", "500")
                .withEnv("AWS_ACCESS_KEY_ID", "test")
                .withEnv("AWS_SECRET_ACCESS_KEY", "test")
                .withCommand(compactorCommand())
                .waitingFor(new LogMessageWaitStrategy()
                        .withRegEx(".*was elected leader.*\\n")
                        .withStartupTimeout(Duration.ofMinutes(5)));

        compactor.start();
        log.info("Compactor container started: image={}", compactorImage);
        return compactor;
    }

    private static String compactorCommand() {
        return """
                set -eu
                conf=/tmp/ursa-storage.properties
                : > "$conf"
                property() { printf '%s=%s\\n' "$1" "$2" >> "$conf"; }
                property metadataStoreUrl "$URSA_METADATA_STORE_URL"
                property oxiaStorageUrl "$URSA_OXIA_STORAGE_URL"
                property backendStorageType "$URSA_BACKEND_STORAGE_TYPE"
                property bucket "$URSA_BUCKET"
                property prefix "$URSA_PREFIX"
                property cloudStorageEndpoint "$URSA_CLOUD_STORAGE_ENDPOINT"
                property region "$URSA_REGION"
                property s3AccessKeyId "$URSA_S3_ACCESS_KEY_ID"
                property s3SecretAccessKey "$URSA_S3_SECRET_ACCESS_KEY"
                property compactionBackendStorageType "$URSA_COMPACTION_BACKEND_STORAGE_TYPE"
                property compactionBucket "$URSA_COMPACTION_BUCKET"
                property compactionPrefix "$URSA_COMPACTION_PREFIX"
                property compactionBucketRegion "$URSA_COMPACTION_BUCKET_REGION"
                property entrySerDeType "$URSA_ENTRY_SERDE_TYPE"
                property compactedFileSizeLimit "$URSA_COMPACTED_FILE_SIZE_LIMIT"
                property tailCompactDataVisibilityIntervalInSeconds "$URSA_TAIL_VISIBILITY_INTERVAL_SECONDS"
                property refreshLocalTopicInternalInSeconds "$URSA_REFRESH_LOG_INTERVAL_SECONDS"
                property internalCompactionTaskPublisherEnabled "$URSA_INTERNAL_COMPACTION_TASK_PUBLISHER_ENABLED"
                property compactedThreadNum "$URSA_COMPACTED_THREAD_NUM"
                property publishThreadNum "$URSA_PUBLISH_THREAD_NUM"
                property commitThreadNum "$URSA_COMMIT_THREAD_NUM"
                property metastoreRequestRateLimitPerSecond "$URSA_METASTORE_RATE_LIMIT"
                exec java $URSA_JAVA_OPTS \
                  -Dio.netty.tryReflectionSetAccessible=true -Djava.net.preferIPv4Stack=true \
                  -cp '/opt/ursa/ursa-storage-compact.jar:/opt/ursa/lib/*' \
                  io.lakestream.ursa.compact.CompactionMain --conf "$conf"
                """;
    }

    private static KafkaClusterTestKit createCluster(String oxiaServiceAddress, URI s3Endpoint,
                                              LocalStackContainer localStackContainer, String s3Prefix) throws Exception {
        log.info("Creating cluster with S3 Ursa diskless storage enabled, Oxia at: {}, S3 endpoint: {}, prefix: {}",
                oxiaServiceAddress, s3Endpoint, s3Prefix);

        return enableBrokerRequestPipelining(new KafkaClusterTestKit.Builder(
                new TestKitNodes.Builder()
                        .setNumBrokerNodes(1)
                        .setNumControllerNodes(1)
                        .build()))
                .setConfigProp(ServerLogConfigs.URSA_STORAGE_ENABLE_CONFIG, "true")
                .setConfigProp(ServerLogConfigs.URSA_CATALOG_OXIA_SERVICE_URL_CONFIG,
                        "oxia://" + oxiaServiceAddress + "/default")
                .setConfigProp(ServerLogConfigs.URSA_OXIA_SERVICE_URL_CONFIG,
                        "oxia://" + oxiaServiceAddress + "/default")
                .setConfigProp(ServerLogConfigs.URSA_STORAGE_BACKEND_TYPE_CONFIG, "S3")
                // Used as S3 key prefix for Ursa storage
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

    private void verifyKafkaConsumerCanReadAllMessages(String bootstrapServers, String topicName) {
        Map<String, Boolean> expected = new HashMap<>();
        for (int i = 0; i < NUM_RECORDS; i++) {
            expected.put("value-" + i, Boolean.FALSE);
        }
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(createConsumerProperties(bootstrapServers))) {
            consumer.subscribe(java.util.Collections.singletonList(topicName));

            long deadlineMs = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(2);
            while (System.currentTimeMillis() < deadlineMs
                    && expected.values().stream().anyMatch(v -> !v)) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    String value = new String(record.value(), StandardCharsets.UTF_8);
                    if (expected.containsKey(value)) {
                        expected.put(value, Boolean.TRUE);
                    }
                }
            }
        }

        long received = expected.values().stream().filter(v -> v).count();
        assertTrue(received == NUM_RECORDS,
                "Expected to consume " + NUM_RECORDS + " records, but got " + received);

        for (Map.Entry<String, Boolean> entry : expected.entrySet()) {
            assertTrue(entry.getValue(), "Missing consumed record value: " + entry.getKey());
        }
        log.info("Kafka consumer verified: consumed {} records from topic {}", NUM_RECORDS, topicName);
    }

    private Properties createConsumerProperties(String bootstrapServers) {
        Properties props = new Properties();
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.ByteArrayDeserializer.class.getName());
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.ByteArrayDeserializer.class.getName());
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG,
                "ursa-lakehouse-compaction-e2e-" + UUID.randomUUID());
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        return props;
    }
}
