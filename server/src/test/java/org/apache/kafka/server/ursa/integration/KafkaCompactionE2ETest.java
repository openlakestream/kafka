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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
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
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import io.opentelemetry.api.OpenTelemetry;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.GetResult;
import io.oxia.client.api.OxiaClientBuilder;
import io.streamnative.oxia.testcontainers.OxiaContainer;
import io.streamnative.ursa.storage.EntryIndex;
import io.streamnative.ursa.storage.Position;
import io.streamnative.ursa.storage.StorageApi;
import io.streamnative.ursa.storage.UrsaStorage;
import io.streamnative.ursa.storage.impl.PulsarStorageConfig;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * E2E test for Kafka compaction with compacted index verification.
 *
 * <p>This test:
 * <ol>
 *   <li>Writes Kafka records to a topic with Ursa storage + ManagedLedger enabled</li>
 *   <li>Starts an external compactor container to compact WAL data to Parquet</li>
 *   <li>Verifies entry indexes show FileType.PARQUET after compaction</li>
 *   <li>Verifies Parquet files exist on disk</li>
 * </ol>
 */
@Timeout(value = 600, unit = TimeUnit.SECONDS)
@Tag("integration")
public class KafkaCompactionE2ETest extends UrsaStorageE2ETestBase {

    private static final int NUM_RECORDS = 100;
    private static final String S3_BUCKET = "kafka-ursa-storage";
    private static final String EXTERNAL_READER_FACTORY_CLASS =
            "io.streamnative.ursa.lakehouse.reader.LakehouseReaderFactory";
    private static final String EXTERNAL_READER_FACTORY_CLASS_PROP = "ursa.externalReaderFactoryClass";
    private static final String KOP_SCHEMA_REGISTRY_URL_PROP = "kopSchemaRegistryUrl";

    private static final String COMPACTOR_IMAGE_ENV = "URSA_COMPACTOR_IMAGE";
    private static final String SN_LICENSE_FILE_ENV = "SN_LICENSE_FILE";

    private static Network network;
    private static OxiaContainer oxiaContainer;
    private static LocalStackContainer localStackContainer;
    private static GenericContainer<?> schemaRegistryContainer;
    private static URI s3Endpoint;
    private static KafkaClusterTestKit cluster;
    private static String s3Prefix;
    private static PulsarStorageConfig pulsarConfig;
    private static AsyncOxiaClient verificationClient;
    private static String schemaRegistryBaseUrl;

    @BeforeAll
    static void startContainers() throws Exception {
        String compactorImage = System.getenv(COMPACTOR_IMAGE_ENV);
        Assumptions.assumeTrue(compactorImage != null && !compactorImage.isBlank(),
                "Set " + COMPACTOR_IMAGE_ENV + " to a compactor image built from ursa-storage/docker/Dockerfile");

        // Used as S3 key prefix (not a local path) for WAL and compacted data.
        s3Prefix = "kafka-compaction-e2e/" + UUID.randomUUID();

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

        schemaRegistryContainer = startSchemaRegistryContainer(network);
        schemaRegistryContainer.start();

        schemaRegistryBaseUrl = "http://"
                + schemaRegistryContainer.getHost()
                + ":"
                + schemaRegistryContainer.getMappedPort(8001);
        awaitSchemaRegistryReady(schemaRegistryBaseUrl);
        log.info("Schema registry started at: {}", schemaRegistryBaseUrl);

        // Create PulsarStorageConfig for compaction verification
        pulsarConfig = createCompactionConfig(
                oxiaContainer.getServiceAddress(),
                s3Endpoint,
                localStackContainer.getRegion(),
                S3_BUCKET,
                s3Prefix);

        verificationClient = OxiaClientBuilder.create(oxiaContainer.getServiceAddress())
                .namespace(ServerLogConfigs.URSA_STORAGE_NAMESPACE_DEFAULT)
                .asyncClient()
                .get();

        System.setProperty(EXTERNAL_READER_FACTORY_CLASS_PROP, EXTERNAL_READER_FACTORY_CLASS);
        System.setProperty(KOP_SCHEMA_REGISTRY_URL_PROP, schemaRegistryBaseUrl);

        cluster = createCluster(
                oxiaContainer.getServiceAddress(), s3Endpoint, localStackContainer, s3Prefix);
        cluster.format();
        cluster.startup();
        cluster.waitForReadyBrokers();
        log.info("Kafka cluster started with bootstrap servers: {}", cluster.bootstrapServers());
    }

    @AfterAll
    static void stopContainers() {
        System.clearProperty(EXTERNAL_READER_FACTORY_CLASS_PROP);
        System.clearProperty(KOP_SCHEMA_REGISTRY_URL_PROP);

        closeQuietly(verificationClient, "verification client");
        closeQuietly(cluster, "Kafka cluster");
        closeQuietly(schemaRegistryContainer, "schema registry container");
        closeQuietly(localStackContainer, "LocalStack container");
        closeQuietly(oxiaContainer, "Oxia container");
        closeQuietly(network, "Docker network");
    }

    @Test
    @DisplayName("Write, compact, and verify Parquet files")
    @SuppressWarnings("NPathComplexity")
    public void testWriteCompactAndVerifyParquetFiles() throws Exception {
        String topicName = uniqueTopicName("kafka-compaction-e2e-topic");

        createTopicWithUrsaStorage(topicName);
        writeRecords(cluster.bootstrapServers(), topicName);
        waitForManagedLedgerMetadata(topicName);

        String compactorImage = System.getenv(COMPACTOR_IMAGE_ENV);
        try (GenericContainer<?> compactor = startCompactorContainer(
                network,
                DockerImageName.parse(compactorImage),
                s3Prefix,
                localStackContainer.getRegion(),
                S3_BUCKET)) {
            waitForParquetCompaction(topicName);
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

    private void waitForManagedLedgerMetadata(String topicName) {
        String managedLedgerPath = "/managed-ledgers/public/default/persistent/" + topicName + "-partition-0";
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(() -> checkManagedLedgerExists(managedLedgerPath));
        log.info("ManagedLedger metadata verified in Oxia at path: {}", managedLedgerPath);
    }

    private boolean checkManagedLedgerExists(String path) {
        try {
            GetResult result = verificationClient.get(path).get(5, TimeUnit.SECONDS);
            return result != null && result.value() != null;
        } catch (Exception e) {
            log.debug("ManagedLedger metadata not yet available: {}", e.getMessage());
            return false;
        }
    }

    private void waitForParquetCompaction(String topicName) {
        log.info("Waiting for compaction output to be visible (max 5 minutes)...");
        Awaitility.await()
                .atMost(5, TimeUnit.MINUTES)
                .pollInterval(5, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    verifyReadsUseParquetIndexes(topicName);
                    verifyParquetFilesExistOnS3();
                });
    }

    private void verifyReadsUseParquetIndexes(String topicName) throws Exception {
        String streamKey = "public/default/persistent/" + topicName + "-partition-0";

        try (UrsaStorage ursaStorage = new UrsaStorage(pulsarConfig, OpenTelemetry.noop())) {
            StorageApi storageApi = ursaStorage.getDefaultStorageApi();

            long streamId = storageApi.getStreamIdByKey(streamKey).get(30, TimeUnit.SECONDS);
            EntryIndex lastEntry = storageApi.getLastEntry(streamId).get(30, TimeUnit.SECONDS);

            assertTrue(lastEntry != EntryIndex.NOT_FOUND, "Expected stream to have data, but last entry was NOT_FOUND");

            long lastOffset = lastEntry.header().offset() + lastEntry.header().numberOfMessages();
            List<EntryIndex> indexes = storageApi.readIndexes(streamId, 0, lastOffset, false).get(30, TimeUnit.SECONDS);
            assertTrue(!indexes.isEmpty(), "Expected indexes to be present for streamId=" + streamId);

            List<EntryIndex> parquetIndexes = indexes.stream()
                    .filter(idx -> idx.position().fileType() == Position.FileType.PARQUET)
                    .toList();
            assertTrue(!parquetIndexes.isEmpty(),
                    "Expected at least one PARQUET index after compaction, streamId=" + streamId);

            log.info("Verified PARQUET index usage: {} of {} indexes are PARQUET for streamId={}",
                    parquetIndexes.size(), indexes.size(), streamId);
        }
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

    private static PulsarStorageConfig createCompactionConfig(
            String oxiaServiceAddress,
            URI s3Endpoint,
            String region,
            String bucket,
            String prefix) {
        String oxiaUrl = "oxia://" + oxiaServiceAddress + "/" + ServerLogConfigs.URSA_STORAGE_NAMESPACE_DEFAULT;

        Properties props = new Properties();
        // Make parquet (s3a://...) resolvable for v2 parquet readers.
        props.setProperty("compactionBackendStorageType", "S3");
        props.setProperty("compactionBucket", bucket);
        props.setProperty("compactionPrefix", prefix);
        props.setProperty("cloudStorageEndpoint", s3Endpoint.toString());
        props.setProperty("compactionBucketRegion", region);

        return PulsarStorageConfig.builder()
                .oxiaPulsarStorageUrl(oxiaUrl)
                .metadataStoreUrl(oxiaUrl)
                .backendStorageType("S3")
                .bucket(bucket)
                .prefix(prefix)
                .cloudStorageEndpoint(s3Endpoint.toString())
                .region(region)
                .s3AccessKeyId("test")
                .s3SecretAccessKey("test")
                .dataSourceForCompaction("URSA")
                // Fast compaction for tests
                .compactedFileSizeLimit(10 * 1024) // 10KB - small for fast tests
                .tailCompactDataVisibilityIntervalInSeconds(5)
                .refreshLocalTopicInternalInSeconds(5)
                .compactedThreadNum(2)
                .publishThreadNum(2)
                .commitThreadNum(2)
                // Write buffer settings
                .writeBufferSize(256 * 1024)
                .writeBufferFlushIntervalMs(100)
                .metastoreRequestRateLimitPerSecond(500)
                .properties(props)
                .build();
    }

    private static GenericContainer<?> startCompactorContainer(
            Network network,
            DockerImageName compactorImage,
            String s3Prefix,
            String region,
            String bucket) {
        String oxiaUrl = "oxia://oxia:6648/" + ServerLogConfigs.URSA_STORAGE_NAMESPACE_DEFAULT;

        GenericContainer<?> compactor = new GenericContainer<>(compactorImage)
                .withNetwork(network)
                .withWorkingDirectory("/pulsar")
                .withCreateContainerCmdModifier(cmd -> cmd.withUser("root"))
                .withEnv("PULSAR_MEM", "-Xmx512M")
                .withEnv("PULSAR_GC", "-XX:+UseZGC")
                .withEnv("OTEL_SDK_DISABLED", "true")
                .withEnv("PULSAR_PREFIX_metadataStoreUrl", oxiaUrl)
                .withEnv("PULSAR_PREFIX_oxiaPulsarStorageUrl", oxiaUrl)
                .withEnv("PULSAR_PREFIX_backendStorageType", "S3")
                .withEnv("PULSAR_PREFIX_bucket", bucket)
                .withEnv("PULSAR_PREFIX_prefix", s3Prefix)
                .withEnv("PULSAR_PREFIX_cloudStorageEndpoint", "http://localstack:4566")
                .withEnv("PULSAR_PREFIX_region", region)
                .withEnv("PULSAR_PREFIX_s3AccessKeyId", "test")
                .withEnv("PULSAR_PREFIX_s3SecretAccessKey", "test")
                // V2 lakehouse writer/reader uses these properties for s3a:// path resolution.
                .withEnv("PULSAR_PREFIX_compactionBackendStorageType", "S3")
                .withEnv("PULSAR_PREFIX_compactionBucket", bucket)
                .withEnv("PULSAR_PREFIX_compactionPrefix", s3Prefix)
                .withEnv("PULSAR_PREFIX_compactionBucketRegion", region)
                .withEnv("PULSAR_PREFIX_dataSourceForCompaction", "URSA")
                .withEnv("PULSAR_PREFIX_compactedFileSizeLimit", String.valueOf(10 * 1024))
                .withEnv("PULSAR_PREFIX_tailCompactDataVisibilityIntervalInSeconds", "5")
                .withEnv("PULSAR_PREFIX_refreshLocalTopicInternalInSeconds", "5")
                .withEnv("PULSAR_PREFIX_compactedThreadNum", "2")
                .withEnv("PULSAR_PREFIX_publishThreadNum", "2")
                .withEnv("PULSAR_PREFIX_commitThreadNum", "2")
                .withEnv("PULSAR_PREFIX_metastoreRequestRateLimitPerSecond", "500")
                // Force V2 compaction worker for Kafka Parquet
                .withEnv("PULSAR_PREFIX_forceToUsePulsarCompactionWorker", "true")
                .withEnv("PULSAR_PREFIX_managedTableSchemaEvolutionEnabled", "true")
                .withEnv("PULSAR_PREFIX_entrySerDeType", "KAFKA_BATCHED_RAW_PARQUET")
                .withEnv("PULSAR_PREFIX_kopSchemaRegistryUrl", "http://schema-registry:8001")
                .withEnv("AWS_ACCESS_KEY_ID", "test")
                .withEnv("AWS_SECRET_ACCESS_KEY", "test")
                .withCommand("bash", "-c",
                        "set -euo pipefail; " +
                                "mkdir -p /mnt/sn-license; " +
                                "conf=/tmp/ursa_storage.conf; : > \"$conf\"; " +
                                "bin/apply-config-from-env.py \"$conf\"; " +
                                "java -Dio.netty.tryReflectionSetAccessible=true -Djava.net.preferIPv4Stack=true " +
                                "-cp '/pulsar/lib/*' io.streamnative.ursa.compact.CompactionMain --conf \"$conf\"")
                .waitingFor(new LogMessageWaitStrategy()
                        .withRegEx(".*was elected leader.*\\n")
                        .withStartupTimeout(Duration.ofMinutes(5)));

        String licenseFile = System.getenv(SN_LICENSE_FILE_ENV);
        if (licenseFile != null && !licenseFile.isBlank()) {
            compactor.withCopyFileToContainer(
                    MountableFile.forHostPath(licenseFile),
                    "/mnt/sn-license/license");
        }

        compactor.start();
        log.info("Compactor container started: image={}", compactorImage);
        return compactor;
    }

    private static GenericContainer<?> startSchemaRegistryContainer(Network network) {
        return new GenericContainer<>(DockerImageName.parse("streamnative/schema-registry:0.1.0"))
                .withCreateContainerCmdModifier(cmd -> cmd.withPlatform("linux/amd64"))
                .withNetwork(network)
                .withNetworkAliases("schema-registry")
                .withWorkingDirectory("/schema-registry")
                .withExposedPorts(8001)
                .withEnv("PULSAR_PREFIX_oxiaServiceUrl", "oxia://oxia:6648")
                .withEnv("PULSAR_PREFIX_schemaRegistryListeners", "http://0.0.0.0:8001")
                .withEnv("PULSAR_PREFIX_schemaRegistryProtocolMap", "HTTP:http")
                .withCommand("sh", "-c",
                        "set -euo pipefail; "
                                + "echo \"Starting Schema Registry...\"; "
                                + "bin/apply-config-from-env.py conf/sr.conf; "
                                + "bin/start.sh -d; "
                                + "echo \"Schema Registry started.\"; "
                                + "tail -f /dev/null")
                .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(3)));
    }

    private static void awaitSchemaRegistryReady(String baseUrl) {
        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(() -> {
                    HttpRequest request = HttpRequest.newBuilder()
                            .GET()
                            .uri(URI.create(baseUrl + "/subjects"))
                            .timeout(Duration.ofSeconds(5))
                            .build();
                    HttpResponse<Void> response = client.send(request, HttpResponse.BodyHandlers.discarding());
                    return response.statusCode() == 200;
                });
    }

    private static KafkaClusterTestKit createCluster(String oxiaServiceAddress, URI s3Endpoint,
                                              LocalStackContainer localStackContainer, String s3Prefix) throws Exception {
        log.info("Creating cluster with S3 Ursa storage + managed-ledger enabled, Oxia at: {}, S3 endpoint: {}, prefix: {}",
                oxiaServiceAddress, s3Endpoint, s3Prefix);

        return new KafkaClusterTestKit.Builder(
                new TestKitNodes.Builder()
                        .setNumBrokerNodes(1)
                        .setNumControllerNodes(1)
                        .build())
                .setConfigProp(ServerLogConfigs.URSA_STORAGE_ENABLE_CONFIG, "true")
                .setConfigProp(ServerLogConfigs.URSA_STORAGE_OXIA_SERVICE_URL_CONFIG, oxiaServiceAddress)
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
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG, "kafka-compaction-e2e-" + UUID.randomUUID());
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        return props;
    }
}
