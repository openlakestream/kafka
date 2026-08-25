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
package org.apache.kafka.storage.diskless.handlers;

import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.test.TestUtils;

import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import io.oxia.client.api.AsyncOxiaClient;
import io.streamnative.ursa.mledger.StorageWalManagedLedgerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class KafkaManagedLedgerFactoryHolderTest {

    @Test
    void testTopicConfigOperationsAreSkippedForUnsupportedManagedLedgerFactory() throws Exception {
        ManagedLedgerFactory managedLedgerFactory = mock(ManagedLedgerFactory.class);

        KafkaManagedLedgerFactoryHolder holder = new KafkaManagedLedgerFactoryHolder(
                managedLedgerFactory, null, null, null);

        assertSame(managedLedgerFactory, holder.factory());
        holder.asyncUpdateTopicConfig("public/default/persistent/topic-partition-0", Map.of("key", "value"))
                .get();
        holder.asyncDeleteTopicConfig("public/default/persistent/topic-partition-0").get();
        verifyNoInteractions(managedLedgerFactory);
    }

    @Test
    void testTopicConfigOperationsDelegateToStorageWalManagedLedgerFactory() {
        StorageWalManagedLedgerFactory managedLedgerFactory = mock(StorageWalManagedLedgerFactory.class);
        String mlName = "public/default/persistent/topic-partition-0";
        Map<String, String> topicConfig = Map.of("key", "value");
        var updateFuture = CompletableFuture.<Void>completedFuture(null);
        var deleteFuture = CompletableFuture.<Void>completedFuture(null);
        when(managedLedgerFactory.asyncUpdateTopicConfig(mlName, topicConfig)).thenReturn(updateFuture);
        when(managedLedgerFactory.asyncDeleteTopicConfig(mlName)).thenReturn(deleteFuture);
        KafkaManagedLedgerFactoryHolder holder = new KafkaManagedLedgerFactoryHolder(
                managedLedgerFactory, null, null, null);

        assertSame(updateFuture, holder.asyncUpdateTopicConfig(mlName, topicConfig));
        assertSame(deleteFuture, holder.asyncDeleteTopicConfig(mlName));
        verify(managedLedgerFactory).asyncUpdateTopicConfig(mlName, topicConfig);
        verify(managedLedgerFactory).asyncDeleteTopicConfig(mlName);
    }

    @Test
    void testLateOxiaClientIsClosedAfterHolderCreationFails() throws Exception {
        CompletableFuture<AsyncOxiaClient> clientFuture = new CompletableFuture<>();
        AsyncOxiaClient client = mock(AsyncOxiaClient.class);

        KafkaManagedLedgerFactoryHolder.closeOxiaClientAfterFailedCreate(
                null, clientFuture, new Exception("creation failed"));
        clientFuture.complete(client);

        verify(client).close();
    }

    @BeforeEach
    void setUp() {
        clearSystemProperties();
    }

    @AfterEach
    void tearDown() {
        clearSystemProperties();
    }

    private static void clearSystemProperties() {
        System.clearProperty(KafkaManagedLedgerFactoryHolder.LEGACY_SYSTEM_EXTERNAL_READER_FACTORY_CLASS_PROP);
        System.clearProperty(KafkaManagedLedgerFactoryHolder.KOP_SCHEMA_REGISTRY_URL_PROP);
        System.clearProperty(KafkaManagedLedgerFactoryHolder.KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_PROP);
        System.clearProperty(KafkaManagedLedgerFactoryHolder.KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_FILE_PROP);
        System.clearProperty("otel.service.name");
        System.clearProperty("otel.metrics.exporter");
        System.clearProperty("otel.traces.exporter");
        System.clearProperty("otel.logs.exporter");
        System.clearProperty("otel.exporter.otlp.endpoint");
        System.clearProperty("otel.sdk.disabled");
        System.clearProperty("otel.java.disabled.resource.providers");
        System.clearProperty("otel.exporter.prometheus.port");
    }

    @Test
    void testDoesNotInjectSchemaRegistryConfigWhenExternalReaderFactoryIsNoop() throws Exception {
        UrsaStorageConfig config = UrsaStorageConfig.fromConfigs(Map.of());

        System.setProperty(KafkaManagedLedgerFactoryHolder.KOP_SCHEMA_REGISTRY_URL_PROP, "http://example:8001");
        System.setProperty(KafkaManagedLedgerFactoryHolder.KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_PROP, "Bearer token");
        System.setProperty(KafkaManagedLedgerFactoryHolder.KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_FILE_PROP,
                "/mnt/secrets/token");

        Properties properties = KafkaManagedLedgerFactoryHolder.buildManagedLedgerProperties(config);

        assertEquals(UrsaStorageConfig.NOOP_EXTERNAL_READER_FACTORY_CLASS,
                properties.getProperty(KafkaManagedLedgerFactoryHolder.EXTERNAL_READER_FACTORY_CLASS_PROP));
        assertFalse(properties.containsKey(KafkaManagedLedgerFactoryHolder.KOP_SCHEMA_REGISTRY_URL_PROP));
        assertFalse(properties.containsKey(KafkaManagedLedgerFactoryHolder.KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_PROP));
        assertFalse(properties.containsKey(KafkaManagedLedgerFactoryHolder.KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_FILE_PROP));
    }

    @Test
    void testInjectsSchemaRegistryAuthorizationWhenExternalReaderConfigured() throws Exception {
        UrsaStorageConfig config = UrsaStorageConfig.fromConfigs(Map.of(
                ServerLogConfigs.URSA_STORAGE_EXTERNAL_READER_FACTORY_CLASS_CONFIG,
                        "io.streamnative.ursa.lakehouse.reader.LakehouseReaderFactory",
                ServerLogConfigs.URSA_STORAGE_KOP_SCHEMA_REGISTRY_URL_CONFIG, "http://example:8001",
                ServerLogConfigs.URSA_STORAGE_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_CONFIG, "Bearer token"
        ));

        Properties properties = KafkaManagedLedgerFactoryHolder.buildManagedLedgerProperties(config);

        assertEquals(
                "io.streamnative.ursa.lakehouse.reader.LakehouseReaderFactory",
                properties.getProperty(KafkaManagedLedgerFactoryHolder.EXTERNAL_READER_FACTORY_CLASS_PROP));
        assertEquals("http://example:8001",
                properties.getProperty(KafkaManagedLedgerFactoryHolder.KOP_SCHEMA_REGISTRY_URL_PROP));
        assertEquals("Bearer token",
                properties.getProperty(KafkaManagedLedgerFactoryHolder.KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_PROP));
        assertFalse(properties.containsKey(KafkaManagedLedgerFactoryHolder.KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_FILE_PROP));
    }

    @Test
    void testAuthorizationFileTakesPrecedenceOverAuthorizationValue() throws Exception {
        UrsaStorageConfig config = UrsaStorageConfig.fromConfigs(Map.of(
                ServerLogConfigs.URSA_STORAGE_EXTERNAL_READER_FACTORY_CLASS_CONFIG,
                        "io.streamnative.ursa.lakehouse.reader.LakehouseReaderFactory",
                ServerLogConfigs.URSA_STORAGE_KOP_SCHEMA_REGISTRY_URL_CONFIG, "http://example:8001",
                ServerLogConfigs.URSA_STORAGE_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_CONFIG, "Bearer token",
                ServerLogConfigs.URSA_STORAGE_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_FILE_CONFIG,
                        "/mnt/secrets/token"
        ));

        Properties properties = KafkaManagedLedgerFactoryHolder.buildManagedLedgerProperties(config);

        assertEquals("http://example:8001",
                properties.getProperty(KafkaManagedLedgerFactoryHolder.KOP_SCHEMA_REGISTRY_URL_PROP));
        assertEquals(
                "/mnt/secrets/token",
                properties.getProperty(KafkaManagedLedgerFactoryHolder.KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_FILE_PROP));
        assertFalse(properties.containsKey(KafkaManagedLedgerFactoryHolder.KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_PROP));
    }

    @Test
    void testBlankAuthorizationIsIgnored() throws Exception {
        UrsaStorageConfig config = UrsaStorageConfig.fromConfigs(Map.of(
                ServerLogConfigs.URSA_STORAGE_EXTERNAL_READER_FACTORY_CLASS_CONFIG,
                        "io.streamnative.ursa.lakehouse.reader.LakehouseReaderFactory",
                ServerLogConfigs.URSA_STORAGE_KOP_SCHEMA_REGISTRY_URL_CONFIG, "http://example:8001",
                ServerLogConfigs.URSA_STORAGE_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_CONFIG, "   "
        ));

        Properties properties = KafkaManagedLedgerFactoryHolder.buildManagedLedgerProperties(config);

        assertEquals("http://example:8001",
                properties.getProperty(KafkaManagedLedgerFactoryHolder.KOP_SCHEMA_REGISTRY_URL_PROP));
        assertFalse(properties.containsKey(KafkaManagedLedgerFactoryHolder.KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_PROP));
        assertFalse(properties.containsKey(KafkaManagedLedgerFactoryHolder.KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_FILE_PROP));
    }

    @Test
    void testBrokerConfigOverridesLegacySystemPropertyFallback() throws Exception {
        UrsaStorageConfig config = UrsaStorageConfig.fromConfigs(Map.of(
                ServerLogConfigs.URSA_STORAGE_EXTERNAL_READER_FACTORY_CLASS_CONFIG,
                        "io.streamnative.ursa.lakehouse.reader.ConfigReaderFactory",
                ServerLogConfigs.URSA_STORAGE_KOP_SCHEMA_REGISTRY_URL_CONFIG, "http://config:8001",
                ServerLogConfigs.URSA_STORAGE_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_CONFIG,
                        "Bearer config-token"
        ));

        System.setProperty(KafkaManagedLedgerFactoryHolder.LEGACY_SYSTEM_EXTERNAL_READER_FACTORY_CLASS_PROP,
                "io.streamnative.ursa.lakehouse.reader.LegacyReaderFactory");
        System.setProperty(KafkaManagedLedgerFactoryHolder.KOP_SCHEMA_REGISTRY_URL_PROP, "http://legacy:8001");
        System.setProperty(KafkaManagedLedgerFactoryHolder.KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_PROP,
                "Bearer legacy-token");

        Properties properties = KafkaManagedLedgerFactoryHolder.buildManagedLedgerProperties(config);

        assertEquals("io.streamnative.ursa.lakehouse.reader.ConfigReaderFactory",
                properties.getProperty(KafkaManagedLedgerFactoryHolder.EXTERNAL_READER_FACTORY_CLASS_PROP));
        assertEquals("http://config:8001",
                properties.getProperty(KafkaManagedLedgerFactoryHolder.KOP_SCHEMA_REGISTRY_URL_PROP));
        assertEquals("Bearer config-token",
                properties.getProperty(KafkaManagedLedgerFactoryHolder.KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_PROP));
    }

    @Test
    void testLegacySystemPropertyFallbackStillWorks() throws Exception {
        UrsaStorageConfig config = UrsaStorageConfig.fromConfigs(Map.of());

        System.setProperty(KafkaManagedLedgerFactoryHolder.LEGACY_SYSTEM_EXTERNAL_READER_FACTORY_CLASS_PROP,
                "io.streamnative.ursa.lakehouse.reader.LakehouseReaderFactory");
        System.setProperty(KafkaManagedLedgerFactoryHolder.KOP_SCHEMA_REGISTRY_URL_PROP, "http://legacy:8001");
        System.setProperty(KafkaManagedLedgerFactoryHolder.KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_PROP,
                "Bearer legacy-token");

        Properties properties = KafkaManagedLedgerFactoryHolder.buildManagedLedgerProperties(config);

        assertEquals("io.streamnative.ursa.lakehouse.reader.LakehouseReaderFactory",
                properties.getProperty(KafkaManagedLedgerFactoryHolder.EXTERNAL_READER_FACTORY_CLASS_PROP));
        assertEquals("http://legacy:8001",
                properties.getProperty(KafkaManagedLedgerFactoryHolder.KOP_SCHEMA_REGISTRY_URL_PROP));
        assertEquals("Bearer legacy-token",
                properties.getProperty(KafkaManagedLedgerFactoryHolder.KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_PROP));
    }

    @Test
    void testGcsBackendUsesGenericObjectStorageProperties() throws Exception {
        UrsaStorageConfig config = UrsaStorageConfig.fromConfigs(Map.of(
                ServerLogConfigs.URSA_STORAGE_BACKEND_TYPE_CONFIG, "GCS",
                ServerLogConfigs.URSA_STORAGE_PATH_CONFIG, "gcs-prefix",
                ServerLogConfigs.URSA_STORAGE_S3_ENDPOINT_CONFIG, "http://fake-gcs:4443",
                ServerLogConfigs.URSA_STORAGE_S3_BUCKET_CONFIG, "gcs-bucket",
                ServerLogConfigs.URSA_STORAGE_S3_REGION_CONFIG, "us-central1"
        ));

        Properties properties = KafkaManagedLedgerFactoryHolder.buildManagedLedgerProperties(config);

        assertEquals("GCS", properties.getProperty("backendStorageType"));
        assertEquals("gcs-bucket", properties.getProperty("bucket"));
        assertEquals("gcs-prefix", properties.getProperty("prefix"));
        assertEquals("us-central1", properties.getProperty("region"));
        assertEquals("http://fake-gcs:4443", properties.getProperty("cloudStorageEndpoint"));
        assertFalse(properties.containsKey("s3AccessKeyId"));
        assertFalse(properties.containsKey("s3SecretAccessKey"));
    }

    @Test
    void testAzureBackendAliasesNormalizeToUrsaValue() throws Exception {
        verifyAzureBackendAlias("AZURE_BLOB");
        verifyAzureBackendAlias("AZUREBLOB");
    }

    @Test
    void testS3BackendStillIncludesS3SpecificCredentials() throws Exception {
        UrsaStorageConfig config = UrsaStorageConfig.fromConfigs(Map.of(
                ServerLogConfigs.URSA_STORAGE_BACKEND_TYPE_CONFIG, "S3",
                ServerLogConfigs.URSA_STORAGE_PATH_CONFIG, "s3-prefix",
                ServerLogConfigs.URSA_STORAGE_S3_ENDPOINT_CONFIG, "http://localstack:4566",
                ServerLogConfigs.URSA_STORAGE_S3_BUCKET_CONFIG, "s3-bucket",
                ServerLogConfigs.URSA_STORAGE_S3_REGION_CONFIG, "us-east-1",
                ServerLogConfigs.URSA_STORAGE_S3_ACCESS_KEY_CONFIG, "access",
                ServerLogConfigs.URSA_STORAGE_S3_SECRET_KEY_CONFIG, "secret"
        ));

        Properties properties = KafkaManagedLedgerFactoryHolder.buildManagedLedgerProperties(config);

        assertEquals("S3", properties.getProperty("backendStorageType"));
        assertEquals("access", properties.getProperty("s3AccessKeyId"));
        assertEquals("secret", properties.getProperty("s3SecretAccessKey"));
        assertEquals("s3-bucket", properties.getProperty("s3Bucket"));
        assertEquals("s3-prefix", properties.getProperty("s3Prefix"));
        assertEquals("us-east-1", properties.getProperty("s3Region"));
    }

    // Verify "pulsar.storage.oxia.service.url" and "ursa.storage.oxia.service.url" are correctly parsed
    @Test
    void testOxiaServiceUrls() throws Exception {
        // Default
        verifyOxiaUrls("", "oxia://localhost:6648/default", "oxia://localhost:6648/default");

        // Inherited from the deprecated "ursa.storage.oxia.service.url" and "ursa.storage.namespace" configs when
        // the configs are not set
        verifyOxiaUrls("""
                ursa.storage.oxia.service.url=localhost:1111
                ursa.storage.namespace=ns
                """, "oxia://localhost:1111/ns", "oxia://localhost:1111/ns");

        // The deprecated configs are ignored if the corresponding config is set
        verifyOxiaUrls("""
                ursa.storage.oxia.service.url=localhost:1111
                ursa.storage.namespace=ns
                pulsar.oxia.service.url=oxia://localhost:2222/ns2
                """, "oxia://localhost:2222/ns2", "oxia://localhost:1111/ns");
        verifyOxiaUrls("""
                ursa.storage.oxia.service.url=localhost:1111
                ursa.storage.namespace=ns
                ursa.oxia.service.url=oxia://localhost:2222/ns2
                """, "oxia://localhost:1111/ns", "oxia://localhost:2222/ns2");
        verifyOxiaUrls("""
                ursa.storage.oxia.service.url=localhost:1111
                ursa.storage.namespace=ns
                pulsar.oxia.service.url=oxia://localhost:2222/ns2
                ursa.oxia.service.url=oxia://localhost:3333/ns3
                """, "oxia://localhost:2222/ns2", "oxia://localhost:3333/ns3");
    }

    private static void verifyOxiaUrls(String content, String expectedPulsarOxiaUrl, String expectedUrsaOxiaUrl) throws Exception {
        final var tempFile = TestUtils.tempFile();
        try {
            Files.writeString(tempFile.toPath(), content);
            final var props = Utils.loadProps(tempFile.getPath(), null);
            final var serviceConfig = KafkaManagedLedgerFactoryHolder.createServiceConfiguration(
                    UrsaStorageConfig.fromConfigs(Utils.propsToMap(props)));
            assertEquals(expectedPulsarOxiaUrl, serviceConfig.getMetadataStoreUrl());
            assertEquals(expectedUrsaOxiaUrl, serviceConfig.getProperties().getProperty("oxiaPulsarStorageUrl"));
        } finally {
            Files.deleteIfExists(tempFile.toPath());
        }
    }

    private static void verifyAzureBackendAlias(String backendType) throws Exception {
        UrsaStorageConfig config = UrsaStorageConfig.fromConfigs(Map.of(
                ServerLogConfigs.URSA_STORAGE_BACKEND_TYPE_CONFIG, backendType,
                ServerLogConfigs.URSA_STORAGE_PATH_CONFIG, "azure-prefix",
                ServerLogConfigs.URSA_STORAGE_S3_ENDPOINT_CONFIG, "https://account.blob.core.windows.net",
                ServerLogConfigs.URSA_STORAGE_S3_BUCKET_CONFIG, "account@container",
                ServerLogConfigs.URSA_STORAGE_S3_REGION_CONFIG, "unused-region"
        ));

        System.setProperty(KafkaManagedLedgerFactoryHolder.LEGACY_SYSTEM_EXTERNAL_READER_FACTORY_CLASS_PROP,
                "io.streamnative.ursa.lakehouse.reader.LakehouseReaderFactory");

        Properties properties = KafkaManagedLedgerFactoryHolder.buildManagedLedgerProperties(config);

        assertEquals("AZUREBLOB", properties.getProperty("backendStorageType"));
        assertEquals("account@container", properties.getProperty("bucket"));
        assertEquals("azure-prefix", properties.getProperty("prefix"));
        assertEquals("unused-region", properties.getProperty("region"));
        assertEquals("https://account.blob.core.windows.net", properties.getProperty("cloudStorageEndpoint"));
        assertEquals("AZUREBLOB", properties.getProperty("compactionBackendStorageType"));
        assertEquals("account@container", properties.getProperty("compactionBucket"));
        assertEquals("/tmp/compaction-data", properties.getProperty("compactionPrefix"));
        assertEquals("unused-region", properties.getProperty("compactionBucketRegion"));
        assertFalse(properties.containsKey("s3AccessKeyId"));
        assertFalse(properties.containsKey("s3SecretAccessKey"));
    }

    @Test
    void testOpenTelemetryPropertiesDefaults() {
        Map<String, String> props = KafkaManagedLedgerFactoryHolder.buildOpenTelemetryProperties();
        assertEquals("kafka-diskless-storage", props.get("otel.service.name"));
        assertEquals("none", props.get("otel.metrics.exporter"));
        assertEquals("none", props.get("otel.traces.exporter"));
        assertEquals("none", props.get("otel.logs.exporter"));
    }

    @Test
    void testOpenTelemetryPropertiesSystemPropertyOverridesDefaults() {
        System.setProperty("otel.service.name", "custom-service");
        System.setProperty("otel.metrics.exporter", "otlp");
        System.setProperty("otel.traces.exporter", "otlp");
        System.setProperty("otel.logs.exporter", "otlp");

        Map<String, String> props = KafkaManagedLedgerFactoryHolder.buildOpenTelemetryProperties();
        assertEquals("custom-service", props.get("otel.service.name"));
        assertEquals("otlp", props.get("otel.metrics.exporter"));
        assertEquals("otlp", props.get("otel.traces.exporter"));
        assertEquals("otlp", props.get("otel.logs.exporter"));
    }

    @Test
    void testOpenTelemetryPropertiesIncludesAdditionalSystemProperties() {
        System.setProperty("otel.exporter.otlp.endpoint", "http://collector:4317");

        Map<String, String> props = KafkaManagedLedgerFactoryHolder.buildOpenTelemetryProperties();
        assertEquals("http://collector:4317", props.get("otel.exporter.otlp.endpoint"));
        // Defaults are still present
        assertEquals("kafka-diskless-storage", props.get("otel.service.name"));
        assertEquals("none", props.get("otel.metrics.exporter"));
    }

    @Test
    void testOpenTelemetryPropertiesPrometheusExporterViaSystemProperties() {
        System.setProperty("otel.sdk.disabled", "false");
        System.setProperty("otel.java.disabled.resource.providers",
                "io.opentelemetry.instrumentation.resources.ProcessResourceProvider");
        System.setProperty("otel.metrics.exporter", "prometheus");
        System.setProperty("otel.exporter.prometheus.port", "9464");

        Map<String, String> props = KafkaManagedLedgerFactoryHolder.buildOpenTelemetryProperties();
        assertEquals("false", props.get("otel.sdk.disabled"));
        assertEquals("io.opentelemetry.instrumentation.resources.ProcessResourceProvider",
                props.get("otel.java.disabled.resource.providers"));
        assertEquals("prometheus", props.get("otel.metrics.exporter"));
        assertEquals("9464", props.get("otel.exporter.prometheus.port"));
        // Other defaults remain
        assertEquals("kafka-diskless-storage", props.get("otel.service.name"));
        assertEquals("none", props.get("otel.traces.exporter"));
        assertEquals("none", props.get("otel.logs.exporter"));
    }
}
