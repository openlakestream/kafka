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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class KafkaManagedLedgerFactoryHolderTest {

    private static final String SYSTEM_EXTERNAL_READER_FACTORY_CLASS_PROP = "ursa.externalReaderFactoryClass";
    private static final String EXTERNAL_READER_FACTORY_CLASS_PROP = "externalReaderFactoryClass";
    private static final String NOOP_EXTERNAL_READER_FACTORY_CLASS =
            "io.streamnative.ursa.mledger.reader.NoopExternalReaderFactory";
    private static final String SYSTEM_KOP_SCHEMA_REGISTRY_URL_PROP = "kopSchemaRegistryUrl";
    private static final String SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_PROP =
            "kopSchemaRegistryHttpHeaderAuthorization";
    private static final String SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_FILE_PROP =
            "kopSchemaRegistryHttpHeaderAuthorizationFile";

    @BeforeEach
    void setUp() {
        clearSystemProperties();
    }

    @AfterEach
    void tearDown() {
        clearSystemProperties();
    }

    private static void clearSystemProperties() {
        System.clearProperty(SYSTEM_EXTERNAL_READER_FACTORY_CLASS_PROP);
        System.clearProperty(SYSTEM_KOP_SCHEMA_REGISTRY_URL_PROP);
        System.clearProperty(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_PROP);
        System.clearProperty(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_FILE_PROP);
    }

    @Test
    void testDoesNotInjectSchemaRegistryConfigWhenExternalReaderFactoryIsNoop() throws Exception {
        UrsaStorageConfig config = UrsaStorageConfig.fromConfigs(Map.of());

        System.setProperty(SYSTEM_KOP_SCHEMA_REGISTRY_URL_PROP, "http://example:8001");
        System.setProperty(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_PROP, "Bearer token");
        System.setProperty(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_FILE_PROP, "/mnt/secrets/token");

        Properties properties = KafkaManagedLedgerFactoryHolder.buildManagedLedgerProperties(config);

        assertEquals(NOOP_EXTERNAL_READER_FACTORY_CLASS, properties.getProperty(EXTERNAL_READER_FACTORY_CLASS_PROP));
        assertFalse(properties.containsKey(SYSTEM_KOP_SCHEMA_REGISTRY_URL_PROP));
        assertFalse(properties.containsKey(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_PROP));
        assertFalse(properties.containsKey(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_FILE_PROP));
    }

    @Test
    void testInjectsSchemaRegistryAuthorizationWhenExternalReaderEnabled() throws Exception {
        UrsaStorageConfig config = UrsaStorageConfig.fromConfigs(Map.of());

        System.setProperty(SYSTEM_EXTERNAL_READER_FACTORY_CLASS_PROP,
                "io.streamnative.ursa.lakehouse.reader.LakehouseReaderFactory");
        System.setProperty(SYSTEM_KOP_SCHEMA_REGISTRY_URL_PROP, "http://example:8001");
        System.setProperty(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_PROP, "Bearer token");

        Properties properties = KafkaManagedLedgerFactoryHolder.buildManagedLedgerProperties(config);

        assertEquals(
                "io.streamnative.ursa.lakehouse.reader.LakehouseReaderFactory",
                properties.getProperty(EXTERNAL_READER_FACTORY_CLASS_PROP));
        assertEquals("http://example:8001", properties.getProperty(SYSTEM_KOP_SCHEMA_REGISTRY_URL_PROP));
        assertEquals("Bearer token", properties.getProperty(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_PROP));
        assertFalse(properties.containsKey(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_FILE_PROP));
    }

    @Test
    void testAuthorizationFileTakesPrecedenceOverAuthorizationValue() throws Exception {
        UrsaStorageConfig config = UrsaStorageConfig.fromConfigs(Map.of());

        System.setProperty(SYSTEM_EXTERNAL_READER_FACTORY_CLASS_PROP,
                "io.streamnative.ursa.lakehouse.reader.LakehouseReaderFactory");
        System.setProperty(SYSTEM_KOP_SCHEMA_REGISTRY_URL_PROP, "http://example:8001");
        System.setProperty(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_PROP, "Bearer token");
        System.setProperty(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_FILE_PROP, "/mnt/secrets/token");

        Properties properties = KafkaManagedLedgerFactoryHolder.buildManagedLedgerProperties(config);

        assertEquals("http://example:8001", properties.getProperty(SYSTEM_KOP_SCHEMA_REGISTRY_URL_PROP));
        assertEquals(
                "/mnt/secrets/token",
                properties.getProperty(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_FILE_PROP));
        assertFalse(properties.containsKey(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_PROP));
    }

    @Test
    void testBlankAuthorizationIsIgnored() throws Exception {
        UrsaStorageConfig config = UrsaStorageConfig.fromConfigs(Map.of());

        System.setProperty(SYSTEM_EXTERNAL_READER_FACTORY_CLASS_PROP,
                "io.streamnative.ursa.lakehouse.reader.LakehouseReaderFactory");
        System.setProperty(SYSTEM_KOP_SCHEMA_REGISTRY_URL_PROP, "http://example:8001");
        System.setProperty(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_PROP, "   ");

        Properties properties = KafkaManagedLedgerFactoryHolder.buildManagedLedgerProperties(config);

        assertEquals("http://example:8001", properties.getProperty(SYSTEM_KOP_SCHEMA_REGISTRY_URL_PROP));
        assertFalse(properties.containsKey(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_PROP));
        assertFalse(properties.containsKey(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_FILE_PROP));
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

        System.setProperty(SYSTEM_EXTERNAL_READER_FACTORY_CLASS_PROP,
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
}
