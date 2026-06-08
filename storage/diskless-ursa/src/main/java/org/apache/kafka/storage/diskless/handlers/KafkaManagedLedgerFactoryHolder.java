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

import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.pulsar.broker.ServiceConfiguration;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;
import io.oxia.client.api.AsyncOxiaClient;
import io.streamnative.ursa.mledger.PersistentStorageWalManagedLedgerStorage;

/**
 * Holds a {@link ManagedLedgerFactory} instance backed by Ursa's StorageWalManagedLedger.
 */
final class KafkaManagedLedgerFactoryHolder implements Closeable {

    public static final String EXTERNAL_READER_FACTORY_CLASS_PROP = "externalReaderFactoryClass";
    public static final String LEGACY_SYSTEM_EXTERNAL_READER_FACTORY_CLASS_PROP = "ursa.externalReaderFactoryClass";
    public static final String KOP_SCHEMA_REGISTRY_URL_PROP = "kopSchemaRegistryUrl";
    public static final String KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_PROP =
            "kopSchemaRegistryHttpHeaderAuthorization";
    public static final String KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_FILE_PROP =
            "kopSchemaRegistryHttpHeaderAuthorizationFile";

    private static final String SERVICE_NAME = "kafka-diskless-storage";

    private final ManagedLedgerFactory managedLedgerFactory;
    private final PersistentStorageWalManagedLedgerStorage managedLedgerStorage;
    private final AsyncOxiaClient oxiaClient;
    private final OpenTelemetrySdk openTelemetrySdk;

    private KafkaManagedLedgerFactoryHolder(
            ManagedLedgerFactory managedLedgerFactory,
            PersistentStorageWalManagedLedgerStorage managedLedgerStorage,
            AsyncOxiaClient oxiaClient,
            OpenTelemetrySdk openTelemetrySdk) {
        this.managedLedgerFactory = managedLedgerFactory;
        this.managedLedgerStorage = managedLedgerStorage;
        this.oxiaClient = oxiaClient;
        this.openTelemetrySdk = openTelemetrySdk;
    }

    ManagedLedgerFactory factory() {
        return managedLedgerFactory;
    }

    AsyncOxiaClient oxiaClient() {
        return oxiaClient;
    }

    static ServiceConfiguration createServiceConfiguration(UrsaStorageConfig config) {
        final var serviceConfiguration = new ServiceConfiguration();
        final var pulsarOxiaUrl = config.getPulsarOxiaServiceUrl();
        Properties properties = buildManagedLedgerProperties(config);
        serviceConfiguration.setProperties(properties);
        serviceConfiguration.setMetadataStoreUrl(pulsarOxiaUrl.toString());
        serviceConfiguration.setConfigurationMetadataStoreUrl(pulsarOxiaUrl.toString());
        return serviceConfiguration;
    }

    static KafkaManagedLedgerFactoryHolder create(UrsaStorageConfig config) throws Exception {
        PersistentStorageWalManagedLedgerStorage managedLedgerStorage = null;
        OpenTelemetrySdk openTelemetrySdk = null;

        try {
            openTelemetrySdk = createOpenTelemetrySdk();
            managedLedgerStorage = new PersistentStorageWalManagedLedgerStorage();
            managedLedgerStorage.initialize(createServiceConfiguration(config), null, null, null, openTelemetrySdk);
            ManagedLedgerFactory managedLedgerFactory =
                    managedLedgerStorage.getDefaultStorageClass().getManagedLedgerFactory();

            AsyncOxiaClient oxiaClient = managedLedgerStorage.getUrsaStorage() != null
                    ? managedLedgerStorage.getUrsaStorage().getOxiaClient()
                    : null;
            if (oxiaClient == null) {
                throw new IllegalStateException("UrsaStorage oxia client is not initialized");
            }

            return new KafkaManagedLedgerFactoryHolder(
                    managedLedgerFactory, managedLedgerStorage, oxiaClient, openTelemetrySdk);
        } catch (Exception e) {
            try {
                if (managedLedgerStorage != null) {
                    managedLedgerStorage.close();
                }
            } catch (Exception ignored) {
            }
            if (openTelemetrySdk != null) {
                openTelemetrySdk.close();
            }
            throw e;
        }
    }

    static OpenTelemetrySdk createOpenTelemetrySdk() {
        return AutoConfiguredOpenTelemetrySdk.builder()
                .addPropertiesSupplier(KafkaManagedLedgerFactoryHolder::buildOpenTelemetryProperties)
                .build()
                .getOpenTelemetrySdk();
    }

    static Map<String, String> buildOpenTelemetryProperties() {
        Map<String, String> props = new HashMap<>();
        props.put("otel.service.name", SERVICE_NAME);
        props.put("otel.metrics.exporter", "none");
        props.put("otel.traces.exporter", "none");
        props.put("otel.logs.exporter", "none");
        // System properties override defaults
        for (String name : System.getProperties().stringPropertyNames()) {
            if (name.startsWith("otel.")) {
                props.put(name, System.getProperty(name));
            }
        }
        return props;
    }

    static Properties buildManagedLedgerProperties(UrsaStorageConfig config) {
        Properties properties = new Properties();
        String normalizedBackendType = normalizeBackendType(config.getBackendType());
        properties.setProperty("backendStorageType", normalizedBackendType);
        properties.setProperty("storagePath", config.getStoragePath());
        properties.setProperty("oxiaPulsarStorageUrl", config.getUrsaOxiaServiceUrl().toString());
        properties.setProperty("writeBufferFlushIntervalMs", String.valueOf(config.getWriteBufferFlushIntervalMs()));
        properties.setProperty("writeBufferSize", String.valueOf(config.getWriteBufferSize()));
        properties.setProperty("writeBufferFlushSize", String.valueOf(config.getWriteBufferFlushSize()));

        if (isRemoteBackend(normalizedBackendType)) {
            setIfNotEmpty(config.getS3Endpoint(), v -> properties.setProperty("cloudStorageEndpoint", v));
            setIfNotEmpty(config.getS3Bucket(), v -> properties.setProperty("bucket", v));
            setIfNotEmpty(config.getStoragePath(), v -> properties.setProperty("prefix", v));
            setIfNotEmpty(config.getS3Region(), v -> properties.setProperty("region", v));
        }

        if (isS3Backend(normalizedBackendType)) {
            setIfNotEmpty(config.getS3AccessKey(), v -> properties.setProperty("s3AccessKeyId", v));
            setIfNotEmpty(config.getS3SecretKey(), v -> properties.setProperty("s3SecretAccessKey", v));
            // Deprecated fields, keep for compatibility with older configs.
            setIfNotEmpty(config.getS3Bucket(), v -> properties.setProperty("s3Bucket", v));
            setIfNotEmpty(config.getStoragePath(), v -> properties.setProperty("s3Prefix", v));
            setIfNotEmpty(config.getS3Region(), v -> properties.setProperty("s3Region", v));
        }

        String externalReaderFactoryClass = firstNonBlank(
                config.getConfiguredExternalReaderFactoryClass(),
                System.getProperty(LEGACY_SYSTEM_EXTERNAL_READER_FACTORY_CLASS_PROP),
                UrsaStorageConfig.NOOP_EXTERNAL_READER_FACTORY_CLASS);
        properties.setProperty(EXTERNAL_READER_FACTORY_CLASS_PROP, externalReaderFactoryClass);
        if (!UrsaStorageConfig.NOOP_EXTERNAL_READER_FACTORY_CLASS.equals(externalReaderFactoryClass)) {
            // Required by LakehouseReaderFactory / KSNSchemaRegistry.
            String kopSchemaRegistryUrl = firstNonBlank(
                    config.getKopSchemaRegistryUrl(),
                    System.getProperty(KOP_SCHEMA_REGISTRY_URL_PROP),
                    null);
            if (kopSchemaRegistryUrl != null) {
                properties.setProperty(KOP_SCHEMA_REGISTRY_URL_PROP, kopSchemaRegistryUrl);
            }

            String kopSchemaRegistryHttpHeaderAuthorizationFile = firstNonBlank(
                    config.getKopSchemaRegistryHttpHeaderAuthorizationFile(),
                    System.getProperty(KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_FILE_PROP),
                    null);
            if (kopSchemaRegistryHttpHeaderAuthorizationFile != null) {
                properties.setProperty(
                        KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_FILE_PROP,
                        kopSchemaRegistryHttpHeaderAuthorizationFile);
            } else {
                String kopSchemaRegistryHttpHeaderAuthorization = firstNonBlank(
                        config.getKopSchemaRegistryHttpHeaderAuthorization(),
                        System.getProperty(KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_PROP),
                        null);
                if (kopSchemaRegistryHttpHeaderAuthorization != null) {
                    properties.setProperty(
                            KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_PROP,
                            kopSchemaRegistryHttpHeaderAuthorization);
                }
            }

            if (isRemoteBackend(normalizedBackendType)) {
                properties.setProperty("compactionBackendStorageType", normalizedBackendType);
                properties.setProperty("compactionBucket", config.getCompactionBucket());
                properties.setProperty("compactionPrefix", config.getCompactionPrefix());
                properties.setProperty("compactionBucketRegion", config.getS3Region());
                setIfNotEmpty(config.getS3Endpoint(), v -> properties.setProperty("cloudStorageEndpoint", v));
            }
        }
        return properties;
    }

    private static boolean isRemoteBackend(String normalizedBackendType) {
        return !"LOCAL".equals(normalizedBackendType);
    }

    private static boolean isS3Backend(String normalizedBackendType) {
        return "S3".equals(normalizedBackendType);
    }

    private static String normalizeBackendType(String backendType) {
        String normalizedBackendType = backendType.toUpperCase(Locale.ROOT);
        if ("AZURE_BLOB".equals(normalizedBackendType) || "AZUREBLOB".equals(normalizedBackendType)) {
            return "AZUREBLOB";
        }
        return normalizedBackendType;
    }

    private static void setIfNotEmpty(String value, java.util.function.Consumer<String> setter) {
        if (value != null && !value.isEmpty()) {
            setter.accept(value);
        }
    }

    private static String firstNonBlank(String first, String second, String defaultValue) {
        if (first != null && !first.isBlank()) {
            return first;
        }
        if (second != null && !second.isBlank()) {
            return second;
        }
        return defaultValue;
    }

    @Override
    public void close() throws IOException {
        try {
            if (managedLedgerStorage != null) {
                managedLedgerStorage.close();
            }
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            if (openTelemetrySdk != null) {
                openTelemetrySdk.close();
            }
        }
    }
}
