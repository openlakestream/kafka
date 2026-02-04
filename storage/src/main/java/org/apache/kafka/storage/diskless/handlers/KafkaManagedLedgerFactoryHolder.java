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
import java.util.Properties;

import io.opentelemetry.api.OpenTelemetry;
import io.oxia.client.api.AsyncOxiaClient;
import io.streamnative.ursa.mledger.PersistentStorageWalManagedLedgerStorage;

/**
 * Holds a {@link ManagedLedgerFactory} instance backed by Ursa's StorageWalManagedLedger.
 */
final class KafkaManagedLedgerFactoryHolder implements Closeable {

    private static final String EXTERNAL_READER_FACTORY_CLASS_PROP = "externalReaderFactoryClass";
    private static final String NOOP_EXTERNAL_READER_FACTORY_CLASS =
            "io.streamnative.ursa.mledger.reader.NoopExternalReaderFactory";
    private static final String SYSTEM_EXTERNAL_READER_FACTORY_CLASS_PROP = "ursa.externalReaderFactoryClass";
    private static final String SYSTEM_KOP_SCHEMA_REGISTRY_URL_PROP = "kopSchemaRegistryUrl";

    private final ManagedLedgerFactory managedLedgerFactory;
    private final PersistentStorageWalManagedLedgerStorage managedLedgerStorage;
    private final AsyncOxiaClient oxiaClient;

    private KafkaManagedLedgerFactoryHolder(
            ManagedLedgerFactory managedLedgerFactory,
            PersistentStorageWalManagedLedgerStorage managedLedgerStorage,
            AsyncOxiaClient oxiaClient) {
        this.managedLedgerFactory = managedLedgerFactory;
        this.managedLedgerStorage = managedLedgerStorage;
        this.oxiaClient = oxiaClient;
    }

    ManagedLedgerFactory factory() {
        return managedLedgerFactory;
    }

    AsyncOxiaClient oxiaClient() {
        return oxiaClient;
    }

    static KafkaManagedLedgerFactoryHolder create(UrsaStorageConfig config) throws Exception {
        PersistentStorageWalManagedLedgerStorage managedLedgerStorage = null;

        try {
            ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
            String oxiaUrl = formatOxiaUrl(config);
            Properties properties = new Properties();
            properties.setProperty("backendStorageType", config.getBackendType());
            properties.setProperty("storagePath", config.getStoragePath());
            properties.setProperty("oxiaPulsarStorageUrl", oxiaUrl);
            properties.setProperty("metadataStoreUrl", oxiaUrl);
            properties.setProperty("writeBufferFlushIntervalMs", String.valueOf(config.getWriteBufferFlushIntervalMs()));
            properties.setProperty("writeBufferSize", String.valueOf(config.getWriteBufferSize()));
            properties.setProperty("writeBufferFlushSize", String.valueOf(config.getWriteBufferFlushSize()));

            if ("S3".equalsIgnoreCase(config.getBackendType())) {
                setIfNotEmpty(config.getS3Endpoint(), v -> properties.setProperty("cloudStorageEndpoint", v));
                setIfNotEmpty(config.getS3AccessKey(), v -> properties.setProperty("s3AccessKeyId", v));
                setIfNotEmpty(config.getS3SecretKey(), v -> properties.setProperty("s3SecretAccessKey", v));
                setIfNotEmpty(config.getS3Bucket(), v -> properties.setProperty("bucket", v));
                setIfNotEmpty(config.getStoragePath(), v -> properties.setProperty("prefix", v));
                setIfNotEmpty(config.getS3Region(), v -> properties.setProperty("region", v));
                // Deprecated fields, keep for compatibility with older configs.
                setIfNotEmpty(config.getS3Bucket(), v -> properties.setProperty("s3Bucket", v));
                setIfNotEmpty(config.getStoragePath(), v -> properties.setProperty("s3Prefix", v));
                setIfNotEmpty(config.getS3Region(), v -> properties.setProperty("s3Region", v));
            }

            String externalReaderFactoryClass =
                    System.getProperty(SYSTEM_EXTERNAL_READER_FACTORY_CLASS_PROP, NOOP_EXTERNAL_READER_FACTORY_CLASS);
            properties.setProperty(EXTERNAL_READER_FACTORY_CLASS_PROP, externalReaderFactoryClass);
            if (!NOOP_EXTERNAL_READER_FACTORY_CLASS.equals(externalReaderFactoryClass)) {
                // Required by LakehouseReaderFactory / KSNSchemaRegistry.
                String kopSchemaRegistryUrl = System.getProperty(SYSTEM_KOP_SCHEMA_REGISTRY_URL_PROP);
                if (kopSchemaRegistryUrl != null && !kopSchemaRegistryUrl.isBlank()) {
                    properties.setProperty(SYSTEM_KOP_SCHEMA_REGISTRY_URL_PROP, kopSchemaRegistryUrl);
                }

                // Required by LakehouseConfiguration for s3a:// path resolution (compaction output).
                if ("S3".equalsIgnoreCase(config.getBackendType())) {
                    properties.setProperty("compactionBackendStorageType", "S3");
                    properties.setProperty("compactionBucket", config.getS3Bucket());
                    properties.setProperty("compactionPrefix", config.getStoragePath());
                    properties.setProperty("compactionBucketRegion", config.getS3Region());
                    properties.setProperty("cloudStorageEndpoint", config.getS3Endpoint());
                }
            }
            serviceConfiguration.setProperties(properties);
            serviceConfiguration.setMetadataStoreUrl(oxiaUrl);
            serviceConfiguration.setConfigurationMetadataStoreUrl(oxiaUrl);

            managedLedgerStorage = new PersistentStorageWalManagedLedgerStorage();
            managedLedgerStorage.initialize(serviceConfiguration, null, null, null, OpenTelemetry.noop());
            ManagedLedgerFactory managedLedgerFactory =
                    managedLedgerStorage.getDefaultStorageClass().getManagedLedgerFactory();

            AsyncOxiaClient oxiaClient = managedLedgerStorage.getUrsaStorage() != null
                    ? managedLedgerStorage.getUrsaStorage().getOxiaClient()
                    : null;
            if (oxiaClient == null) {
                throw new IllegalStateException("UrsaStorage oxia client is not initialized");
            }

            return new KafkaManagedLedgerFactoryHolder(managedLedgerFactory, managedLedgerStorage, oxiaClient);
        } catch (Exception e) {
            try {
                if (managedLedgerStorage != null) {
                    managedLedgerStorage.close();
                }
            } catch (Exception ignored) {
            }
            throw e;
        }
    }

    /**
     * Formats the Oxia URL for UrsaStorage.
     * The format is: oxia://host:port/namespace
     */
    static String formatOxiaUrl(UrsaStorageConfig config) {
        return "oxia://" + config.getOxiaServiceUrl() + "/" + config.getNamespace();
    }

    private static void setIfNotEmpty(String value, java.util.function.Consumer<String> setter) {
        if (value != null && !value.isEmpty()) {
            setter.accept(value);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (managedLedgerStorage != null) {
                managedLedgerStorage.close();
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
    }
}
