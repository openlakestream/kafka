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

import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.storage.diskless.OxiaServiceUrl;

import java.util.Map;

/**
 * Configuration for Ursa storage integration using ManagedLedger.
 */
public class UrsaStorageConfig {

    private final boolean enabled;
    /**
     * The metadata store URL used for Pulsar's metadata store:
     * 1. Managed ledger: used for Ursa storage to store managed ledger's metadata
     * 2. {@link org.apache.kafka.storage.diskless.UrsaPartitionedTopicsMetadataSync}
     */
    private final OxiaServiceUrl pulsarOxiaServiceUrl;

    // The metadata store URL used for Ursa storage, e.g. the offset generation
    private final OxiaServiceUrl ursaOxiaServiceUrl;

    private final String backendType;
    private final String storagePath;
    private final String compactionPrefix;
    private final long writeBufferFlushIntervalMs;
    private final int writeBufferSize;
    private final long writeBufferFlushSize;
    private final int nonIdempotentMaxInFlightAppendsPerPartition;
    private final long nonIdempotentMaxInFlightBytesPerPartition;
    private final String s3Endpoint;
    private final String s3AccessKey;
    private final String s3SecretKey;
    private final String s3Bucket;
    private final String compactionBucket;
    private final String s3Region;
    private final long producerStateSnapshotIntervalMs;
    private final int producerStateSnapshotRecordThreshold;

    @SuppressWarnings("checkstyle:ParameterNumber")
    private UrsaStorageConfig(boolean enabled,
                              OxiaServiceUrl pulsarOxiaServiceUrl,
                              OxiaServiceUrl ursaOxiaServiceUrl,
                              String backendType,
                              String storagePath,
                              String compactionPrefix,
                              long writeBufferFlushIntervalMs,
                              int writeBufferSize,
                              long writeBufferFlushSize,
                              String s3Endpoint,
                              String s3AccessKey,
                              String s3SecretKey,
                              String s3Bucket,
                              String compactionBucket,
                              String s3Region,
                              long producerStateSnapshotIntervalMs,
                              int producerStateSnapshotRecordThreshold,
                              int nonIdempotentMaxInFlightAppendsPerPartition,
                              long nonIdempotentMaxInFlightBytesPerPartition) {

        this.enabled = enabled;
        this.pulsarOxiaServiceUrl = pulsarOxiaServiceUrl;
        this.ursaOxiaServiceUrl = ursaOxiaServiceUrl;
        this.backendType = backendType;
        this.storagePath = storagePath;
        this.compactionPrefix = compactionPrefix;
        this.writeBufferFlushIntervalMs = writeBufferFlushIntervalMs;
        this.writeBufferSize = writeBufferSize;
        this.writeBufferFlushSize = writeBufferFlushSize;
        this.nonIdempotentMaxInFlightAppendsPerPartition = nonIdempotentMaxInFlightAppendsPerPartition;
        this.nonIdempotentMaxInFlightBytesPerPartition = nonIdempotentMaxInFlightBytesPerPartition;
        this.s3Endpoint = s3Endpoint;
        this.s3AccessKey = s3AccessKey;
        this.s3SecretKey = s3SecretKey;
        this.s3Bucket = s3Bucket;
        this.compactionBucket = compactionBucket;
        this.s3Region = s3Region;
        this.producerStateSnapshotIntervalMs = producerStateSnapshotIntervalMs;
        this.producerStateSnapshotRecordThreshold = producerStateSnapshotRecordThreshold;
    }

    /**
     * Creates an UrsaStorageConfig from broker configuration.
     *
     * @param configs the broker configuration map
     * @return a new UrsaStorageConfig
     */
    public static UrsaStorageConfig fromConfigs(Map<String, ?> configs) throws Exception {
        boolean enabled = getStringConfig(configs, ServerLogConfigs.URSA_STORAGE_ENABLE_CONFIG,
                String.valueOf(ServerLogConfigs.URSA_STORAGE_ENABLE_DEFAULT)).equals("true");

        String oxiaServiceUrl = getStringConfig(configs, ServerLogConfigs.URSA_STORAGE_OXIA_SERVICE_URL_CONFIG,
                ServerLogConfigs.URSA_STORAGE_OXIA_SERVICE_URL_DEFAULT);
        String namespace = getStringConfig(configs, ServerLogConfigs.URSA_STORAGE_NAMESPACE_CONFIG,
                ServerLogConfigs.URSA_STORAGE_NAMESPACE_DEFAULT);
        final var defaultOxiaServiceUrl = new OxiaServiceUrl("oxia://" + oxiaServiceUrl + "/" + namespace);

        final var pulsarOxiaServiceUrl = getOxiaServiceUrlConfig(configs, ServerLogConfigs.PULSAR_OXIA_SERVICE_URL_CONFIG,
                defaultOxiaServiceUrl);
        final var ursaOxiaServiceUrl = getOxiaServiceUrlConfig(configs, ServerLogConfigs.URSA_OXIA_SERVICE_URL_CONFIG,
                defaultOxiaServiceUrl);

        String backendType = getStringConfig(configs, ServerLogConfigs.URSA_STORAGE_BACKEND_TYPE_CONFIG,
                ServerLogConfigs.URSA_STORAGE_BACKEND_TYPE_DEFAULT);

        String storagePath = getStringConfig(configs, ServerLogConfigs.URSA_STORAGE_PATH_CONFIG,
                ServerLogConfigs.URSA_STORAGE_PATH_DEFAULT);

        String compactionPrefix = getStringConfig(configs, ServerLogConfigs.URSA_STORAGE_COMPACTION_PREFIX_CONFIG,
                ServerLogConfigs.URSA_STORAGE_COMPACTION_PREFIX_DEFAULT);

        long writeBufferFlushIntervalMs = getLongConfig(configs,
                ServerLogConfigs.URSA_STORAGE_WRITE_BUFFER_FLUSH_INTERVAL_MS_CONFIG,
                ServerLogConfigs.URSA_STORAGE_WRITE_BUFFER_FLUSH_INTERVAL_MS_DEFAULT);

        int writeBufferSize = getIntConfig(configs,
                ServerLogConfigs.URSA_STORAGE_WRITE_BUFFER_SIZE_CONFIG,
                ServerLogConfigs.URSA_STORAGE_WRITE_BUFFER_SIZE_DEFAULT);

        long writeBufferFlushSize = getLongConfig(configs,
                ServerLogConfigs.URSA_STORAGE_WRITE_BUFFER_FLUSH_SIZE_CONFIG,
                ServerLogConfigs.URSA_STORAGE_WRITE_BUFFER_FLUSH_SIZE_DEFAULT);

        int nonIdempotentMaxInFlightAppendsPerPartition = getIntConfig(configs,
                ServerLogConfigs.URSA_STORAGE_NON_IDEMPOTENT_MAX_IN_FLIGHT_APPENDS_PER_PARTITION_CONFIG,
                ServerLogConfigs.URSA_STORAGE_NON_IDEMPOTENT_MAX_IN_FLIGHT_APPENDS_PER_PARTITION_DEFAULT);

        long nonIdempotentMaxInFlightBytesPerPartition = getLongConfig(configs,
                ServerLogConfigs.URSA_STORAGE_NON_IDEMPOTENT_MAX_IN_FLIGHT_BYTES_PER_PARTITION_CONFIG,
                ServerLogConfigs.URSA_STORAGE_NON_IDEMPOTENT_MAX_IN_FLIGHT_BYTES_PER_PARTITION_DEFAULT);

        String s3Endpoint = getStringConfig(configs, ServerLogConfigs.URSA_STORAGE_S3_ENDPOINT_CONFIG,
                ServerLogConfigs.URSA_STORAGE_S3_ENDPOINT_DEFAULT);

        String s3AccessKey = getStringConfig(configs, ServerLogConfigs.URSA_STORAGE_S3_ACCESS_KEY_CONFIG,
                ServerLogConfigs.URSA_STORAGE_S3_ACCESS_KEY_DEFAULT);

        String s3SecretKey = getStringConfig(configs, ServerLogConfigs.URSA_STORAGE_S3_SECRET_KEY_CONFIG,
                ServerLogConfigs.URSA_STORAGE_S3_SECRET_KEY_DEFAULT);

        String s3Bucket = getStringConfig(configs, ServerLogConfigs.URSA_STORAGE_S3_BUCKET_CONFIG,
                ServerLogConfigs.URSA_STORAGE_S3_BUCKET_DEFAULT);

        String compactionBucket = getStringConfig(configs, ServerLogConfigs.URSA_STORAGE_COMPACTION_BUCKET_CONFIG,
                s3Bucket);

        String s3Region = getStringConfig(configs, ServerLogConfigs.URSA_STORAGE_S3_REGION_CONFIG,
                ServerLogConfigs.URSA_STORAGE_S3_REGION_DEFAULT);

        long producerStateSnapshotIntervalMs = getLongConfig(configs,
                ServerLogConfigs.URSA_STORAGE_PRODUCER_STATE_SNAPSHOT_INTERVAL_MS_CONFIG,
                ServerLogConfigs.URSA_STORAGE_PRODUCER_STATE_SNAPSHOT_INTERVAL_MS_DEFAULT);

        int producerStateSnapshotRecordThreshold = getIntConfig(configs,
                ServerLogConfigs.URSA_STORAGE_PRODUCER_STATE_SNAPSHOT_RECORD_THRESHOLD_CONFIG,
                ServerLogConfigs.URSA_STORAGE_PRODUCER_STATE_SNAPSHOT_RECORD_THRESHOLD_DEFAULT);

        return new UrsaStorageConfig(enabled, pulsarOxiaServiceUrl, ursaOxiaServiceUrl,
                backendType, storagePath, compactionPrefix,
                writeBufferFlushIntervalMs, writeBufferSize, writeBufferFlushSize,
                s3Endpoint, s3AccessKey, s3SecretKey, s3Bucket, compactionBucket, s3Region,
                producerStateSnapshotIntervalMs, producerStateSnapshotRecordThreshold,
                nonIdempotentMaxInFlightAppendsPerPartition, nonIdempotentMaxInFlightBytesPerPartition
        );
    }

    private static String getStringConfig(Map<String, ?> configs, String key, String defaultValue) {
        Object value = configs.get(key);
        return value != null ? String.valueOf(value) : defaultValue;
    }

    private static long getLongConfig(Map<String, ?> configs, String key, long defaultValue) {
        Object value = configs.get(key);
        return value != null ? Long.parseLong(String.valueOf(value)) : defaultValue;
    }

    private static int getIntConfig(Map<String, ?> configs, String key, int defaultValue) {
        Object value = configs.get(key);
        return value != null ? Integer.parseInt(String.valueOf(value)) : defaultValue;
    }

    private static OxiaServiceUrl getOxiaServiceUrlConfig(Map<String, ?> configs, String key, OxiaServiceUrl defaultValue)
            throws Exception {
        Object value = configs.get(key);
        return value != null ? new OxiaServiceUrl(String.valueOf(value)) : defaultValue;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public OxiaServiceUrl getPulsarOxiaServiceUrl() {
        return pulsarOxiaServiceUrl;
    }

    public OxiaServiceUrl getUrsaOxiaServiceUrl() {
        return ursaOxiaServiceUrl;
    }

    public String getBackendType() {
        return backendType;
    }

    public String getStoragePath() {
        return storagePath;
    }

    public String getCompactionPrefix() {
        return compactionPrefix;
    }

    public long getWriteBufferFlushIntervalMs() {
        return writeBufferFlushIntervalMs;
    }

    public int getWriteBufferSize() {
        return writeBufferSize;
    }

    public long getWriteBufferFlushSize() {
        return writeBufferFlushSize;
    }

    public int getNonIdempotentMaxInFlightAppendsPerPartition() {
        return nonIdempotentMaxInFlightAppendsPerPartition;
    }

    public long getNonIdempotentMaxInFlightBytesPerPartition() {
        return nonIdempotentMaxInFlightBytesPerPartition;
    }

    public String getS3Endpoint() {
        return s3Endpoint;
    }

    public String getS3AccessKey() {
        return s3AccessKey;
    }

    public String getS3SecretKey() {
        return s3SecretKey;
    }

    public String getS3Bucket() {
        return s3Bucket;
    }

    public String getCompactionBucket() {
        return compactionBucket;
    }

    public String getS3Region() {
        return s3Region;
    }

    public long getProducerStateSnapshotIntervalMs() {
        return producerStateSnapshotIntervalMs;
    }

    public int getProducerStateSnapshotRecordThreshold() {
        return producerStateSnapshotRecordThreshold;
    }
}
