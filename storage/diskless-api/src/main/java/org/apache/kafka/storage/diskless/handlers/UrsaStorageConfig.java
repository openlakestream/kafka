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

import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.server.config.ServerLogConfigs;

import java.util.Map;

/**
 * Configuration for Ursa storage integration using Lakestream.
 */
public class UrsaStorageConfig {
    private final boolean enabled;
    /** The metadata store URL used by the Lakestream catalog. */
    private final String catalogOxiaServiceUrl;

    // The metadata store URL used for Ursa storage, e.g. the offset generation
    private final String ursaOxiaServiceUrl;

    private final String backendType;
    private final String storagePath;
    private final String compactionPrefix;
    private final long writeBufferFlushIntervalMs;
    private final int writeBufferSize;
    private final long writeBufferFlushSize;
    private final String s3Endpoint;
    private final String s3AccessKey;
    private final String s3SecretKey;
    private final String s3SessionToken;
    private final Boolean s3PathStyleAccess;
    private final String s3Bucket;
    private final String compactionBucket;
    private final String s3Region;
    private final long producerStateSnapshotIntervalMs;
    private final int producerStateSnapshotRecordThreshold;
    private final String classPath;

    @SuppressWarnings("checkstyle:ParameterNumber")
    private UrsaStorageConfig(boolean enabled,
                              String catalogOxiaServiceUrl,
                              String ursaOxiaServiceUrl,
                              String backendType,
                              String storagePath,
                              String compactionPrefix,
                              long writeBufferFlushIntervalMs,
                              int writeBufferSize,
                              long writeBufferFlushSize,
                              String s3Endpoint,
                              String s3AccessKey,
                              String s3SecretKey,
                              String s3SessionToken,
                              Boolean s3PathStyleAccess,
                              String s3Bucket,
                              String compactionBucket,
                              String s3Region,
                              long producerStateSnapshotIntervalMs,
                              int producerStateSnapshotRecordThreshold,
                              String classPath) {

        this.enabled = enabled;
        this.catalogOxiaServiceUrl = catalogOxiaServiceUrl;
        this.ursaOxiaServiceUrl = ursaOxiaServiceUrl;
        this.backendType = backendType;
        this.storagePath = storagePath;
        this.compactionPrefix = compactionPrefix;
        this.writeBufferFlushIntervalMs = writeBufferFlushIntervalMs;
        this.writeBufferSize = writeBufferSize;
        this.writeBufferFlushSize = writeBufferFlushSize;
        this.s3Endpoint = s3Endpoint;
        this.s3AccessKey = s3AccessKey;
        this.s3SecretKey = s3SecretKey;
        this.s3SessionToken = s3SessionToken;
        this.s3PathStyleAccess = s3PathStyleAccess;
        this.s3Bucket = s3Bucket;
        this.compactionBucket = compactionBucket;
        this.s3Region = s3Region;
        this.producerStateSnapshotIntervalMs = producerStateSnapshotIntervalMs;
        this.producerStateSnapshotRecordThreshold = producerStateSnapshotRecordThreshold;
        this.classPath = classPath;
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

        final var catalogOxiaServiceUrl = getOxiaServiceUrlConfig(
                configs, ServerLogConfigs.URSA_CATALOG_OXIA_SERVICE_URL_CONFIG,
                ServerLogConfigs.URSA_CATALOG_OXIA_SERVICE_URL_DEFAULT);
        final var ursaOxiaServiceUrl = getOxiaServiceUrlConfig(configs, ServerLogConfigs.URSA_OXIA_SERVICE_URL_CONFIG,
                ServerLogConfigs.URSA_OXIA_SERVICE_URL_DEFAULT);

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

        String s3Endpoint = getStringConfig(configs, ServerLogConfigs.URSA_STORAGE_S3_ENDPOINT_CONFIG,
                ServerLogConfigs.URSA_STORAGE_S3_ENDPOINT_DEFAULT);

        String s3AccessKey = getStringConfig(configs, ServerLogConfigs.URSA_STORAGE_S3_ACCESS_KEY_CONFIG,
                ServerLogConfigs.URSA_STORAGE_S3_ACCESS_KEY_DEFAULT);

        String s3SecretKey = getStringConfig(configs, ServerLogConfigs.URSA_STORAGE_S3_SECRET_KEY_CONFIG,
                ServerLogConfigs.URSA_STORAGE_S3_SECRET_KEY_DEFAULT);

        String s3SessionToken = getOptionalStringConfig(configs,
                ServerLogConfigs.URSA_STORAGE_S3_SESSION_TOKEN_CONFIG);

        Boolean s3PathStyleAccess = getOptionalBooleanConfig(configs,
                ServerLogConfigs.URSA_STORAGE_S3_PATH_STYLE_ACCESS_CONFIG);

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
        String classPath = getOptionalStringConfig(configs, ServerLogConfigs.URSA_STORAGE_CLASS_PATH_CONFIG);

        return new UrsaStorageConfig(enabled, catalogOxiaServiceUrl, ursaOxiaServiceUrl,
                backendType, storagePath, compactionPrefix,
                writeBufferFlushIntervalMs, writeBufferSize, writeBufferFlushSize,
                s3Endpoint, s3AccessKey, s3SecretKey, s3SessionToken, s3PathStyleAccess,
                s3Bucket, compactionBucket, s3Region,
                producerStateSnapshotIntervalMs, producerStateSnapshotRecordThreshold, classPath
        );
    }

    private static String getStringConfig(Map<String, ?> configs, String key, String defaultValue) {
        Object value = configs.get(key);
        return value != null ? String.valueOf(value) : defaultValue;
    }

    private static String getOptionalStringConfig(Map<String, ?> configs, String key) {
        Object value = configs.get(key);
        if (value == null) {
            return null;
        }
        String stringValue = value instanceof Password
                ? ((Password) value).value()
                : String.valueOf(value);
        if (stringValue == null) {
            return null;
        }
        return stringValue.isBlank() ? null : stringValue;
    }

    private static Boolean getOptionalBooleanConfig(Map<String, ?> configs, String key) {
        Object value = configs.get(key);
        return value == null ? null : Boolean.valueOf(String.valueOf(value));
    }

    private static long getLongConfig(Map<String, ?> configs, String key, long defaultValue) {
        Object value = configs.get(key);
        return value != null ? Long.parseLong(String.valueOf(value)) : defaultValue;
    }

    private static int getIntConfig(Map<String, ?> configs, String key, int defaultValue) {
        Object value = configs.get(key);
        return value != null ? Integer.parseInt(String.valueOf(value)) : defaultValue;
    }

    private static String getOxiaServiceUrlConfig(Map<String, ?> configs, String key, String defaultValue) {
        Object value = configs.get(key);
        return value != null ? String.valueOf(value) : defaultValue;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public String getCatalogOxiaServiceUrl() {
        return catalogOxiaServiceUrl;
    }

    public String getUrsaOxiaServiceUrl() {
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

    public String getS3Endpoint() {
        return s3Endpoint;
    }

    public String getS3AccessKey() {
        return s3AccessKey;
    }

    public String getS3SecretKey() {
        return s3SecretKey;
    }

    public String getS3SessionToken() {
        return s3SessionToken;
    }

    public Boolean getS3PathStyleAccess() {
        return s3PathStyleAccess;
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

    public String getClassPath() {
        return classPath;
    }
}
