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

import java.util.Map;

/**
 * Configuration for Ursa storage integration using ManagedLedger.
 */
public class UrsaStorageConfig {

    private final boolean enabled;
    private final String oxiaServiceUrl;
    private final String walDirectory;
    private final String namespace;
    private final String backendType;
    private final String storagePath;
    private final String compactionPrefix;
    private final long writeBufferFlushIntervalMs;
    private final int writeBufferSize;
    private final long writeBufferFlushSize;
    private final long boundaryCacheRefreshIntervalMs;
    public static final long BOUNDARY_CACHE_REFRESH_INTERVAL_MS_DEFAULT = 100L;
    private final int nonIdempotentMaxInFlightAppendsPerPartition;
    private final long nonIdempotentMaxInFlightBytesPerPartition;
    private final String s3Endpoint;
    private final String s3AccessKey;
    private final String s3SecretKey;
    private final String s3Bucket;
    private final String compactionBucket;
    private final String s3Region;

    @SuppressWarnings("checkstyle:ParameterNumber")
    public UrsaStorageConfig(boolean enabled,
                             String oxiaServiceUrl,
                             String walDirectory,
                             String namespace,
                             String backendType,
                             String storagePath,
                             String compactionPrefix,
                             long writeBufferFlushIntervalMs,
                             int writeBufferSize,
                             long writeBufferFlushSize,
                             long boundaryCacheRefreshIntervalMs,
                             String s3Endpoint,
                             String s3AccessKey,
                             String s3SecretKey,
                             String s3Bucket,
                             String compactionBucket,
                             String s3Region,
                             int nonIdempotentMaxInFlightAppendsPerPartition,
                             long nonIdempotentMaxInFlightBytesPerPartition) {
        this.enabled = enabled;
        this.oxiaServiceUrl = oxiaServiceUrl;
        this.walDirectory = walDirectory;
        this.namespace = namespace;
        this.backendType = backendType;
        this.storagePath = storagePath;
        this.compactionPrefix = compactionPrefix;
        this.writeBufferFlushIntervalMs = writeBufferFlushIntervalMs;
        this.writeBufferSize = writeBufferSize;
        this.writeBufferFlushSize = writeBufferFlushSize;
        this.boundaryCacheRefreshIntervalMs = boundaryCacheRefreshIntervalMs;
        this.nonIdempotentMaxInFlightAppendsPerPartition = nonIdempotentMaxInFlightAppendsPerPartition;
        this.nonIdempotentMaxInFlightBytesPerPartition = nonIdempotentMaxInFlightBytesPerPartition;
        this.s3Endpoint = s3Endpoint;
        this.s3AccessKey = s3AccessKey;
        this.s3SecretKey = s3SecretKey;
        this.s3Bucket = s3Bucket;
        this.compactionBucket = compactionBucket;
        this.s3Region = s3Region;
    }

    /**
     * Creates an UrsaStorageConfig from broker configuration.
     *
     * @param configs the broker configuration map
     * @return a new UrsaStorageConfig
     */
    public static UrsaStorageConfig fromConfigs(Map<String, ?> configs) {
        boolean enabled = getStringConfig(configs, ServerLogConfigs.URSA_STORAGE_ENABLE_CONFIG,
                String.valueOf(ServerLogConfigs.URSA_STORAGE_ENABLE_DEFAULT)).equals("true");

        String oxiaServiceUrl = getStringConfig(configs, ServerLogConfigs.URSA_STORAGE_OXIA_SERVICE_URL_CONFIG,
                ServerLogConfigs.URSA_STORAGE_OXIA_SERVICE_URL_DEFAULT);

        String walDirectory = getStringConfig(configs, ServerLogConfigs.URSA_STORAGE_WAL_DIRECTORY_CONFIG,
                ServerLogConfigs.URSA_STORAGE_WAL_DIRECTORY_DEFAULT);

        String namespace = getStringConfig(configs, ServerLogConfigs.URSA_STORAGE_NAMESPACE_CONFIG,
                ServerLogConfigs.URSA_STORAGE_NAMESPACE_DEFAULT);

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

        return new UrsaStorageConfig(enabled, oxiaServiceUrl, walDirectory, namespace,
                backendType, storagePath, compactionPrefix,
                writeBufferFlushIntervalMs, writeBufferSize, writeBufferFlushSize,
                BOUNDARY_CACHE_REFRESH_INTERVAL_MS_DEFAULT,
                s3Endpoint, s3AccessKey, s3SecretKey, s3Bucket, compactionBucket, s3Region,
                nonIdempotentMaxInFlightAppendsPerPartition, nonIdempotentMaxInFlightBytesPerPartition);
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

    public boolean isEnabled() {
        return enabled;
    }

    public String getOxiaServiceUrl() {
        return oxiaServiceUrl;
    }

    public String getWalDirectory() {
        return walDirectory;
    }

    public String getNamespace() {
        return namespace;
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

    public long getBoundaryCacheRefreshIntervalMs() {
        return boundaryCacheRefreshIntervalMs;
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

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private boolean enabled = false;
        private String oxiaServiceUrl = "localhost:6648";
        private String walDirectory = "/tmp/ursa-wal";
        private String namespace = "default";
        private String backendType = ServerLogConfigs.URSA_STORAGE_BACKEND_TYPE_DEFAULT;
        private String storagePath = ServerLogConfigs.URSA_STORAGE_PATH_DEFAULT;
        private String compactionPrefix = ServerLogConfigs.URSA_STORAGE_COMPACTION_PREFIX_DEFAULT;
        private long writeBufferFlushIntervalMs = ServerLogConfigs.URSA_STORAGE_WRITE_BUFFER_FLUSH_INTERVAL_MS_DEFAULT;
        private int writeBufferSize = ServerLogConfigs.URSA_STORAGE_WRITE_BUFFER_SIZE_DEFAULT;
        private long writeBufferFlushSize = ServerLogConfigs.URSA_STORAGE_WRITE_BUFFER_FLUSH_SIZE_DEFAULT;
        private long boundaryCacheRefreshIntervalMs = BOUNDARY_CACHE_REFRESH_INTERVAL_MS_DEFAULT;
        private int nonIdempotentMaxInFlightAppendsPerPartition =
                ServerLogConfigs.URSA_STORAGE_NON_IDEMPOTENT_MAX_IN_FLIGHT_APPENDS_PER_PARTITION_DEFAULT;
        private long nonIdempotentMaxInFlightBytesPerPartition =
                ServerLogConfigs.URSA_STORAGE_NON_IDEMPOTENT_MAX_IN_FLIGHT_BYTES_PER_PARTITION_DEFAULT;
        private String s3Endpoint = ServerLogConfigs.URSA_STORAGE_S3_ENDPOINT_DEFAULT;
        private String s3AccessKey = ServerLogConfigs.URSA_STORAGE_S3_ACCESS_KEY_DEFAULT;
        private String s3SecretKey = ServerLogConfigs.URSA_STORAGE_S3_SECRET_KEY_DEFAULT;
        private String s3Bucket = ServerLogConfigs.URSA_STORAGE_S3_BUCKET_DEFAULT;
        private String compactionBucket = ServerLogConfigs.URSA_STORAGE_COMPACTION_BUCKET_DEFAULT;
        private String s3Region = ServerLogConfigs.URSA_STORAGE_S3_REGION_DEFAULT;

        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public Builder oxiaServiceUrl(String oxiaServiceUrl) {
            this.oxiaServiceUrl = oxiaServiceUrl;
            return this;
        }

        public Builder walDirectory(String walDirectory) {
            this.walDirectory = walDirectory;
            return this;
        }

        public Builder namespace(String namespace) {
            this.namespace = namespace;
            return this;
        }

        public Builder backendType(String backendType) {
            this.backendType = backendType;
            return this;
        }

        public Builder storagePath(String storagePath) {
            this.storagePath = storagePath;
            return this;
        }

        public Builder compactionPrefix(String compactionPrefix) {
            this.compactionPrefix = compactionPrefix;
            return this;
        }

        public Builder compactionBucket(String compactionBucket) {
            this.compactionBucket = compactionBucket;
            return this;
        }

        public Builder writeBufferFlushIntervalMs(long writeBufferFlushIntervalMs) {
            this.writeBufferFlushIntervalMs = writeBufferFlushIntervalMs;
            return this;
        }

        public Builder writeBufferSize(int writeBufferSize) {
            this.writeBufferSize = writeBufferSize;
            return this;
        }

        public Builder writeBufferFlushSize(long writeBufferFlushSize) {
            this.writeBufferFlushSize = writeBufferFlushSize;
            return this;
        }

        public Builder boundaryCacheRefreshIntervalMs(long boundaryCacheRefreshIntervalMs) {
            this.boundaryCacheRefreshIntervalMs = boundaryCacheRefreshIntervalMs;
            return this;
        }

        public Builder nonIdempotentMaxInFlightAppendsPerPartition(int nonIdempotentMaxInFlightAppendsPerPartition) {
            this.nonIdempotentMaxInFlightAppendsPerPartition = nonIdempotentMaxInFlightAppendsPerPartition;
            return this;
        }

        public Builder nonIdempotentMaxInFlightBytesPerPartition(long nonIdempotentMaxInFlightBytesPerPartition) {
            this.nonIdempotentMaxInFlightBytesPerPartition = nonIdempotentMaxInFlightBytesPerPartition;
            return this;
        }

        public Builder s3Endpoint(String s3Endpoint) {
            this.s3Endpoint = s3Endpoint;
            return this;
        }

        public Builder s3AccessKey(String s3AccessKey) {
            this.s3AccessKey = s3AccessKey;
            return this;
        }

        public Builder s3SecretKey(String s3SecretKey) {
            this.s3SecretKey = s3SecretKey;
            return this;
        }

        public Builder s3Bucket(String s3Bucket) {
            this.s3Bucket = s3Bucket;
            return this;
        }

        public Builder s3Region(String s3Region) {
            this.s3Region = s3Region;
            return this;
        }

        public UrsaStorageConfig build() {
            return new UrsaStorageConfig(enabled, oxiaServiceUrl, walDirectory, namespace,
                    backendType, storagePath, compactionPrefix,
                    writeBufferFlushIntervalMs, writeBufferSize, writeBufferFlushSize,
                    boundaryCacheRefreshIntervalMs,
                    s3Endpoint, s3AccessKey, s3SecretKey, s3Bucket, compactionBucket, s3Region,
                    nonIdempotentMaxInFlightAppendsPerPartition,
                    nonIdempotentMaxInFlightBytesPerPartition);
        }
    }
}
