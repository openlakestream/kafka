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
package org.apache.kafka.storage.diskless;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.storage.diskless.handlers.UrsaManagedLedgerReader;
import org.apache.kafka.storage.diskless.handlers.UrsaManagedLedgerWriter;
import org.apache.kafka.storage.diskless.handlers.UrsaStorageConfig;
import org.apache.kafka.storage.diskless.handlers.UrsaStorageState;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Support class for integrating Ursa storage with ReplicaManager.
 * This class provides methods to partition requests by storage mode
 * and route them to the appropriate handlers using ManagedLedger.
 */
public class DisklessStorageReplicaManagerSupport implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(DisklessStorageReplicaManagerSupport.class);

    private final DisklessStorageMetadataView metadataView;
    private final boolean enabled;

    private final Writer writer;
    private final Reader reader;
    private final UrsaStorageState ursaState;

    /**
     * Creates a disabled instance.
     */
    public DisklessStorageReplicaManagerSupport() {
        this.metadataView = DisklessStorageMetadataView.DISABLED;
        this.writer = null;
        this.reader = null;
        this.ursaState = null;
        this.enabled = false;
    }

    /**
     * Creates an enabled instance with the specified configuration.
     *
     * @param time               the time instance
     * @param brokerId           the broker ID
     * @param ursaConfig         the Ursa storage configuration
     * @param brokerTopicStats   the broker topic stats
     * @param logConfigDefaults  default log configuration values
     * @param topicConfigSupplier function to get topic configuration
     */
    public DisklessStorageReplicaManagerSupport(
            Time time,
            int brokerId,
            UrsaStorageConfig ursaConfig,
            BrokerTopicStats brokerTopicStats,
            Map<String, Object> logConfigDefaults,
            Function<String, Map<String, String>> topicConfigSupplier) {

        if (ursaConfig == null || !ursaConfig.isEnabled()) {
            log.info("Ursa storage is disabled. Diskless storage will not be used.");
            this.metadataView = DisklessStorageMetadataView.DISABLED;
            this.writer = null;
            this.reader = null;
            this.ursaState = null;
            this.enabled = false;
            return;
        }

        this.metadataView = new MetadataCacheDisklessStorageView(topicConfigSupplier, true,
                ursaConfig.isTopicDefaultEnabled());
        this.enabled = true;

        this.ursaState = new UrsaStorageState(
                time,
                brokerId,
                ursaConfig,
                brokerTopicStats,
                logConfigDefaults,
                topicConfigSupplier
        );

        this.writer = new UrsaManagedLedgerWriter(ursaState);
        this.reader = new UrsaManagedLedgerReader(ursaState);
        log.info("Diskless support initialized with ManagedLedger, oxia URL: {}", ursaConfig.getOxiaServiceUrl());
    }

    /**
     * Returns whether diskless storage is enabled.
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Checks if a topic uses diskless storage.
     */
    public boolean isDisklessStorageTopic(String topic) {
        return metadataView.isDisklessStorageTopic(topic);
    }

    /**
     * Partitions append entries by storage mode.
     */
    public PartitionedEntries<TopicIdPartition, MemoryRecords> partitionAppendEntries(
            Map<TopicIdPartition, MemoryRecords> entriesPerPartition) {

        // Preserve request iteration order.
        Map<TopicIdPartition, MemoryRecords> disklessEntries = new LinkedHashMap<>();
        Map<TopicIdPartition, MemoryRecords> classicEntries = new LinkedHashMap<>();

        for (Map.Entry<TopicIdPartition, MemoryRecords> entry : entriesPerPartition.entrySet()) {
            if (enabled && metadataView.isDisklessStorageTopic(entry.getKey().topic())) {
                disklessEntries.put(entry.getKey(), entry.getValue());
            } else {
                classicEntries.put(entry.getKey(), entry.getValue());
            }
        }

        return new PartitionedEntries<>(disklessEntries, classicEntries);
    }

    /**
     * Partitions fetch infos by storage mode.
     */
    public PartitionedEntries<TopicIdPartition, FetchRequest.PartitionData> partitionFetchInfos(
            Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos) {

        // Preserve request iteration order.
        Map<TopicIdPartition, FetchRequest.PartitionData> disklessFetches = new LinkedHashMap<>();
        Map<TopicIdPartition, FetchRequest.PartitionData> classicFetches = new LinkedHashMap<>();

        for (Map.Entry<TopicIdPartition, FetchRequest.PartitionData> entry : fetchInfos.entrySet()) {
            if (enabled && metadataView.isDisklessStorageTopic(entry.getKey().topic())) {
                disklessFetches.put(entry.getKey(), entry.getValue());
            } else {
                classicFetches.put(entry.getKey(), entry.getValue());
            }
        }

        return new PartitionedEntries<>(disklessFetches, classicFetches);
    }

    /**
     * Handles append requests for diskless storage topics.
     */
    public CompletableFuture<Map<TopicIdPartition, PartitionResponse>> handleAppend(
            Map<TopicIdPartition, MemoryRecords> entries) {

        if (!enabled || writer == null) {
            return CompletableFuture.completedFuture(Map.of());
        }

        return writer.write(entries);
    }

    /**
     * Handles fetch requests for diskless storage topics.
     */
    public CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> handleFetch(
            FetchParams params,
            Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos) {

        if (!enabled || reader == null) {
            return CompletableFuture.completedFuture(Map.of());
        }

        return reader.fetch(params, fetchInfos);
    }

    /**
     * Handles listOffsets requests for diskless storage topics.
     */
    public CompletableFuture<Map<TopicIdPartition, ListOffsetsPartitionResponse>> handleListOffsets(
            Map<TopicIdPartition, ListOffsetsPartitionRequest> requests) {

        if (!enabled || reader == null) {
            return CompletableFuture.completedFuture(Map.of());
        }

        return reader.listOffsets(requests);
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
        if (writer != null) {
            writer.close();
        }
        if (ursaState != null) {
            ursaState.close();
        }
    }

    /**
     * Holder for partitioned entries.
     */
    public static class PartitionedEntries<K, V> {
        private final Map<K, V> diskless;
        private final Map<K, V> classic;

        public PartitionedEntries(Map<K, V> diskless, Map<K, V> classic) {
            this.diskless = diskless;
            this.classic = classic;
        }

        public Map<K, V> diskless() {
            return diskless;
        }

        public Map<K, V> classic() {
            return classic;
        }
    }
}
