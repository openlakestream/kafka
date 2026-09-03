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

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.storage.diskless.handlers.UrsaStorageConfig;
import org.apache.kafka.storage.internals.log.UnifiedLog;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * Support class for integrating Ursa storage with ReplicaManager.
 * This class provides methods to partition requests by storage mode
 * and route them to the configured diskless storage engine.
 */
public class DisklessStorageReplicaManagerSupport implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(DisklessStorageReplicaManagerSupport.class);

    private final DisklessStorageMetadataView metadataView;
    private final boolean enabled;
    private final int brokerId;
    private final DisklessBrokerSelector brokerSelector;

    private final DisklessStorageEngine engine;

    /**
     * Creates a disabled instance.
     */
    public DisklessStorageReplicaManagerSupport() {
        this.metadataView = DisklessStorageMetadataView.DISABLED;
        this.brokerId = -1;
        this.brokerSelector = new DisklessBrokerSelector(ln -> Collections.emptyList(), new ListenerName("DISABLED"));
        this.engine = null;
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
     * @param topicIdSupplier function to get topic ID
     * @param partitionCountSupplier function to get the current partition count of a topic
     * @param imageOffsetSupplier function to get the last metadata offset this broker has applied
     * @param aliveBrokerNodesSupplier function to get alive brokers for a listener
     * @param ownerSelectionListener listener used for canonical diskless owner selection
     */
    public DisklessStorageReplicaManagerSupport(
            Time time,
            int brokerId,
            UrsaStorageConfig ursaConfig,
            BrokerTopicStats brokerTopicStats,
            Map<String, Object> logConfigDefaults,
            Function<String, Map<String, String>> topicConfigSupplier,
            Function<String, Uuid> topicIdSupplier,
            Function<String, OptionalInt> partitionCountSupplier,
            LongSupplier imageOffsetSupplier,
            Function<ListenerName, Iterable<Node>> aliveBrokerNodesSupplier,
            ListenerName ownerSelectionListener) {

        if (ursaConfig == null || !ursaConfig.isEnabled()) {
            log.info("Ursa storage is disabled. Diskless storage will not be used.");
            this.metadataView = DisklessStorageMetadataView.DISABLED;
            this.brokerId = brokerId;
            this.brokerSelector = new DisklessBrokerSelector(ln -> Collections.emptyList(), ownerSelectionListener);
            this.engine = null;
            this.enabled = false;
            return;
        }

        this.metadataView = new MetadataCacheDisklessStorageView(
                topicConfigSupplier,
                aliveBrokerNodesSupplier,
                topicIdSupplier,
                partitionCountSupplier,
                imageOffsetSupplier,
                true
        );
        this.brokerId = brokerId;
        this.brokerSelector = new DisklessBrokerSelector(aliveBrokerNodesSupplier, ownerSelectionListener);
        this.enabled = true;

        this.engine = DisklessStorageEngineLoader.load(
                time,
                brokerId,
                ursaConfig,
                brokerTopicStats,
                logConfigDefaults,
                topicConfigSupplier,
                partitionCountSupplier,
                imageOffsetSupplier
        );

        log.info("Diskless support initialized with Lakestream, oxia URLs: {} {}",
                ursaConfig.getCatalogOxiaServiceUrl(), ursaConfig.getUrsaOxiaServiceUrl());
    }

    // Visible for testing.
    DisklessStorageReplicaManagerSupport(
            DisklessStorageMetadataView metadataView,
            int brokerId,
            DisklessBrokerSelector brokerSelector,
            DisklessStorageEngine engine) {
        this.metadataView = Objects.requireNonNull(metadataView, "metadataView cannot be null");
        this.brokerId = brokerId;
        this.brokerSelector = Objects.requireNonNull(brokerSelector, "brokerSelector cannot be null");
        this.engine = Objects.requireNonNull(engine, "engine cannot be null");
        this.enabled = true;
    }

    /**
     * Returns whether diskless storage is enabled.
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Updates the topic configuration in the diskless storage backend.
     * This is called when topic configs change (e.g., alter configs).
     */
    public void applyTopicConfig(String topic, Properties config) {
        if (!enabled || engine == null || topic == null || config == null) {
            return;
        }
        Uuid topicId = metadataView.getTopicId(topic);
        if (topicId == null || Uuid.ZERO_UUID.equals(topicId)) {
            log.debug("Skip diskless config update for {} without a current topic ID", topic);
            return;
        }
        Map<String, String> configMap = new LinkedHashMap<>();
        config.forEach((k, v) -> configMap.put(k.toString(), v.toString()));
        engine.applyTopicConfig(topic, topicId, configMap);
    }

    /**
     * Deletes the topic configuration after the topic has been deleted.
     */
    public void fenceDeletedTopic(String topicName, Uuid topicId) {
        if (!enabled || engine == null || topicName == null || topicId == null) {
            return;
        }
        engine.fenceDeletedTopic(topicName, topicId);
    }

    /**
     * Checks if a topic uses diskless storage.
     */
    public boolean isDisklessStorageTopic(String topic) {
        return metadataView.isDisklessStorageTopic(topic);
    }

    /** Returns whether the supplied topic ID is still the current Kafka incarnation for its name. */
    public boolean isCurrentTopicIncarnation(TopicIdPartition topicIdPartition) {
        if (!enabled || topicIdPartition == null) {
            return false;
        }
        Uuid currentTopicId = metadataView.getTopicId(topicIdPartition.topic());
        return currentTopicId != null && currentTopicId.equals(topicIdPartition.topicId());
    }

    public boolean isCurrentDisklessOwner(TopicIdPartition topicIdPartition, String zone) {
        if (!enabled
                || topicIdPartition == null
                || !isCurrentTopicIncarnation(topicIdPartition)
                || !metadataView.isDisklessStorageTopic(topicIdPartition.topic())) {
            return false;
        }

        return isSelectedBrokerForZone(topicIdPartition, zone);
    }

    private boolean isSelectedBrokerForZone(TopicIdPartition topicIdPartition, String zone) {
        OptionalInt selected = brokerSelector.selectBrokerForZone(
                topicIdPartition.topicId(),
                topicIdPartition.partition(),
                zone);
        return selected.isPresent() && selected.getAsInt() == brokerId;
    }

    /** Returns whether this broker owns the partition for at least one currently active client zone. */
    public boolean isCurrentDisklessOwnerInAnyZone(TopicIdPartition topicIdPartition) {
        return enabled
                && topicIdPartition != null
                && isCurrentTopicIncarnation(topicIdPartition)
                && metadataView.isDisklessStorageTopic(topicIdPartition.topic())
                && !ownedZones(topicIdPartition).isEmpty();
    }

    public boolean hasTrackedPartitionsForTopic(String topic) {
        for (TopicIdPartition trackedPartition : partitionsNeedingReconcile()) {
            if (trackedPartition.topic().equals(topic)) {
                return true;
            }
        }
        return false;
    }

    public Set<String> trackedTopicNames() {
        Set<String> topics = new LinkedHashSet<>();
        for (TopicIdPartition trackedPartition : partitionsNeedingReconcile()) {
            topics.add(trackedPartition.topic());
        }
        return topics;
    }

    public Set<TopicIdPartition> snapshotTrackedPartitions() {
        return Set.copyOf(partitionsNeedingReconcile());
    }

    public void reconcileTrackedPartitions(
            Set<TopicIdPartition> deletedPartitions,
            Consumer<String> onTopicMaybeEmptied
    ) {
        Set<TopicIdPartition> partitionsToCheck = partitionsNeedingReconcile();
        Set<TopicIdPartition> permanentlyDeletedPartitions =
                deletedPartitions != null ? deletedPartitions : Collections.emptySet();
        if (!enabled || (partitionsToCheck.isEmpty() && permanentlyDeletedPartitions.isEmpty())) {
            return;
        }

        Consumer<String> topicMaybeEmptied = onTopicMaybeEmptied != null ? onTopicMaybeEmptied : ignored -> { };
        Set<TopicIdPartition> trackedPartitionsToCleanup = new LinkedHashSet<>();
        Set<TopicIdPartition> trackedPartitionsToDelete = new LinkedHashSet<>();
        Map<TopicIdPartition, Set<String>> retainedOwnedZones = new LinkedHashMap<>();

        for (TopicIdPartition trackedPartition : partitionsToCheck) {
            if (permanentlyDeletedPartitions.contains(trackedPartition)) {
                trackedPartitionsToDelete.add(trackedPartition);
            } else {
                Set<String> ownedZones = ownedZones(trackedPartition);
                if (shouldCleanupTrackedPartition(trackedPartition) || ownedZones.isEmpty()) {
                    trackedPartitionsToCleanup.add(trackedPartition);
                } else {
                    retainedOwnedZones.put(trackedPartition, ownedZones);
                }
            }
        }

        cleanupTrackedPartitions(trackedPartitionsToCleanup, topicMaybeEmptied);
        cleanupRetainedProducerStates(retainedOwnedZones);
        cleanupDeletedTrackedPartitions(trackedPartitionsToDelete, topicMaybeEmptied);
    }

    private void cleanupTrackedPartitions(
            Set<TopicIdPartition> trackedPartitionsToCleanup,
            Consumer<String> topicMaybeEmptied
    ) {
        for (TopicIdPartition trackedPartition : trackedPartitionsToCleanup) {
            if (cleanupPartitionInternal(trackedPartition, false)) {
                topicMaybeEmptied.accept(trackedPartition.topic());
            }
        }
    }

    private void cleanupDeletedTrackedPartitions(
            Set<TopicIdPartition> trackedPartitionsToDelete,
            Consumer<String> topicMaybeEmptied
    ) {
        for (TopicIdPartition trackedPartition : trackedPartitionsToDelete) {
            if (cleanupPartitionInternal(trackedPartition, true)) {
                topicMaybeEmptied.accept(trackedPartition.topic());
            }
        }
    }

    private void cleanupRetainedProducerStates(Map<TopicIdPartition, Set<String>> retainedOwnedZones) {
        if (engine == null || retainedOwnedZones.isEmpty()) {
            return;
        }

        retainedOwnedZones.forEach((trackedPartition, ownedZones) ->
                cleanupStep("producer-state zones", trackedPartition,
                        () -> engine.cleanupNonOwnedProducerStates(trackedPartition, ownedZones, false)));
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
            Map<TopicIdPartition, MemoryRecords> entries,
            String clientId) {
        String zone = brokerSelector.effectiveZone(clientId);
        return handleWithOwnership(
                entries,
                "append",
                ownedEntries -> handleOwnedAppend(ownedEntries, clientId),
                (ignored, response) -> response,
                tp -> new PartitionResponse(Errors.NOT_LEADER_OR_FOLLOWER),
                tp -> new PartitionResponse(Errors.UNKNOWN_SERVER_ERROR),
                zone
        );
    }

    /**
     * Handles fetch requests for diskless storage topics.
     */
    public CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> handleFetch(
            FetchParams params,
            Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos,
            String clientId) {
        String zone = brokerSelector.effectiveZone(clientId);
        Map<TopicIdPartition, TopicIdPartition> requestToCanonical = new LinkedHashMap<>();
        Map<TopicIdPartition, FetchRequest.PartitionData> canonicalFetchInfos = new LinkedHashMap<>();
        fetchInfos.forEach((requestPartition, partitionData) -> {
            TopicIdPartition canonicalPartition = canonicalFetchPartition(requestPartition);
            requestToCanonical.put(requestPartition, canonicalPartition);
            if (canonicalPartition != null) {
                canonicalFetchInfos.put(canonicalPartition, partitionData);
            }
        });

        return handleWithOwnership(
                canonicalFetchInfos,
                "fetch",
                ownedFetchInfos -> handleOwnedFetch(params, ownedFetchInfos),
                (ignored, response) -> response,
                ignored -> notLeaderFetchPartitionData(),
                ignored -> unknownErrorFetchPartitionData(),
                zone
        ).thenApply(canonicalResponses -> {
            Map<TopicIdPartition, FetchPartitionData> requestResponses = new LinkedHashMap<>();
            requestToCanonical.forEach((requestPartition, canonicalPartition) -> {
                FetchPartitionData response = canonicalPartition != null
                        ? canonicalResponses.get(canonicalPartition)
                        : null;
                requestResponses.put(
                        requestPartition,
                        response != null ? response : notLeaderFetchPartitionData());
            });
            return requestResponses;
        });
    }

    /**
     * Fetch protocol versions before topic IDs use {@link Uuid#ZERO_UUID} in their internal
     * request key. Resolve only that legacy sentinel to the current metadata image before
     * ownership selection and storage access. A non-zero stale topic ID must not be rewritten to
     * a newer incarnation.
     */
    private TopicIdPartition canonicalFetchPartition(TopicIdPartition requestPartition) {
        if (requestPartition == null) {
            return null;
        }
        if (!Uuid.ZERO_UUID.equals(requestPartition.topicId())) {
            return requestPartition;
        }
        Uuid currentTopicId = metadataView.getTopicId(requestPartition.topic());
        if (currentTopicId == null || Uuid.ZERO_UUID.equals(currentTopicId)) {
            return null;
        }
        return new TopicIdPartition(currentTopicId, requestPartition.topicPartition());
    }

    /**
     * Handles listOffsets requests for diskless storage topics.
     */
    public CompletableFuture<Map<TopicIdPartition, ListOffsetsPartitionResponse>> handleListOffsets(
            Map<TopicIdPartition, ListOffsetsPartitionRequest> requests,
            String clientId) {
        String zone = brokerSelector.effectiveZone(clientId);
        return handleWithOwnership(
                requests,
                "listOffsets",
                this::handleOwnedListOffsets,
                (ignored, response) -> response,
                tp -> ListOffsetsPartitionResponse.error(tp, Errors.NOT_LEADER_OR_FOLLOWER),
                tp -> ListOffsetsPartitionResponse.error(tp, Errors.UNKNOWN_SERVER_ERROR),
                zone
        );
    }

    private CompletableFuture<Map<TopicIdPartition, PartitionResponse>> handleOwnedAppend(
            Map<TopicIdPartition, MemoryRecords> entries,
            String clientId) {

        if (!enabled || engine == null) {
            return CompletableFuture.completedFuture(Map.of());
        }

        String zone = DisklessClientZone.get(clientId);
        return engine.write(entries, zone);
    }

    private CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> handleOwnedFetch(
            FetchParams params,
            Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos) {

        if (!enabled || engine == null) {
            return CompletableFuture.completedFuture(Map.of());
        }

        return engine.fetch(params, fetchInfos);
    }

    private CompletableFuture<Map<TopicIdPartition, ListOffsetsPartitionResponse>> handleOwnedListOffsets(
            Map<TopicIdPartition, ListOffsetsPartitionRequest> requests) {

        if (!enabled || engine == null) {
            return CompletableFuture.completedFuture(Map.of());
        }

        return engine.listOffsets(requests);
    }

    private boolean cleanupPartitionInternal(TopicIdPartition tp, boolean deletePartition) {
        if (!enabled || tp == null) {
            return true;
        }

        return cleanupStep("storage", tp, () -> {
            if (engine != null) {
                engine.cleanupPartition(tp, deletePartition);
            }
        });
    }

    private <V, O, R> CompletableFuture<Map<TopicIdPartition, R>> handleWithOwnership(
            Map<TopicIdPartition, V> entries,
            String operationName,
            Function<Map<TopicIdPartition, V>, CompletableFuture<Map<TopicIdPartition, O>>> ownedHandler,
            BiFunction<TopicIdPartition, O, R> successResponseFactory,
            Function<TopicIdPartition, R> redirectedResponseFactory,
            Function<TopicIdPartition, R> errorResponseFactory,
            String zone) {
        if (!enabled || entries == null || entries.isEmpty()) {
            return CompletableFuture.completedFuture(Map.of());
        }

        OwnershipRouting<V, R> routing = routeByOwnership(entries, redirectedResponseFactory, zone);
        if (routing.ownedEntries.isEmpty()) {
            return CompletableFuture.completedFuture(routing.redirectedResponses);
        }

        try {
            CompletableFuture<Map<TopicIdPartition, O>> ownedFuture = ownedHandler.apply(routing.ownedEntries);
            if (ownedFuture == null) {
                return CompletableFuture.completedFuture(buildOrderedResponses(
                        entries.keySet(),
                        Map.of(),
                        successResponseFactory,
                        routing.redirectedResponses,
                        errorResponseFactory
                ));
            }

            return ownedFuture.handle((ownedResponses, error) -> {
                if (error != null) {
                    log.error("Diskless storage {} future failed", operationName, error);
                }
                Map<TopicIdPartition, O> safeOwnedResponses =
                        error == null && ownedResponses != null ? ownedResponses : Map.of();
                return buildOrderedResponses(
                        entries.keySet(),
                        safeOwnedResponses,
                        successResponseFactory,
                        routing.redirectedResponses,
                        errorResponseFactory
                );
            });
        } catch (Throwable t) {
            log.error("Diskless storage {} setup failed", operationName, t);
            return CompletableFuture.completedFuture(buildOrderedResponses(
                    entries.keySet(),
                    Map.of(),
                    successResponseFactory,
                    routing.redirectedResponses,
                    errorResponseFactory
            ));
        }
    }

    private <V, R> OwnershipRouting<V, R> routeByOwnership(
            Map<TopicIdPartition, V> entries,
            Function<TopicIdPartition, R> redirectedResponseFactory,
            String zone) {
        Map<TopicIdPartition, V> ownedEntries = new LinkedHashMap<>();
        Map<TopicIdPartition, R> redirectedResponses = new LinkedHashMap<>();

        for (Map.Entry<TopicIdPartition, V> entry : entries.entrySet()) {
            if (isCurrentDisklessOwner(entry.getKey(), zone)) {
                ownedEntries.put(entry.getKey(), entry.getValue());
            } else {
                redirectedResponses.put(entry.getKey(), redirectedResponseFactory.apply(entry.getKey()));
            }
        }

        return new OwnershipRouting<>(ownedEntries, redirectedResponses);
    }

    private <O, R> Map<TopicIdPartition, R> buildOrderedResponses(
            Collection<TopicIdPartition> requestOrder,
            Map<TopicIdPartition, O> ownedResponses,
            BiFunction<TopicIdPartition, O, R> successResponseFactory,
            Map<TopicIdPartition, R> redirectedResponses,
            Function<TopicIdPartition, R> errorResponseFactory) {
        Map<TopicIdPartition, R> orderedResponses = new LinkedHashMap<>();
        for (TopicIdPartition topicIdPartition : requestOrder) {
            if (redirectedResponses.containsKey(topicIdPartition)) {
                orderedResponses.put(topicIdPartition, redirectedResponses.get(topicIdPartition));
            } else {
                O ownedResponse = ownedResponses.get(topicIdPartition);
                if (ownedResponse != null) {
                    orderedResponses.put(topicIdPartition, successResponseFactory.apply(topicIdPartition, ownedResponse));
                } else {
                    orderedResponses.put(topicIdPartition, errorResponseFactory.apply(topicIdPartition));
                }
            }
        }
        return orderedResponses;
    }

    private boolean cleanupStep(String component, TopicIdPartition tp, Runnable action) {
        try {
            action.run();
            return true;
        } catch (Throwable t) {
            log.warn("Failed to cleanup diskless {} state for partition {}", component, tp, t);
            return false;
        }
    }

    @Override
    public void close() throws IOException {
        if (engine != null) {
            engine.close();
        }
    }

    private Set<TopicIdPartition> partitionsNeedingReconcile() {
        Set<TopicIdPartition> partitions = new LinkedHashSet<>();
        if (engine != null) {
            partitions.addAll(engine.snapshotTrackedPartitions());
        }
        return partitions;
    }

    private boolean shouldCleanupTrackedPartition(TopicIdPartition trackedPartition) {
        Uuid currentTopicId = metadataView.getTopicId(trackedPartition.topic());
        if (currentTopicId == null || Uuid.ZERO_UUID.equals(currentTopicId) || !currentTopicId.equals(trackedPartition.topicId())) {
            return true;
        }

        return !metadataView.isDisklessStorageTopic(trackedPartition.topic());
    }

    private Set<String> ownedZones(TopicIdPartition trackedPartition) {
        Set<String> ownedZones = new LinkedHashSet<>();
        Set<String> activeZones = brokerSelector.activeZones();
        if (activeZones == null || activeZones.isEmpty()) {
            activeZones = Set.of(DisklessClientZone.NO_ZONE);
        }
        for (String zone : activeZones) {
            OptionalInt selectedBroker = brokerSelector.selectBrokerForZone(
                    trackedPartition.topicId(),
                    trackedPartition.partition(),
                    zone);
            if (selectedBroker.isPresent() && selectedBroker.getAsInt() == brokerId) {
                ownedZones.add(zone);
            }
        }
        return ownedZones;
    }

    private FetchPartitionData notLeaderFetchPartitionData() {
        return new FetchPartitionData(
                Errors.NOT_LEADER_OR_FOLLOWER,
                UnifiedLog.UNKNOWN_OFFSET,
                UnifiedLog.UNKNOWN_OFFSET,
                MemoryRecords.EMPTY,
                Optional.empty(),
                OptionalLong.empty(),
                Optional.empty(),
                OptionalInt.empty(),
                false
        );
    }

    private FetchPartitionData unknownErrorFetchPartitionData() {
        return new FetchPartitionData(
                Errors.UNKNOWN_SERVER_ERROR,
                0,
                0,
                MemoryRecords.EMPTY,
                Optional.empty(),
                OptionalLong.empty(),
                Optional.empty(),
                OptionalInt.empty(),
                false
        );
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

    private static class OwnershipRouting<V, R> {
        private final Map<TopicIdPartition, V> ownedEntries;
        private final Map<TopicIdPartition, R> redirectedResponses;

        private OwnershipRouting(
                Map<TopicIdPartition, V> ownedEntries,
                Map<TopicIdPartition, R> redirectedResponses
        ) {
            this.ownedEntries = ownedEntries;
            this.redirectedResponses = redirectedResponses;
        }
    }
}
