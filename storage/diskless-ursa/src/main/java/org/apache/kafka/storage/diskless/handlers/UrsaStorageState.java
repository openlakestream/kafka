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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.diskless.DisklessStorageStateOperations;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import io.oxia.client.api.AsyncOxiaClient;

/**
 * Shared state container for Ursa storage components.
 * Manages ManagedLedger instances for UrsaManagedLedgerReader and UrsaManagedLedgerWriter.
 */
public class UrsaStorageState implements DisklessStorageStateOperations {

    private static final Logger log = LoggerFactory.getLogger(UrsaStorageState.class);
    private static final long TOPIC_CONFIG_UPDATE_TIMEOUT_SECONDS = 30;

    private final Time time;
    private final int brokerId;
    private final UrsaStorageConfig config;
    private final BrokerTopicStats brokerTopicStats;
    private final DisklessLogMetrics logMetrics = new DisklessLogMetrics();

    private final ConcurrentHashMap<TopicIdPartition, UrsaPartitionLog> partitionLogs = new ConcurrentHashMap<>();

    private final KafkaManagedLedgerFactoryHolder managedLedgerFactoryHolder;
    private final ManagedLedgerFactory managedLedgerFactory;
    private final Supplier<AsyncOxiaClient> oxiaClientSupplier;
    private final ScheduledExecutorService producerStateScheduler;
    private final Map<String, Object> logConfigDefaults;
    private final Function<String, Map<String, String>> topicConfigSupplier;

    /**
     * Creates UrsaStorageState for production use.
     */
    public UrsaStorageState(
            Time time,
            int brokerId,
            UrsaStorageConfig config,
            BrokerTopicStats brokerTopicStats) {
        this(time, brokerId, config, brokerTopicStats,
            (Map<String, Object>) null,
            (Function<String, Map<String, String>>) null);
    }

    public UrsaStorageState(
            Time time,
            int brokerId,
            UrsaStorageConfig config,
            BrokerTopicStats brokerTopicStats,
            Map<String, Object> logConfigDefaults,
            Function<String, Map<String, String>> topicConfigSupplier) {
        this.time = time;
        this.brokerId = brokerId;
        this.config = config;
        this.brokerTopicStats = brokerTopicStats;
        this.logConfigDefaults = logConfigDefaults != null ? logConfigDefaults : Collections.emptyMap();
        this.topicConfigSupplier = topicConfigSupplier;
        this.producerStateScheduler = new ScheduledThreadPoolExecutor(1, runnable -> {
            Thread thread = new Thread(runnable, "producer-state-manager");
            thread.setDaemon(true);
            return thread;
        });

        KafkaManagedLedgerFactoryHolder holder;
        try {
            holder = KafkaManagedLedgerFactoryHolder.create(config);
            log.info("Initialized ManagedLedgerFactory for Kafka diskless storage");
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize ManagedLedgerFactory", e);
        }
        this.managedLedgerFactoryHolder = holder;
        this.managedLedgerFactory = holder.factory();
        this.oxiaClientSupplier = holder::oxiaClient;
    }

    UrsaStorageState(
            Time time,
            int brokerId,
            UrsaStorageConfig config,
            BrokerTopicStats brokerTopicStats,
            ManagedLedgerFactory managedLedgerFactory) {
        this.time = Objects.requireNonNull(time, "time must not be null");
        this.brokerId = brokerId;
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.brokerTopicStats = Objects.requireNonNull(brokerTopicStats, "brokerTopicStats must not be null");
        this.managedLedgerFactoryHolder = null;
        this.managedLedgerFactory = Objects.requireNonNull(managedLedgerFactory, "managedLedgerFactory must not be null");
        this.oxiaClientSupplier = () -> null;
        this.producerStateScheduler = new ScheduledThreadPoolExecutor(1, runnable -> {
            Thread thread = new Thread(runnable, "producer-state-manager-test");
            thread.setDaemon(true);
            return thread;
        });
        this.logConfigDefaults = Collections.emptyMap();
        this.topicConfigSupplier = null;
    }

    public Time time() {
        return time;
    }

    public int brokerId() {
        return brokerId;
    }

    public UrsaStorageConfig config() {
        return config;
    }

    public BrokerTopicStats brokerTopicStats() {
        return brokerTopicStats;
    }

    public void updateTopicConfig(String topic, Map<String, String> topicConfig) {
        try {
            updateTopicConfigAsync(topic, topicConfig).get(TOPIC_CONFIG_UPDATE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while updating topic config for " + topic, e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to update topic config for " + topic, e.getCause());
        } catch (TimeoutException e) {
            throw new RuntimeException("Timed out after " + TOPIC_CONFIG_UPDATE_TIMEOUT_SECONDS
                    + " seconds while updating topic config for " + topic, e);
        }
    }

    private CompletableFuture<Void> updateTopicConfigAsync(String topic, Map<String, String> topicConfig) {
        if (topic == null || topicConfig == null || managedLedgerFactoryHolder == null) {
            return CompletableFuture.completedFuture(null);
        }
        Map<String, String> configSnapshot = Map.copyOf(topicConfig);
        String mlName = KafkaManagedLedgerNaming.managedLedgerName(topic, 0);
        return managedLedgerFactoryHolder.asyncUpdateTopicConfig(mlName, configSnapshot)
                .whenComplete((ignored, error) -> {
                    if (error != null) {
                        log.warn("Failed to update topic config for managed ledger {}", mlName, error);
                    }
                });
    }

    public void deleteTopicConfig(String topic) {
        if (topic == null || managedLedgerFactoryHolder == null) {
            return;
        }
        String mlName = KafkaManagedLedgerNaming.managedLedgerName(topic, 0);
        managedLedgerFactoryHolder.asyncDeleteTopicConfig(mlName).join();
    }

    UrsaPartitionLog getOrCreatePartitionLog(TopicIdPartition tp) {
        return partitionLogs.computeIfAbsent(tp,
                ignored -> new UrsaPartitionLog(
                        tp,
                        this,
                        logMetrics,
                        openManagedLedger(tp),
                        oxiaClientSupplier,
                        config.getProducerStateSnapshotIntervalMs(),
                        config.getProducerStateSnapshotRecordThreshold(),
                        producerStateScheduler));
    }

    UrsaPartitionLog partitionLog(TopicIdPartition tp) {
        return partitionLogs.get(tp);
    }

    void removePartitionLog(TopicIdPartition tp, UrsaPartitionLog partitionLog) {
        if (tp == null || partitionLog == null) {
            return;
        }
        partitionLogs.remove(tp, partitionLog);
    }

    /**
     * Best-effort cleanup for a topic-partition that is no longer hosted by this broker.
     * <p>
     * Removes in-memory state and closes any cached {@link ManagedLedger} instance.
     *
     * @return true if any state was cleaned up; false if there was nothing to do.
     */
    public boolean cleanupPartition(TopicIdPartition tp) {
        return cleanupPartition(tp, false);
    }

    /**
     * Best-effort cleanup for a topic-partition that is no longer hosted by this broker.
     * <p>
     * Removes in-memory state and closes any cached {@link ManagedLedger} instance. Optionally deletes any
     * persisted producer-state snapshot when the partition is permanently deleted (for example, topic deletion).
     *
     * @param deletePartition whether to delete persisted producer-state data for this partition
     * @return true if any in-memory state was cleaned up; false if there was nothing to do.
     */
    public boolean cleanupPartition(TopicIdPartition tp, boolean deletePartition) {
        if (tp == null) {
            return false;
        }
        UrsaPartitionLog partitionLog = partitionLogs.remove(tp);
        if (partitionLog != null) {
            partitionLog.close(deletePartition);
            return true;
        }
        return false;
    }

    public boolean cleanupNonOwnedProducerStates(
            TopicIdPartition tp,
            Set<String> ownedZones,
            boolean deletePartition) {
        if (tp == null) {
            return false;
        }
        UrsaPartitionLog partitionLog = partitionLogs.get(tp);
        if (partitionLog == null) {
            return false;
        }

        return partitionLog.cleanupNonOwnedProducerStates(ownedZones, deletePartition);
    }

    /**
     * Permanently delete the managed ledger and underlying Ursa stream for a partition.
     *
     * <p>This path is idempotent: missing metadata is treated as success because the desired end state is already
     * reached.
     */
    public void deletePartitionData(TopicIdPartition tp) {
        if (tp == null) {
            return;
        }

        String managedLedgerName = KafkaManagedLedgerNaming.managedLedgerName(tp);
        try {
            managedLedgerFactory.delete(managedLedgerName, CompletableFuture.completedFuture(null));
        } catch (ManagedLedgerException.MetadataNotFoundException e) {
            log.debug("ManagedLedger metadata already deleted for partition {}", tp, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted deleting partition data for " + tp, e);
        } catch (ManagedLedgerException e) {
            throw new RuntimeException("Failed to delete partition data for " + tp, e);
        }
    }

    public Set<TopicIdPartition> snapshotTrackedPartitions() {
        return new LinkedHashSet<>(partitionLogs.keySet());
    }

    ManagedLedger applyRetentionConfig(TopicIdPartition tp, ManagedLedger managedLedger) {
        try {
            RetentionConfig retentionConfig = buildRetentionConfig(tp);
            maybeUpdateRetentionConfig(managedLedger, retentionConfig.retentionMs, retentionConfig.retentionBytes);
        } catch (Exception e) {
            log.warn("Failed to apply retention config for {}", tp, e);
        }
        return managedLedger;
    }

    static void maybeUpdateRetentionConfig(ManagedLedger managedLedger, long retentionMs, long retentionBytes) {
        ManagedLedgerConfig currentConfig = managedLedger.getConfig();
        long targetRetentionMs = normalizeRetentionMs(retentionMs);
        long targetRetentionSizeMb = toRetentionSizeMb(retentionBytes);

        if (currentConfig.getRetentionTimeMillis() == targetRetentionMs
                && currentConfig.getRetentionSizeInMB() == targetRetentionSizeMb) {
            return;
        }

        ManagedLedgerConfig updatedConfig = copyConfig(currentConfig);
        setRetentionTime(updatedConfig, targetRetentionMs);
        updatedConfig.setRetentionSizeInMB(targetRetentionSizeMb);
        managedLedger.setConfig(updatedConfig);
    }

    private RetentionConfig buildRetentionConfig(TopicIdPartition tp) {
        Properties overrides = new Properties();
        if (topicConfigSupplier != null) {
            Map<String, String> topicConfigs = topicConfigSupplier.apply(tp.topic());
            if (topicConfigs != null) {
                topicConfigs.forEach(overrides::setProperty);
            }
        }
        long retentionMs = getLongConfig(overrides, TopicConfig.RETENTION_MS_CONFIG,
                getDefaultLongConfig(TopicConfig.RETENTION_MS_CONFIG, -1L));
        long retentionBytes = getLongConfig(overrides, TopicConfig.RETENTION_BYTES_CONFIG,
                getDefaultLongConfig(TopicConfig.RETENTION_BYTES_CONFIG, -1L));
        return new RetentionConfig(retentionMs, retentionBytes);
    }

    private long getDefaultLongConfig(String key, long fallback) {
        Object value = logConfigDefaults.get(key);
        if (value == null) {
            return fallback;
        }
        return Long.parseLong(String.valueOf(value));
    }

    private static long getLongConfig(Properties overrides, String key, long fallback) {
        String value = overrides.getProperty(key);
        if (value == null || value.isBlank()) {
            return fallback;
        }
        return Long.parseLong(value);
    }

    private static long toRetentionSizeMb(long retentionBytes) {
        if (retentionBytes <= 0) {
            return retentionBytes;
        }
        long mb = retentionBytes / (1024 * 1024);
        return retentionBytes % (1024 * 1024) == 0 ? mb : mb + 1;
    }

    private static long normalizeRetentionMs(long retentionMs) {
        if (retentionMs <= 0) {
            return retentionMs;
        }
        return normalizeDurationMillis(retentionMs, true);
    }

    private static void setRetentionTime(ManagedLedgerConfig config, long retentionMs) {
        if (retentionMs < 0) {
            config.setRetentionTime(-1, TimeUnit.MILLISECONDS);
            return;
        }
        if (retentionMs == 0) {
            config.setRetentionTime(0, TimeUnit.MILLISECONDS);
            return;
        }
        setDurationMillis(config::setRetentionTime, retentionMs);
    }

    private static void setDurationMillis(BiConsumer<Integer, TimeUnit> setter, long millis) {
        setDurationMillisInternal(millis, true, (value, unit) -> setter.accept(value.intValue(), unit));
    }

    private static void setDurationMillisLong(BiConsumer<Long, TimeUnit> setter, long millis) {
        setDurationMillisInternal(millis, false, setter);
    }

    private static long normalizeDurationMillis(long millis, boolean intBased) {
        return setDurationMillisInternal(millis, intBased, null);
    }

    private static long setDurationMillisInternal(long millis,
                                                  boolean intBased,
                                                  BiConsumer<Long, TimeUnit> setter) {
        TimeUnit[] units = new TimeUnit[]{
            TimeUnit.MILLISECONDS,
            TimeUnit.SECONDS,
            TimeUnit.MINUTES,
            TimeUnit.HOURS,
            TimeUnit.DAYS
        };
        for (TimeUnit unit : units) {
            long unitMillis = unit.toMillis(1);
            long value = (millis + unitMillis - 1) / unitMillis;
            long maxValue = intBased ? Integer.MAX_VALUE : Long.MAX_VALUE;
            if (value <= maxValue) {
                if (setter != null) {
                    setter.accept(value, unit);
                }
                return unit.toMillis(value);
            }
        }
        if (setter != null) {
            setter.accept(intBased ? (long) Integer.MAX_VALUE : Long.MAX_VALUE, TimeUnit.DAYS);
        }
        long clampedValue = intBased ? Integer.MAX_VALUE : Long.MAX_VALUE;
        return TimeUnit.DAYS.toMillis(clampedValue);
    }

    private static ManagedLedgerConfig copyConfig(ManagedLedgerConfig source) {
        ManagedLedgerConfig target = new ManagedLedgerConfig();
        target.setCreateIfMissing(source.isCreateIfMissing());
        target.setLazyCursorRecovery(source.isLazyCursorRecovery());
        target.setMaxEntriesPerLedger(source.getMaxEntriesPerLedger());
        target.setMaxSizePerLedgerMb(source.getMaxSizePerLedgerMb());
        setDurationMillis(target::setMinimumRolloverTime, source.getMinimumRolloverTimeMs());
        setDurationMillis(target::setMaximumRolloverTime, source.getMaximumRolloverTimeMs());
        target.setEnsembleSize(source.getEnsembleSize());
        target.setWriteQuorumSize(source.getWriteQuorumSize());
        target.setAckQuorumSize(source.getAckQuorumSize());
        target.setDigestType(source.getDigestType());
        target.setPassword(new String(source.getPassword(), StandardCharsets.UTF_8));
        target.setUnackedRangesOpenCacheSetEnabled(source.isUnackedRangesOpenCacheSetEnabled());
        target.setMetadataEnsembleSize(source.getMetadataEnsemblesize());
        target.setMetadataWriteQuorumSize(source.getMetadataWriteQuorumSize());
        target.setMetadataAckQuorumSize(source.getMetadataAckQuorumSize());
        target.setMetadataMaxEntriesPerLedger(source.getMetadataMaxEntriesPerLedger());
        target.setLedgerRolloverTimeout(source.getLedgerRolloverTimeout());
        target.setThrottleMarkDelete(source.getThrottleMarkDelete());
        target.setAutoSkipNonRecoverableData(source.isAutoSkipNonRecoverableData());
        target.setLedgerForceRecovery(source.isLedgerForceRecovery());
        target.setPersistIndividualAckAsLongArray(source.isPersistIndividualAckAsLongArray());
        target.setMaxBatchDeletedIndexToPersist(source.getMaxBatchDeletedIndexToPersist());
        target.setPersistentUnackedRangesWithMultipleEntriesEnabled(
                source.isPersistentUnackedRangesWithMultipleEntriesEnabled());
        target.setMaxUnackedRangesToPersist(source.getMaxUnackedRangesToPersist());
        target.setMaxUnackedRangesToPersistInMetadataStore(source.getMaxUnackedRangesToPersistInMetadataStore());
        target.setLedgerOffloader(source.getLedgerOffloader());
        target.setClock(source.getClock());
        target.setMetadataOperationsTimeoutSeconds(source.getMetadataOperationsTimeoutSeconds());
        target.setReadEntryTimeoutSeconds(source.getReadEntryTimeoutSeconds());
        target.setAddEntryTimeoutSeconds(source.getAddEntryTimeoutSeconds());
        target.setBookKeeperEnsemblePlacementPolicyClassName(source.getBookKeeperEnsemblePlacementPolicyClassName());
        target.setBookKeeperEnsemblePlacementPolicyProperties(source.getBookKeeperEnsemblePlacementPolicyProperties());
        if (source.getProperties() != null) {
            target.setProperties(new HashMap<>(source.getProperties()));
        }
        target.setDeletionAtBatchIndexLevelEnabled(source.isDeletionAtBatchIndexLevelEnabled());
        target.setNewEntriesCheckDelayInMillis(source.getNewEntriesCheckDelayInMillis());
        target.setManagedLedgerInterceptor(source.getManagedLedgerInterceptor());
        setDurationMillis(target::setInactiveLedgerRollOverTime, source.getInactiveLedgerRollOverTimeMs());
        setDurationMillisLong(target::setInactiveOffloadedLedgerEvictionTime,
                source.getInactiveOffloadedLedgerEvictionTimeMs());
        target.setMinimumBacklogCursorsForCaching(source.getMinimumBacklogCursorsForCaching());
        target.setMinimumBacklogEntriesForCaching(source.getMinimumBacklogEntriesForCaching());
        target.setMaxBacklogBetweenCursorsForCaching(source.getMaxBacklogBetweenCursorsForCaching());
        target.setTriggerOffloadOnTopicLoad(source.isTriggerOffloadOnTopicLoad());
        target.setStorageClassName(source.getStorageClassName());
        target.setShadowSourceName(source.getShadowSourceName());
        target.setCacheEvictionByMarkDeletedPosition(source.isCacheEvictionByMarkDeletedPosition());
        target.setCacheEvictionByExpectedReadCount(source.isCacheEvictionByExpectedReadCount());
        return target;
    }

    private record RetentionConfig(long retentionMs, long retentionBytes) {
    }

    @Override
    public void close() throws IOException {
        partitionLogs.values().forEach(UrsaPartitionLog::close);
        partitionLogs.clear();
        producerStateScheduler.shutdown();
        try {
            if (!producerStateScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                producerStateScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            producerStateScheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        if (managedLedgerFactoryHolder != null) {
            managedLedgerFactoryHolder.close();
        }
    }

    CompletableFuture<ManagedLedger> openManagedLedger(TopicIdPartition tp) {
        String name = KafkaManagedLedgerNaming.managedLedgerName(tp);
        ManagedLedgerConfig mlConfig = new ManagedLedgerConfig().setCreateIfMissing(true);

        Map<String, String> topicConfig = topicConfigSupplier != null
                ? topicConfigSupplier.apply(tp.topic())
                : null;
        CompletableFuture<Void> updateConfigFuture = topicConfig != null
                ? updateTopicConfigAsync(tp.topic(), topicConfig)
                : CompletableFuture.completedFuture(null);

        return updateConfigFuture.thenCompose(ignored -> {
            CompletableFuture<ManagedLedger> openFuture = new CompletableFuture<>();
            managedLedgerFactory.asyncOpen(name, mlConfig, new AsyncCallbacks.OpenLedgerCallback() {
                @Override
                public void openLedgerComplete(ManagedLedger ledger, Object ctx) {
                    openFuture.complete(ledger);
                }

                @Override
                public void openLedgerFailed(ManagedLedgerException exception, Object ctx) {
                    openFuture.completeExceptionally(exception);
                }
            }, null, null);
            return openFuture;
        }).thenApply(managedLedger -> applyRetentionConfig(tp, managedLedger));
    }

}
