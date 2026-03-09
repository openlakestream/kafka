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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;
import org.apache.kafka.storage.diskless.idempotent.ProducerStateStore;
import org.apache.kafka.storage.diskless.idempotent.UrsaProducerStateStore;
import org.apache.kafka.storage.internals.log.LogMetricNames;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;

import io.streamnative.ursa.mledger.UrsaPosition;

/**
 * Shared state container for Ursa storage components.
 * Manages ManagedLedger instances for UrsaManagedLedgerReader and UrsaManagedLedgerWriter.
 */
public class UrsaStorageState implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(UrsaStorageState.class);

    private final Time time;
    private final int brokerId;
    private final UrsaStorageConfig config;
    private final BrokerTopicStats brokerTopicStats;
    private final KafkaMetricsGroup logMetricsGroup = new KafkaMetricsGroup("kafka.log", "Log");

    private final ProducerStateStore producerStateStore;

    private final KafkaManagedLedgerFactoryHolder managedLedgerFactoryHolder;
    private final ManagedLedgerFactory managedLedgerFactory;
    private final Map<String, Object> logConfigDefaults;
    private final Function<String, Map<String, String>> topicConfigSupplier;
    private final ConcurrentHashMap<TopicIdPartition, CompletableFuture<ManagedLedger>> managedLedgerCache =
            new ConcurrentHashMap<>();

    /**
     * Creates UrsaStorageState for production use.
     */
    public UrsaStorageState(
            Time time,
            int brokerId,
            UrsaStorageConfig config,
            BrokerTopicStats brokerTopicStats) {
        this(time, brokerId, config, brokerTopicStats, null, null, null);
    }

    /**
     * Creates UrsaStorageState with a custom ProducerStateStore.
     * Useful for testing idempotent validation logic.
     */
    public UrsaStorageState(
            Time time,
            int brokerId,
            UrsaStorageConfig config,
            BrokerTopicStats brokerTopicStats,
            ProducerStateStore producerStateStore) {
        this(time, brokerId, config, brokerTopicStats, producerStateStore, null, null);
    }

    public UrsaStorageState(
            Time time,
            int brokerId,
            UrsaStorageConfig config,
            BrokerTopicStats brokerTopicStats,
            Map<String, Object> logConfigDefaults,
            Function<String, Map<String, String>> topicConfigSupplier) {
        this(time, brokerId, config, brokerTopicStats, null, logConfigDefaults, topicConfigSupplier);
    }

    public UrsaStorageState(
            Time time,
            int brokerId,
            UrsaStorageConfig config,
            BrokerTopicStats brokerTopicStats,
            ProducerStateStore producerStateStore,
            Map<String, Object> logConfigDefaults,
            Function<String, Map<String, String>> topicConfigSupplier) {
        this.time = time;
        this.brokerId = brokerId;
        this.config = config;
        this.brokerTopicStats = brokerTopicStats;
        this.logConfigDefaults = logConfigDefaults != null ? logConfigDefaults : Collections.emptyMap();
        this.topicConfigSupplier = topicConfigSupplier;

        KafkaManagedLedgerFactoryHolder holder;
        try {
            holder = KafkaManagedLedgerFactoryHolder.create(config);
            log.info("Initialized ManagedLedgerFactory for Kafka diskless storage");
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize ManagedLedgerFactory", e);
        }
        this.managedLedgerFactoryHolder = holder;
        this.managedLedgerFactory = holder.factory();

        this.producerStateStore = producerStateStore != null 
                ? producerStateStore 
                : new UrsaProducerStateStore(holder::oxiaClient, time);
    }

    UrsaStorageState(
            Time time,
            int brokerId,
            UrsaStorageConfig config,
            BrokerTopicStats brokerTopicStats,
            ProducerStateStore producerStateStore,
            ManagedLedgerFactory managedLedgerFactory) {
        this.time = Objects.requireNonNull(time, "time must not be null");
        this.brokerId = brokerId;
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.brokerTopicStats = Objects.requireNonNull(brokerTopicStats, "brokerTopicStats must not be null");
        this.producerStateStore = Objects.requireNonNull(producerStateStore, "producerStateStore must not be null");
        this.managedLedgerFactoryHolder = null;
        this.managedLedgerFactory = Objects.requireNonNull(managedLedgerFactory, "managedLedgerFactory must not be null");
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

    public ProducerStateStore producerStateStore() {
        return producerStateStore;
    }

    public CompletableFuture<ManagedLedger> getOrCreateManagedLedger(TopicIdPartition tp) {
        CompletableFuture<ManagedLedger> existing = managedLedgerCache.get(tp);
        if (existing != null) {
            return existing.thenApply(managedLedger -> applyRetentionConfig(tp, managedLedger));
        }

        String name = KafkaManagedLedgerNaming.managedLedgerName(tp);
        ManagedLedgerConfig mlConfig = new ManagedLedgerConfig().setCreateIfMissing(true);

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

        CompletableFuture<ManagedLedger> configuredFuture = openFuture.thenApply(managedLedger ->
                applyRetentionConfig(tp, managedLedger));

        CompletableFuture<ManagedLedger> raced = managedLedgerCache.putIfAbsent(tp, configuredFuture);
        if (raced != null) {
            return raced.thenApply(managedLedger -> applyRetentionConfig(tp, managedLedger));
        }

        // Evict from cache on failure to allow retry after transient errors
        configuredFuture.whenComplete((ledger, error) -> {
            if (error != null) {
                managedLedgerCache.remove(tp, configuredFuture);
                log.warn("Failed to open ManagedLedger for partition {}, evicting from cache", tp, error);
                return;
            }
            if (managedLedgerCache.get(tp) != configuredFuture) {
                log.debug("Skipping diskless log metric registration for partition {} because this future is not cache owner", tp);
                return;
            }
            maybeRegisterDisklessLogMetrics(tp, ledger);
        });

        return configuredFuture;
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

        boolean cleaned = removeDisklessLogMetrics(tp);

        CompletableFuture<ManagedLedger> ledgerFuture = managedLedgerCache.remove(tp);
        if (ledgerFuture != null) {
            cleaned = true;
            ledgerFuture.whenComplete((ledger, error) -> {
                if (ledger == null) {
                    return;
                }
                try {
                    ledger.close();
                } catch (Exception e) {
                    log.warn("Failed to close ManagedLedger for partition {}", tp, e);
                }
            });
        }

        try {
            CompletableFuture<Void> producerStateFuture = deletePartition
                    ? producerStateStore.deletePartition(tp)
                    : producerStateStore.clearPartition(tp);
            producerStateFuture.exceptionally(e -> {
                log.warn("Failed to clear producer state for partition {}", tp, e);
                return null;
            });
        } catch (Throwable t) {
            log.warn("Failed to clear producer state for partition {}", tp, t);
        }

        return cleaned;
    }

    private ManagedLedger applyRetentionConfig(TopicIdPartition tp, ManagedLedger managedLedger) {
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

    private void maybeRegisterDisklessLogMetrics(TopicIdPartition topicIdPartition, ManagedLedger managedLedger) {
        if (topicIdPartition == null || managedLedger == null) {
            return;
        }
        TopicPartition topicPartition = topicIdPartition.topicPartition();
        Map<String, String> tags = createLogMetricTags(topicPartition);

        logMetricsGroup.newGauge(
                LogMetricNames.SIZE,
                () -> Math.max(0L, readPartitionSize(managedLedger)),
                tags
        );
        logMetricsGroup.newGauge(
                LogMetricNames.LOG_START_OFFSET,
                () -> Math.max(0L, readPartitionLogStartOffset(managedLedger)),
                tags
        );
        logMetricsGroup.newGauge(
                LogMetricNames.LOG_END_OFFSET,
                () -> Math.max(0L, readPartitionLogEndOffset(managedLedger)),
                tags
        );
    }

    private boolean hasAnyExistingLogMetrics(Map<String, String> tags) {
        return metricExists(LogMetricNames.SIZE, tags)
                || metricExists(LogMetricNames.LOG_START_OFFSET, tags)
                || metricExists(LogMetricNames.LOG_END_OFFSET, tags);
    }

    private boolean metricExists(String metricName, Map<String, String> tags) {
        return KafkaYammerMetrics.defaultRegistry().allMetrics().containsKey(logMetricsGroup.metricName(metricName, tags));
    }

    private Map<String, String> createLogMetricTags(TopicPartition topicPartition) {
        LinkedHashMap<String, String> tags = new LinkedHashMap<>();
        tags.put("topic", topicPartition.topic());
        tags.put("partition", String.valueOf(topicPartition.partition()));
        return tags;
    }

    private boolean removeDisklessLogMetrics(TopicIdPartition topicIdPartition) {
        Map<String, String> tags = createLogMetricTags(topicIdPartition.topicPartition());
        boolean existing = hasAnyExistingLogMetrics(tags);
        removeLogMetrics(tags);
        return existing;
    }

    private void removeAllDisklessLogMetrics() {
        managedLedgerCache.keySet().stream()
                .map(TopicIdPartition::topicPartition)
                .distinct()
                .forEach(topicPartition -> removeLogMetrics(createLogMetricTags(topicPartition)));
    }

    private void removeLogMetrics(Map<String, String> tags) {
        logMetricsGroup.removeMetric(LogMetricNames.SIZE, tags);
        logMetricsGroup.removeMetric(LogMetricNames.LOG_START_OFFSET, tags);
        logMetricsGroup.removeMetric(LogMetricNames.LOG_END_OFFSET, tags);
    }

    private long readPartitionSize(ManagedLedger managedLedger) {
        return Math.max(0L, managedLedger.getTotalSize());
    }

    private long readPartitionLogStartOffset(ManagedLedger managedLedger) {
        return resolveLogStartOffset(managedLedger);
    }

    private long readPartitionLogEndOffset(ManagedLedger managedLedger) {
        return resolveLogEndOffset(managedLedger);
    }

    private long resolveLogStartOffset(ManagedLedger managedLedger) {
        Position firstPosition = managedLedger.getFirstPosition();
        if (isInvalidPosition(firstPosition)) {
            return 0L;
        }
        if (firstPosition.compareTo(PositionFactory.EARLIEST) == 0) {
            return 0L;
        }
        return Math.max(0L, firstPosition.getEntryId());
    }

    private long resolveLogEndOffset(ManagedLedger managedLedger) {
        Position lastConfirmedEntry = managedLedger.getLastConfirmedEntry();
        if (isInvalidPosition(lastConfirmedEntry)) {
            return 0L;
        }
        if (lastConfirmedEntry instanceof UrsaPosition ursaPosition) {
            return Math.max(0L, ursaPosition.getEntryId() + Math.max(1, ursaPosition.numMessages()));
        }
        return Math.max(0L, lastConfirmedEntry.getEntryId() + 1);
    }

    private boolean isInvalidPosition(Position position) {
        return position == null || position.getEntryId() < 0;
    }

    @Override
    public void close() throws IOException {
        removeAllDisklessLogMetrics();
        managedLedgerCache.clear();
        producerStateStore.close();
        if (managedLedgerFactoryHolder != null) {
            managedLedgerFactoryHolder.close();
        }
    }

}
