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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;
import org.apache.kafka.storage.diskless.DisklessClientZone;
import org.apache.kafka.storage.diskless.idempotent.ProducerStateManager;
import org.apache.kafka.storage.internals.log.LogMetricNames;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class UrsaStorageStateTest {
    private static final String LOG_METRIC_GROUP = "kafka.log";
    private static final String LOG_METRIC_TYPE = "Log";

    @Test
    void testCleanupPartitionClosesManagedLedgerAndClearsProducerState() throws Exception {
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("test-topic", 0));

        ProducerStateManager producerStateManager = mock(ProducerStateManager.class);
        when(producerStateManager.cleanup(false)).thenReturn(CompletableFuture.completedFuture(null));

        ManagedLedgerFactory managedLedgerFactory = mock(ManagedLedgerFactory.class);
        ManagedLedger ledger1 = mock(ManagedLedger.class);
        ManagedLedger ledger2 = mock(ManagedLedger.class);
        CountDownLatch closeLatch = new CountDownLatch(1);
        doAnswer(invocation -> {
            closeLatch.countDown();
            return null;
        }).when(ledger1).close();

        AtomicInteger openCount = new AtomicInteger(0);
        doAnswer(invocation -> {
            AsyncCallbacks.OpenLedgerCallback callback = invocation.getArgument(2);
            int attempt = openCount.getAndIncrement();
            callback.openLedgerComplete(attempt == 0 ? ledger1 : ledger2, invocation.getArgument(3));
            return null;
        }).when(managedLedgerFactory).asyncOpen(
                anyString(),
                any(ManagedLedgerConfig.class),
                any(AsyncCallbacks.OpenLedgerCallback.class),
                any(),
                any()
        );

        try (UrsaStorageState state = new UrsaStorageState(
                Time.SYSTEM,
                1,
                mock(UrsaStorageConfig.class),
                mock(BrokerTopicStats.class),
                managedLedgerFactory)) {
            ensureManagedLedger(state, tp);
            installProducerStateManager(state, tp, DisklessClientZone.NO_ZONE, producerStateManager);

            assertTrue(state.cleanupPartition(tp));

            verify(producerStateManager, times(1)).cleanup(false);
            verify(managedLedgerFactory, times(1)).asyncOpen(anyString(), any(), any(), any(), any());

            assertTrue(closeLatch.await(5, TimeUnit.SECONDS));

            ensureManagedLedger(state, tp);
            verify(managedLedgerFactory, times(2)).asyncOpen(anyString(), any(), any(), any(), any());
        }
    }

    @Test
    void testCleanupPartitionNoopWhenNoState() {
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("test-topic", 0));

        ProducerStateManager producerStateManager = mock(ProducerStateManager.class);
        when(producerStateManager.cleanup(false)).thenReturn(CompletableFuture.completedFuture(null));
        ManagedLedgerFactory managedLedgerFactory = mock(ManagedLedgerFactory.class);

        UrsaStorageState state = new UrsaStorageState(
                Time.SYSTEM,
                1,
                mock(UrsaStorageConfig.class),
                mock(BrokerTopicStats.class),
                managedLedgerFactory);

        assertFalse(state.cleanupPartition(tp));

        verify(producerStateManager, never()).cleanup(false);
        verify(managedLedgerFactory, never()).asyncOpen(anyString(), any(), any(), any(), any());
    }

    @Test
    void testCleanupNonOwnedProducerStatesRemovesOnlyStaleZones() throws Exception {
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("test-topic", 1));

        ProducerStateManager noZoneManager = mock(ProducerStateManager.class);
        ProducerStateManager zoneAManager = mock(ProducerStateManager.class);
        when(noZoneManager.cleanup(false)).thenReturn(CompletableFuture.completedFuture(null));
        when(zoneAManager.cleanup(false)).thenReturn(CompletableFuture.completedFuture(null));

        ManagedLedgerFactory managedLedgerFactory = mock(ManagedLedgerFactory.class);

        try (UrsaStorageState state = new UrsaStorageState(
                Time.SYSTEM,
                1,
                mock(UrsaStorageConfig.class),
                mock(BrokerTopicStats.class),
                managedLedgerFactory)) {
            installProducerStateManager(state, tp, DisklessClientZone.NO_ZONE, noZoneManager);
            installProducerStateManager(state, tp, "zone-a", zoneAManager);

            assertTrue(state.cleanupNonOwnedProducerStates(tp, Set.of("zone-a"), false));
            verify(noZoneManager).cleanup(false);
            verify(zoneAManager, never()).cleanup(false);
            assertEquals(Set.of(tp), state.snapshotTrackedPartitions());

            assertTrue(state.cleanupPartition(tp, false));
            verify(zoneAManager).cleanup(false);
        }
    }

    @Test
    void testDeletePartitionDataUsesManagedLedgerFactoryDelete() throws Exception {
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("delete-topic", 0));
        ManagedLedgerFactory managedLedgerFactory = mock(ManagedLedgerFactory.class);

        try (UrsaStorageState state = new UrsaStorageState(
                Time.SYSTEM,
                1,
                mock(UrsaStorageConfig.class),
                mock(BrokerTopicStats.class),
                managedLedgerFactory)) {
            state.deletePartitionData(tp);

            verify(managedLedgerFactory).delete(
                    eq("public/default/persistent/delete-topic-partition-0"),
                    any(CompletableFuture.class)
            );
            verify(managedLedgerFactory, never()).asyncOpen(anyString(), any(), any(), any(), any());
        }
    }

    @Test
    void testDeletePartitionDataTreatsMissingMetadataAsSuccess() throws Exception {
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("delete-missing-topic", 0));
        ManagedLedgerFactory managedLedgerFactory = mock(ManagedLedgerFactory.class);
        doThrow(new org.apache.bookkeeper.mledger.ManagedLedgerException.MetadataNotFoundException("missing"))
                .when(managedLedgerFactory)
                .delete(eq("public/default/persistent/delete-missing-topic-partition-0"), any(CompletableFuture.class));

        try (UrsaStorageState state = new UrsaStorageState(
                Time.SYSTEM,
                1,
                mock(UrsaStorageConfig.class),
                mock(BrokerTopicStats.class),
                managedLedgerFactory)) {
            state.deletePartitionData(tp);

            verify(managedLedgerFactory).delete(
                    eq("public/default/persistent/delete-missing-topic-partition-0"),
                    any(CompletableFuture.class)
            );
        }
    }

    @Test
    void testDisklessLogMetricsRegisteredAfterManagedLedgerOpen() throws Exception {
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("metric-open-topic", 0));

        ProducerStateManager producerStateManager = mock(ProducerStateManager.class);
        when(producerStateManager.cleanup(false)).thenReturn(CompletableFuture.completedFuture(null));

        ManagedLedgerFactory managedLedgerFactory = mock(ManagedLedgerFactory.class);
        ManagedLedger managedLedger = mock(ManagedLedger.class);
        Position firstPosition = mock(Position.class);
        Position lastConfirmedPosition = mock(Position.class);
        when(managedLedger.getTotalSize()).thenReturn(1234L);
        when(firstPosition.getEntryId()).thenReturn(5L);
        when(firstPosition.compareTo(PositionFactory.EARLIEST)).thenReturn(1);
        when(lastConfirmedPosition.getEntryId()).thenReturn(41L);
        when(managedLedger.getFirstPosition()).thenReturn(firstPosition);
        when(managedLedger.getLastConfirmedEntry()).thenReturn(lastConfirmedPosition);
        doAnswer(invocation -> {
            AsyncCallbacks.OpenLedgerCallback callback = invocation.getArgument(2);
            callback.openLedgerComplete(managedLedger, invocation.getArgument(3));
            return null;
        }).when(managedLedgerFactory).asyncOpen(anyString(), any(ManagedLedgerConfig.class), any(), any(), any());

        try (UrsaStorageState state = new UrsaStorageState(
                Time.SYSTEM,
                1,
                mock(UrsaStorageConfig.class),
                mock(BrokerTopicStats.class),
                managedLedgerFactory)) {
            assertNull(jmxGaugeLongValue(LogMetricNames.SIZE, tp.topicPartition()));
            assertNull(jmxGaugeLongValue(LogMetricNames.LOG_START_OFFSET, tp.topicPartition()));
            assertNull(jmxGaugeLongValue(LogMetricNames.LOG_END_OFFSET, tp.topicPartition()));

            ensureManagedLedger(state, tp);

            assertEquals(1234L, jmxGaugeLongValue(LogMetricNames.SIZE, tp.topicPartition()));
            assertEquals(5L, jmxGaugeLongValue(LogMetricNames.LOG_START_OFFSET, tp.topicPartition()));
            assertEquals(42L, jmxGaugeLongValue(LogMetricNames.LOG_END_OFFSET, tp.topicPartition()));
        }
    }

    @Test
    void testDisklessLogMetricsRemovedOnCleanupPartition() throws Exception {
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("metric-cleanup-topic", 0));

        ProducerStateManager producerStateManager = mock(ProducerStateManager.class);
        when(producerStateManager.cleanup(false)).thenReturn(CompletableFuture.completedFuture(null));

        ManagedLedgerFactory managedLedgerFactory = mock(ManagedLedgerFactory.class);
        ManagedLedger managedLedger = mock(ManagedLedger.class);
        when(managedLedger.getTotalSize()).thenReturn(100L);
        doAnswer(invocation -> {
            AsyncCallbacks.OpenLedgerCallback callback = invocation.getArgument(2);
            callback.openLedgerComplete(managedLedger, invocation.getArgument(3));
            return null;
        }).when(managedLedgerFactory).asyncOpen(anyString(), any(ManagedLedgerConfig.class), any(), any(), any());

        try (UrsaStorageState state = new UrsaStorageState(
                Time.SYSTEM,
                1,
                mock(UrsaStorageConfig.class),
                mock(BrokerTopicStats.class),
                managedLedgerFactory)) {
            ensureManagedLedger(state, tp);
            assertNotNull(jmxGaugeLongValue(LogMetricNames.SIZE, tp.topicPartition()));
            assertNotNull(jmxGaugeLongValue(LogMetricNames.LOG_START_OFFSET, tp.topicPartition()));
            assertNotNull(jmxGaugeLongValue(LogMetricNames.LOG_END_OFFSET, tp.topicPartition()));

            state.cleanupPartition(tp, false);

            assertNull(jmxGaugeLongValue(LogMetricNames.SIZE, tp.topicPartition()));
            assertNull(jmxGaugeLongValue(LogMetricNames.LOG_START_OFFSET, tp.topicPartition()));
            assertNull(jmxGaugeLongValue(LogMetricNames.LOG_END_OFFSET, tp.topicPartition()));
        }
    }

    @Test
    void testManagedLedgerOpenInitializesPartitionLog() throws Exception {
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("read-state-topic", 0));

        ProducerStateManager producerStateManager = mock(ProducerStateManager.class);
        when(producerStateManager.cleanup(false)).thenReturn(CompletableFuture.completedFuture(null));

        ManagedLedgerFactory managedLedgerFactory = mock(ManagedLedgerFactory.class);
        ManagedLedger managedLedger = mock(ManagedLedger.class);
        doAnswer(invocation -> {
            AsyncCallbacks.OpenLedgerCallback callback = invocation.getArgument(2);
            callback.openLedgerComplete(managedLedger, invocation.getArgument(3));
            return null;
        }).when(managedLedgerFactory).asyncOpen(anyString(), any(ManagedLedgerConfig.class), any(), any(), any());

        try (UrsaStorageState state = new UrsaStorageState(
                Time.SYSTEM,
                1,
                mock(UrsaStorageConfig.class),
                mock(BrokerTopicStats.class),
                managedLedgerFactory)) {
            assertNull(state.partitionLog(tp));

            ensureManagedLedger(state, tp);

            assertNotNull(state.partitionLog(tp));
        }
    }

    @Test
    void testCleanupPartitionSuppressesMetricRegistrationForInFlightManagedLedgerOpen() throws Exception {
        TopicIdPartition tp = new TopicIdPartition(
                Uuid.randomUuid(),
                new TopicPartition("metric-cleanup-race-topic-" + Uuid.randomUuid(), 0)
        );

        ProducerStateManager producerStateManager = mock(ProducerStateManager.class);
        when(producerStateManager.cleanup(false)).thenReturn(CompletableFuture.completedFuture(null));

        ManagedLedgerFactory managedLedgerFactory = mock(ManagedLedgerFactory.class);
        ManagedLedger firstLedger = mock(ManagedLedger.class);
        ManagedLedger secondLedger = mock(ManagedLedger.class);
        CountDownLatch firstCloseLatch = new CountDownLatch(1);
        doAnswer(invocation -> {
            firstCloseLatch.countDown();
            return null;
        }).when(firstLedger).close();
        when(secondLedger.getTotalSize()).thenReturn(222L);

        AtomicInteger openCount = new AtomicInteger(0);
        AtomicReference<AsyncCallbacks.OpenLedgerCallback> firstOpenCallback = new AtomicReference<>();
        doAnswer(invocation -> {
            AsyncCallbacks.OpenLedgerCallback callback = invocation.getArgument(2);
            if (openCount.getAndIncrement() == 0) {
                firstOpenCallback.set(callback);
            } else {
                callback.openLedgerComplete(secondLedger, invocation.getArgument(3));
            }
            return null;
        }).when(managedLedgerFactory).asyncOpen(
                anyString(),
                any(ManagedLedgerConfig.class),
                any(AsyncCallbacks.OpenLedgerCallback.class),
                any(),
                any()
        );

        try (UrsaStorageState state = new UrsaStorageState(
                Time.SYSTEM,
                1,
                mock(UrsaStorageConfig.class),
                mock(BrokerTopicStats.class),
                managedLedgerFactory)) {
            ensureManagedLedger(state, tp);

            assertTrue(state.cleanupPartition(tp, false));
            assertNull(jmxGaugeLongValue(LogMetricNames.SIZE, tp.topicPartition()));
            assertNull(jmxGaugeLongValue(LogMetricNames.LOG_START_OFFSET, tp.topicPartition()));
            assertNull(jmxGaugeLongValue(LogMetricNames.LOG_END_OFFSET, tp.topicPartition()));

            firstOpenCallback.get().openLedgerComplete(firstLedger, null);
            assertTrue(firstCloseLatch.await(5, TimeUnit.SECONDS));
            assertNull(jmxGaugeLongValue(LogMetricNames.SIZE, tp.topicPartition()));
            assertNull(jmxGaugeLongValue(LogMetricNames.LOG_START_OFFSET, tp.topicPartition()));
            assertNull(jmxGaugeLongValue(LogMetricNames.LOG_END_OFFSET, tp.topicPartition()));

            ensureManagedLedger(state, tp);

            verify(managedLedgerFactory, times(2)).asyncOpen(anyString(), any(), any(), any(), any());
            assertEquals(222L, jmxGaugeLongValue(LogMetricNames.SIZE, tp.topicPartition()));
        }
    }

    @Test
    void testDisklessLogMetricsRemovedOnStateClose() throws Exception {
        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("metric-close-topic", 0));
        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("metric-close-topic", 1));

        ProducerStateManager producerStateManager = mock(ProducerStateManager.class);
        when(producerStateManager.cleanup(false)).thenReturn(CompletableFuture.completedFuture(null));

        ManagedLedgerFactory managedLedgerFactory = mock(ManagedLedgerFactory.class);
        ManagedLedger managedLedger = mock(ManagedLedger.class);
        when(managedLedger.getTotalSize()).thenReturn(100L);
        doAnswer(invocation -> {
            AsyncCallbacks.OpenLedgerCallback callback = invocation.getArgument(2);
            callback.openLedgerComplete(managedLedger, invocation.getArgument(3));
            return null;
        }).when(managedLedgerFactory).asyncOpen(anyString(), any(ManagedLedgerConfig.class), any(), any(), any());

        UrsaStorageState state = new UrsaStorageState(
                Time.SYSTEM,
                1,
                mock(UrsaStorageConfig.class),
                mock(BrokerTopicStats.class),
                managedLedgerFactory);

        ensureManagedLedger(state, tp0);
        ensureManagedLedger(state, tp1);
        assertNotNull(jmxGaugeLongValue(LogMetricNames.SIZE, tp0.topicPartition()));
        assertNotNull(jmxGaugeLongValue(LogMetricNames.SIZE, tp1.topicPartition()));

        state.close();

        assertNull(jmxGaugeLongValue(LogMetricNames.SIZE, tp0.topicPartition()));
        assertNull(jmxGaugeLongValue(LogMetricNames.LOG_START_OFFSET, tp0.topicPartition()));
        assertNull(jmxGaugeLongValue(LogMetricNames.LOG_END_OFFSET, tp0.topicPartition()));
        assertNull(jmxGaugeLongValue(LogMetricNames.SIZE, tp1.topicPartition()));
        assertNull(jmxGaugeLongValue(LogMetricNames.LOG_START_OFFSET, tp1.topicPartition()));
        assertNull(jmxGaugeLongValue(LogMetricNames.LOG_END_OFFSET, tp1.topicPartition()));
    }

    @Test
    void testDisklessLogMetricsSkipWhenMetricAlreadyExists() throws Exception {
        TopicPartition topicPartition = new TopicPartition("metric-conflict-topic-" + Uuid.randomUuid(), 0);
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), topicPartition);

        Map<String, String> tags = new LinkedHashMap<>();
        tags.put("topic", topicPartition.topic());
        tags.put("partition", String.valueOf(topicPartition.partition()));
        KafkaMetricsGroup externalMetricsGroup = new KafkaMetricsGroup(LOG_METRIC_GROUP, LOG_METRIC_TYPE);
        externalMetricsGroup.newGauge(LogMetricNames.SIZE, () -> 777L, tags);
        externalMetricsGroup.newGauge(LogMetricNames.LOG_START_OFFSET, () -> 11L, tags);
        externalMetricsGroup.newGauge(LogMetricNames.LOG_END_OFFSET, () -> 22L, tags);

        ProducerStateManager producerStateManager = mock(ProducerStateManager.class);
        when(producerStateManager.cleanup(false)).thenReturn(CompletableFuture.completedFuture(null));

        ManagedLedgerFactory managedLedgerFactory = mock(ManagedLedgerFactory.class);
        ManagedLedger managedLedger = mock(ManagedLedger.class);
        when(managedLedger.getTotalSize()).thenReturn(1234L);
        doAnswer(invocation -> {
            AsyncCallbacks.OpenLedgerCallback callback = invocation.getArgument(2);
            callback.openLedgerComplete(managedLedger, invocation.getArgument(3));
            return null;
        }).when(managedLedgerFactory).asyncOpen(anyString(), any(ManagedLedgerConfig.class), any(), any(), any());

        try (UrsaStorageState state = new UrsaStorageState(
                Time.SYSTEM,
                1,
                mock(UrsaStorageConfig.class),
                mock(BrokerTopicStats.class),
                managedLedgerFactory)) {
            assertEquals(777L, jmxGaugeLongValue(LogMetricNames.SIZE, topicPartition));
            assertEquals(11L, jmxGaugeLongValue(LogMetricNames.LOG_START_OFFSET, topicPartition));
            assertEquals(22L, jmxGaugeLongValue(LogMetricNames.LOG_END_OFFSET, topicPartition));

            ensureManagedLedger(state, tp);
            assertEquals(777L, jmxGaugeLongValue(LogMetricNames.SIZE, topicPartition));
            assertEquals(11L, jmxGaugeLongValue(LogMetricNames.LOG_START_OFFSET, topicPartition));
            assertEquals(22L, jmxGaugeLongValue(LogMetricNames.LOG_END_OFFSET, topicPartition));
        } finally {
            externalMetricsGroup.removeMetric(LogMetricNames.SIZE, tags);
            externalMetricsGroup.removeMetric(LogMetricNames.LOG_START_OFFSET, tags);
            externalMetricsGroup.removeMetric(LogMetricNames.LOG_END_OFFSET, tags);
        }
    }

    private static Long jmxGaugeLongValue(String metricName, TopicPartition topicPartition) {
        String suffix = "type=Log,name=" + metricName
                + ",topic=" + topicPartition.topic()
                + ",partition=" + topicPartition.partition();
        for (var entry : KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet()) {
            var name = entry.getKey();
            if (!LOG_METRIC_GROUP.equals(name.getGroup()) || !LOG_METRIC_TYPE.equals(name.getType())) {
                continue;
            }
            if (!name.getMBeanName().endsWith(suffix)) {
                continue;
            }
            Object metric = entry.getValue();
            if (metric instanceof com.yammer.metrics.core.Gauge<?> gauge) {
                Object value = gauge.value();
                if (value instanceof Number numberValue) {
                    return numberValue.longValue();
                }
            }
        }
        return null;
    }

    private static void ensureManagedLedger(UrsaStorageState state, TopicIdPartition tp) {
        state.getOrCreatePartitionLog(tp);
    }

    private static void installProducerStateManager(
            UrsaStorageState state,
            TopicIdPartition tp,
            String zone,
            ProducerStateManager producerStateManager) {
        state.getOrCreatePartitionLog(tp).installProducerStateManager(zone, producerStateManager);
    }
}
