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
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import io.lakestream.api.Log;
import io.lakestream.api.LogOffset;
import io.lakestream.api.StreamCatalog;
import io.lakestream.api.StreamIdentifier;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class UrsaStorageStateRetentionTest {

    @Test
    void testLogOpenAppliesDefaultDisabledRetention() throws Exception {
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("retention-test-topic", 0));
        StreamCatalog catalog = mock(StreamCatalog.class);
        Log logInstance = mock(Log.class);
        stubCatalogLog(catalog, tp, logInstance);

        try (UrsaStorageState state = newState(catalog)) {
            Log result = state.openLog(tp).get(5, TimeUnit.SECONDS);

            assertSame(logInstance, result);
            verify(logInstance, never()).getLastOffset();
            verify(logInstance, never()).computeRetentionTrimOffset(anyLong(), anyLong(), anyLong());
            verify(logInstance, never()).softTrim(anyLong());
        }
    }

    @Test
    void testPeriodicRetentionTrimsWritesAppendedAfterEmptyOpen() throws Exception {
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("periodic-retention", 0));
        StreamCatalog catalog = mock(StreamCatalog.class);
        Log logInstance = mock(Log.class);
        LogOffset firstOffset = mockOffset(5L);
        LogOffset lastOffset = mockOffset(42L);
        stubCatalogLog(catalog, tp, logInstance);
        when(logInstance.getFirstOffset()).thenReturn(CompletableFuture.completedFuture(firstOffset));
        when(logInstance.getLastOffset())
                .thenReturn(CompletableFuture.completedFuture(LogOffset.NOT_FOUND))
                .thenReturn(CompletableFuture.completedFuture(LogOffset.NOT_FOUND))
                .thenReturn(CompletableFuture.completedFuture(lastOffset));
        when(logInstance.computeRetentionTrimOffset(42L, 120_000L, -1L))
                .thenReturn(CompletableFuture.completedFuture(10L));
        when(logInstance.softTrim(10L)).thenReturn(CompletableFuture.completedFuture(null));

        Map<String, Object> defaults = Map.of(
                TopicConfig.RETENTION_MS_CONFIG, 120_000L,
                TopicConfig.RETENTION_BYTES_CONFIG, -1L,
                ServerLogConfigs.LOG_CLEANUP_INTERVAL_MS_CONFIG, 60_000L);
        try (UrsaStorageState state = new UrsaStorageState(
                Time.SYSTEM,
                1,
                mock(UrsaStorageConfig.class),
                mock(BrokerTopicStats.class),
                catalog,
                defaults,
                ignored -> Map.of())) {
            state.getOrCreatePartitionLog(tp);
            verify(logInstance, never()).softTrim(10L);

            state.triggerPeriodicRetention();

            verify(logInstance).softTrim(10L);
        }
    }

    @Test
    void testTopicConfigUpdateTriggersRetentionForOpenPartition() throws Exception {
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("dynamic-retention", 0));
        StreamCatalog catalog = mock(StreamCatalog.class);
        Log logInstance = mock(Log.class);
        Map<String, String> updatedConfig = Map.of(TopicConfig.RETENTION_MS_CONFIG, "120000");
        LogOffset firstOffset = mockOffset(5L);
        LogOffset lastOffset = mockOffset(42L);

        stubCatalogLog(catalog, tp, logInstance);
        when(logInstance.getFirstOffset()).thenReturn(CompletableFuture.completedFuture(firstOffset));
        when(logInstance.getLastOffset()).thenReturn(CompletableFuture.completedFuture(lastOffset));
        when(logInstance.computeRetentionTrimOffset(42L, 120_000L, -1L))
                .thenReturn(CompletableFuture.completedFuture(10L));
        when(logInstance.softTrim(10L)).thenReturn(CompletableFuture.completedFuture(null));

        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, null);
        try (UrsaStorageState state = new UrsaStorageState(
                Time.SYSTEM,
                1,
                mock(UrsaStorageConfig.class),
                mock(BrokerTopicStats.class),
                holder,
                Map.of(
                        TopicConfig.RETENTION_MS_CONFIG, -1L,
                        TopicConfig.RETENTION_BYTES_CONFIG, -1L,
                        ServerLogConfigs.LOG_CLEANUP_INTERVAL_MS_CONFIG, 60_000L),
                ignored -> Map.of())) {
            state.getOrCreatePartitionLog(tp);
            state.applyTopicConfig(tp.topic(), tp.topicId(), updatedConfig);

            verify(logInstance).softTrim(10L);
        }
    }

    @Test
    void testCleanupReturnsWithoutWaitingForSubmittedTrimAndClosesAfterItSettles() throws Exception {
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("in-flight-trim", 0));
        StreamCatalog catalog = mock(StreamCatalog.class);
        Log logInstance = mock(Log.class);
        LogOffset firstOffset = mockOffset(5L);
        LogOffset lastOffset = mockOffset(42L);
        CompletableFuture<Long> trimResult = new CompletableFuture<>();

        stubCatalogLog(catalog, tp, logInstance);
        when(logInstance.getFirstOffset()).thenReturn(CompletableFuture.completedFuture(firstOffset));
        when(logInstance.getLastOffset()).thenReturn(CompletableFuture.completedFuture(lastOffset));
        when(logInstance.computeRetentionTrimOffset(42L, 120_000L, -1L))
                .thenReturn(CompletableFuture.completedFuture(10L));
        when(logInstance.softTrim(10L)).thenReturn(trimResult);

        Map<String, Object> defaults = Map.of(
                TopicConfig.RETENTION_MS_CONFIG, 120_000L,
                TopicConfig.RETENTION_BYTES_CONFIG, -1L,
                ServerLogConfigs.LOG_CLEANUP_INTERVAL_MS_CONFIG, 60_000L);
        try (UrsaStorageState state = new UrsaStorageState(
                Time.SYSTEM,
                1,
                mock(UrsaStorageConfig.class),
                mock(BrokerTopicStats.class),
                catalog,
                defaults,
                ignored -> Map.of())) {
            state.getOrCreatePartitionLog(tp);
            verify(logInstance).softTrim(10L);

            // The trim is still in flight, so the caller returns while the handle stays open.
            assertTrue(state.cleanupPartition(tp, false));
            verify(logInstance, never()).closeAsync();

            trimResult.complete(11L);

            verify(logInstance, timeout(5_000)).closeAsync();
            InOrder closeOrder = inOrder(logInstance);
            closeOrder.verify(logInstance).softTrim(10L);
            closeOrder.verify(logInstance).closeAsync();
            verify(logInstance, never()).fence();
        }
    }

    @Test
    void testCleanupPreventsLateRetentionTrim() throws Exception {
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("late-retention", 0));
        StreamCatalog catalog = mock(StreamCatalog.class);
        Log logInstance = mock(Log.class);
        LogOffset firstOffset = mockOffset(5L);
        LogOffset lastOffset = mockOffset(42L);
        CompletableFuture<Long> trimOffset = new CompletableFuture<>();

        stubCatalogLog(catalog, tp, logInstance);
        when(logInstance.getFirstOffset()).thenReturn(CompletableFuture.completedFuture(firstOffset));
        when(logInstance.getLastOffset()).thenReturn(CompletableFuture.completedFuture(lastOffset));
        when(logInstance.computeRetentionTrimOffset(42L, 120_000L, -1L)).thenReturn(trimOffset);

        Map<String, Object> defaults = Map.of(
                TopicConfig.RETENTION_MS_CONFIG, 120_000L,
                TopicConfig.RETENTION_BYTES_CONFIG, -1L,
                ServerLogConfigs.LOG_CLEANUP_INTERVAL_MS_CONFIG, 60_000L);
        try (UrsaStorageState state = new UrsaStorageState(
                Time.SYSTEM,
                1,
                mock(UrsaStorageConfig.class),
                mock(BrokerTopicStats.class),
                catalog,
                defaults,
                ignored -> Map.of())) {
            state.getOrCreatePartitionLog(tp);
            verify(logInstance).computeRetentionTrimOffset(42L, 120_000L, -1L);

            state.cleanupPartition(tp, false);
            trimOffset.complete(10L);

            verify(logInstance, never()).softTrim(10L);
        }
    }

    private static UrsaStorageState newState(StreamCatalog catalog) {
        return new UrsaStorageState(
                Time.SYSTEM,
                1,
                mock(UrsaStorageConfig.class),
                mock(BrokerTopicStats.class),
                catalog);
    }

    private static void stubCatalogLog(
            StreamCatalog catalog,
            TopicIdPartition tp,
            Log logInstance) {
        StreamIdentifier identifier = KafkaStreamIdentity.streamIdentifier(tp.topic(), tp.topicId());
        when(catalog.openLog(identifier, tp.partition()))
                .thenReturn(CompletableFuture.completedFuture(logInstance));
    }

    private static LogOffset mockOffset(long offset) {
        LogOffset logOffset = mock(LogOffset.class);
        when(logOffset.offset()).thenReturn(offset);
        return logOffset;
    }
}
