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
import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;
import org.apache.kafka.storage.internals.log.LogMetricNames;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import io.lakestream.api.Log;
import io.lakestream.api.LogOffset;

final class DisklessLogMetrics {

    private static final long REFRESH_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(1);

    private final KafkaMetricsGroup logMetricsGroup = new KafkaMetricsGroup("kafka.log", "Log");
    private final ConcurrentHashMap<TopicIdPartition, MetricSnapshot> snapshots = new ConcurrentHashMap<>();

    void register(TopicIdPartition topicIdPartition, Log log) {
        if (topicIdPartition == null || log == null) {
            return;
        }
        Map<String, String> tags = createLogMetricTags(topicIdPartition.topicPartition());
        MetricSnapshot snapshot = new MetricSnapshot();
        snapshots.put(topicIdPartition, snapshot);
        snapshot.refresh(log);

        logMetricsGroup.newGauge(
                LogMetricNames.SIZE,
                () -> snapshot.values(log).size(),
                tags
        );
        logMetricsGroup.newGauge(
                LogMetricNames.LOG_START_OFFSET,
                () -> snapshot.values(log).startOffset(),
                tags
        );
        logMetricsGroup.newGauge(
                LogMetricNames.LOG_END_OFFSET,
                () -> snapshot.values(log).endOffset(),
                tags
        );
    }

    boolean remove(TopicIdPartition topicIdPartition) {
        if (topicIdPartition == null) {
            return false;
        }

        Map<String, String> tags = createLogMetricTags(topicIdPartition.topicPartition());
        snapshots.remove(topicIdPartition);
        boolean existing = hasAnyExistingLogMetrics(tags);
        removeMetrics(tags);
        return existing;
    }

    void removeAll(Collection<TopicIdPartition> topicIdPartitions) {
        topicIdPartitions.forEach(this::remove);
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
        tags.put("storage", "diskless");
        return tags;
    }

    private void removeMetrics(Map<String, String> tags) {
        logMetricsGroup.removeMetric(LogMetricNames.SIZE, tags);
        logMetricsGroup.removeMetric(LogMetricNames.LOG_START_OFFSET, tags);
        logMetricsGroup.removeMetric(LogMetricNames.LOG_END_OFFSET, tags);
    }

    private static boolean isInvalidOffset(LogOffset offset) {
        return offset == null || offset.offset() < 0;
    }

    private static MetricValues resolveValues(LogOffset firstOffset, LogOffset lastOffset) {
        if (isInvalidOffset(firstOffset) || isInvalidOffset(lastOffset)) {
            return MetricValues.EMPTY;
        }
        long startOffset = firstOffset.offset();
        long endOffset = Math.max(startOffset, lastOffset.offset() + lastOffset.numberOfRecords());
        long size = Math.max(0L, lastOffset.cumulativeSize()
                - firstOffset.cumulativeSize()
                + firstOffset.entrySize());
        return new MetricValues(size, startOffset, endOffset);
    }

    private static final class MetricSnapshot {
        private final AtomicReference<MetricValues> current = new AtomicReference<>(MetricValues.EMPTY);
        private final AtomicBoolean refreshInProgress = new AtomicBoolean();
        private final AtomicLong nextRefreshNanos = new AtomicLong();

        MetricValues values(Log log) {
            refresh(log);
            return current.get();
        }

        void refresh(Log log) {
            long now = System.nanoTime();
            if (now < nextRefreshNanos.get() || !refreshInProgress.compareAndSet(false, true)) {
                return;
            }
            nextRefreshNanos.set(now + REFRESH_INTERVAL_NANOS);

            CompletableFuture<LogOffset> firstOffsetFuture;
            CompletableFuture<LogOffset> lastOffsetFuture;
            try {
                firstOffsetFuture = log.getFirstOffset();
                lastOffsetFuture = log.getLastOffset();
                if (firstOffsetFuture == null || lastOffsetFuture == null) {
                    refreshInProgress.set(false);
                    return;
                }
            } catch (Throwable error) {
                refreshInProgress.set(false);
                return;
            }

            firstOffsetFuture.thenCombine(lastOffsetFuture, DisklessLogMetrics::resolveValues)
                    .whenComplete((values, error) -> {
                        if (error == null && values != null) {
                            current.set(values);
                        }
                        refreshInProgress.set(false);
                    });
        }
    }

    private record MetricValues(long size, long startOffset, long endOffset) {
        private static final MetricValues EMPTY = new MetricValues(0L, 0L, 0L);
    }
}
