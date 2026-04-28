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

import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import io.streamnative.ursa.mledger.UrsaPosition;

final class DisklessLogMetrics {

    private final KafkaMetricsGroup logMetricsGroup = new KafkaMetricsGroup("kafka.log", "Log");

    void register(TopicIdPartition topicIdPartition, ManagedLedger managedLedger) {
        if (topicIdPartition == null || managedLedger == null) {
            return;
        }
        Map<String, String> tags = createLogMetricTags(topicIdPartition.topicPartition());

        logMetricsGroup.newGauge(
                LogMetricNames.SIZE,
                () -> Math.max(0L, managedLedger.getTotalSize()),
                tags
        );
        logMetricsGroup.newGauge(
                LogMetricNames.LOG_START_OFFSET,
                () -> Math.max(0L, resolveLogStartOffset(managedLedger)),
                tags
        );
        logMetricsGroup.newGauge(
                LogMetricNames.LOG_END_OFFSET,
                () -> Math.max(0L, resolveLogEndOffset(managedLedger)),
                tags
        );
    }

    boolean remove(TopicIdPartition topicIdPartition) {
        if (topicIdPartition == null) {
            return false;
        }

        Map<String, String> tags = createLogMetricTags(topicIdPartition.topicPartition());
        boolean existing = hasAnyExistingLogMetrics(tags);
        removeMetrics(tags);
        return existing;
    }

    void removeAll(Collection<TopicIdPartition> topicIdPartitions) {
        topicIdPartitions.stream()
                .map(TopicIdPartition::topicPartition)
                .distinct()
                .forEach(topicPartition -> removeMetrics(createLogMetricTags(topicPartition)));
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

    private void removeMetrics(Map<String, String> tags) {
        logMetricsGroup.removeMetric(LogMetricNames.SIZE, tags);
        logMetricsGroup.removeMetric(LogMetricNames.LOG_START_OFFSET, tags);
        logMetricsGroup.removeMetric(LogMetricNames.LOG_END_OFFSET, tags);
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
}
