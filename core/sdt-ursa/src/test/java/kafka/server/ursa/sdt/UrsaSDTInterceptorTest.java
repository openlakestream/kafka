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

package kafka.server.ursa.sdt;

import kafka.cluster.Partition;
import kafka.server.KafkaConfig;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.metadata.ConfigRepository;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import io.streamnative.ursa.compaction.CompactionManager;
import scala.Option;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class UrsaSDTInterceptorTest {

    @Test
    void shouldReplaceClosedPublisherOnAppend() {
        var clusterConfig = mock(KafkaConfig.class);
        var topicConfigRepository = mock(ConfigRepository.class);
        var compactionManager = mock(CompactionManager.class);
        var executor = mock(ScheduledExecutorService.class);
        var partition = mock(Partition.class);
        @SuppressWarnings("unchecked")
        ScheduledFuture<?> firstScheduledFuture = mock(ScheduledFuture.class);
        @SuppressWarnings("unchecked")
        ScheduledFuture<?> secondScheduledFuture = mock(ScheduledFuture.class);
        var publishCaptor = ArgumentCaptor.forClass(Runnable.class);
        var properties = new Properties();
        properties.put("clusterTailCompactDataVisibilityIntervalInSeconds", "1");

        doReturn(properties).when(clusterConfig).props();
        when(partition.topic()).thenReturn("test-topic");
        when(partition.partitionId()).thenReturn(0);
        when(partition.topicId()).thenReturn(Option.apply(Uuid.randomUuid()));
        when(partition.isLeader()).thenReturn(false);
        doReturn(firstScheduledFuture, secondScheduledFuture).when(executor).scheduleAtFixedRate(
            publishCaptor.capture(),
            eq(1L),
            eq(1L),
            eq(TimeUnit.SECONDS)
        );

        var interceptor = new UrsaSDTInterceptor(
            clusterConfig,
            topicConfigRepository,
            executor,
            null,
            compactionManager);

        interceptor.onAppend(null, null, partition);
        publishCaptor.getValue().run();
        interceptor.onAppend(null, null, partition);

        verify(executor, times(2)).scheduleAtFixedRate(
            org.mockito.ArgumentMatchers.any(),
            eq(1L),
            eq(1L),
            eq(TimeUnit.SECONDS)
        );
        verify(firstScheduledFuture, atLeastOnce()).cancel(false);
    }

    @Test
    void shouldSkipDefaultInternalTopicsOnAppend() {
        var defaultInternalTopics = List.of(
            "__consumer_offsets",
            "sn.system.__kafka_aliveness",
            "strimzi.cruisecontrol.metrics",
            "strimzi.cruisecontrol.modeltrainingsamples",
            "strimzi.cruisecontrol.partitionmetricsamples"
        );

        for (var internalTopic : defaultInternalTopics) {
            var clusterConfig = mock(KafkaConfig.class);
            var topicConfigRepository = mock(ConfigRepository.class);
            var compactionManager = mock(CompactionManager.class);
            var executor = mock(ScheduledExecutorService.class);
            var partition = mock(Partition.class);
            var properties = new Properties();
            properties.put("clusterTailCompactDataVisibilityIntervalInSeconds", "1");

            doReturn(properties).when(clusterConfig).props();
            when(partition.topic()).thenReturn(internalTopic);

            var interceptor = new UrsaSDTInterceptor(
                clusterConfig,
                topicConfigRepository,
                executor,
                null,
                compactionManager);

            interceptor.onAppend(null, null, partition);

            verify(executor, never()).scheduleAtFixedRate(
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.anyLong(),
                org.mockito.ArgumentMatchers.anyLong(),
                org.mockito.ArgumentMatchers.any()
            );
        }
    }

    @Test
    void shouldSkipExtraInternalTopicsOnAppend() {
        var clusterConfig = mock(KafkaConfig.class);
        var topicConfigRepository = mock(ConfigRepository.class);
        var compactionManager = mock(CompactionManager.class);
        var executor = mock(ScheduledExecutorService.class);
        var partition = mock(Partition.class);
        var properties = new Properties();
        properties.put("clusterTailCompactDataVisibilityIntervalInSeconds", "1");
        properties.put(UrsaSDTInterceptor.EXTRA_INTERNAL_TOPICS_CONFIG, "my.extra.topic, another.extra.topic");

        doReturn(properties).when(clusterConfig).props();
        when(partition.topic()).thenReturn("my.extra.topic");

        var interceptor = new UrsaSDTInterceptor(
            clusterConfig,
            topicConfigRepository,
            executor,
            null,
            compactionManager);

        interceptor.onAppend(null, null, partition);

        verify(executor, never()).scheduleAtFixedRate(
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.anyLong(),
            org.mockito.ArgumentMatchers.anyLong(),
            org.mockito.ArgumentMatchers.any()
        );

        // also verify the second extra topic is skipped
        when(partition.topic()).thenReturn("another.extra.topic");
        interceptor.onAppend(null, null, partition);

        verify(executor, never()).scheduleAtFixedRate(
            org.mockito.ArgumentMatchers.any(),
            org.mockito.ArgumentMatchers.anyLong(),
            org.mockito.ArgumentMatchers.anyLong(),
            org.mockito.ArgumentMatchers.any()
        );
    }

    @Test
    void shouldProcessNonInternalTopicOnAppend() {
        var clusterConfig = mock(KafkaConfig.class);
        var topicConfigRepository = mock(ConfigRepository.class);
        var compactionManager = mock(CompactionManager.class);
        var executor = mock(ScheduledExecutorService.class);
        var partition = mock(Partition.class);
        @SuppressWarnings("unchecked")
        ScheduledFuture<?> scheduledFuture = mock(ScheduledFuture.class);
        var properties = new Properties();
        properties.put("clusterTailCompactDataVisibilityIntervalInSeconds", "1");

        doReturn(properties).when(clusterConfig).props();
        when(partition.topic()).thenReturn("regular-topic");
        when(partition.partitionId()).thenReturn(0);
        when(partition.topicId()).thenReturn(Option.apply(Uuid.randomUuid()));
        when(partition.isLeader()).thenReturn(false);
        doReturn(scheduledFuture).when(executor).scheduleAtFixedRate(
            org.mockito.ArgumentMatchers.any(),
            eq(1L),
            eq(1L),
            eq(TimeUnit.SECONDS)
        );

        var interceptor = new UrsaSDTInterceptor(
            clusterConfig,
            topicConfigRepository,
            executor,
            null,
            compactionManager);

        interceptor.onAppend(null, null, partition);

        verify(executor, times(1)).scheduleAtFixedRate(
            org.mockito.ArgumentMatchers.any(),
            eq(1L),
            eq(1L),
            eq(TimeUnit.SECONDS)
        );
    }
}
