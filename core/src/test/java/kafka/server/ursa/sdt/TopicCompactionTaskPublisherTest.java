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
import kafka.server.UrsaSDTInterceptor;

import org.apache.kafka.common.Uuid;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import io.streamnative.ursa.compaction.CompactionManager;
import scala.Option;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TopicCompactionTaskPublisherTest {

    @Test
    void shouldCancelScheduledTaskWhenBrokerIsNotLeader() throws Exception {
        var interceptor = mock(UrsaSDTInterceptor.class);
        var compactionManager = mock(CompactionManager.class);
        var partition = mock(Partition.class);
        var executor = mock(ScheduledExecutorService.class);
        @SuppressWarnings("unchecked")
        ScheduledFuture<?> scheduledFuture = mock(ScheduledFuture.class);
        var publishCaptor = ArgumentCaptor.forClass(Runnable.class);

        when(partition.topic()).thenReturn("test-topic");
        when(partition.partitionId()).thenReturn(0);
        when(partition.topicId()).thenReturn(Option.apply(Uuid.randomUuid()));
        when(partition.isLeader()).thenReturn(false);
        when(interceptor.getTopicName(partition)).thenReturn("test-topic-partition-0");
        doReturn(scheduledFuture).when(executor).scheduleAtFixedRate(
            publishCaptor.capture(),
            eq(1L),
            eq(1L),
            eq(TimeUnit.SECONDS)
        );

        var publisher = new TopicCompactionTaskPublisher(interceptor, compactionManager, partition, executor);
        publisher.start(Duration.ofSeconds(1));

        publishCaptor.getValue().run();

        verify(scheduledFuture).cancel(false);
        verify(partition, never()).unifiedLog();
        verify(compactionManager, never()).recoverPreparedTasks(anyString());
        verify(compactionManager, never()).publishTask(org.mockito.ArgumentMatchers.any());
    }
}
