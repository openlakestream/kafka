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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.streamnative.ursa.compaction.CompactionManager;
import io.streamnative.ursa.compaction.DynamicConfigs;
import io.streamnative.ursa.compaction.task.CompactStreamTask;
import io.streamnative.ursa.compaction.task.PreparedCompactStreamTask;

public class TopicCompactionTaskPublisher implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TopicCompactionTaskPublisher.class);

    private final CompactionManager compactionManager;
    private final UrsaSDTInterceptor interceptor;
    private final Partition partition;
    private final String topic;
    private final long idForCompactionTask;
    private final ScheduledExecutorService executors;
    private Duration duration;

    private ScheduledFuture<?> scheduledFuture;
    private long lastPublishedOffset;
    private final AtomicBoolean publishRunning = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public TopicCompactionTaskPublisher(UrsaSDTInterceptor interceptor,
                                        CompactionManager compactionManager,
                                        Partition partition,
                                        ScheduledExecutorService executors) {
        this.interceptor = interceptor;
        this.compactionManager = compactionManager;
        this.partition = partition;
        this.topic = partition.topic();
        if (partition.topicId().isEmpty()) {
            throw new IllegalStateException("please check that your Kafka version is new enough to provide a topic id");
        }
        var topicId = partition.topicId().get();
        long id = topicId.getMostSignificantBits() ^ topicId.getLeastSignificantBits();
        this.idForCompactionTask = id & Long.MAX_VALUE;
        this.executors = executors;
        this.lastPublishedOffset = -1;
    }

    public void start(Duration duration) {
        this.duration = duration;
        closed.set(false);
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }
        var seconds = duration.toSeconds();
        scheduledFuture = executors.scheduleAtFixedRate(this::publish, seconds, seconds, TimeUnit.SECONDS);
    }

    public void publish() {
        if (!publishRunning.compareAndSet(false, true)) {
            return;
        }
        try {
            if (!partition.isLeader()) {
                log.info("Skip publishing the compaction task for topic {} because this broker is not the leader",
                    interceptor.getTopicName(partition));
                close();
                return;
            }
            if (partition.unifiedLog().isEmpty()) {
                log.warn("Unified log is not found when publishing the compaction task for the topic {}", topic);
                return;
            }
            var unifiedLog = partition.unifiedLog().get();
            var configs = interceptor.getDynamicConfigs(topic);
            if (!configs.sdtEnabled()) {
                log.info("SDT is not enabled for topic {}, skip compaction task creation", topic);
                return;
            }
            if (configs.sdtSuspended()) {
                log.info("SDT is suspended for topic {}, skip compaction task creation", topic);
                return;
            }

            adjustPublishInterval(configs);

            try {
                var topicNameInTask = interceptor.getTopicName(partition);
                compactionManager.recoverPreparedTasks(topicNameInTask);
                this.lastPublishedOffset = compactionManager.lastPublishedOffset(topicNameInTask);
                var lastOffset = unifiedLog.highWatermark() - 1;
                if (lastOffset <= lastPublishedOffset) {
                    log.info("No new data to compact for topic {}, last published offset is {}, last offset is {}",
                        topicNameInTask, lastPublishedOffset, lastOffset);
                    return;
                }

                var taskProperties = new HashMap<>(configs.toTaskProperties());
                taskProperties.put(UrsaSDTInterceptor.PUBLISHER_BROKER_ID_PROPERTY,
                    Integer.toString(interceptor.brokerId()));

                var preparedTask = PreparedCompactStreamTask.builder()
                    .streamId(idForCompactionTask)
                    .startOffset(lastPublishedOffset + 1)
                    .endOffset(lastOffset)
                    .topic(topicNameInTask)
                    .taskName(UUID.randomUUID().toString())
                    .status(PreparedCompactStreamTask.INIT)
                    .properties(taskProperties)
                    .type(CompactStreamTask.Type.KAFKA)
                    .build();
                if (!partition.isLeader()) {
                    log.info("Skip publishing the compaction task for topic {} because this broker is not the leader",
                        interceptor.getTopicName(partition));
                    close();
                    return;
                }
                compactionManager.publishTask(preparedTask);
                lastPublishedOffset = lastOffset;
            } catch (Throwable e) {
                log.error("Failed to publish the compaction task for the topic {}", topic, e);
            }
        } finally {
            publishRunning.set(false);
        }
    }

    private void adjustPublishInterval(DynamicConfigs dynamicConfigs) {
        var currentSeconds = duration.getSeconds();
        var seconds = Long.parseLong(dynamicConfigs.getProperty("clusterTailCompactDataVisibilityIntervalInSeconds")
            .orElse(Long.toString(currentSeconds)));
        if (currentSeconds != seconds) {
            log.info("Adjust the compaction task publish interval for topic {} from {} seconds to {} seconds",
                topic, currentSeconds, seconds);
            start(Duration.ofSeconds(seconds));
        }
    }

    @Override
    public void close() {
        closed.set(true);
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }
    }

    public boolean isClosed() {
        return closed.get();
    }
}
