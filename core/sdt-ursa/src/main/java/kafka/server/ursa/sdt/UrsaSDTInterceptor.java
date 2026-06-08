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
import kafka.server.ReplicaManagerInterceptor;

import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.metadata.ConfigRepository;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.storage.internals.log.LogAppendInfo;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.OxiaClientBuilder;
import io.streamnative.ursa.compaction.CompactionManager;
import io.streamnative.ursa.compaction.DynamicConfigs;
import io.streamnative.ursa.compaction.OxiaCompactTaskManager;
import io.streamnative.ursa.storage.UrsaStorage;

/**
 * UrsaSDTInterceptor to initialize the ursa compaction task publisher to prepare the compaction task
 * for write the data to the lakehouse table.
 */
public class UrsaSDTInterceptor implements ReplicaManagerInterceptor {

    private static final Logger log = LoggerFactory.getLogger(UrsaSDTInterceptor.class);
    public static final String PUBLISHER_BROKER_ID_PROPERTY = "publisherBrokerId";

    private final KafkaConfig clusterConfig;
    private final ConfigRepository topicConfigRepository;
    private final ScheduledExecutorService scheduledExecutorService;
    private final DynamicConfigs clusterDynamicConfigs;
    private final AsyncOxiaClient oxiaClient;
    private final CompactionManager compactionManager;
    private final long publishTaskCheckInterval;

    private Cache<String, TopicCompactionTaskPublisher> compactionTaskPublisherCache;

    private final Set<String> internalTopics = Set.of("__consumer_offsets", "sn.system.__kafka_aliveness");

    public UrsaSDTInterceptor(KafkaConfig clusterConfig, ConfigRepository topicConfigRepository) {
        this(clusterConfig, topicConfigRepository, null, null, null);
    }

    UrsaSDTInterceptor(KafkaConfig clusterConfig,
                       ConfigRepository topicConfigRepository,
                       ScheduledExecutorService scheduledExecutorService,
                       AsyncOxiaClient oxiaClient,
                       CompactionManager compactionManager) {
        this.clusterConfig = clusterConfig;
        this.topicConfigRepository = topicConfigRepository;
        var properties = new Properties();
        properties.putAll(clusterConfig.props());
        this.clusterDynamicConfigs = new DynamicConfigs("default", properties, false);
        this.publishTaskCheckInterval = Long.parseLong(clusterDynamicConfigs
            .getProperty("clusterTailCompactDataVisibilityIntervalInSeconds").orElse("180"));
        this.compactionTaskPublisherCache = createPublisherCache();
        this.scheduledExecutorService = scheduledExecutorService == null
            ? Executors.newScheduledThreadPool(getUrsaCompactionTaskPublisherThreadNum())
            : scheduledExecutorService;
        if (compactionManager == null) {
            this.oxiaClient = oxiaClient == null ? getOxiaClient() : oxiaClient;
            this.compactionManager = getCompactionManager();
        } else {
            this.oxiaClient = oxiaClient;
            this.compactionManager = compactionManager;
        }
    }

    private Cache<String, TopicCompactionTaskPublisher> createPublisherCache() {
        var expirationTimeInSeconds = 3 * this.publishTaskCheckInterval;
        return CacheBuilder.newBuilder()
            .expireAfterAccess(Duration.ofSeconds(expirationTimeInSeconds))
            .removalListener(notification -> {
                var publisher = (TopicCompactionTaskPublisher) notification.getValue();
                if (publisher != null) {
                    try {
                        publisher.close();
                    } catch (Exception e) {
                        log.warn("Failed to close the compaction task publisher for topic {}", notification.getKey(), e);
                    }
                }
            })
            .build();
    }

    private int getUrsaCompactionTaskPublisherThreadNum() {
        var configs = clusterConfig.props();
        return Optional.ofNullable(configs.get("ursaCompactionTaskPublisherThreadNum"))
            .map(Object::toString)
            .map(Integer::parseInt)
            .orElse(10);
    }

    AsyncOxiaClient getOxiaClient() {
        try {
            var oxiaPulsarStorageUrl = clusterConfig.getString(ServerLogConfigs.URSA_OXIA_SERVICE_URL_CONFIG);
            var oxiaServiceUrl = UrsaStorage.validateOxiaUrl(oxiaPulsarStorageUrl);
            return OxiaClientBuilder.create(oxiaServiceUrl.getLeft())
                .namespace(oxiaServiceUrl.getRight())
                .asyncClient().get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize the oxia client for the compaction manager", e);
        }
    }

    CompactionManager getCompactionManager() {
        try {
            var compactTaskManager = new OxiaCompactTaskManager(oxiaClient);
            return new CompactionManager(compactTaskManager);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize the compaction manager with the configuration, please "
                                       + "check if the oxia service url is set right", e);
        }
    }

    public DynamicConfigs getDynamicConfigs(String topic) {
        var topicProperties = topicConfigRepository.topicConfig(topic).entrySet()
            .stream().collect(
                Collectors.toMap(
                    entry -> entry.getKey().toString(),
                    entry -> entry.getValue().toString()));
        var configs = DynamicConfigs.of(clusterDynamicConfigs);
        configs.overrideWith(topicProperties);
        return configs;
    }

    @Override
    public void onAppend(MemoryRecords records, LogAppendInfo appendInfo, Partition partition) {
        try {
            var topic = partition.topic();
            if (internalTopics.contains(topic)) {
                return;
            }
            var topicName = getTopicName(partition);
            getOrCreatePublisher(topicName, partition);
        } catch (Throwable throwable) {
            // handle all the exception internally, shouldn't break the append logic
            log.error("Unexpected error in the ursa sdt interceptor", throwable);
        }
    }

    private TopicCompactionTaskPublisher getOrCreatePublisher(String topicName, Partition partition) {
        var currentPublisher = compactionTaskPublisherCache.getIfPresent(topicName);
        if (currentPublisher != null && !currentPublisher.isClosed()) {
            return currentPublisher;
        }

        return compactionTaskPublisherCache.asMap().compute(topicName, (key, publisher) -> {
            if (publisher != null && !publisher.isClosed()) {
                return publisher;
            }
            if (publisher != null) {
                publisher.close();
            }
            return createAndStartPublisher(topicName, partition);
        });
    }

    private TopicCompactionTaskPublisher createAndStartPublisher(String topicName, Partition partition) {
        var publisher = new TopicCompactionTaskPublisher(this, compactionManager,
            partition, scheduledExecutorService);
        publisher.start(Duration.ofSeconds(publishTaskCheckInterval));
        log.info("Starting the compaction task publisher for the topic {}", topicName);
        return publisher;
    }

    public String getTopicName(Partition partition) {
        return String.format("%s-partition-%d", partition.topic(), partition.partitionId());
    }

    public int brokerId() {
        return clusterConfig.nodeId();
    }

    @Override
    public void close() throws Exception {
        compactionTaskPublisherCache.invalidateAll();
        scheduledExecutorService.shutdown();
        try {
            if (!scheduledExecutorService.awaitTermination(30, TimeUnit.SECONDS)) {
                scheduledExecutorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduledExecutorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
        if (oxiaClient != null) {
            oxiaClient.close();
        }
    }
}
