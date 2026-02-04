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
package kafka.server.metadata;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.image.ConfigurationsDelta;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.TopicsDelta;
import org.apache.kafka.image.loader.LoaderManifest;
import org.apache.kafka.image.publisher.MetadataPublisher;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.server.fault.FaultHandler;
import org.apache.kafka.storage.diskless.UrsaPartitionedTopicsMetadataSync;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Controller-side metadata publisher which mirrors diskless topic lifecycle into Oxia keys used by ursa-storage.
 */
public final class UrsaPartitionedTopicsPublisher implements MetadataPublisher {

    private final int nodeId;
    private final FaultHandler faultHandler;
    private final AtomicBoolean isActiveController = new AtomicBoolean(false);
    private final UrsaPartitionedTopicsMetadataSync sync;

    public UrsaPartitionedTopicsPublisher(
            int nodeId,
            String oxiaServiceUrl,
            String namespace,
            FaultHandler faultHandler) {
        this.nodeId = nodeId;
        this.faultHandler = Objects.requireNonNull(faultHandler, "faultHandler must not be null");
        this.sync = new UrsaPartitionedTopicsMetadataSync(oxiaServiceUrl, namespace,
                (message, cause) -> faultHandler.handleFault(message, cause));
    }

    @Override
    public String name() {
        return "UrsaPartitionedTopicsPublisher id=" + nodeId;
    }

    @Override
    public void onControllerChange(LeaderAndEpoch newLeaderAndEpoch) {
        isActiveController.set(newLeaderAndEpoch.isLeader(nodeId));
    }

    @Override
    public void onMetadataUpdate(MetadataDelta delta, MetadataImage newImage, LoaderManifest manifest) {
        if (!isActiveController.get()) {
            return;
        }

        MetadataImage oldImage = delta.image();
        Set<String> candidateTopicNames = topicNamesToConsider(delta, oldImage, newImage);
        if (candidateTopicNames.isEmpty()) {
            return;
        }

        String context = "MetadataDelta up to " + newImage.highestOffsetAndEpoch().offset();

        for (String topicName : candidateTopicNames) {
            TopicImage oldTopicImage = oldImage.topics().getTopic(topicName);
            TopicImage newTopicImage = newImage.topics().getTopic(topicName);

            boolean oldEnabled = isDisklessTopic(oldImage, topicName);
            boolean newEnabled = isDisklessTopic(newImage, topicName);

            if (oldEnabled && !newEnabled) {
                int partitions = oldTopicImage != null ? oldTopicImage.partitions().size() : -1;
                sync.deleteTopicMetadata(topicName, partitions, context);
            } else if (newEnabled) {
                if (newTopicImage == null) {
                    continue;
                }
                int partitions = newTopicImage.partitions().size();
                ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
                Map<String, String> properties = newImage.configs().configMapForResource(resource);
                sync.upsertPartitionedTopicMetadata(topicName, partitions, properties, context);
            }
        }
    }

    @Override
    public void close() throws Exception {
        try {
            sync.close();
        } catch (Exception e) {
            faultHandler.handleFault("Error closing " + name(), e);
        }
    }

    private Set<String> topicNamesToConsider(MetadataDelta delta, MetadataImage oldImage, MetadataImage newImage) {
        Set<String> names = new HashSet<>();

        TopicsDelta topicsDelta = delta.topicsDelta();
        if (topicsDelta != null) {
            for (Uuid topicId : topicsDelta.createdTopicIds()) {
                TopicImage topic = newImage.topics().getTopic(topicId);
                if (topic != null) {
                    names.add(topic.name());
                }
            }
            for (Uuid topicId : topicsDelta.deletedTopicIds()) {
                TopicImage topic = oldImage.topics().getTopic(topicId);
                if (topic != null) {
                    names.add(topic.name());
                }
            }
            for (Uuid topicId : topicsDelta.changedTopics().keySet()) {
                TopicImage topic = newImage.topics().getTopic(topicId);
                if (topic != null) {
                    names.add(topic.name());
                }
            }
        }

        ConfigurationsDelta configsDelta = delta.configsDelta();
        if (configsDelta != null) {
            for (ConfigResource resource : configsDelta.changes().keySet()) {
                if (resource.type() == ConfigResource.Type.TOPIC) {
                    names.add(resource.name());
                }
            }
        }

        return names;
    }

    private boolean isDisklessTopic(MetadataImage image, String topicName) {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        Map<String, String> configs = image.configs().configMapForResource(resource);
        String enabledValue = configs.get(TopicConfig.URSA_STORAGE_ENABLE_CONFIG);
        return enabledValue != null && Boolean.parseBoolean(enabledValue);
    }
}
