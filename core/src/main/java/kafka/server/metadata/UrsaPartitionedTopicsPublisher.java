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
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.TopicsDelta;
import org.apache.kafka.image.loader.LoaderManifest;
import org.apache.kafka.image.publisher.MetadataPublisher;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.storage.diskless.UrsaPartitionedTopicsMetadataSync;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Controller-side metadata publisher which mirrors diskless topic lifecycle
 * into Oxia keys used by ursa-storage.
 *
 * <p>Topic creation metadata is written pre-commit by
 * {@code DisklessTopicPreCommitHandler}. This publisher handles post-commit
 * topic deletion cleanup.
 */
public final class UrsaPartitionedTopicsPublisher implements MetadataPublisher {

    private final int nodeId;
    private final AtomicBoolean isActiveController = new AtomicBoolean(false);
    private final UrsaPartitionedTopicsMetadataSync sync;

    public UrsaPartitionedTopicsPublisher(
            int nodeId,
            UrsaPartitionedTopicsMetadataSync sync) {
        this.nodeId = nodeId;
        this.sync = Objects.requireNonNull(sync, "sync must not be null");
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

        TopicsDelta topicsDelta = delta.topicsDelta();
        if (topicsDelta == null || topicsDelta.deletedTopicIds().isEmpty()) {
            return;
        }

        MetadataImage oldImage = delta.image();
        String context = "MetadataDelta up to " + newImage.highestOffsetAndEpoch().offset();

        for (Uuid topicId : topicsDelta.deletedTopicIds()) {
            TopicImage oldTopicImage = oldImage.topics().getTopic(topicId);
            if (oldTopicImage == null) {
                continue;
            }
            String topicName = oldTopicImage.name();
            if (!isDisklessTopic(oldImage, topicName)) {
                continue;
            }
            int partitions = oldTopicImage.partitions().size();
            sync.deleteTopicMetadata(topicName, topicId.toString(), partitions, context);
        }
    }

    @Override
    public void close() {
    }

    private boolean isDisklessTopic(MetadataImage image, String topicName) {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        Map<String, String> configs = image.configs().configMapForResource(resource);
        String enabledValue = configs.get(TopicConfig.URSA_STORAGE_ENABLE_CONFIG);
        return enabledValue != null && Boolean.parseBoolean(enabledValue);
    }
}
