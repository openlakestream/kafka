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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.TopicsDelta;
import org.apache.kafka.image.loader.LoaderManifest;
import org.apache.kafka.image.publisher.MetadataPublisher;
import org.apache.kafka.storage.diskless.DisklessStorageReplicaManagerSupport;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

public class DisklessStateReconcilerPublisher implements MetadataPublisher {

    private final DisklessStorageReplicaManagerSupport disklessStorageSupport;
    private final Consumer<String> onTopicMaybeEmptied;

    public DisklessStateReconcilerPublisher(
            DisklessStorageReplicaManagerSupport disklessStorageSupport,
            Consumer<String> onTopicMaybeEmptied
    ) {
        this.disklessStorageSupport = Objects.requireNonNull(
                disklessStorageSupport, "disklessStorageSupport cannot be null");
        this.onTopicMaybeEmptied = Objects.requireNonNull(
                onTopicMaybeEmptied, "onTopicMaybeEmptied cannot be null");
    }

    @Override
    public String name() {
        return "DisklessStateReconcilerPublisher";
    }

    @Override
    public void onMetadataUpdate(
            MetadataDelta delta,
            MetadataImage newImage,
            LoaderManifest manifest
    ) {
        disklessStorageSupport.reconcileTrackedPartitions(deletedDisklessPartitions(delta), onTopicMaybeEmptied);
    }

    private Set<TopicIdPartition> deletedDisklessPartitions(MetadataDelta delta) {
        TopicsDelta topicsDelta = delta.topicsDelta();
        if (topicsDelta == null || topicsDelta.deletedTopicIds().isEmpty()) {
            return Collections.emptySet();
        }

        MetadataImage oldImage = delta.image();
        Set<TopicIdPartition> deletedPartitions = new HashSet<>();
        for (Uuid topicId : topicsDelta.deletedTopicIds()) {
            TopicImage oldTopicImage = oldImage.topics().getTopic(topicId);
            if (oldTopicImage == null || !isDisklessTopic(oldImage, oldTopicImage.name())) {
                continue;
            }

            for (Integer partitionId : oldTopicImage.partitions().keySet()) {
                deletedPartitions.add(new TopicIdPartition(
                        topicId,
                        new TopicPartition(oldTopicImage.name(), partitionId)
                ));
            }
        }
        return deletedPartitions;
    }

    private boolean isDisklessTopic(MetadataImage image, String topicName) {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        Map<String, String> configs = image.configs().configMapForResource(resource);
        String enabledValue = configs.get(TopicConfig.URSA_STORAGE_ENABLE_CONFIG);
        return Boolean.parseBoolean(enabledValue);
    }
}
