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

import org.apache.kafka.common.DirectoryId;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.image.AclsImage;
import org.apache.kafka.image.ClientQuotasImage;
import org.apache.kafka.image.ClusterImage;
import org.apache.kafka.image.ConfigurationImage;
import org.apache.kafka.image.ConfigurationsImage;
import org.apache.kafka.image.DelegationTokenImage;
import org.apache.kafka.image.FeaturesImage;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.image.ProducerIdsImage;
import org.apache.kafka.image.ScramImage;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.TopicsImage;
import org.apache.kafka.image.loader.LoaderManifest;
import org.apache.kafka.metadata.LeaderRecoveryState;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.storage.diskless.DisklessStorageReplicaManagerSupport;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class DisklessStateReconcilerPublisherTest {

    @Test
    void testOnMetadataUpdateReconcilesEvenWithoutTopicsDelta() {
        DisklessStorageReplicaManagerSupport support = mock(DisklessStorageReplicaManagerSupport.class);
        @SuppressWarnings("unchecked")
        Consumer<String> callback = mock(Consumer.class);
        DisklessStateReconcilerPublisher publisher = new DisklessStateReconcilerPublisher(support, callback);

        publisher.onMetadataUpdate(
                new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build(),
                MetadataImage.EMPTY,
                mock(LoaderManifest.class)
        );

        verify(support).reconcileTrackedPartitions(eq(Collections.emptySet()), same(callback));
    }

    @Test
    void testOnMetadataUpdatePassesDeletedDisklessPartitionsToReconcile() {
        Uuid topicId = Uuid.randomUuid();
        String topicName = "diskless-topic";
        MetadataImage oldImage = metadataImage(topicName, topicId, 3, true);

        MetadataDelta delta = new MetadataDelta.Builder().setImage(oldImage).build();
        delta.replay(new RemoveTopicRecord().setTopicId(topicId));

        DisklessStorageReplicaManagerSupport support = mock(DisklessStorageReplicaManagerSupport.class);
        @SuppressWarnings("unchecked")
        Consumer<String> callback = mock(Consumer.class);
        DisklessStateReconcilerPublisher publisher = new DisklessStateReconcilerPublisher(support, callback);

        publisher.onMetadataUpdate(delta, MetadataImage.EMPTY, mock(LoaderManifest.class));

        verify(support).reconcileTrackedPartitions(eq(Set.of(
                new TopicIdPartition(topicId, new TopicPartition(topicName, 0)),
                new TopicIdPartition(topicId, new TopicPartition(topicName, 1)),
                new TopicIdPartition(topicId, new TopicPartition(topicName, 2))
        )), same(callback));
    }

    @Test
    void testOnMetadataUpdateIgnoresDeletedClassicTopics() {
        Uuid topicId = Uuid.randomUuid();
        MetadataImage oldImage = metadataImage("classic-topic", topicId, 2, false);

        MetadataDelta delta = new MetadataDelta.Builder().setImage(oldImage).build();
        delta.replay(new RemoveTopicRecord().setTopicId(topicId));

        DisklessStorageReplicaManagerSupport support = mock(DisklessStorageReplicaManagerSupport.class);
        @SuppressWarnings("unchecked")
        Consumer<String> callback = mock(Consumer.class);
        DisklessStateReconcilerPublisher publisher = new DisklessStateReconcilerPublisher(support, callback);

        publisher.onMetadataUpdate(delta, MetadataImage.EMPTY, mock(LoaderManifest.class));

        verify(support).reconcileTrackedPartitions(eq(Collections.emptySet()), same(callback));
    }

    private static MetadataImage metadataImage(String topicName, Uuid topicId, int partitions, boolean disklessEnabled) {
        return new MetadataImage(
                new MetadataProvenance(1, 0, 0, true),
                FeaturesImage.EMPTY,
                ClusterImage.EMPTY,
                TopicsImage.EMPTY.including(new TopicImage(topicName, topicId, partitionMap(partitions))),
                configsImage(topicName, disklessEnabled),
                ClientQuotasImage.EMPTY,
                ProducerIdsImage.EMPTY,
                AclsImage.EMPTY,
                ScramImage.EMPTY,
                DelegationTokenImage.EMPTY
        );
    }

    private static Map<Integer, PartitionRegistration> partitionMap(int partitions) {
        Map<Integer, PartitionRegistration> partitionMap = new HashMap<>();
        for (int partition = 0; partition < partitions; partition++) {
            partitionMap.put(partition, new PartitionRegistration.Builder()
                    .setReplicas(new int[]{0})
                    .setDirectories(DirectoryId.migratingArray(1))
                    .setIsr(new int[]{0})
                    .setLeader(0)
                    .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED)
                    .setLeaderEpoch(0)
                    .setPartitionEpoch(0)
                    .build());
        }
        return partitionMap;
    }

    private static ConfigurationsImage configsImage(String topicName, boolean disklessEnabled) {
        if (!disklessEnabled) {
            return ConfigurationsImage.EMPTY;
        }
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        return new ConfigurationsImage(Map.of(
                resource,
                new ConfigurationImage(resource, Map.of(TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true"))
        ));
    }
}
