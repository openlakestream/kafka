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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.metadata.ConfigRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.metadata.TopicRecord;
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
import org.apache.kafka.image.loader.LoaderManifestType;
import org.apache.kafka.metadata.LeaderRecoveryState;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.storage.diskless.OxiaStore;
import org.apache.kafka.storage.diskless.UrsaPartitionedTopicsMetadataSync;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertTrue;

class UrsaPartitionedTopicsPublisherTest {

    private static final class RecordingStore implements OxiaStore {
        final List<String> putKeys = new CopyOnWriteArrayList<>();
        final List<String> deleteKeys = new CopyOnWriteArrayList<>();

        @Override
        public CompletableFuture<Void> put(String key, byte[] value) {
            putKeys.add(key);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> delete(String key) {
            deleteKeys.add(key);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void close() {
        }
    }

    private static UrsaPartitionedTopicsMetadataSync createSync(RecordingStore store) {
        return new UrsaPartitionedTopicsMetadataSync(
                (message, cause) -> { },
                store);
    }

    private static final String KEY_PREFIX = "/admin/partitioned-topics/public/default/persistent/";

    @Test
    void testDeltaIgnoredBeforeBecomingLeader() throws Exception {
        String disklessTopic = "diskless-topic";
        Uuid topicId = Uuid.randomUuid();
        int partitions = 3;

        MetadataImage newImage = new MetadataImage(
                new MetadataProvenance(10, 0, 0, true),
                FeaturesImage.EMPTY, ClusterImage.EMPTY,
                topicsImage(disklessTopic, topicId, partitions),
                configsImageWithDisklessEnabled(disklessTopic),
                ClientQuotasImage.EMPTY, ProducerIdsImage.EMPTY,
                AclsImage.EMPTY, ScramImage.EMPTY, DelegationTokenImage.EMPTY);

        RecordingStore store = new RecordingStore();
        UrsaPartitionedTopicsMetadataSync sync = createSync(store);
        UrsaPartitionedTopicsPublisher publisher = new UrsaPartitionedTopicsPublisher(1, sync);

        MetadataDelta delta = new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build();
        delta.replay(new TopicRecord().setName(disklessTopic).setTopicId(topicId));
        for (int p = 0; p < partitions; p++) {
            delta.replay(partitionRecord(topicId, p));
        }
        delta.replay(disklessConfigRecord(disklessTopic));

        publisher.onMetadataUpdate(delta, newImage, loaderManifest(newImage.provenance()));
        assertTrue(store.putKeys.isEmpty(), "No calls expected before becoming leader");
        publisher.close();
        sync.close();
    }

    @Test
    void testDisklessTopicDeletionCleansUpOxia() throws Exception {
        String disklessTopic = "diskless-topic";
        Uuid topicId = Uuid.randomUuid();
        int partitions = 2;

        MetadataImage oldImage = new MetadataImage(
                new MetadataProvenance(1, 0, 0, true),
                FeaturesImage.EMPTY, ClusterImage.EMPTY,
                topicsImage(disklessTopic, topicId, partitions),
                configsImageWithDisklessEnabled(disklessTopic),
                ClientQuotasImage.EMPTY, ProducerIdsImage.EMPTY,
                AclsImage.EMPTY, ScramImage.EMPTY, DelegationTokenImage.EMPTY);

        MetadataDelta delta = new MetadataDelta.Builder().setImage(oldImage).build();
        delta.replay(new RemoveTopicRecord().setTopicId(topicId));
        MetadataImage newImage = delta.apply(new MetadataProvenance(2, 0, 0, true));

        RecordingStore store = new RecordingStore();
        UrsaPartitionedTopicsMetadataSync sync = createSync(store);
        UrsaPartitionedTopicsPublisher publisher = new UrsaPartitionedTopicsPublisher(1, sync);
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        publisher.onMetadataUpdate(delta, newImage, loaderManifest(newImage.provenance()));

        String partitionedKey = KEY_PREFIX + disklessTopic;
        org.apache.kafka.test.TestUtils.waitForCondition(
                () -> store.deleteKeys.contains(partitionedKey),
                5_000, "Expected delete for " + partitionedKey);
        assertTrue(store.putKeys.isEmpty(),
                "No upsert expected for deletion. putKeys=" + store.putKeys);
        publisher.close();
        sync.close();
    }

    @Test
    void testNonDisklessTopicDeletionIgnored() throws Exception {
        String normalTopic = "normal-topic";
        Uuid topicId = Uuid.randomUuid();
        int partitions = 1;

        MetadataImage oldImage = new MetadataImage(
                new MetadataProvenance(1, 0, 0, true),
                FeaturesImage.EMPTY, ClusterImage.EMPTY,
                topicsImage(normalTopic, topicId, partitions),
                ConfigurationsImage.EMPTY,
                ClientQuotasImage.EMPTY, ProducerIdsImage.EMPTY,
                AclsImage.EMPTY, ScramImage.EMPTY, DelegationTokenImage.EMPTY);

        MetadataDelta delta = new MetadataDelta.Builder().setImage(oldImage).build();
        delta.replay(new RemoveTopicRecord().setTopicId(topicId));
        MetadataImage newImage = delta.apply(new MetadataProvenance(2, 0, 0, true));

        RecordingStore store = new RecordingStore();
        UrsaPartitionedTopicsMetadataSync sync = createSync(store);
        UrsaPartitionedTopicsPublisher publisher = new UrsaPartitionedTopicsPublisher(1, sync);
        publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 1));
        publisher.onMetadataUpdate(delta, newImage, loaderManifest(newImage.provenance()));

        assertTrue(store.deleteKeys.isEmpty(),
                "Non-diskless topic deletion should not trigger Oxia delete. deleteKeys=" + store.deleteKeys);
        publisher.close();
        sync.close();
    }

    @Test
    void testDeltaIgnoredWhenNotActiveController() throws Exception {
        RecordingStore store = new RecordingStore();
        UrsaPartitionedTopicsMetadataSync sync = createSync(store);
        UrsaPartitionedTopicsPublisher publisher = new UrsaPartitionedTopicsPublisher(1, sync);

        String disklessTopic = "diskless-topic";
        Uuid topicId = Uuid.randomUuid();
        MetadataImage image = new MetadataImage(
                new MetadataProvenance(10, 0, 0, true),
                FeaturesImage.EMPTY, ClusterImage.EMPTY,
                topicsImage(disklessTopic, topicId, 1),
                configsImageWithDisklessEnabled(disklessTopic),
                ClientQuotasImage.EMPTY, ProducerIdsImage.EMPTY,
                AclsImage.EMPTY, ScramImage.EMPTY, DelegationTokenImage.EMPTY);

        MetadataDelta delta = new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build();
        publisher.onMetadataUpdate(delta, image, loaderManifest(image.provenance()));

        assertTrue(store.putKeys.isEmpty(), "No calls expected when not active controller");
        publisher.close();
        sync.close();
    }

    private static TopicsImage topicsImage(String topicName, Uuid topicId, int partitions) {
        return TopicsImage.EMPTY.including(new TopicImage(topicName, topicId, partitionMap(partitions)));
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

    private static PartitionRecord partitionRecord(Uuid topicId, int partition) {
        return new PartitionRecord()
                .setTopicId(topicId)
                .setPartitionId(partition)
                .setReplicas(List.of(0))
                .setIsr(List.of(0))
                .setRemovingReplicas(List.of())
                .setAddingReplicas(List.of())
                .setLeader(0)
                .setLeaderEpoch(0)
                .setPartitionEpoch(0);
    }

    private static ConfigRecord disklessConfigRecord(String topicName) {
        return new ConfigRecord()
                .setResourceType(ConfigResource.Type.TOPIC.id())
                .setResourceName(topicName)
                .setName(TopicConfig.URSA_STORAGE_ENABLE_CONFIG)
                .setValue("true");
    }

    private static ConfigurationsImage configsImageWithDisklessEnabled(String topicName) {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        Map<String, String> configs = Map.of(TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true");
        return new ConfigurationsImage(Map.of(resource, new ConfigurationImage(resource, configs)));
    }

    private static LoaderManifest loaderManifest(MetadataProvenance provenance) {
        return new LoaderManifest() {
            @Override
            public LoaderManifestType type() {
                return LoaderManifestType.LOG_DELTA;
            }

            @Override
            public MetadataProvenance provenance() {
                return provenance;
            }
        };
    }
}
