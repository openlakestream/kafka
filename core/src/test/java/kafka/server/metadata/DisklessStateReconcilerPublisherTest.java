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
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.image.loader.LoaderManifest;
import org.apache.kafka.storage.diskless.DisklessStorageReplicaManagerSupport;

import org.junit.jupiter.api.Test;

import java.util.Collections;
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
    void testOnMetadataUpdatePassesDeletedTopicIdsToReconcile() {
        Uuid topicId = Uuid.randomUuid();
        MetadataDelta createdTopicDelta = new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build();
        createdTopicDelta.replay(new TopicRecord().setName("diskless-topic").setTopicId(topicId));
        MetadataImage oldImage = createdTopicDelta.apply(new MetadataProvenance(1, 0, 0, true));

        MetadataDelta delta = new MetadataDelta.Builder().setImage(oldImage).build();
        delta.replay(new RemoveTopicRecord().setTopicId(topicId));

        DisklessStorageReplicaManagerSupport support = mock(DisklessStorageReplicaManagerSupport.class);
        @SuppressWarnings("unchecked")
        Consumer<String> callback = mock(Consumer.class);
        DisklessStateReconcilerPublisher publisher = new DisklessStateReconcilerPublisher(support, callback);

        publisher.onMetadataUpdate(delta, MetadataImage.EMPTY, mock(LoaderManifest.class));

        verify(support).reconcileTrackedPartitions(eq(Set.of(topicId)), same(callback));
    }
}
