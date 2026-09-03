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
package org.apache.kafka.storage.diskless;

import org.apache.kafka.common.Uuid;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Storage-neutral lifecycle operations for Kafka topics whose data is managed by a diskless
 * storage implementation.
 *
 * <p>The implementation owns its catalog schema and metadata-store layout. Kafka supplies only
 * topic identity, partition count, and topic configuration. Every operation is idempotent: the
 * active controller retries it until it succeeds or its desired state changes.
 */
public interface DisklessTopicLifecycle extends AutoCloseable {

    /**
     * A Kafka topic incarnation durably owned by this diskless storage implementation.
     *
     * <p>The topic ID, rather than the topic name alone, identifies the immutable Kafka topic
     * incarnation. Implementations must only return entries carrying explicit Kafka ownership
     * metadata; an arbitrary storage object whose name resembles a Kafka topic is not managed.
     */
    record ManagedTopic(String topicName, Uuid topicId, long sourceRevision) {
        public ManagedTopic {
            Objects.requireNonNull(topicName, "topicName must not be null");
            Objects.requireNonNull(topicId, "topicId must not be null");
            if (topicName.isBlank()) {
                throw new IllegalArgumentException("topicName must not be blank");
            }
            if (Uuid.ZERO_UUID.equals(topicId)) {
                throw new IllegalArgumentException("topicId must not be zero");
            }
            if (sourceRevision < 0) {
                throw new IllegalArgumentException("sourceRevision must not be negative");
            }
        }
    }

    /**
     * Ensures one logical diskless topic exists at the supplied KRaft metadata revision.
     *
     * <p>The implementation creates the topic when it is absent, grows its partition layout when
     * needed, and exactly replaces its configuration. The source revision lets the storage catalog
     * reject a delayed update from an older metadata image.
     */
    CompletableFuture<Void> ensureTopic(String topicName, Uuid topicId, int partitions,
                                        Map<String, String> configs, long sourceRevision);

    /**
     * Permanently deletes the logical topic after its Kafka metadata has been deleted.
     *
     * <p>Kafka topic IDs identify immutable topic incarnations. Implementations must durably fence
     * this ID so that an already in-flight create attempt cannot recreate it after this future
     * completes. A topic recreated with the same name receives a different ID.
     */
    CompletableFuture<Void> deleteTopic(String topicName, Uuid topicId);

    /**
     * Lists non-terminal Kafka topic incarnations managed by this storage implementation.
     *
     * <p>The active Kafka controller uses this semantic inventory to reconcile storage objects
     * left behind by a controller restart. The inventory must include objects still being created
     * or deleted so an abandoned lifecycle claim cannot remain hidden. Implementations must filter
     * using durable ownership metadata and must not infer ownership from a storage-specific name
     * alone. Each entry must also carry the KRaft source revision from the reconciliation that made
     * it visible, so a newly elected but lagging controller cannot delete newer state.
     */
    CompletableFuture<List<ManagedTopic>> listManagedTopics();

    /**
     * Deletes managed topics that are no longer live in the controller's metadata image.
     *
     * <p>{@code liveTopicIds} is the set of diskless topic IDs present in the image at
     * {@code imageOffset}. An implementation must only delete an entry whose source revision is at
     * or below {@code imageOffset}, so state created from a newer image than the caller has seen
     * survives.
     */
    CompletableFuture<Void> sweepOrphans(Set<Uuid> liveTopicIds, long imageOffset);
}
