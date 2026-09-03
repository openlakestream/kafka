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
package org.apache.kafka.storage.diskless.idempotent;

import org.apache.kafka.common.Uuid;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ProducerStateSnapshotKeysTest {

    @Test
    void testUnzonedSnapshotKeyResolvesToItsTopicId() {
        Uuid topicId = Uuid.fromString("65WMNfybQpCDVulYOxMCTw");

        assertEquals(
                Optional.of(topicId),
                ProducerStateSnapshotKeys.topicIdOfSnapshotKey(
                        ProducerStateSnapshotKeys.snapshotKey(topicId.toString(), 7)));
    }

    @Test
    void testZonedSnapshotKeyResolvesToItsTopicId() {
        Uuid topicId = Uuid.randomUuid();

        assertEquals(
                Optional.of(topicId),
                ProducerStateSnapshotKeys.topicIdOfSnapshotKey(
                        ProducerStateSnapshotKeys.snapshotKey(topicId.toString(), 3, "zone-a")));
    }

    @Test
    void testEveryRandomTopicIdRoundTrips() {
        // The topic ID is base64url and may itself contain the '-' that separates the partition.
        for (int i = 0; i < 2000; i++) {
            Uuid topicId = Uuid.randomUuid();
            assertEquals(
                    Optional.of(topicId),
                    ProducerStateSnapshotKeys.topicIdOfSnapshotKey(
                            ProducerStateSnapshotKeys.snapshotKey(topicId.toString(), i)));
        }
    }

    @Test
    void testKeysThatAreNotSnapshotKeysResolveToNothing() {
        Uuid topicId = Uuid.randomUuid();

        assertEquals(Optional.empty(), ProducerStateSnapshotKeys.topicIdOfSnapshotKey(null));
        assertEquals(Optional.empty(), ProducerStateSnapshotKeys.topicIdOfSnapshotKey(""));
        assertEquals(Optional.empty(), ProducerStateSnapshotKeys.topicIdOfSnapshotKey("garbage"));
        // A deleted-topic marker shares neither the prefix nor the partition suffix.
        assertEquals(
                Optional.empty(),
                ProducerStateSnapshotKeys.topicIdOfSnapshotKey(
                        ProducerStateSnapshotKeys.deletedTopicMarkerKey(topicId.toString())));
        // Right prefix, but no partition index.
        assertEquals(
                Optional.empty(),
                ProducerStateSnapshotKeys.topicIdOfSnapshotKey(
                        ProducerStateSnapshotKeys.topicSnapshotPrefix(topicId.toString())));
        assertEquals(
                Optional.empty(),
                ProducerStateSnapshotKeys.topicIdOfSnapshotKey(
                        "producer-state-snapshot/" + topicId + "-not-a-partition"));
        // Right shape, but the ID is not a Kafka topic ID.
        assertEquals(
                Optional.empty(),
                ProducerStateSnapshotKeys.topicIdOfSnapshotKey("producer-state-snapshot/not-a-topic-id-0"));
        assertEquals(
                Optional.empty(),
                ProducerStateSnapshotKeys.topicIdOfSnapshotKey("producer-state-snapshot/-0"));
        assertEquals(
                Optional.empty(),
                ProducerStateSnapshotKeys.topicIdOfSnapshotKey(
                        ProducerStateSnapshotKeys.snapshotKey(Uuid.ZERO_UUID.toString(), 0)));
    }
}
