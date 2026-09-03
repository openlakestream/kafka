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
package org.apache.kafka.storage.diskless.handlers;

import org.apache.kafka.common.Uuid;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import io.lakestream.api.StreamIdentifier;

/** Ursa catalog identity and ownership metadata for Kafka diskless streams. */
public final class KafkaStreamIdentity {

    public static final String NAMESPACE = "default";
    public static final String KAFKA_MANAGED_PROPERTY = "lakestream.kafka.managed";
    public static final String KAFKA_TOPIC_NAME_PROPERTY = "lakestream.kafka.topic.name";
    public static final String KAFKA_TOPIC_ID_PROPERTY = "lakestream.kafka.topic.id";
    public static final String KAFKA_SOURCE_REVISION_PROPERTY = "lakestream.kafka.source.revision";

    private KafkaStreamIdentity() {
    }

    /**
     * Stable catalog name for one Kafka topic incarnation.
     *
     * <p>The topic ID is intentionally part of the name. Kafka permits a deleted topic to be
     * recreated with the same name, and cleanup of the deleted topic is best effort. A name-only
     * key would let the recreated topic attach to the deleted incarnation's log when that cleanup
     * failed.
     */
    public static String streamName(String topicName, Uuid topicId) {
        Objects.requireNonNull(topicName, "topicName must not be null");
        Objects.requireNonNull(topicId, "topicId must not be null");
        if (Uuid.ZERO_UUID.equals(topicId)) {
            throw new IllegalArgumentException("topicId must not be zero");
        }
        return topicName + "-topic-id-" + topicId;
    }

    /** Catalog identifier of the stream backing one Kafka topic incarnation. */
    public static StreamIdentifier streamIdentifier(String topicName, Uuid topicId) {
        return StreamIdentifier.of(NAMESPACE, streamName(topicName, topicId));
    }

    /** Adds Kafka's stable logical topic identity to the stream ownership metadata. */
    public static Map<String, String> streamProperties(
            String topicName,
            Uuid topicId,
            Map<String, String> topicConfig,
            long sourceRevision) {
        Objects.requireNonNull(topicName, "topicName must not be null");
        Objects.requireNonNull(topicId, "topicId must not be null");
        if (Uuid.ZERO_UUID.equals(topicId)) {
            throw new IllegalArgumentException("topicId must not be zero");
        }
        if (sourceRevision < 0) {
            throw new IllegalArgumentException("sourceRevision must not be negative");
        }
        Map<String, String> properties = new HashMap<>(topicConfig == null ? Map.of() : topicConfig);
        properties.put(KAFKA_MANAGED_PROPERTY, "true");
        properties.put(KAFKA_TOPIC_NAME_PROPERTY, topicName);
        properties.put(KAFKA_TOPIC_ID_PROPERTY, topicId.toString());
        properties.put(KAFKA_SOURCE_REVISION_PROPERTY, Long.toString(sourceRevision));
        return Map.copyOf(properties);
    }
}
