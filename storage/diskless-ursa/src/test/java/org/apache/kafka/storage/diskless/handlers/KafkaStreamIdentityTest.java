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

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KafkaStreamIdentityTest {

    @Test
    void testStreamNameIncludesTopicIncarnation() {
        Uuid topicId = Uuid.randomUuid();
        assertEquals(
                "test-topic-topic-id-" + topicId,
                KafkaStreamIdentity.streamName("test-topic", topicId)
        );
    }

    @Test
    void testSameNameRecreatedTopicUsesDifferentStreamNames() {
        Uuid deletedTopicId = Uuid.randomUuid();
        Uuid recreatedTopicId = Uuid.randomUuid();

        assertNotEquals(
                KafkaStreamIdentity.streamName("recreated-topic", deletedTopicId),
                KafkaStreamIdentity.streamName("recreated-topic", recreatedTopicId));
    }

    @Test
    void testUnknownTopicIncarnationIsRejected() {
        assertThrows(
                IllegalArgumentException.class,
                () -> KafkaStreamIdentity.streamName("test-topic", Uuid.ZERO_UUID));
    }

    @Test
    void testStreamPropertiesPreserveLogicalKafkaTopicName() {
        Map<String, String> properties = KafkaStreamIdentity.streamProperties(
                "orders",
                Uuid.fromString("65WMNfybQpCDVulYOxMCTw"),
                Map.of("retention.ms", "60000"),
                42L);

        assertEquals("orders", properties.get(KafkaStreamIdentity.KAFKA_TOPIC_NAME_PROPERTY));
        assertEquals("true", properties.get(KafkaStreamIdentity.KAFKA_MANAGED_PROPERTY));
        assertEquals(
                "65WMNfybQpCDVulYOxMCTw",
                properties.get(KafkaStreamIdentity.KAFKA_TOPIC_ID_PROPERTY));
        assertEquals("42", properties.get(KafkaStreamIdentity.KAFKA_SOURCE_REVISION_PROPERTY));
        assertEquals("60000", properties.get("retention.ms"));
    }
}
