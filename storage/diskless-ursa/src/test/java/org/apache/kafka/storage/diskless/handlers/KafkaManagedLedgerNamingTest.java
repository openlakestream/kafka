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

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaManagedLedgerNamingTest {

    @Test
    void testManagedLedgerName() {
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("test-topic", 2));
        assertEquals(
                "public/default/persistent/test-topic-partition-2",
                KafkaManagedLedgerNaming.managedLedgerName(tp)
        );
    }

    @Test
    void testManagedLedgerNameFromTopicAndPartition() {
        assertEquals(
                "public/default/persistent/test-topic-partition-0",
                KafkaManagedLedgerNaming.managedLedgerName("test-topic", 0)
        );
    }

    @Test
    void testManagedLedgerMetadataPath() {
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("test-topic", 0));
        assertEquals(
                "/managed-ledgers/public/default/persistent/test-topic-partition-0",
                KafkaManagedLedgerNaming.managedLedgerMetadataPath(tp)
        );
    }
}
