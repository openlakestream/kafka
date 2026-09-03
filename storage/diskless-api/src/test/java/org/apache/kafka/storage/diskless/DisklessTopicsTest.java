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

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DisklessTopicsTest {
    @Test
    void internalTopicsAreNeverDiskless() {
        assertFalse(DisklessTopics.isDiskless("__consumer_offsets", Map.of("ursa.storage.enable", "true")));
    }
    @Test
    void enabledConfigMakesTopicDiskless() {
        assertTrue(DisklessTopics.isDiskless("orders", Map.of("ursa.storage.enable", "true")));
        assertFalse(DisklessTopics.isDiskless("orders", Map.of("ursa.storage.enable", "false")));
        assertFalse(DisklessTopics.isDiskless("orders", Map.of()));
    }
    @Test
    void partitionCountAdaptsAMissingMetadataCacheAnswer() {
        assertEquals(OptionalInt.of(3), DisklessTopics.partitionCount(Optional.of(3)));
        assertEquals(OptionalInt.empty(), DisklessTopics.partitionCount(Optional.empty()));
        assertEquals(OptionalInt.empty(), DisklessTopics.partitionCount(null));
    }
}
