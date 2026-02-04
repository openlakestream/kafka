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

import org.apache.kafka.common.config.TopicConfig;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MetadataCacheDisklessStorageViewTest {

    @Test
    void testDisabledSystemReturnsFalse() {
        Function<String, Map<String, String>> supplier = topic -> Map.of(
                TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true"
        );

        MetadataCacheDisklessStorageView view = new MetadataCacheDisklessStorageView(supplier, false);

        assertFalse(view.isDisklessStorageTopic("test-topic"));
    }

    @Test
    void testEnabledSystemWithDisklessTopic() {
        Function<String, Map<String, String>> supplier = topic -> Map.of(
                TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true"
        );

        MetadataCacheDisklessStorageView view = new MetadataCacheDisklessStorageView(supplier, true);

        assertTrue(view.isDisklessStorageTopic("test-topic"));
    }

    @Test
    void testEnabledSystemWithClassicTopic() {
        Function<String, Map<String, String>> supplier = topic -> Map.of(
                TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "false"
        );

        MetadataCacheDisklessStorageView view = new MetadataCacheDisklessStorageView(supplier, true);

        assertFalse(view.isDisklessStorageTopic("test-topic"));
    }

    @Test
    void testEnabledSystemWithMissingConfig() {
        Function<String, Map<String, String>> supplier = topic -> Map.of();

        MetadataCacheDisklessStorageView view = new MetadataCacheDisklessStorageView(supplier, true);

        assertFalse(view.isDisklessStorageTopic("test-topic"));
    }

    @Test
    void testNullConfigReturnsEmpty() {
        Function<String, Map<String, String>> supplier = topic -> null;

        MetadataCacheDisklessStorageView view = new MetadataCacheDisklessStorageView(supplier, true);

        assertFalse(view.isDisklessStorageTopic("test-topic"));
        assertTrue(view.getTopicConfig("test-topic").isEmpty());
    }

    @Test
    void testGetTopicConfig() {
        Map<String, String> expectedConfig = Map.of(
                TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true",
                "custom.config", "value"
        );
        Function<String, Map<String, String>> supplier = topic -> expectedConfig;

        MetadataCacheDisklessStorageView view = new MetadataCacheDisklessStorageView(supplier, true);

        Map<String, String> config = view.getTopicConfig("test-topic");
        assertTrue(config.containsKey(TopicConfig.URSA_STORAGE_ENABLE_CONFIG));
        assertTrue(config.containsKey("custom.config"));
    }

    @Test
    void testDisabledViewAlwaysReturnsFalse() {
        assertFalse(DisklessStorageMetadataView.DISABLED.isDisklessStorageTopic("any-topic"));
        assertTrue(DisklessStorageMetadataView.DISABLED.getTopicConfig("any-topic").isEmpty());
    }
}
