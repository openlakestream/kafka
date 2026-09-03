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
import org.apache.kafka.common.internals.Topic;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

public final class DisklessTopics {
    private DisklessTopics() { }

    /**
     * Adapts a Kafka metadata cache partition count to the {@link OptionalInt} the diskless SPI
     * takes, so every broker-side supplier converts it the same way.
     */
    public static OptionalInt partitionCount(Optional<Integer> numPartitions) {
        return numPartitions == null || numPartitions.isEmpty()
                ? OptionalInt.empty()
                : OptionalInt.of(numPartitions.get());
    }

    public static boolean isDiskless(String topic, Map<String, String> configs) {
        if (topic == null || Topic.isInternal(topic) || configs == null) {
            return false;
        }
        return Boolean.parseBoolean(configs.get(TopicConfig.URSA_STORAGE_ENABLE_CONFIG));
    }
}
