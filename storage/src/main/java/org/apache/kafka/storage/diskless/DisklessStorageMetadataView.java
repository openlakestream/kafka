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

import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.network.ListenerName;

import java.util.Collections;
import java.util.Map;

public interface DisklessStorageMetadataView {

    boolean isDisklessStorageTopic(String topic);

    Map<String, String> getTopicConfig(String topic);

    Iterable<Node> getAliveBrokerNodes(ListenerName listenerName);

    Uuid getTopicId(String topicName);

    DisklessStorageMetadataView DISABLED = new DisklessStorageMetadataView() {
        @Override
        public boolean isDisklessStorageTopic(String topic) {
            return false;
        }

        @Override
        public Map<String, String> getTopicConfig(String topic) {
            return Map.of();
        }

        @Override
        public Iterable<Node> getAliveBrokerNodes(ListenerName listenerName) {
            return Collections.emptyList();
        }

        @Override
        public Uuid getTopicId(String topicName) {
            return Uuid.ZERO_UUID;
        }
    };
}
