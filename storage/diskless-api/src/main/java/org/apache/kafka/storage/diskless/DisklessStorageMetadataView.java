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
import java.util.OptionalInt;

public interface DisklessStorageMetadataView {

    boolean isDisklessStorageTopic(String topic);

    Map<String, String> getTopicConfig(String topic);

    Iterable<Node> getAliveBrokerNodes(ListenerName listenerName);

    Uuid getTopicId(String topicName);

    /** Returns the current partition count for the topic, or empty when it is unknown. */
    OptionalInt partitionCount(String topic);

    /**
     * The last metadata offset this broker has applied.
     *
     * <p>Storage this broker provisions is stamped with it, so the active controller's orphan sweep
     * can tell state created from an image newer than its own from state it may delete. Never
     * negative: a broker that has applied no metadata yet reports 0.
     *
     * <p>Nothing reads the offset through this view: the diskless storage engine is handed the raw
     * {@code LongSupplier} instead. This method states the contract that supplier has to fulfil,
     * and the implementations here are what hold it to it.
     */
    long imageOffset();

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

        @Override
        public OptionalInt partitionCount(String topic) {
            return OptionalInt.empty();
        }

        @Override
        public long imageOffset() {
            // Diskless storage is off here, so nothing is ever created against this offset.
            return 0L;
        }
    };
}
