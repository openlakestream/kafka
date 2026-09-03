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

import java.util.Map;

public interface DisklessStorageEngine extends Reader, Writer, DisklessStorageStateOperations {

    /** Applies the latest broker-visible configuration for one immutable topic incarnation. */
    void applyTopicConfig(String topicName, Uuid topicId, Map<String, String> config);

    /**
     * Fences one deleted topic incarnation before the broker closes its cached partition handles.
     * Implementations must prevent a concurrent request from opening a new handle after this
     * method returns.
     */
    void fenceDeletedTopic(String topicName, Uuid topicId);
}
