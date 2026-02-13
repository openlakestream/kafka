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
package org.apache.kafka.server.common;

import java.util.Map;

/**
 * Handler for pre-commit Oxia writes during diskless topic creation.
 *
 * <p>Implementations write to Oxia BEFORE KRaft commit so that failures
 * can be surfaced to the user as topic creation errors.
 *
 * <p>Topic deletion cleanup is handled post-commit by {@code UrsaPartitionedTopicsPublisher},
 * because deleting Oxia metadata before KRaft commit risks inconsistency if KRaft deletion
 * fails after Oxia metadata is already removed.
 */
public interface DisklessTopicPreCommitHandler {

    /**
     * Write diskless topic metadata to Oxia before KRaft commit.
     *
     * @param topicName  the topic name
     * @param partitions the number of partitions
     * @param configs    the topic creation configs
     * @throws Exception if the Oxia write fails; the topic creation will be rejected
     */
    void preCommitCreateTopic(String topicName, int partitions, Map<String, String> configs) throws Exception;
}
