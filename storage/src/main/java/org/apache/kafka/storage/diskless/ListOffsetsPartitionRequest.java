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

import org.apache.kafka.common.TopicIdPartition;

import java.util.Optional;

/**
 * Request for listing offsets for a partition.
 *
 * @param topicIdPartition the topic-partition to query
 * @param timestamp the timestamp to search for. Special values:
 *                  -2 (EARLIEST_TIMESTAMP): return the first offset
 *                  -1 (LATEST_TIMESTAMP): return the high watermark
 *                  -3 (MAX_TIMESTAMP): return offset of message with max timestamp
 *                  -4 (EARLIEST_LOCAL_TIMESTAMP): same as EARLIEST for diskless storage
 *                  -5 (LATEST_TIERED_TIMESTAMP): not applicable, returns -1
 *                  >= 0: return the first offset with timestamp >= this value
 * @param currentLeaderEpoch the current leader epoch for fencing
 */
public record ListOffsetsPartitionRequest(
        TopicIdPartition topicIdPartition,
        long timestamp,
        Optional<Integer> currentLeaderEpoch
) {
    // Kafka timestamp constants
    public static final long EARLIEST_TIMESTAMP = -2L;
    public static final long LATEST_TIMESTAMP = -1L;
    public static final long MAX_TIMESTAMP = -3L;
    public static final long EARLIEST_LOCAL_TIMESTAMP = -4L;
    public static final long LATEST_TIERED_TIMESTAMP = -5L;
}
