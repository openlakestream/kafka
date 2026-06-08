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
import org.apache.kafka.common.protocol.Errors;

/**
 * Response for listing offsets for a partition.
 *
 * @param topicIdPartition the topic-partition queried
 * @param error the error code, NONE if successful
 * @param timestamp the timestamp of the message at the returned offset, or -1 if not found
 * @param offset the offset found, or -1 if not found
 * @param leaderEpoch the leader epoch of the message at the returned offset
 */
public record ListOffsetsPartitionResponse(
        TopicIdPartition topicIdPartition,
        Errors error,
        long timestamp,
        long offset,
        int leaderEpoch
) {
    /**
     * Creates an error response.
     */
    public static ListOffsetsPartitionResponse error(TopicIdPartition tp, Errors error) {
        return new ListOffsetsPartitionResponse(tp, error, -1L, -1L, -1);
    }

    /**
     * Creates a success response with offset and timestamp.
     */
    public static ListOffsetsPartitionResponse success(TopicIdPartition tp, long offset, long timestamp) {
        return new ListOffsetsPartitionResponse(tp, Errors.NONE, timestamp, offset, -1);
    }

    /**
     * Creates a success response with offset, timestamp, and leader epoch.
     */
    public static ListOffsetsPartitionResponse success(TopicIdPartition tp, long offset, long timestamp, int leaderEpoch) {
        return new ListOffsetsPartitionResponse(tp, Errors.NONE, timestamp, offset, leaderEpoch);
    }
}
