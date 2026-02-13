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
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface Reader extends Closeable {

    /**
     * Fetches data for the specified partitions.
     *
     * @param params the fetch parameters including isolation level, max bytes, and other fetch constraints
     * @param fetchInfos map of partitions to their respective fetch request data
     * @return a future containing a map of partitions to their fetched data
     */
    CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> fetch(
            FetchParams params,
            Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos);

    /**
     * Lists offsets for partitions based on timestamp queries.
     *
     * @param requests map of partition to list offset request
     * @return future with map of partition to list offset response
     */
    default CompletableFuture<Map<TopicIdPartition, ListOffsetsPartitionResponse>> listOffsets(
            Map<TopicIdPartition, ListOffsetsPartitionRequest> requests) {
        return CompletableFuture.completedFuture(Map.of());
    }

    /**
     * Cleans up resources associated with the given partition.
     *
     * @param tp the topic partition to clean up
     */
    void cleanupPartition(TopicIdPartition tp);
}
