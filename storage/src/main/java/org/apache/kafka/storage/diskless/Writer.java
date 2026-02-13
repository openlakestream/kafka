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
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.ProduceResponse;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface Writer extends Closeable {
    /**
     * Writes records to diskless storage.
     *
     * @param entriesPerPartition records to write per partition
     * @return future with partition responses
     */
    CompletableFuture<Map<TopicIdPartition, ProduceResponse.PartitionResponse>> write(
            Map<TopicIdPartition, MemoryRecords> entriesPerPartition);

    /**
     * Cleans up resources associated with the given partition.
     *
     * @param tp the topic partition to clean up
     */
    void cleanupPartition(TopicIdPartition tp);
}
