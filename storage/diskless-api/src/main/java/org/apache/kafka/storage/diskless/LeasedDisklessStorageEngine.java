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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

final class LeasedDisklessStorageEngine implements DisklessStorageEngine {
    private final DisklessStorageEngine delegate;
    private final DisklessClassLoaderRegistry.Lease classLoaderLease;

    LeasedDisklessStorageEngine(
            DisklessStorageEngine delegate,
            DisklessClassLoaderRegistry.Lease classLoaderLease) {
        this.delegate = delegate;
        this.classLoaderLease = classLoaderLease;
    }

    @Override
    public CompletableFuture<Map<TopicIdPartition, ProduceResponse.PartitionResponse>> write(
            Map<TopicIdPartition, MemoryRecords> entriesPerPartition,
            String zone) {
        return callWithClassLoader(() -> delegate.write(entriesPerPartition, zone));
    }

    @Override
    public CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> fetch(
            FetchParams params,
            Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos) {
        return callWithClassLoader(() -> delegate.fetch(params, fetchInfos));
    }

    @Override
    public CompletableFuture<Map<TopicIdPartition, ListOffsetsPartitionResponse>> listOffsets(
            Map<TopicIdPartition, ListOffsetsPartitionRequest> requests) {
        return callWithClassLoader(() -> delegate.listOffsets(requests));
    }

    @Override
    public boolean cleanupPartition(TopicIdPartition tp, boolean deletePartition) {
        return callWithClassLoader(() -> delegate.cleanupPartition(tp, deletePartition));
    }

    @Override
    public void deletePartitionData(TopicIdPartition tp) {
        callWithClassLoader(() -> {
            delegate.deletePartitionData(tp);
            return null;
        });
    }

    @Override
    public void updateTopicConfig(String topic, Map<String, String> config) {
        callWithClassLoader(() -> {
            delegate.updateTopicConfig(topic, config);
            return null;
        });
    }

    @Override
    public void deleteTopicConfig(String topic) {
        callWithClassLoader(() -> {
            delegate.deleteTopicConfig(topic);
            return null;
        });
    }

    @Override
    public Set<TopicIdPartition> snapshotTrackedPartitions() {
        return callWithClassLoader(delegate::snapshotTrackedPartitions);
    }

    @Override
    public boolean cleanupNonOwnedProducerStates(
            TopicIdPartition tp,
            Set<String> retainedZones,
            boolean deleteSnapshot) {
        return callWithClassLoader(() -> delegate.cleanupNonOwnedProducerStates(tp, retainedZones, deleteSnapshot));
    }

    @Override
    public void close() throws IOException {
        try {
            DisklessClassLoaderContext.call(classLoaderLease.classLoader(), () -> {
                delegate.close();
                return null;
            });
        } catch (IOException | RuntimeException | Error e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            classLoaderLease.close();
        }
    }

    private <T> T callWithClassLoader(DisklessClassLoaderContext.Action<T> action) {
        try {
            return DisklessClassLoaderContext.call(classLoaderLease.classLoader(), action);
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Exception e) {
            throw new KafkaException("Failed to invoke Ursa diskless storage engine", e);
        }
    }
}
