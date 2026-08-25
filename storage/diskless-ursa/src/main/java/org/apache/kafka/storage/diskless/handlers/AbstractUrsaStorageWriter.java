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
package org.apache.kafka.storage.diskless.handlers;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.storage.diskless.Writer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Abstract base class for Ursa storage writers.
 * Consolidates common logic for both StorageApi and ManagedLedger implementations.
 */
public abstract class AbstractUrsaStorageWriter implements Writer {

    private static final Logger log = LoggerFactory.getLogger(AbstractUrsaStorageWriter.class);

    protected final UrsaStorageState state;

    protected AbstractUrsaStorageWriter(UrsaStorageState state) {
        this.state = state;
    }

    /**
     * Returns the name of this writer implementation for logging purposes.
     */
    protected abstract String writerName();

    @Override
    public CompletableFuture<Map<TopicIdPartition, PartitionResponse>> write(
            Map<TopicIdPartition, MemoryRecords> entriesPerPartition,
            String zone) {

        log.debug("Writing to {} partitions via {}", entriesPerPartition.size(), writerName());

        if (entriesPerPartition.isEmpty()) {
            return CompletableFuture.completedFuture(Map.of());
        }

        List<CompletableFuture<AbstractMap.SimpleEntry<TopicIdPartition, PartitionResponse>>> futures =
                entriesPerPartition.entrySet().stream()
                        .map(entry -> state.getOrCreatePartitionLog(entry.getKey())
                                .write(entry.getValue(), zone, writerName())
                                .thenApply(response -> new AbstractMap.SimpleEntry<>(entry.getKey(), response)))
                        .toList();

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]))
                .thenApply(v -> {
                    Map<TopicIdPartition, PartitionResponse> result = futures.stream()
                            .map(CompletableFuture::join)
                            .collect(Collectors.toMap(
                                    Map.Entry::getKey,
                                    Map.Entry::getValue,
                                    (oldValue, newValue) -> oldValue,
                                    LinkedHashMap::new));
                    log.debug("Completed writing to {} partitions via {}", result.size(), writerName());
                    return result;
                });
    }

    @Override
    public void close() throws IOException {
    }
}
