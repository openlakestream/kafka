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
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.storage.diskless.ListOffsetsPartitionRequest;
import org.apache.kafka.storage.diskless.ListOffsetsPartitionResponse;
import org.apache.kafka.storage.diskless.Reader;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Reader implementation using ManagedLedger for storage.
 * Per-partition read state and logic live in {@link UrsaPartitionLog}.
 */
public class UrsaManagedLedgerReader implements Reader {

    private final UrsaStorageState state;

    public UrsaManagedLedgerReader(UrsaStorageState state) {
        this.state = state;
    }

    @Override
    public CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> fetch(
            FetchParams params,
            Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos) {
        if (fetchInfos.isEmpty()) {
            return CompletableFuture.completedFuture(Map.of());
        }

        List<CompletableFuture<AbstractMap.SimpleEntry<TopicIdPartition, FetchPartitionData>>> futures =
                fetchInfos.entrySet().stream()
                        .map(entry -> state.getOrCreatePartitionLog(entry.getKey())
                                .fetch(entry.getValue())
                                .thenApply(response -> new AbstractMap.SimpleEntry<>(entry.getKey(), response)))
                        .toList();

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]))
                .thenApply(ignored -> futures.stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue,
                                (oldValue, newValue) -> oldValue,
                                LinkedHashMap::new)));
    }

    @Override
    public CompletableFuture<Map<TopicIdPartition, ListOffsetsPartitionResponse>> listOffsets(
            Map<TopicIdPartition, ListOffsetsPartitionRequest> requests) {
        if (requests.isEmpty()) {
            return CompletableFuture.completedFuture(Map.of());
        }

        List<CompletableFuture<AbstractMap.SimpleEntry<TopicIdPartition, ListOffsetsPartitionResponse>>> futures =
                requests.entrySet().stream()
                        .map(entry -> state.getOrCreatePartitionLog(entry.getKey())
                                .listOffsets(entry.getValue())
                                .thenApply(response -> new AbstractMap.SimpleEntry<>(entry.getKey(), response)))
                        .toList();

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]))
                .thenApply(ignored -> futures.stream()
                        .map(CompletableFuture::join)
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue,
                                (oldValue, newValue) -> oldValue,
                                LinkedHashMap::new)));
    }

    @Override
    public void close() throws IOException {
    }
}
