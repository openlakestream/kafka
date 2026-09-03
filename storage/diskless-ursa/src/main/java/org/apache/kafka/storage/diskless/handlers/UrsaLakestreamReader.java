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
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.storage.diskless.ListOffsetsPartitionRequest;
import org.apache.kafka.storage.diskless.ListOffsetsPartitionResponse;
import org.apache.kafka.storage.diskless.Reader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Reader implementation using Lakestream for storage.
 * Per-partition read state and logic live in {@link UrsaPartitionLog}.
 *
 * <p>Long polling is decided for the request as a whole, the way the classic fetch purgatory does
 * it. Every requested partition registers an append waiter <em>before</em> the first read, so an
 * append that lands while a read is in flight wakes this request instead of being missed. If the
 * first pass answers the request — any records, any error, or nothing that waiting could change —
 * the waiters are completed at once and the answer is returned. Otherwise the request waits for the
 * first append on any of the caught-up partitions, or for the one timeout the request schedules for
 * all of them, and re-reads exactly those partitions once.
 */
public class UrsaLakestreamReader implements Reader {

    private static final Logger log = LoggerFactory.getLogger(UrsaLakestreamReader.class);

    private final UrsaStorageState state;

    public UrsaLakestreamReader(UrsaStorageState state) {
        this.state = state;
    }

    @Override
    public CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> fetch(
            FetchParams params,
            Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos) {
        if (fetchInfos.isEmpty()) {
            return CompletableFuture.completedFuture(Map.of());
        }

        // A partition whose log cannot be resolved fails on its own; the rest of the request is
        // read and, if it has nothing to send yet, still allowed to wait.
        Map<TopicIdPartition, UrsaPartitionLog> partitionLogs = new LinkedHashMap<>();
        Map<TopicIdPartition, FetchPartitionData> unresolved = new LinkedHashMap<>();
        fetchInfos.keySet().forEach(tp -> {
            try {
                partitionLogs.put(tp, state.getOrCreatePartitionLog(tp));
            } catch (Throwable lookupError) {
                unresolved.put(tp, UrsaPartitionLog.createFetchErrorResponse(
                        UrsaPartitionLog.unresolved(tp, "fetch", lookupError)));
            }
        });

        // Registered before the first read: an append that lands between the read's high-watermark
        // snapshot and the decision to wait must still wake this request.
        Map<TopicIdPartition, CompletableFuture<Void>> waiters = params.maxWaitMs > 0
                ? registerWaiters(partitionLogs)
                : Map.of();
        // One deadline for the whole request, shared by every partition's waiter: a fetch over a
        // thousand partitions costs the timer a single task rather than a thousand.
        CompletableFuture<Void> deadline = waiters.isEmpty()
                ? CompletableFuture.completedFuture(null)
                : scheduleDeadline(params.maxWaitMs);

        CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> result =
                read(partitionLogs, fetchInfos, partitionLogs.keySet())
                        .thenCompose(responses -> {
                            List<TopicIdPartition> caughtUp = caughtUpPartitions(responses, fetchInfos);
                            if (waiters.isEmpty() || caughtUp.isEmpty()
                                    || !unresolved.isEmpty() || answered(responses)) {
                                return CompletableFuture.completedFuture(responses);
                            }
                            return awaitAnyAppend(waiters, deadline)
                                    .thenCompose(ignored -> read(partitionLogs, fetchInfos, caughtUp))
                                    .thenApply(rereads -> merge(partitionLogs.keySet(), responses, rereads));
                        })
                        .thenApply(responses -> withUnresolved(fetchInfos.keySet(), responses, unresolved));

        if (waiters.isEmpty()) {
            return result;
        }
        // Whatever ends the request — an answer, a wait, or a failure — releases every waiter it
        // registered and cancels the one timeout it scheduled.
        return result.whenComplete((responses, error) -> {
            completeWaiters(waiters.values());
            deadline.complete(null);
        });
    }

    @Override
    public CompletableFuture<Map<TopicIdPartition, ListOffsetsPartitionResponse>> listOffsets(
            Map<TopicIdPartition, ListOffsetsPartitionRequest> requests) {
        if (requests.isEmpty()) {
            return CompletableFuture.completedFuture(Map.of());
        }

        List<CompletableFuture<AbstractMap.SimpleEntry<TopicIdPartition, ListOffsetsPartitionResponse>>> futures =
                requests.entrySet().stream()
                        .map(entry -> listOffsets(entry.getKey(), entry.getValue())
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

    /** Lists one partition's offsets, failing only that partition when its log cannot be resolved. */
    private CompletableFuture<ListOffsetsPartitionResponse> listOffsets(
            TopicIdPartition topicIdPartition,
            ListOffsetsPartitionRequest request) {
        UrsaPartitionLog partitionLog;
        try {
            partitionLog = state.getOrCreatePartitionLog(topicIdPartition);
        } catch (Throwable lookupError) {
            return CompletableFuture.completedFuture(ListOffsetsPartitionResponse.error(
                    topicIdPartition,
                    UrsaPartitionLog.unresolved(topicIdPartition, "listOffsets", lookupError)));
        }
        return partitionLog.listOffsets(request);
    }

    /** Puts the partitions that never reached storage back into the response, in request order. */
    private static Map<TopicIdPartition, FetchPartitionData> withUnresolved(
            Collection<TopicIdPartition> requestOrder,
            Map<TopicIdPartition, FetchPartitionData> responses,
            Map<TopicIdPartition, FetchPartitionData> unresolved) {
        if (unresolved.isEmpty()) {
            return responses;
        }
        Map<TopicIdPartition, FetchPartitionData> merged = new LinkedHashMap<>();
        requestOrder.forEach(tp -> {
            FetchPartitionData response = responses.get(tp);
            merged.put(tp, response != null ? response : unresolved.get(tp));
        });
        return merged;
    }

    private static Map<TopicIdPartition, CompletableFuture<Void>> registerWaiters(
            Map<TopicIdPartition, UrsaPartitionLog> partitionLogs) {
        Map<TopicIdPartition, CompletableFuture<Void>> waiters = new LinkedHashMap<>();
        partitionLogs.forEach((tp, partitionLog) -> waiters.put(tp, partitionLog.awaitAppend()));
        return waiters;
    }

    /**
     * The request's single long-poll timeout. It is cancelled as soon as the request ends, whether
     * it ends on an append, on this deadline, or on a failure.
     */
    private CompletableFuture<Void> scheduleDeadline(long maxWaitMs) {
        CompletableFuture<Void> deadline = new CompletableFuture<>();
        try {
            ScheduledFuture<?> timeout = state.timer()
                    .schedule(() -> deadline.complete(null), maxWaitMs, TimeUnit.MILLISECONDS);
            deadline.whenComplete((ignored, error) -> timeout.cancel(false));
        } catch (RuntimeException scheduleError) {
            // The timer is shutting down; answering the fetch immediately is always correct.
            log.debug("Failed to schedule the long-poll timeout for a diskless fetch", scheduleError);
            deadline.complete(null);
        }
        return deadline;
    }

    private static CompletableFuture<Object> awaitAnyAppend(
            Map<TopicIdPartition, CompletableFuture<Void>> waiters,
            CompletableFuture<Void> deadline) {
        List<CompletableFuture<?>> wakeups = new ArrayList<>(waiters.values());
        wakeups.add(deadline);
        return CompletableFuture.anyOf(wakeups.toArray(new CompletableFuture<?>[0]));
    }

    private static void completeWaiters(Collection<CompletableFuture<Void>> waiters) {
        waiters.forEach(waiter -> waiter.complete(null));
    }

    private static CompletableFuture<Map<TopicIdPartition, FetchPartitionData>> read(
            Map<TopicIdPartition, UrsaPartitionLog> partitionLogs,
            Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos,
            Collection<TopicIdPartition> partitions) {
        Map<TopicIdPartition, CompletableFuture<FetchPartitionData>> reads = new LinkedHashMap<>();
        partitions.forEach(tp -> reads.put(tp, partitionLogs.get(tp).fetch(fetchInfos.get(tp))));
        return CompletableFuture.allOf(reads.values().toArray(new CompletableFuture<?>[0]))
                .thenApply(ignored -> {
                    Map<TopicIdPartition, FetchPartitionData> responses = new LinkedHashMap<>();
                    reads.forEach((tp, read) -> responses.put(tp, read.join()));
                    return responses;
                });
    }

    /** True once the request carries something to send: records to deliver, or an error to report. */
    private static boolean answered(Map<TopicIdPartition, FetchPartitionData> responses) {
        return responses.values().stream()
                .anyMatch(response -> response.error != Errors.NONE || response.records.sizeInBytes() > 0);
    }

    /** The partitions a later append would answer: empty, healthy, and read at the high watermark. */
    private static List<TopicIdPartition> caughtUpPartitions(
            Map<TopicIdPartition, FetchPartitionData> responses,
            Map<TopicIdPartition, FetchRequest.PartitionData> fetchInfos) {
        List<TopicIdPartition> caughtUp = new ArrayList<>();
        responses.forEach((tp, response) -> {
            if (response.error == Errors.NONE
                    && response.records.sizeInBytes() == 0
                    && fetchInfos.get(tp).fetchOffset == response.highWatermark) {
                caughtUp.add(tp);
            }
        });
        return caughtUp;
    }

    private static Map<TopicIdPartition, FetchPartitionData> merge(
            Collection<TopicIdPartition> requestOrder,
            Map<TopicIdPartition, FetchPartitionData> responses,
            Map<TopicIdPartition, FetchPartitionData> rereads) {
        Map<TopicIdPartition, FetchPartitionData> merged = new LinkedHashMap<>();
        requestOrder.forEach(tp -> merged.put(tp, rereads.getOrDefault(tp, responses.get(tp))));
        return merged;
    }
}
