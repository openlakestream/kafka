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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.storage.diskless.handlers.UrsaStorageState;
import org.apache.kafka.storage.internals.log.UnifiedLog;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DisklessStorageReplicaManagerSupportTest {

    @Test
    void testHandleAppendRedirectsNonOwnerWithoutCallingWriter() {
        int localBrokerId = 1;
        TopicIdPartition tp = topicIdPartition("diskless-append-redirect-topic", 0);
        Writer writer = mock(Writer.class);
        Reader reader = mock(Reader.class);
        UrsaStorageState ursaState = mock(UrsaStorageState.class);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);
        when(selector.selectBroker(tp.topicId(), tp.partition())).thenReturn(java.util.OptionalInt.of(localBrokerId + 1));

        try (DisklessStorageReplicaManagerSupport support =
                     new DisklessStorageReplicaManagerSupport(metadataView, localBrokerId, selector, writer, reader, ursaState)) {
            Map<TopicIdPartition, org.apache.kafka.common.requests.ProduceResponse.PartitionResponse> responses =
                    support.handleAppend(Map.of(tp, MemoryRecords.EMPTY)).join();

            assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, responses.get(tp).error);
            verify(writer, never()).write(any());
            assertTrue(support.snapshotTrackedPartitions().isEmpty());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testHandleFetchRedirectsNonOwnerWithoutCallingReader() {
        int localBrokerId = 1;
        TopicIdPartition tp = topicIdPartition("diskless-fetch-redirect-topic", 0);
        Writer writer = mock(Writer.class);
        Reader reader = mock(Reader.class);
        UrsaStorageState ursaState = mock(UrsaStorageState.class);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);
        when(selector.selectBroker(tp.topicId(), tp.partition())).thenReturn(java.util.OptionalInt.of(localBrokerId + 1));

        FetchRequest.PartitionData partitionData = new FetchRequest.PartitionData(
                tp.topicId(),
                0L,
                0L,
                1024,
                Optional.empty()
        );

        try (DisklessStorageReplicaManagerSupport support =
                     new DisklessStorageReplicaManagerSupport(metadataView, localBrokerId, selector, writer, reader, ursaState)) {
            Map<TopicIdPartition, FetchPartitionData> responses =
                    support.handleFetch(mock(FetchParams.class), Map.of(tp, partitionData)).join();

            FetchPartitionData response = responses.get(tp);
            assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, response.error);
            assertEquals(UnifiedLog.UNKNOWN_OFFSET, response.highWatermark);
            verify(reader, never()).fetch(any(), any());
            assertTrue(support.snapshotTrackedPartitions().isEmpty());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testHandleListOffsetsRedirectsNonOwnerWithoutCallingReader() {
        int localBrokerId = 1;
        TopicIdPartition tp = topicIdPartition("diskless-list-offsets-redirect-topic", 0);
        Writer writer = mock(Writer.class);
        Reader reader = mock(Reader.class);
        UrsaStorageState ursaState = mock(UrsaStorageState.class);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);
        when(selector.selectBroker(tp.topicId(), tp.partition())).thenReturn(java.util.OptionalInt.of(localBrokerId + 1));

        ListOffsetsPartitionRequest request = new ListOffsetsPartitionRequest(tp, ListOffsetsRequest.LATEST_TIMESTAMP, Optional.empty());

        try (DisklessStorageReplicaManagerSupport support =
                     new DisklessStorageReplicaManagerSupport(metadataView, localBrokerId, selector, writer, reader, ursaState)) {
            Map<TopicIdPartition, ListOffsetsPartitionResponse> responses = support.handleListOffsets(Map.of(tp, request)).join();

            assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, responses.get(tp).error());
            verify(reader, never()).listOffsets(any());
            assertTrue(support.snapshotTrackedPartitions().isEmpty());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testReconcileCleansTrackedPartitionWhenTopicDeleted() {
        int localBrokerId = 1;
        String topic = "diskless-topic";
        TopicIdPartition tp = topicIdPartition(topic, 0);
        Writer writer = mock(Writer.class);
        Reader reader = mock(Reader.class);
        UrsaStorageState ursaState = mock(UrsaStorageState.class);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);
        configureWriterState(writer, true);
        when(selector.selectBroker(tp.topicId(), tp.partition())).thenReturn(java.util.OptionalInt.of(localBrokerId));

        try (DisklessStorageReplicaManagerSupport support =
                     new DisklessStorageReplicaManagerSupport(metadataView, localBrokerId, selector, writer, reader, ursaState)) {
            support.handleAppend(Map.of(tp, MemoryRecords.EMPTY)).join();

            assertEquals(Set.of(tp), support.snapshotTrackedPartitions());
            assertTrue(support.hasTrackedPartitionsForTopic(topic));
            assertEquals(Set.of(topic), support.trackedTopicNames());

            AtomicInteger callbackCount = new AtomicInteger(0);
            support.reconcileTrackedPartitions(Set.of(tp.topicId()), ignored -> callbackCount.incrementAndGet());

            verify(writer).cleanupPartition(tp);
            verify(reader).cleanupPartition(tp);
            verify(ursaState).cleanupPartition(tp, true);
            assertEquals(1, callbackCount.get());
            assertTrue(support.snapshotTrackedPartitions().isEmpty());
            assertFalse(support.hasTrackedPartitionsForTopic(topic));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testReconcileCleansTrackedPartitionWhenOwnerChanges() {
        int localBrokerId = 1;
        TopicIdPartition tp = topicIdPartition("diskless-owner-change-topic", 0);
        Writer writer = mock(Writer.class);
        Reader reader = mock(Reader.class);
        UrsaStorageState ursaState = mock(UrsaStorageState.class);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);
        configureWriterState(writer, true);
        when(metadataView.getTopicId(tp.topic())).thenReturn(tp.topicId());
        when(metadataView.isDisklessStorageTopic(tp.topic())).thenReturn(true);
        when(selector.selectBroker(tp.topicId(), tp.partition())).thenReturn(
                java.util.OptionalInt.of(localBrokerId),
                java.util.OptionalInt.of(localBrokerId + 1)
        );

        try (DisklessStorageReplicaManagerSupport support =
                     new DisklessStorageReplicaManagerSupport(metadataView, localBrokerId, selector, writer, reader, ursaState)) {
            support.handleAppend(Map.of(tp, MemoryRecords.EMPTY)).join();

            support.reconcileTrackedPartitions(Set.of(), ignored -> { });

            verify(writer).cleanupPartition(tp);
            verify(reader).cleanupPartition(tp);
            verify(ursaState).cleanupPartition(tp, false);
            assertTrue(support.snapshotTrackedPartitions().isEmpty());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testReconcileCleansTrackedPartitionWhenTopicIdChanges() {
        int localBrokerId = 1;
        TopicIdPartition tp = topicIdPartition("diskless-topicid-change-topic", 0);
        Writer writer = mock(Writer.class);
        Reader reader = mock(Reader.class);
        UrsaStorageState ursaState = mock(UrsaStorageState.class);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);
        configureWriterState(writer, true);
        when(metadataView.getTopicId(tp.topic())).thenReturn(Uuid.randomUuid());
        when(metadataView.isDisklessStorageTopic(tp.topic())).thenReturn(true);
        when(selector.selectBroker(tp.topicId(), tp.partition())).thenReturn(java.util.OptionalInt.of(localBrokerId));

        try (DisklessStorageReplicaManagerSupport support =
                     new DisklessStorageReplicaManagerSupport(metadataView, localBrokerId, selector, writer, reader, ursaState)) {
            support.handleAppend(Map.of(tp, MemoryRecords.EMPTY)).join();

            support.reconcileTrackedPartitions(Set.of(), ignored -> { });

            verify(writer).cleanupPartition(tp);
            verify(reader).cleanupPartition(tp);
            verify(ursaState).cleanupPartition(tp, false);
            assertTrue(support.snapshotTrackedPartitions().isEmpty());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testReconcileKeepsTrackedPartitionWhenBrokerIsStillOwner() {
        int localBrokerId = 1;
        TopicIdPartition tp = topicIdPartition("diskless-owned-topic", 0);
        Writer writer = mock(Writer.class);
        Reader reader = mock(Reader.class);
        UrsaStorageState ursaState = mock(UrsaStorageState.class);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);
        configureWriterState(writer, true);
        when(metadataView.getTopicId(tp.topic())).thenReturn(tp.topicId());
        when(metadataView.isDisklessStorageTopic(tp.topic())).thenReturn(true);
        when(selector.selectBroker(tp.topicId(), tp.partition())).thenReturn(java.util.OptionalInt.of(localBrokerId));

        try (DisklessStorageReplicaManagerSupport support =
                     new DisklessStorageReplicaManagerSupport(metadataView, localBrokerId, selector, writer, reader, ursaState)) {
            support.handleAppend(Map.of(tp, MemoryRecords.EMPTY)).join();

            support.reconcileTrackedPartitions(Set.of(), ignored -> { });

            verify(writer, never()).cleanupPartition(tp);
            verify(reader, never()).cleanupPartition(tp);
            verify(ursaState, never()).cleanupPartition(any(), eq(false));
            assertEquals(Set.of(tp), support.snapshotTrackedPartitions());
            assertTrue(support.hasTrackedPartitionsForTopic(tp.topic()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testReconcileRetainsTrackedPartitionWhenCleanupFails() {
        int localBrokerId = 1;
        TopicIdPartition tp = topicIdPartition("diskless-retry-topic", 0);
        Writer writer = mock(Writer.class);
        Reader reader = mock(Reader.class);
        UrsaStorageState ursaState = mock(UrsaStorageState.class);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);
        configureWriterState(writer, false);
        when(metadataView.getTopicId(tp.topic())).thenReturn(tp.topicId());
        when(metadataView.isDisklessStorageTopic(tp.topic())).thenReturn(false);
        when(selector.selectBroker(tp.topicId(), tp.partition())).thenReturn(java.util.OptionalInt.of(localBrokerId));
        doThrow(new RuntimeException("boom")).when(writer).cleanupPartition(tp);

        try (DisklessStorageReplicaManagerSupport support =
                     new DisklessStorageReplicaManagerSupport(metadataView, localBrokerId, selector, writer, reader, ursaState)) {
            support.handleAppend(Map.of(tp, MemoryRecords.EMPTY)).join();

            AtomicInteger callbackCount = new AtomicInteger(0);
            support.reconcileTrackedPartitions(Set.of(), ignored -> callbackCount.incrementAndGet());

            verify(writer, times(1)).cleanupPartition(tp);
            verify(reader, times(1)).cleanupPartition(tp);
            verify(ursaState, times(1)).cleanupPartition(tp, false);
            assertEquals(0, callbackCount.get());
            assertEquals(Set.of(tp), support.snapshotTrackedPartitions());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testReconcileCleansUntrackedUrsaStateDiscoveredViaStateSnapshot() {
        int localBrokerId = 1;
        TopicIdPartition tp = topicIdPartition("diskless-untracked-state-topic", 0);
        Writer writer = mock(Writer.class);
        Reader reader = mock(Reader.class);
        UrsaStorageState ursaState = mock(UrsaStorageState.class);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);
        when(metadataView.getTopicId(tp.topic())).thenReturn(tp.topicId());
        when(metadataView.isDisklessStorageTopic(tp.topic())).thenReturn(false);
        when(ursaState.snapshotPartitionsWithLocalState()).thenReturn(Set.of(tp));

        try (DisklessStorageReplicaManagerSupport support =
                     new DisklessStorageReplicaManagerSupport(metadataView, localBrokerId, selector, writer, reader, ursaState)) {
            support.reconcileTrackedPartitions(Set.of(), ignored -> { });

            verify(writer).cleanupPartition(tp);
            verify(reader).cleanupPartition(tp);
            verify(ursaState).cleanupPartition(tp, false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private TopicIdPartition topicIdPartition(String topic, int partition) {
        return new TopicIdPartition(Uuid.randomUuid(), new TopicPartition(topic, partition));
    }

    private void configureWriterState(Writer writer, boolean removeOnCleanup) {
        Set<TopicIdPartition> localState = new LinkedHashSet<>();
        when(writer.snapshotPartitionsWithLocalState()).thenAnswer(invocation -> Set.copyOf(localState));
        when(writer.write(any())).thenAnswer(invocation -> {
            Map<TopicIdPartition, MemoryRecords> entries = invocation.getArgument(0);
            localState.addAll(entries.keySet());
            return CompletableFuture.completedFuture(Map.of());
        });
        if (removeOnCleanup) {
            org.mockito.Mockito.doAnswer(invocation -> {
                TopicIdPartition tp = invocation.getArgument(0);
                localState.remove(tp);
                return null;
            }).when(writer).cleanupPartition(any());
        }
    }
}
