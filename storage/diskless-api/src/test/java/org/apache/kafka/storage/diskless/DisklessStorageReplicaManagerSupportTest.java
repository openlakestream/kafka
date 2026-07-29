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

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.replica.ClientMetadata.DefaultClientMetadata;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.common.requests.ListOffsetsRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.storage.log.FetchParams;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.storage.internals.log.UnifiedLog;

import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DisklessStorageReplicaManagerSupportTest {

    @Test
    void testUpdateTopicConfigForwardsStringMapToEngine() throws Exception {
        DisklessStorageEngine engine = mock(DisklessStorageEngine.class);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);
        Properties config = new Properties();
        config.setProperty("retention.ms", "1000");
        config.setProperty("retention.bytes", "2048");

        try (DisklessStorageReplicaManagerSupport support =
                     new DisklessStorageReplicaManagerSupport(metadataView, 1, selector, engine)) {
            support.updateTopicConfig("diskless-topic", config);

            verify(engine).updateTopicConfig("diskless-topic", Map.of(
                    "retention.ms", "1000",
                    "retention.bytes", "2048"));
        }
    }

    @Test
    void testDeleteTopicConfigForwardsTopicToEngine() throws Exception {
        DisklessStorageEngine engine = mock(DisklessStorageEngine.class);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);

        try (DisklessStorageReplicaManagerSupport support =
                     new DisklessStorageReplicaManagerSupport(metadataView, 1, selector, engine)) {
            support.deleteTopicConfig("diskless-topic");

            verify(engine).deleteTopicConfig("diskless-topic");
        }
    }

    @Test
    void testHandleAppendRedirectsNonOwnerWithoutCallingWriter() {
        int localBrokerId = 1;
        TopicIdPartition tp = topicIdPartition("diskless-append-redirect-topic", 0);
        Writer writer = mock(Writer.class);
        Reader reader = mock(Reader.class);
        DisklessStorageStateOperations ursaState = mock(DisklessStorageStateOperations.class);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);
        stubNoZoneSelection(selector, tp, localBrokerId + 1);

        try (DisklessStorageReplicaManagerSupport support =
                     new DisklessStorageReplicaManagerSupport(metadataView, localBrokerId, selector, new TestDisklessStorageEngine(writer, reader, ursaState))) {
            Map<TopicIdPartition, org.apache.kafka.common.requests.ProduceResponse.PartitionResponse> responses =
                    support.handleAppend(Map.of(tp, MemoryRecords.EMPTY), null).join();

            assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, responses.get(tp).error);
            verify(writer, never()).write(any(), anyString());
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
        DisklessStorageStateOperations ursaState = mock(DisklessStorageStateOperations.class);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);
        stubNoZoneSelection(selector, tp, localBrokerId + 1);

        FetchRequest.PartitionData partitionData = new FetchRequest.PartitionData(
                tp.topicId(),
                0L,
                0L,
                1024,
                Optional.empty()
        );

        try (DisklessStorageReplicaManagerSupport support =
                     new DisklessStorageReplicaManagerSupport(metadataView, localBrokerId, selector, new TestDisklessStorageEngine(writer, reader, ursaState))) {
            Map<TopicIdPartition, FetchPartitionData> responses =
                    support.handleFetch(mock(FetchParams.class), Map.of(tp, partitionData), null).join();

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
        DisklessStorageStateOperations ursaState = mock(DisklessStorageStateOperations.class);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);
        stubNoZoneSelection(selector, tp, localBrokerId + 1);

        ListOffsetsPartitionRequest request = new ListOffsetsPartitionRequest(tp, ListOffsetsRequest.LATEST_TIMESTAMP, Optional.empty());

        try (DisklessStorageReplicaManagerSupport support =
                     new DisklessStorageReplicaManagerSupport(metadataView, localBrokerId, selector, new TestDisklessStorageEngine(writer, reader, ursaState))) {
            Map<TopicIdPartition, ListOffsetsPartitionResponse> responses = support.handleListOffsets(Map.of(tp, request), null).join();

            assertEquals(Errors.NOT_LEADER_OR_FOLLOWER, responses.get(tp).error());
            verify(reader, never()).listOffsets(any());
            assertTrue(support.snapshotTrackedPartitions().isEmpty());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testHandleAppendUsesClientZoneAwareOwnership() {
        int localBrokerId = 0;
        TopicIdPartition tp = topicIdPartition("diskless-zone-aware-append-topic", 0);
        Writer writer = mock(Writer.class);
        Reader reader = mock(Reader.class);
        DisklessStorageStateOperations ursaState = mock(DisklessStorageStateOperations.class);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = new DisklessBrokerSelector(
                ignored -> List.of(
                        new Node(0, "host0", 9092, "zone-a"),
                        new Node(1, "host1", 9092, "zone-b")
                ),
                new ListenerName("INTERNAL")
        );
        when(writer.write(any(), eq("zone-a"))).thenReturn(
                CompletableFuture.completedFuture(
                        Map.of(tp, new ProduceResponse.PartitionResponse(Errors.NONE))));

        try (DisklessStorageReplicaManagerSupport support =
                     new DisklessStorageReplicaManagerSupport(metadataView, localBrokerId, selector, new TestDisklessStorageEngine(writer, reader, ursaState))) {
            Map<TopicIdPartition, ProduceResponse.PartitionResponse> responses =
                    support.handleAppend(Map.of(tp, MemoryRecords.withRecords(Compression.NONE, new SimpleRecord("test".getBytes()))),
                            "producer,zone_id=zone-a").join();

            assertEquals(Errors.NONE, responses.get(tp).error);
            verify(writer).write(any(), eq("zone-a"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testHandleFetchUsesClientMetadataZoneAwareOwnership() {
        int localBrokerId = 0;
        TopicIdPartition tp = topicIdPartition("diskless-zone-aware-fetch-topic", 0);
        Writer writer = mock(Writer.class);
        Reader reader = mock(Reader.class);
        DisklessStorageStateOperations ursaState = mock(DisklessStorageStateOperations.class);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = new DisklessBrokerSelector(
                ignored -> List.of(
                        new Node(0, "host0", 9092, "zone-a"),
                        new Node(1, "host1", 9092, "zone-b")
                ),
                new ListenerName("INTERNAL")
        );

        FetchRequest.PartitionData partitionData = new FetchRequest.PartitionData(
                tp.topicId(),
                0L,
                0L,
                1024,
                Optional.empty()
        );
        FetchPartitionData fetchResponse = new FetchPartitionData(
                Errors.NONE,
                10L,
                0L,
                MemoryRecords.EMPTY,
                Optional.empty(),
                java.util.OptionalLong.empty(),
                Optional.empty(),
                java.util.OptionalInt.empty(),
                false
        );
        FetchParams params = new FetchParams(
                -1,
                -1,
                500,
                1,
                1024,
                FetchIsolation.HIGH_WATERMARK,
                Optional.of(new DefaultClientMetadata(
                        "",
                        "consumer,zone_id=zone-a",
                        InetAddress.getLoopbackAddress(),
                        KafkaPrincipal.ANONYMOUS,
                        "PLAINTEXT"
                ))
        );
        when(reader.fetch(eq(params), any())).thenReturn(CompletableFuture.completedFuture(Map.of(tp, fetchResponse)));

        try (DisklessStorageReplicaManagerSupport support =
                     new DisklessStorageReplicaManagerSupport(metadataView, localBrokerId, selector, new TestDisklessStorageEngine(writer, reader, ursaState))) {
            Map<TopicIdPartition, FetchPartitionData> responses =
                    support.handleFetch(params, Map.of(tp, partitionData), "consumer,zone_id=zone-a").join();

            assertEquals(Errors.NONE, responses.get(tp).error);
            verify(reader).fetch(eq(params), any());
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
        DisklessStorageStateOperations ursaState = mock(DisklessStorageStateOperations.class);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);
        configureWriterState(writer, ursaState, true);
        stubNoZoneSelection(selector, tp, localBrokerId);

        try (DisklessStorageReplicaManagerSupport support =
                     new DisklessStorageReplicaManagerSupport(metadataView, localBrokerId, selector, new TestDisklessStorageEngine(writer, reader, ursaState))) {
            support.handleAppend(Map.of(tp, MemoryRecords.EMPTY), null).join();

            assertEquals(Set.of(tp), support.snapshotTrackedPartitions());
            assertTrue(support.hasTrackedPartitionsForTopic(topic));
            assertEquals(Set.of(topic), support.trackedTopicNames());

            AtomicInteger callbackCount = new AtomicInteger(0);
            support.reconcileTrackedPartitions(Set.of(tp), ignored -> callbackCount.incrementAndGet());

            verify(ursaState).cleanupPartition(tp, true);
            verify(ursaState).deletePartitionData(tp);
            assertEquals(1, callbackCount.get());
            assertTrue(support.snapshotTrackedPartitions().isEmpty());
            assertFalse(support.hasTrackedPartitionsForTopic(topic));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testReconcileDeletesPartitionDataOnlyOnOwnerBroker() {
        int localBrokerId = 1;
        TopicIdPartition tp = topicIdPartition("diskless-delete-owned-by-other-topic", 0);
        Writer writer = mock(Writer.class);
        Reader reader = mock(Reader.class);
        DisklessStorageStateOperations ursaState = mock(DisklessStorageStateOperations.class);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);
        configureWriterState(writer, ursaState, true);
        when(selector.effectiveZone(any())).thenReturn(DisklessClientZone.NO_ZONE);
        when(selector.selectBrokerForZone(tp.topicId(), tp.partition(), DisklessClientZone.NO_ZONE)).thenReturn(
                java.util.OptionalInt.of(localBrokerId),
                java.util.OptionalInt.of(localBrokerId + 1)
        );

        try (DisklessStorageReplicaManagerSupport support =
                     new DisklessStorageReplicaManagerSupport(metadataView, localBrokerId, selector, new TestDisklessStorageEngine(writer, reader, ursaState))) {
            support.handleAppend(Map.of(tp, MemoryRecords.EMPTY), null).join();

            support.reconcileTrackedPartitions(Set.of(tp), ignored -> { });

            verify(ursaState).cleanupPartition(tp, true);
            verify(ursaState, never()).deletePartitionData(tp);
            assertTrue(support.snapshotTrackedPartitions().isEmpty());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testReconcileCleansTrackedPartitionWhenBrokerOwnsNoActiveZone() {
        int localBrokerId = 1;
        TopicIdPartition tp = topicIdPartition("diskless-owner-change-topic", 0);
        Writer writer = mock(Writer.class);
        Reader reader = mock(Reader.class);
        DisklessStorageStateOperations ursaState = mock(DisklessStorageStateOperations.class);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);
        configureWriterState(writer, ursaState, true);
        when(metadataView.getTopicId(tp.topic())).thenReturn(tp.topicId());
        when(metadataView.isDisklessStorageTopic(tp.topic())).thenReturn(true);
        stubNoZoneSelection(selector, tp, localBrokerId);

        try (DisklessStorageReplicaManagerSupport support =
                     new DisklessStorageReplicaManagerSupport(metadataView, localBrokerId, selector, new TestDisklessStorageEngine(writer, reader, ursaState))) {
            support.handleAppend(Map.of(tp, MemoryRecords.EMPTY), null).join();
            clearInvocations(selector);
            when(selector.activeZones()).thenReturn(Set.of(DisklessClientZone.NO_ZONE));
            when(selector.selectBrokerForZone(tp.topicId(), tp.partition(), DisklessClientZone.NO_ZONE)).thenReturn(
                    java.util.OptionalInt.of(localBrokerId + 1));

            support.reconcileTrackedPartitions(Set.of(), ignored -> { });

            verify(ursaState).cleanupPartition(tp, false);
            verify(ursaState, never()).deletePartitionData(tp);
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
        DisklessStorageStateOperations ursaState = mock(DisklessStorageStateOperations.class);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);
        configureWriterState(writer, ursaState, true);
        when(metadataView.getTopicId(tp.topic())).thenReturn(Uuid.randomUuid());
        when(metadataView.isDisklessStorageTopic(tp.topic())).thenReturn(true);
        stubNoZoneSelection(selector, tp, localBrokerId);

        try (DisklessStorageReplicaManagerSupport support =
                     new DisklessStorageReplicaManagerSupport(metadataView, localBrokerId, selector, new TestDisklessStorageEngine(writer, reader, ursaState))) {
            support.handleAppend(Map.of(tp, MemoryRecords.EMPTY), null).join();
            clearInvocations(selector);

            support.reconcileTrackedPartitions(Set.of(), ignored -> { });

            verify(ursaState).cleanupPartition(tp, false);
            verify(ursaState, never()).deletePartitionData(tp);
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
        DisklessStorageStateOperations ursaState = mock(DisklessStorageStateOperations.class);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);
        configureWriterState(writer, ursaState, true);
        when(metadataView.getTopicId(tp.topic())).thenReturn(tp.topicId());
        when(metadataView.isDisklessStorageTopic(tp.topic())).thenReturn(true);
        stubNoZoneSelection(selector, tp, localBrokerId);
        when(selector.activeZones()).thenReturn(Set.of(DisklessClientZone.NO_ZONE, "zone-b"));
        when(selector.selectBrokerForZone(tp.topicId(), tp.partition(), DisklessClientZone.NO_ZONE))
                .thenReturn(java.util.OptionalInt.of(localBrokerId));
        when(selector.selectBrokerForZone(tp.topicId(), tp.partition(), "zone-b"))
                .thenReturn(java.util.OptionalInt.of(localBrokerId + 1));

        try (DisklessStorageReplicaManagerSupport support =
                     new DisklessStorageReplicaManagerSupport(metadataView, localBrokerId, selector, new TestDisklessStorageEngine(writer, reader, ursaState))) {
            support.handleAppend(Map.of(tp, MemoryRecords.EMPTY), null).join();
            clearInvocations(selector);

            support.reconcileTrackedPartitions(Set.of(), ignored -> { });

            verify(ursaState, never()).cleanupPartition(any(), eq(false));
            verify(ursaState, never()).deletePartitionData(any());
            assertEquals(Set.of(tp), support.snapshotTrackedPartitions());
            assertTrue(support.hasTrackedPartitionsForTopic(tp.topic()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testReconcileCleansNonOwnedProducerStatesOnRetainedPartition() {
        int localBrokerId = 1;
        TopicIdPartition tp = topicIdPartition("diskless-no-owner-check-topic", 0);
        Writer writer = mock(Writer.class);
        Reader reader = mock(Reader.class);
        DisklessStorageStateOperations ursaState = mock(DisklessStorageStateOperations.class);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);
        configureWriterState(writer, ursaState, true);
        when(metadataView.getTopicId(tp.topic())).thenReturn(tp.topicId());
        when(metadataView.isDisklessStorageTopic(tp.topic())).thenReturn(true);
        stubNoZoneSelection(selector, tp, localBrokerId);
        when(selector.activeZones()).thenReturn(Set.of(DisklessClientZone.NO_ZONE, "zone-b"));
        when(selector.selectBrokerForZone(tp.topicId(), tp.partition(), DisklessClientZone.NO_ZONE))
                .thenReturn(java.util.OptionalInt.of(localBrokerId));
        when(selector.selectBrokerForZone(tp.topicId(), tp.partition(), "zone-b"))
                .thenReturn(java.util.OptionalInt.of(localBrokerId + 1));

        try (DisklessStorageReplicaManagerSupport support =
                     new DisklessStorageReplicaManagerSupport(metadataView, localBrokerId, selector, new TestDisklessStorageEngine(writer, reader, ursaState))) {
            support.handleAppend(Map.of(tp, MemoryRecords.EMPTY), null).join();
            clearInvocations(selector);

            support.reconcileTrackedPartitions(Set.of(), ignored -> { });

            verify(ursaState, never()).cleanupPartition(any(), eq(false));
            verify(ursaState).cleanupNonOwnedProducerStates(tp, Set.of(DisklessClientZone.NO_ZONE), false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testReconcileRetainsPartitionStateWhenBrokerOnlyOwnsZoneScopedState() {
        int localBrokerId = 1;
        TopicIdPartition tp = topicIdPartition("diskless-zone-only-owned-topic", 0);
        Writer writer = mock(Writer.class);
        Reader reader = mock(Reader.class);
        DisklessStorageStateOperations ursaState = mock(DisklessStorageStateOperations.class);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);
        configureWriterState(writer, ursaState, true);
        when(metadataView.getTopicId(tp.topic())).thenReturn(tp.topicId());
        when(metadataView.isDisklessStorageTopic(tp.topic())).thenReturn(true);
        stubNoZoneSelection(selector, tp, localBrokerId);

        try (DisklessStorageReplicaManagerSupport support =
                     new DisklessStorageReplicaManagerSupport(metadataView, localBrokerId, selector, new TestDisklessStorageEngine(writer, reader, ursaState))) {
            support.handleAppend(Map.of(tp, MemoryRecords.EMPTY), null).join();
            clearInvocations(selector);
            when(selector.activeZones()).thenReturn(Set.of(DisklessClientZone.NO_ZONE, "zone-a"));
            when(selector.selectBrokerForZone(tp.topicId(), tp.partition(), DisklessClientZone.NO_ZONE))
                    .thenReturn(java.util.OptionalInt.of(localBrokerId + 1));
            when(selector.selectBrokerForZone(tp.topicId(), tp.partition(), "zone-a"))
                    .thenReturn(java.util.OptionalInt.of(localBrokerId));

            support.reconcileTrackedPartitions(Set.of(), ignored -> { });

            verify(ursaState, never()).cleanupPartition(tp, false);
            verify(ursaState).cleanupNonOwnedProducerStates(tp, Set.of("zone-a"), false);
            assertEquals(Set.of(tp), support.snapshotTrackedPartitions());
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
        DisklessStorageStateOperations ursaState = mock(DisklessStorageStateOperations.class);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);
        configureWriterState(writer, ursaState, false);
        when(metadataView.getTopicId(tp.topic())).thenReturn(tp.topicId());
        when(metadataView.isDisklessStorageTopic(tp.topic())).thenReturn(false);
        stubNoZoneSelection(selector, tp, localBrokerId);
        doThrow(new RuntimeException("boom")).when(ursaState).cleanupPartition(tp, false);

        try (DisklessStorageReplicaManagerSupport support =
                     new DisklessStorageReplicaManagerSupport(metadataView, localBrokerId, selector, new TestDisklessStorageEngine(writer, reader, ursaState))) {
            support.handleAppend(Map.of(tp, MemoryRecords.EMPTY), null).join();

            AtomicInteger callbackCount = new AtomicInteger(0);
            support.reconcileTrackedPartitions(Set.of(), ignored -> callbackCount.incrementAndGet());

            verify(ursaState, times(1)).cleanupPartition(tp, false);
            verify(ursaState, never()).deletePartitionData(tp);
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
        DisklessStorageStateOperations ursaState = mock(DisklessStorageStateOperations.class);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);
        when(metadataView.getTopicId(tp.topic())).thenReturn(tp.topicId());
        when(metadataView.isDisklessStorageTopic(tp.topic())).thenReturn(false);
        when(ursaState.snapshotTrackedPartitions()).thenReturn(Set.of(tp));

        try (DisklessStorageReplicaManagerSupport support =
                     new DisklessStorageReplicaManagerSupport(metadataView, localBrokerId, selector, new TestDisklessStorageEngine(writer, reader, ursaState))) {
            support.reconcileTrackedPartitions(Set.of(), ignored -> { });

            verify(ursaState).cleanupPartition(tp, false);
            verify(ursaState, never()).deletePartitionData(tp);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testReconcileDeletesUntrackedDeletedPartitionDataWhenCurrentBrokerIsOwner() {
        int localBrokerId = 1;
        TopicIdPartition tp = topicIdPartition("diskless-deleted-untracked-topic", 0);
        Writer writer = mock(Writer.class);
        Reader reader = mock(Reader.class);
        DisklessStorageStateOperations ursaState = mock(DisklessStorageStateOperations.class);
        DisklessStorageMetadataView metadataView = mock(DisklessStorageMetadataView.class);
        DisklessBrokerSelector selector = mock(DisklessBrokerSelector.class);
        stubNoZoneSelection(selector, tp, localBrokerId);

        try (DisklessStorageReplicaManagerSupport support =
                     new DisklessStorageReplicaManagerSupport(metadataView, localBrokerId, selector, new TestDisklessStorageEngine(writer, reader, ursaState))) {
            support.reconcileTrackedPartitions(Set.of(tp), ignored -> { });

            verify(ursaState, never()).cleanupPartition(any(), eq(false));
            verify(ursaState).deletePartitionData(tp);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private TopicIdPartition topicIdPartition(String topic, int partition) {
        return new TopicIdPartition(Uuid.randomUuid(), new TopicPartition(topic, partition));
    }

    private void stubNoZoneSelection(DisklessBrokerSelector selector, TopicIdPartition tp, int brokerId) {
        when(selector.effectiveZone(any())).thenReturn(DisklessClientZone.NO_ZONE);
        when(selector.selectBrokerForZone(tp.topicId(), tp.partition(), DisklessClientZone.NO_ZONE))
                .thenReturn(java.util.OptionalInt.of(brokerId));
    }

    private void configureWriterState(Writer writer, DisklessStorageStateOperations ursaState, boolean removeOnCleanup) {
        Set<TopicIdPartition> trackedState = new LinkedHashSet<>();
        when(ursaState.snapshotTrackedPartitions()).thenAnswer(invocation -> Set.copyOf(trackedState));
        when(writer.write(any(), anyString())).thenAnswer(invocation -> {
            Map<TopicIdPartition, MemoryRecords> entries = invocation.getArgument(0);
            trackedState.addAll(entries.keySet());
            return CompletableFuture.completedFuture(Map.of());
        });
        if (removeOnCleanup) {
            org.mockito.Mockito.doAnswer(invocation -> {
                TopicIdPartition tp = invocation.getArgument(0);
                trackedState.remove(tp);
                return true;
            }).when(ursaState).cleanupPartition(any(), anyBoolean());
        }
    }
}
