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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.storage.diskless.DisklessTopicLifecycle;
import org.apache.kafka.storage.diskless.idempotent.ProducerStateSnapshotKeys;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.lakestream.api.LifecycleState;
import io.lakestream.api.Partitioning;
import io.lakestream.api.PartitioningStrategy;
import io.lakestream.api.SchemaConfig;
import io.lakestream.api.StreamCatalog;
import io.lakestream.api.StreamCatalogEntry;
import io.lakestream.api.StreamConfig;
import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.StreamMetadata;
import io.lakestream.api.exception.AlreadyExistsException;
import io.lakestream.api.exception.NoSuchStreamException;
import io.lakestream.api.exception.StreamPermanentlyDeletedException;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.PutResult;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.options.ListOption;
import io.oxia.client.api.options.PutOption;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class UrsaDisklessTopicLifecycleTest {

    private static final long TIMEOUT_SECONDS = 30;

    private final StreamCatalog catalog = mock(StreamCatalog.class);
    private final AsyncOxiaClient oxia = mock(AsyncOxiaClient.class);

    @Test
    void testListsOnlyStreamsWithVerifiedKafkaOwnershipMetadata() throws Exception {
        Uuid topicId = Uuid.fromString("65WMNfybQpCDVulYOxMCTw");
        StreamIdentifier owned = KafkaStreamIdentity.streamIdentifier("orders", topicId);
        StreamIdentifier missingManagedMarker = StreamIdentifier.of("default", "missing-managed-marker");
        StreamIdentifier missingTopicId = StreamIdentifier.of("default", "missing-topic-id");
        StreamIdentifier malformedTopicId = StreamIdentifier.of("default", "malformed-topic-id");
        StreamIdentifier mismatchedIdentifier = StreamIdentifier.of("default", "unrelated-stream");
        Uuid missingRevisionTopicId = Uuid.randomUuid();
        StreamIdentifier missingRevision = KafkaStreamIdentity.streamIdentifier(
                "missing-revision", missingRevisionTopicId);
        Uuid malformedRevisionTopicId = Uuid.randomUuid();
        StreamIdentifier malformedRevision = KafkaStreamIdentity.streamIdentifier(
                "malformed-revision", malformedRevisionTopicId);
        Uuid negativeRevisionTopicId = Uuid.randomUuid();
        StreamIdentifier negativeRevision = KafkaStreamIdentity.streamIdentifier(
                "negative-revision", negativeRevisionTopicId);
        when(catalog.listStreamEntries(KafkaStreamIdentity.NAMESPACE)).thenReturn(
                CompletableFuture.completedFuture(List.of(
                        entry(
                                owned,
                                Map.of(
                                        KafkaStreamIdentity.KAFKA_MANAGED_PROPERTY, "true",
                                        KafkaStreamIdentity.KAFKA_TOPIC_NAME_PROPERTY, "orders",
                                        KafkaStreamIdentity.KAFKA_TOPIC_ID_PROPERTY, topicId.toString(),
                                        KafkaStreamIdentity.KAFKA_SOURCE_REVISION_PROPERTY, "42"),
                                LifecycleState.CREATING),
                        entry(
                                missingManagedMarker,
                                Map.of(
                                        KafkaStreamIdentity.KAFKA_TOPIC_NAME_PROPERTY, "orders",
                                        KafkaStreamIdentity.KAFKA_TOPIC_ID_PROPERTY, topicId.toString(),
                                        KafkaStreamIdentity.KAFKA_SOURCE_REVISION_PROPERTY, "42"),
                                LifecycleState.ACTIVE),
                        entry(
                                missingTopicId,
                                Map.of(
                                        KafkaStreamIdentity.KAFKA_MANAGED_PROPERTY, "true",
                                        KafkaStreamIdentity.KAFKA_TOPIC_NAME_PROPERTY, "orders",
                                        KafkaStreamIdentity.KAFKA_SOURCE_REVISION_PROPERTY, "42"),
                                LifecycleState.DELETING),
                        entry(
                                malformedTopicId,
                                Map.of(
                                        KafkaStreamIdentity.KAFKA_MANAGED_PROPERTY, "true",
                                        KafkaStreamIdentity.KAFKA_TOPIC_NAME_PROPERTY, "orders",
                                        KafkaStreamIdentity.KAFKA_TOPIC_ID_PROPERTY, "not-a-topic-id",
                                        KafkaStreamIdentity.KAFKA_SOURCE_REVISION_PROPERTY, "42"),
                                LifecycleState.ACTIVE),
                        entry(
                                mismatchedIdentifier,
                                Map.of(
                                        KafkaStreamIdentity.KAFKA_MANAGED_PROPERTY, "true",
                                        KafkaStreamIdentity.KAFKA_TOPIC_NAME_PROPERTY, "orders",
                                        KafkaStreamIdentity.KAFKA_TOPIC_ID_PROPERTY, topicId.toString(),
                                        KafkaStreamIdentity.KAFKA_SOURCE_REVISION_PROPERTY, "42"),
                                LifecycleState.ACTIVE),
                        entry(
                                missingRevision,
                                Map.of(
                                        KafkaStreamIdentity.KAFKA_MANAGED_PROPERTY, "true",
                                        KafkaStreamIdentity.KAFKA_TOPIC_NAME_PROPERTY, "missing-revision",
                                        KafkaStreamIdentity.KAFKA_TOPIC_ID_PROPERTY, missingRevisionTopicId.toString()),
                                LifecycleState.ACTIVE),
                        entry(
                                malformedRevision,
                                Map.of(
                                        KafkaStreamIdentity.KAFKA_MANAGED_PROPERTY, "true",
                                        KafkaStreamIdentity.KAFKA_TOPIC_NAME_PROPERTY, "malformed-revision",
                                        KafkaStreamIdentity.KAFKA_TOPIC_ID_PROPERTY, malformedRevisionTopicId.toString(),
                                        KafkaStreamIdentity.KAFKA_SOURCE_REVISION_PROPERTY, "not-a-revision"),
                                LifecycleState.ACTIVE),
                        entry(
                                negativeRevision,
                                Map.of(
                                        KafkaStreamIdentity.KAFKA_MANAGED_PROPERTY, "true",
                                        KafkaStreamIdentity.KAFKA_TOPIC_NAME_PROPERTY, "negative-revision",
                                        KafkaStreamIdentity.KAFKA_TOPIC_ID_PROPERTY, negativeRevisionTopicId.toString(),
                                        KafkaStreamIdentity.KAFKA_SOURCE_REVISION_PROPERTY, "-1"),
                                LifecycleState.ACTIVE))));

        try (UrsaDisklessTopicLifecycle lifecycle = lifecycle()) {
            assertEquals(
                    List.of(new DisklessTopicLifecycle.ManagedTopic("orders", topicId, 42L)),
                    lifecycle.listManagedTopics().get());
        }
    }

    @Test
    void testListManagedTopicsIncludesKafkaOwnedDeletingEntry() throws Exception {
        Uuid topicId = Uuid.randomUuid();
        StreamIdentifier identifier = KafkaStreamIdentity.streamIdentifier(
                "deleting-topic", topicId);
        when(catalog.listStreamEntries(KafkaStreamIdentity.NAMESPACE))
                .thenReturn(CompletableFuture.completedFuture(List.of(entry(
                        identifier,
                        Map.of(
                                KafkaStreamIdentity.KAFKA_MANAGED_PROPERTY, "true",
                                KafkaStreamIdentity.KAFKA_TOPIC_NAME_PROPERTY, "deleting-topic",
                                KafkaStreamIdentity.KAFKA_TOPIC_ID_PROPERTY, topicId.toString(),
                                KafkaStreamIdentity.KAFKA_SOURCE_REVISION_PROPERTY, "43"),
                        LifecycleState.DELETING))));

        try (UrsaDisklessTopicLifecycle lifecycle = lifecycle()) {
            assertEquals(
                    List.of(new DisklessTopicLifecycle.ManagedTopic("deleting-topic", topicId, 43L)),
                    lifecycle.listManagedTopics().get());
        }
    }

    @Test
    void testEnsureTopicCreatesGrowsAndReplacesProperties() throws Exception {
        Uuid topicId = Uuid.randomUuid();
        StreamIdentifier id = KafkaStreamIdentity.streamIdentifier("orders", topicId);
        StreamMetadata created = metadata(2);
        StreamMetadata grown = metadata(4);
        when(catalog.loadStream(id))
                .thenReturn(CompletableFuture.failedFuture(new NoSuchStreamException(id)));
        when(catalog.createStream(eq(id), any(), any(), any(), anyMap()))
                .thenReturn(CompletableFuture.completedFuture(created));
        when(catalog.increasePartitions(id, 4))
                .thenReturn(CompletableFuture.completedFuture(grown));
        when(catalog.replaceStreamProperties(eq(id), anyMap(), eq(42L)))
                .thenReturn(CompletableFuture.completedFuture(grown));

        try (UrsaDisklessTopicLifecycle lifecycle = lifecycle()) {
            lifecycle.ensureTopic("orders", topicId, 4, Map.of("retention.ms", "5"), 42L).get();
        }

        verify(catalog).increasePartitions(id, 4);
        verify(catalog).replaceStreamProperties(
                eq(id),
                argThat(properties ->
                        "orders".equals(properties.get(KafkaStreamIdentity.SOURCE_LOGICAL_NAME_PROPERTY))
                                && "orders".equals(
                                        properties.get(KafkaStreamIdentity.KAFKA_TOPIC_NAME_PROPERTY))
                                && "5".equals(properties.get("retention.ms"))),
                eq(42L));
    }

    @Test
    void testCreatesMissingStreamAndRecordsKafkaRevision() throws Exception {
        Uuid topicId = Uuid.fromString("65WMNfybQpCDVulYOxMCTw");
        StreamIdentifier identifier = KafkaStreamIdentity.streamIdentifier("orders", topicId);
        Map<String, String> properties = Map.of("retention.ms", "60000");
        Map<String, String> streamProperties = KafkaStreamIdentity.streamProperties(
                "orders", topicId, properties, 17L);
        StreamMetadata created = metadata(3);
        when(catalog.loadStream(identifier))
                .thenReturn(CompletableFuture.failedFuture(new NoSuchStreamException(identifier)));
        when(catalog.createStream(
                eq(identifier),
                any(StreamConfig.class),
                eq(indexedPartitions(3)),
                any(SchemaConfig.class),
                eq(streamProperties)))
                .thenReturn(CompletableFuture.completedFuture(created));
        when(catalog.replaceStreamProperties(identifier, streamProperties, 17))
                .thenReturn(CompletableFuture.completedFuture(created));

        try (UrsaDisklessTopicLifecycle lifecycle = lifecycle()) {
            lifecycle.ensureTopic("orders", topicId, 3, properties, 17).get();
        }

        verify(catalog).createStream(
                eq(identifier),
                any(StreamConfig.class),
                eq(indexedPartitions(3)),
                any(SchemaConfig.class),
                eq(streamProperties));
        verify(catalog, never()).increasePartitions(identifier, 3);
        verify(catalog).replaceStreamProperties(identifier, streamProperties, 17);
        verify(catalog).close();
    }

    @Test
    void testExistingStreamIsGrownBeforePropertiesAreReplaced() throws Exception {
        Uuid topicId = Uuid.randomUuid();
        StreamIdentifier identifier = KafkaStreamIdentity.streamIdentifier("orders", topicId);
        Map<String, String> streamProperties = KafkaStreamIdentity.streamProperties(
                "orders", topicId, Map.of(), 18L);
        StreamMetadata existing = metadata(1);
        StreamMetadata expanded = metadata(3);
        when(catalog.loadStream(identifier))
                .thenReturn(CompletableFuture.completedFuture(existing));
        when(catalog.increasePartitions(identifier, 3))
                .thenReturn(CompletableFuture.completedFuture(expanded));
        when(catalog.replaceStreamProperties(identifier, streamProperties, 18))
                .thenReturn(CompletableFuture.completedFuture(expanded));

        try (UrsaDisklessTopicLifecycle lifecycle = lifecycle()) {
            lifecycle.ensureTopic("orders", topicId, 3, Map.of(), 18).get();
        }

        InOrder inOrder = inOrder(catalog);
        inOrder.verify(catalog).increasePartitions(identifier, 3);
        inOrder.verify(catalog).replaceStreamProperties(identifier, streamProperties, 18);
        verify(catalog, never()).createStream(any(), any(), any(), any(), any());
    }

    @Test
    void testConcurrentCreateLoadsWinnerAndReconcilesIt() throws Exception {
        Uuid topicId = Uuid.randomUuid();
        StreamIdentifier identifier = KafkaStreamIdentity.streamIdentifier("orders", topicId);
        Map<String, String> streamProperties = KafkaStreamIdentity.streamProperties(
                "orders", topicId, Map.of(), 19L);
        StreamMetadata winner = metadata(3);
        when(catalog.loadStream(identifier))
                .thenReturn(CompletableFuture.failedFuture(new NoSuchStreamException(identifier)))
                .thenReturn(CompletableFuture.completedFuture(winner));
        when(catalog.createStream(
                eq(identifier), any(), eq(indexedPartitions(3)), any(), eq(streamProperties)))
                .thenReturn(CompletableFuture.failedFuture(new AlreadyExistsException("racing create")));
        when(catalog.replaceStreamProperties(identifier, streamProperties, 19))
                .thenReturn(CompletableFuture.completedFuture(winner));

        try (UrsaDisklessTopicLifecycle lifecycle = lifecycle()) {
            lifecycle.ensureTopic("orders", topicId, 3, Map.of(), 19).get();
        }

        verify(catalog).replaceStreamProperties(identifier, streamProperties, 19);
    }

    @Test
    void testEnsureTopicFailsTerminallyWhenPermanentlyDeleted() throws Exception {
        Uuid topicId = Uuid.randomUuid();
        StreamIdentifier id = KafkaStreamIdentity.streamIdentifier("orders", topicId);
        when(catalog.loadStream(id))
                .thenReturn(CompletableFuture.failedFuture(new StreamPermanentlyDeletedException(id)));

        try (UrsaDisklessTopicLifecycle lifecycle = lifecycle()) {
            ExecutionException failure = assertThrows(
                    ExecutionException.class,
                    () -> lifecycle.ensureTopic("orders", topicId, 1, Map.of(), 1L).get());
            assertInstanceOf(StreamPermanentlyDeletedException.class, failure.getCause());
        }

        verify(catalog, never()).createStream(any(), any(), any(), any(), anyMap());
        verify(catalog, never()).replaceStreamProperties(any(), anyMap(), anyLong());
    }

    @Test
    void testConcurrentGrowIsNotAFailureWhenTheLayoutReachedTheTarget() throws Exception {
        Uuid topicId = Uuid.randomUuid();
        StreamIdentifier id = KafkaStreamIdentity.streamIdentifier("orders", topicId);
        Map<String, String> streamProperties = KafkaStreamIdentity.streamProperties(
                "orders", topicId, Map.of(), 20L);
        StreamMetadata beforeGrow = metadata(1);
        StreamMetadata afterConcurrentGrow = metadata(3);
        when(catalog.loadStream(id))
                .thenReturn(CompletableFuture.completedFuture(beforeGrow))
                .thenReturn(CompletableFuture.completedFuture(afterConcurrentGrow));
        when(catalog.increasePartitions(id, 3)).thenReturn(
                CompletableFuture.failedFuture(new IllegalStateException("grown concurrently")));
        when(catalog.replaceStreamProperties(id, streamProperties, 20))
                .thenReturn(CompletableFuture.completedFuture(afterConcurrentGrow));

        try (UrsaDisklessTopicLifecycle lifecycle = lifecycle()) {
            lifecycle.ensureTopic("orders", topicId, 3, Map.of(), 20).get();
        }

        verify(catalog).replaceStreamProperties(id, streamProperties, 20);
    }

    @Test
    void testFailedGrowPropagatesWhenTheLayoutIsStillTooSmall() throws Exception {
        Uuid topicId = Uuid.randomUuid();
        StreamIdentifier id = KafkaStreamIdentity.streamIdentifier("orders", topicId);
        StreamMetadata beforeGrow = metadata(1);
        StreamMetadata stillTooSmall = metadata(2);
        when(catalog.loadStream(id))
                .thenReturn(CompletableFuture.completedFuture(beforeGrow))
                .thenReturn(CompletableFuture.completedFuture(stillTooSmall));
        when(catalog.increasePartitions(id, 3)).thenReturn(
                CompletableFuture.failedFuture(new IllegalStateException("grow rejected")));

        try (UrsaDisklessTopicLifecycle lifecycle = lifecycle()) {
            ExecutionException failure = assertThrows(
                    ExecutionException.class,
                    () -> lifecycle.ensureTopic("orders", topicId, 3, Map.of(), 21).get());
            assertInstanceOf(IllegalStateException.class, failure.getCause());
        }

        verify(catalog, never()).replaceStreamProperties(any(), anyMap(), anyLong());
    }

    @Test
    void testNullReloadAfterAFailedGrowStillReportsTheGrowFailure() throws Exception {
        Uuid topicId = Uuid.randomUuid();
        StreamIdentifier id = KafkaStreamIdentity.streamIdentifier("orders", topicId);
        StreamMetadata beforeGrow = metadata(1);
        IllegalStateException growFailure = new IllegalStateException("grow rejected");
        when(catalog.loadStream(id))
                .thenReturn(CompletableFuture.completedFuture(beforeGrow))
                // A catalog that answers the reload with null must not turn the grow failure into
                // an NPE that hides why the reconcile did not converge.
                .thenReturn(CompletableFuture.completedFuture(null));
        when(catalog.increasePartitions(id, 3)).thenReturn(CompletableFuture.failedFuture(growFailure));

        try (UrsaDisklessTopicLifecycle lifecycle = lifecycle()) {
            ExecutionException failure = assertThrows(
                    ExecutionException.class,
                    () -> lifecycle.ensureTopic("orders", topicId, 3, Map.of(), 21).get());
            assertSame(growFailure, failure.getCause());
        }

        verify(catalog, never()).replaceStreamProperties(any(), anyMap(), anyLong());
    }

    @Test
    void testDeleteTopicDropsStreamAndFencesThenDeletesSnapshots() throws Exception {
        Uuid topicId = Uuid.randomUuid();
        StreamIdentifier id = KafkaStreamIdentity.streamIdentifier("orders", topicId);
        String fenceKey = ProducerStateSnapshotKeys.deletedTopicMarkerKey(topicId.toString());
        String prefix = ProducerStateSnapshotKeys.topicSnapshotPrefix(topicId.toString());
        when(catalog.dropStream(id, true)).thenReturn(CompletableFuture.completedFuture(true));
        when(oxia.put(eq(fenceKey), any(), eq(Set.of(PutOption.IfRecordDoesNotExist))))
                .thenReturn(CompletableFuture.completedFuture(new PutResult(fenceKey, null)));
        when(oxia.deleteRange(anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
        // Oxia compares keys one path segment at a time, so the range bound reaches the unzoned
        // snapshots and no further. A zoned snapshot -- whose zone may itself carry separators --
        // is only reachable through the key the topic index reports.
        String zoned = ProducerStateSnapshotKeys.snapshotKey(topicId.toString(), 0, "rack/0");
        when(oxia.list(
                eq(ProducerStateSnapshotKeys.topicIndexKey(topicId.toString())),
                eq(ProducerStateSnapshotKeys.topicIndexEndExclusive(topicId.toString())),
                eq(Set.of(ListOption.UseIndex(ProducerStateSnapshotKeys.topicIndexName())))))
                .thenReturn(CompletableFuture.completedFuture(List.of(zoned)));
        when(oxia.delete(anyString())).thenReturn(CompletableFuture.completedFuture(true));

        try (UrsaDisklessTopicLifecycle lifecycle = lifecycle()) {
            lifecycle.deleteTopic("orders", topicId).get();
        }

        // The fence must be durable before any snapshot is removed, so a broker that reads it
        // afterwards can never recreate one behind the delete.
        InOrder inOrder = inOrder(oxia);
        inOrder.verify(oxia).put(eq(fenceKey), any(), anySet());
        inOrder.verify(oxia).deleteRange(prefix, prefix + '\uffff');
        inOrder.verify(oxia).delete(zoned);
        verify(catalog).dropStream(id, true);
    }

    @Test
    void testOverlappingDeleteTopicCallsBothComplete() throws Exception {
        Uuid topicId = Uuid.randomUuid();
        StreamIdentifier id = KafkaStreamIdentity.streamIdentifier("orders", topicId);
        String fenceKey = ProducerStateSnapshotKeys.deletedTopicMarkerKey(topicId.toString());
        String prefix = ProducerStateSnapshotKeys.topicSnapshotPrefix(topicId.toString());
        CompletableFuture<Boolean> firstDrop = new CompletableFuture<>();
        CompletableFuture<Boolean> secondDrop = new CompletableFuture<>();
        CompletableFuture<PutResult> firstFence = new CompletableFuture<>();
        CompletableFuture<PutResult> secondFence = new CompletableFuture<>();
        when(catalog.dropStream(id, true)).thenReturn(firstDrop, secondDrop);
        when(oxia.put(eq(fenceKey), any(), anySet())).thenReturn(firstFence, secondFence);
        when(oxia.deleteRange(anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
        when(oxia.list(anyString(), anyString(), anySet()))
                .thenReturn(CompletableFuture.completedFuture(List.of()));

        try (UrsaDisklessTopicLifecycle lifecycle = lifecycle()) {
            // The reconciler only cancels a timed-out operation best effort, so a retry can overlap
            // the original delete inside this class.
            CompletableFuture<Void> first = lifecycle.deleteTopic("orders", topicId);
            CompletableFuture<Void> second = lifecycle.deleteTopic("orders", topicId);

            secondFence.completeExceptionally(new KeyAlreadyExistsException(fenceKey));
            firstFence.complete(new PutResult(fenceKey, null));
            secondDrop.complete(false);
            firstDrop.complete(true);

            first.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            second.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }

        verify(oxia, times(2)).deleteRange(prefix, prefix + '\uffff');
    }

    @Test
    void testSweepDeletesCatalogOrphansAndUnreferencedProducerState() throws Exception {
        Uuid live = Uuid.randomUuid();
        Uuid orphan = Uuid.randomUuid();
        Uuid staleSnapshotOnly = Uuid.randomUuid();
        Uuid tooNew = Uuid.randomUuid();
        when(catalog.listStreamEntries(KafkaStreamIdentity.NAMESPACE))
                .thenReturn(CompletableFuture.completedFuture(List.of(
                        managedEntry("a", live, 10L),
                        managedEntry("b", orphan, 10L),
                        managedEntry("c", tooNew, 99L))));
        // A per-topic index listing drives the delete of each swept topic's zoned snapshots.
        when(oxia.list(anyString(), anyString(), anySet()))
                .thenReturn(CompletableFuture.completedFuture(List.of()));
        when(oxia.list(eq(ProducerStateSnapshotKeys.topicIndexKey("")), anyString(), anySet()))
                .thenReturn(CompletableFuture.completedFuture(List.of(
                        ProducerStateSnapshotKeys.snapshotKey(live.toString(), 0),
                        ProducerStateSnapshotKeys.snapshotKey(staleSnapshotOnly.toString(), 0))));
        when(oxia.delete(anyString())).thenReturn(CompletableFuture.completedFuture(true));
        when(catalog.dropStream(any(), eq(true))).thenReturn(CompletableFuture.completedFuture(true));
        when(oxia.put(anyString(), any(), anySet()))
                .thenAnswer(invocation -> CompletableFuture.completedFuture(
                        new PutResult(invocation.getArgument(0), null)));
        when(oxia.deleteRange(anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));

        try (UrsaDisklessTopicLifecycle lifecycle = lifecycle()) {
            lifecycle.sweepOrphans(Set.of(live), 50L).get();
        }

        verify(catalog).dropStream(KafkaStreamIdentity.streamIdentifier("b", orphan), true);
        // A stream created from a newer image than the caller has seen is never dropped.
        verify(catalog, never()).dropStream(KafkaStreamIdentity.streamIdentifier("c", tooNew), true);
        verify(catalog, never()).dropStream(KafkaStreamIdentity.streamIdentifier("a", live), true);
        verify(oxia).deleteRange(
                startsWith(ProducerStateSnapshotKeys.topicSnapshotPrefix(staleSnapshotOnly.toString())),
                anyString());
        // Neither the range delete nor its permanent fence may touch a live topic.
        verify(oxia, never()).deleteRange(
                startsWith(ProducerStateSnapshotKeys.topicSnapshotPrefix(live.toString())), anyString());
        verify(oxia, never()).put(
                eq(ProducerStateSnapshotKeys.deletedTopicMarkerKey(live.toString())), any(), anySet());
        verify(oxia, never()).deleteRange(
                startsWith(ProducerStateSnapshotKeys.topicSnapshotPrefix(tooNew.toString())), anyString());
        verify(oxia, never()).put(
                eq(ProducerStateSnapshotKeys.deletedTopicMarkerKey(tooNew.toString())), any(), anySet());
    }

    @Test
    void testSweepKeepsAtMostEightDeletionsInFlight() throws Exception {
        List<StreamCatalogEntry> orphans = new ArrayList<>();
        for (int index = 0; index < 20; index++) {
            orphans.add(managedEntry("orphan-" + index, Uuid.randomUuid(), 10L));
        }
        when(catalog.listStreamEntries(KafkaStreamIdentity.NAMESPACE))
                .thenReturn(CompletableFuture.completedFuture(orphans));
        when(oxia.list(anyString(), anyString(), anySet()))
                .thenReturn(CompletableFuture.completedFuture(List.of()));
        when(oxia.put(anyString(), any(), anySet()))
                .thenAnswer(invocation -> CompletableFuture.completedFuture(
                        new PutResult(invocation.getArgument(0), null)));
        when(oxia.deleteRange(anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));

        // Every deletion hangs on its drop until this test releases it, one at a time.
        List<CompletableFuture<Boolean>> drops = new CopyOnWriteArrayList<>();
        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger peakInFlight = new AtomicInteger();
        when(catalog.dropStream(any(), eq(true))).thenAnswer(invocation -> {
            peakInFlight.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
            CompletableFuture<Boolean> drop = new CompletableFuture<>();
            drops.add(drop);
            return drop;
        });

        try (UrsaDisklessTopicLifecycle lifecycle = lifecycle()) {
            CompletableFuture<Void> sweep = lifecycle.sweepOrphans(Set.of(), 50L);

            assertEquals(8, drops.size(), "a sweep must not start every deletion at once");
            for (int index = 0; index < orphans.size(); index++) {
                assertTrue(drops.size() > index, "each completed deletion should start the next one");
                inFlight.decrementAndGet();
                drops.get(index).complete(true);
            }
            sweep.get(5, TimeUnit.SECONDS);
        }

        assertEquals(orphans.size(), drops.size(), "every orphan must still be deleted");
        assertEquals(8, peakInFlight.get(), "at most eight deletions may be in flight at once");
    }

    @Test
    void testSweepReportsADeletionFailureAfterFinishingTheRest() throws Exception {
        Uuid failing = Uuid.randomUuid();
        when(catalog.listStreamEntries(KafkaStreamIdentity.NAMESPACE))
                .thenReturn(CompletableFuture.completedFuture(List.of(
                        managedEntry("a", failing, 10L),
                        managedEntry("b", Uuid.randomUuid(), 10L))));
        when(oxia.list(anyString(), anyString(), anySet()))
                .thenReturn(CompletableFuture.completedFuture(List.of()));
        when(oxia.put(anyString(), any(), anySet()))
                .thenAnswer(invocation -> CompletableFuture.completedFuture(
                        new PutResult(invocation.getArgument(0), null)));
        when(oxia.deleteRange(anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
        IOException dropFailure = new IOException("catalog unavailable");
        when(catalog.dropStream(any(), eq(true)))
                .thenReturn(CompletableFuture.failedFuture(dropFailure))
                .thenReturn(CompletableFuture.completedFuture(true));

        try (UrsaDisklessTopicLifecycle lifecycle = lifecycle()) {
            ExecutionException thrown = assertThrows(
                    ExecutionException.class, () -> lifecycle.sweepOrphans(Set.of(), 50L).get());
            assertSame(dropFailure, thrown.getCause());
        }

        // The failure is reported, but not before every other deletion has had its turn.
        verify(catalog, times(2)).dropStream(any(), eq(true));
    }

    @Test
    void testSweepKeepsABrokerCreatedStreamStampedAfterTheImage() throws Exception {
        // A broker provisions a stream on first partition open and stamps it with the metadata
        // offset it has applied, which is at or after the record that created the topic. A topic
        // created after the sweep's image is therefore never mistaken for an orphan.
        Uuid createdAfterImage = Uuid.randomUuid();
        when(catalog.listStreamEntries(KafkaStreamIdentity.NAMESPACE))
                .thenReturn(CompletableFuture.completedFuture(List.of(
                        managedEntry("new-orders", createdAfterImage, 51L))));
        when(oxia.list(anyString(), anyString(), anySet()))
                .thenReturn(CompletableFuture.completedFuture(List.of(
                        ProducerStateSnapshotKeys.snapshotKey(createdAfterImage.toString(), 0))));

        try (UrsaDisklessTopicLifecycle lifecycle = lifecycle()) {
            lifecycle.sweepOrphans(Set.of(), 50L).get();
        }

        verify(catalog, never()).dropStream(any(), anyBoolean());
        verify(oxia, never()).put(anyString(), any(), anySet());
        verify(oxia, never()).deleteRange(anyString(), anyString());
    }

    @Test
    void testSweepReadsTheSnapshotIndexBeforeTheCatalog() throws Exception {
        when(catalog.listStreamEntries(KafkaStreamIdentity.NAMESPACE))
                .thenReturn(CompletableFuture.completedFuture(List.of()));
        when(oxia.list(anyString(), anyString(), anySet()))
                .thenReturn(CompletableFuture.completedFuture(List.of()));

        try (UrsaDisklessTopicLifecycle lifecycle = lifecycle()) {
            lifecycle.sweepOrphans(Set.of(), 50L).get();
        }

        // A stream is always created before a snapshot can be written for it, so reading the
        // snapshot index first guarantees the later catalog listing covers every topic seen here.
        InOrder inOrder = inOrder(oxia, catalog);
        inOrder.verify(oxia).list(anyString(), anyString(), anySet());
        inOrder.verify(catalog).listStreamEntries(KafkaStreamIdentity.NAMESPACE);
    }

    @Test
    void testCloseClosesTheCatalogAndThenTheProducerStateClient() throws Exception {
        lifecycle().close();

        InOrder inOrder = inOrder(catalog, oxia);
        inOrder.verify(catalog).close();
        inOrder.verify(oxia).close();
    }

    @Test
    void testCloseStillClosesTheProducerStateClientWhenTheCatalogFails() throws Exception {
        IOException catalogFailure = new IOException("catalog close failed");
        doThrow(catalogFailure).when(catalog).close();

        UrsaDisklessTopicLifecycle lifecycle = lifecycle();
        assertSame(catalogFailure, assertThrows(IOException.class, lifecycle::close));

        verify(oxia).close();
    }

    @Test
    void testSameNameTopicIncarnationsUseDifferentStreamIdentifiers() {
        StreamIdentifier first = KafkaStreamIdentity.streamIdentifier(
                "orders", Uuid.fromString("65WMNfybQpCDVulYOxMCTw"));
        StreamIdentifier second = KafkaStreamIdentity.streamIdentifier(
                "orders", Uuid.fromString("VkZ5AkuESPGkMc2OxpKUjw"));

        assertNotEquals(first, second);
    }

    private UrsaDisklessTopicLifecycle lifecycle() {
        return new UrsaDisklessTopicLifecycle(catalog, oxia);
    }

    private static StreamMetadata metadata(int partitions) {
        StreamMetadata metadata = mock(StreamMetadata.class);
        when(metadata.partitioning()).thenReturn(indexedPartitions(partitions));
        return metadata;
    }

    private static StreamCatalogEntry entry(
            StreamIdentifier identifier,
            Map<String, String> properties,
            LifecycleState state) {
        return new StreamCatalogEntry(identifier, state, properties, 1L);
    }

    private static StreamCatalogEntry managedEntry(String topicName, Uuid topicId, long sourceRevision) {
        return entry(
                KafkaStreamIdentity.streamIdentifier(topicName, topicId),
                KafkaStreamIdentity.streamProperties(topicName, topicId, Map.of(), sourceRevision),
                LifecycleState.ACTIVE);
    }

    private static Partitioning indexedPartitions(int partitions) {
        return new Partitioning(
                PartitioningStrategy.INDEXED,
                Map.of("numPartitions", String.valueOf(partitions)));
    }
}
