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
import org.apache.kafka.storage.diskless.DisklessFutures;
import org.apache.kafka.storage.diskless.DisklessTopicLifecycle;
import org.apache.kafka.storage.diskless.idempotent.ProducerStateSnapshotKeys;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import io.lakestream.api.Partitioning;
import io.lakestream.api.PartitioningStrategy;
import io.lakestream.api.SchemaConfig;
import io.lakestream.api.StreamCatalog;
import io.lakestream.api.StreamCatalogLoader;
import io.lakestream.api.StreamConfig;
import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.StreamMetadata;
import io.lakestream.api.exception.AlreadyExistsException;
import io.lakestream.api.exception.NoSuchStreamException;
import io.lakestream.api.exception.StreamPermanentlyDeletedException;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.exceptions.KeyAlreadyExistsException;
import io.oxia.client.api.options.ListOption;
import io.oxia.client.api.options.PutOption;

/**
 * Ursa implementation of Kafka's logical diskless topic lifecycle.
 *
 * <p>A Kafka topic incarnation owns one Lakestream stream in the catalog and one Oxia key range of
 * producer-state snapshots. Every operation here is idempotent and safe to overlap with itself: the
 * active controller only cancels a timed-out operation best effort, so a retry can run while the
 * attempt it replaces is still in flight.
 */
public final class UrsaDisklessTopicLifecycle implements DisklessTopicLifecycle {

    private static final Logger log = LoggerFactory.getLogger(UrsaDisklessTopicLifecycle.class);
    /** The deleted-topic fence is a marker: only its existence is read. */
    private static final byte[] DELETED_TOPIC_MARKER = new byte[0];
    /** How many topic deletions one sweep may have in flight at once. */
    private static final int MAX_CONCURRENT_SWEEP_DELETIONS = 8;

    private final StreamCatalog catalog;
    private final AsyncOxiaClient producerStateClient;
    private boolean catalogClosed;
    private boolean producerStateClientClosed;

    public UrsaDisklessTopicLifecycle(UrsaStorageConfig config) throws Exception {
        Objects.requireNonNull(config, "config must not be null");
        StreamCatalog openedCatalog = StreamCatalogLoader.open(
                config.getCatalogOxiaServiceUrl(), LakestreamStorageHolder.buildStorageProperties(config));
        try {
            this.producerStateClient = LakestreamStorageHolder.connectProducerStateOxiaClient(config);
        } catch (Exception e) {
            try {
                openedCatalog.close();
            } catch (Exception closeFailure) {
                e.addSuppressed(closeFailure);
            }
            throw e;
        }
        this.catalog = openedCatalog;
    }

    UrsaDisklessTopicLifecycle(StreamCatalog catalog, AsyncOxiaClient producerStateClient) {
        this.catalog = Objects.requireNonNull(catalog, "catalog must not be null");
        this.producerStateClient =
                Objects.requireNonNull(producerStateClient, "producerStateClient must not be null");
    }

    @Override
    public CompletableFuture<List<ManagedTopic>> listManagedTopics() {
        return catalog.listStreamEntries(KafkaStreamIdentity.NAMESPACE)
                .thenApply(entries -> entries.stream()
                        .map(entry -> managedTopic(entry.identifier(), entry.properties()))
                        .filter(Objects::nonNull)
                        .toList());
    }

    @Override
    public CompletableFuture<Void> ensureTopic(
            String topicName,
            Uuid topicId,
            int partitions,
            Map<String, String> configs,
            long sourceRevision) {
        if (partitions < 1) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("partitions must be at least 1"));
        }
        StreamIdentifier id = KafkaStreamIdentity.streamIdentifier(topicName, topicId);
        Map<String, String> properties = KafkaStreamIdentity.streamProperties(
                topicName, topicId, configs, sourceRevision);
        return loadOrCreate(id, partitions, properties)
                .thenCompose(metadata -> growTo(id, metadata, partitions))
                // A replace at an older or equal revision is an idempotent no-op in the catalog, so
                // an overlapping retry cannot roll the property snapshot back.
                .thenCompose(ignored -> catalog.replaceStreamProperties(id, properties, sourceRevision))
                .thenApply(ignored -> null);
    }

    @Override
    public CompletableFuture<Void> deleteTopic(String topicName, Uuid topicId) {
        StreamIdentifier id = KafkaStreamIdentity.streamIdentifier(topicName, topicId);
        return CompletableFuture.allOf(dropStream(id), deleteProducerState(topicId));
    }

    @Override
    public CompletableFuture<Void> sweepOrphans(Set<Uuid> liveTopicIds, long imageOffset) {
        Set<Uuid> live = Set.copyOf(Objects.requireNonNull(liveTopicIds, "liveTopicIds must not be null"));
        // The snapshot index is read before the catalog: a stream exists before any snapshot for it
        // can be written, so every topic still alive here is in the catalog listing that follows.
        // Reading them the other way around would let a topic created during the sweep look
        // unreferenced, and fencing its producer state is permanent.
        return listSnapshotTopicIds().thenCompose(snapshotTopicIds ->
                listManagedTopics().thenCompose(managed ->
                        sweep(managed, snapshotTopicIds, live, imageOffset)));
    }

    private CompletableFuture<Void> sweep(
            List<ManagedTopic> managed,
            Set<Uuid> snapshotTopicIds,
            Set<Uuid> liveTopicIds,
            long imageOffset) {
        Set<Uuid> catalogIds = managed.stream()
                .map(ManagedTopic::topicId)
                .collect(Collectors.toSet());
        List<Supplier<CompletableFuture<Void>>> work = new ArrayList<>();
        for (ManagedTopic topic : managed) {
            // State created from a newer image than the caller has seen must survive the sweep.
            if (!liveTopicIds.contains(topic.topicId()) && topic.sourceRevision() <= imageOffset) {
                work.add(() -> {
                    log.info("Deleting diskless storage of topic {} ({}), which is missing from the "
                            + "metadata image at offset {}", topic.topicName(), topic.topicId(), imageOffset);
                    return deleteTopic(topic.topicName(), topic.topicId());
                });
            }
        }
        for (Uuid topicId : snapshotTopicIds) {
            // A broker's recovery claim can recreate a snapshot inside its own fence-read window,
            // so producer state outlives the stream it belonged to until it is swept here.
            if (!liveTopicIds.contains(topicId) && !catalogIds.contains(topicId)) {
                work.add(() -> {
                    log.info("Deleting diskless producer state of topic {}, which no longer has a stream",
                            topicId);
                    return deleteProducerState(topicId);
                });
            }
        }
        // A controller that has been away for a while can come back to thousands of orphans; every
        // deletion is several catalog and Oxia round trips, so they run a few at a time.
        return BoundedFanOut.run(work, MAX_CONCURRENT_SWEEP_DELETIONS);
    }

    /**
     * Loads the stream, creating it when it does not exist yet. A permanently deleted incarnation
     * must never be recreated, so it fails instead; a create lost to a concurrent creator resolves
     * to what that creator committed.
     */
    private CompletableFuture<StreamMetadata> loadOrCreate(
            StreamIdentifier id,
            int partitions,
            Map<String, String> properties) {
        return catalog.loadStream(id).handle((metadata, error) -> {
            if (error == null) {
                return CompletableFuture.completedFuture(metadata);
            }
            Throwable cause = DisklessFutures.unwrap(error);
            // StreamPermanentlyDeletedException is a NoSuchStreamException, and is terminal.
            if (cause instanceof StreamPermanentlyDeletedException || !(cause instanceof NoSuchStreamException)) {
                return CompletableFuture.<StreamMetadata>failedFuture(cause);
            }
            return createStream(id, partitions, properties);
        }).thenCompose(Function.identity());
    }

    private CompletableFuture<StreamMetadata> createStream(
            StreamIdentifier id,
            int partitions,
            Map<String, String> properties) {
        Partitioning partitioning = new Partitioning(
                PartitioningStrategy.INDEXED,
                Map.of("numPartitions", String.valueOf(partitions)));
        return catalog.createStream(id, new StreamConfig(), partitioning, new SchemaConfig(), properties)
                .handle((created, error) -> {
                    if (error == null) {
                        return CompletableFuture.completedFuture(created);
                    }
                    Throwable cause = DisklessFutures.unwrap(error);
                    if (cause instanceof AlreadyExistsException) {
                        log.debug("Stream {} was created concurrently", id.fullName());
                        return catalog.loadStream(id);
                    }
                    return CompletableFuture.<StreamMetadata>failedFuture(cause);
                })
                .thenCompose(Function.identity());
    }

    /** Grows the committed layout to {@code partitions} when it is smaller. */
    private CompletableFuture<StreamMetadata> growTo(
            StreamIdentifier id,
            StreamMetadata metadata,
            int partitions) {
        if (metadata.partitioning().numPartitions() >= partitions) {
            return CompletableFuture.completedFuture(metadata);
        }
        return catalog.increasePartitions(id, partitions)
                .handle((grown, error) -> error == null
                        ? CompletableFuture.completedFuture(grown)
                        : reloadAfterFailedGrow(id, partitions, DisklessFutures.unwrap(error)))
                .thenCompose(Function.identity());
    }

    /**
     * An overlapping retry can grow the same stream concurrently. That is only a failure when the
     * committed layout still has not reached the target.
     */
    private CompletableFuture<StreamMetadata> reloadAfterFailedGrow(
            StreamIdentifier id,
            int partitions,
            Throwable growFailure) {
        return catalog.loadStream(id).handle((reloaded, reloadError) -> {
            if (reloadError == null && reloaded != null
                    && reloaded.partitioning().numPartitions() >= partitions) {
                return reloaded;
            }
            if (reloadError != null) {
                log.debug("Reloading stream {} after a failed grow failed", id.fullName(), reloadError);
            }
            log.debug("Growing stream {} to {} partitions failed", id.fullName(), partitions, growFailure);
            throw new CompletionException(growFailure);
        });
    }

    /**
     * Drops the stream permanently. A drop is idempotent: {@code false} only means no live stream
     * existed when the tombstone was installed, and a tombstoned identifier is already deleted.
     */
    private CompletableFuture<Void> dropStream(StreamIdentifier id) {
        return catalog.dropStream(id, true).handle((dropped, error) -> {
            if (error == null) {
                return null;
            }
            Throwable cause = DisklessFutures.unwrap(error);
            // Covers StreamPermanentlyDeletedException, which is a NoSuchStreamException.
            if (cause instanceof NoSuchStreamException) {
                return null;
            }
            throw new CompletionException(cause);
        });
    }

    /**
     * Fences the topic incarnation out of producer state and then deletes its snapshots.
     *
     * <p>The fence is written first and on purpose: a broker checks it before claiming a snapshot,
     * so only a fence that is already durable stops a claim from recreating a snapshot behind the
     * range delete. A snapshot that a claim recreates inside its own fence-read window is removed
     * by the next {@link #sweepOrphans}.
     */
    private CompletableFuture<Void> deleteProducerState(Uuid topicId) {
        String id = topicId.toString();
        return producerStateClient.put(
                        ProducerStateSnapshotKeys.deletedTopicMarkerKey(id),
                        DELETED_TOPIC_MARKER,
                        Set.of(PutOption.IfRecordDoesNotExist))
                .handle((ignored, error) -> {
                    if (error == null) {
                        return null;
                    }
                    Throwable cause = DisklessFutures.unwrap(error);
                    // An overlapping delete already fenced this incarnation, which is the outcome
                    // this call wanted.
                    if (cause instanceof KeyAlreadyExistsException) {
                        return null;
                    }
                    throw new CompletionException(cause);
                })
                .thenCompose(ignored -> deleteSnapshots(id));
    }

    /**
     * Removes every producer-state snapshot of one topic incarnation.
     *
     * <p>Oxia compares keys one path segment at a time, so a range whose bounds hold no separator
     * past the prefix reaches the unzoned {@code <prefix><partition>} snapshots and nothing else: a
     * zoned {@code <prefix><partition>/<zone>} key sorts after {@code <prefix>\uffff} because it
     * carries a segment the bound does not, and a zone may hold any number of separators of its
     * own, so no fixed bound covers every depth. The range therefore only retires snapshots written
     * before the topic index existed, and the indexed ones -- every snapshot this code writes --
     * are enumerated through that index and deleted by key.
     */
    private CompletableFuture<Void> deleteSnapshots(String topicId) {
        String prefix = ProducerStateSnapshotKeys.topicSnapshotPrefix(topicId);
        return producerStateClient.deleteRange(prefix, prefix + '\uffff')
                .thenCompose(ignored -> producerStateClient.list(
                        ProducerStateSnapshotKeys.topicIndexKey(topicId),
                        ProducerStateSnapshotKeys.topicIndexEndExclusive(topicId),
                        Set.of(ListOption.UseIndex(ProducerStateSnapshotKeys.topicIndexName()))))
                .thenCompose(keys -> {
                    List<CompletableFuture<Boolean>> deletes = new ArrayList<>(keys.size());
                    for (String key : keys) {
                        deletes.add(producerStateClient.delete(key));
                    }
                    return CompletableFuture.allOf(deletes.toArray(new CompletableFuture<?>[0]));
                });
    }

    /** Distinct topic IDs that still have producer-state snapshots, from the secondary index. */
    private CompletableFuture<Set<Uuid>> listSnapshotTopicIds() {
        return producerStateClient.list(
                        ProducerStateSnapshotKeys.topicIndexKey(""),
                        ProducerStateSnapshotKeys.topicIndexEndExclusive(""),
                        Set.of(ListOption.UseIndex(ProducerStateSnapshotKeys.topicIndexName())))
                .thenApply(keys -> keys.stream()
                        .map(ProducerStateSnapshotKeys::topicIdOfSnapshotKey)
                        .flatMap(Optional::stream)
                        .collect(Collectors.toSet()));
    }

    /**
     * Runs a list of operations with a fixed number of them in flight: the first {@code limit}
     * start together, and every completion starts the next one.
     *
     * <p>The result completes once the last operation has, carrying the first failure if any of
     * them failed. Failing early would leave the remaining orphans unstarted, and the sweep is
     * retried as a whole -- so every operation gets its turn even when one of them has failed.
     */
    private static final class BoundedFanOut {

        private final List<Supplier<CompletableFuture<Void>>> work;
        private final CompletableFuture<Void> done = new CompletableFuture<>();
        private final AtomicInteger next = new AtomicInteger();
        private final AtomicInteger remaining;
        private final AtomicReference<Throwable> firstFailure = new AtomicReference<>();

        private BoundedFanOut(List<Supplier<CompletableFuture<Void>>> work) {
            this.work = work;
            this.remaining = new AtomicInteger(work.size());
        }

        static CompletableFuture<Void> run(List<Supplier<CompletableFuture<Void>>> work, int limit) {
            if (work.isEmpty()) {
                return CompletableFuture.completedFuture(null);
            }
            BoundedFanOut fanOut = new BoundedFanOut(work);
            for (int slot = Math.min(limit, work.size()); slot > 0; slot--) {
                fanOut.startNext();
            }
            return fanOut.done;
        }

        /**
         * Starts operations until one of them is still running. An operation that is already
         * complete continues this loop instead of nesting another frame, so a batch that fails
         * synchronously cannot grow the stack with its own size.
         */
        private void startNext() {
            while (true) {
                int index = next.getAndIncrement();
                if (index >= work.size()) {
                    return;
                }
                CompletableFuture<Void> operation;
                try {
                    operation = work.get(index).get();
                    if (operation == null) {
                        throw new IllegalStateException("a sweep deletion returned a null future");
                    }
                } catch (Throwable startError) {
                    operation = CompletableFuture.failedFuture(startError);
                }
                if (!operation.isDone()) {
                    operation.whenComplete((ignored, error) -> {
                        if (settle(error)) {
                            startNext();
                        }
                    });
                    return;
                }
                if (!settle(failureOf(operation))) {
                    return;
                }
            }
        }

        /** Records one completion; false once it was the last one and {@link #done} is settled. */
        private boolean settle(Throwable error) {
            if (error != null) {
                firstFailure.compareAndSet(null, DisklessFutures.unwrap(error));
            }
            if (remaining.decrementAndGet() > 0) {
                return true;
            }
            Throwable failure = firstFailure.get();
            if (failure != null) {
                done.completeExceptionally(failure);
            } else {
                done.complete(null);
            }
            return false;
        }

        private static Throwable failureOf(CompletableFuture<Void> completed) {
            try {
                completed.join();
                return null;
            } catch (Throwable error) {
                return error;
            }
        }
    }

    private static ManagedTopic managedTopic(
            StreamIdentifier identifier,
            Map<String, String> properties) {
        if (!"true".equals(properties.get(KafkaStreamIdentity.KAFKA_MANAGED_PROPERTY))) {
            return null;
        }
        String topicName = properties.get(KafkaStreamIdentity.KAFKA_TOPIC_NAME_PROPERTY);
        String topicIdValue = properties.get(KafkaStreamIdentity.KAFKA_TOPIC_ID_PROPERTY);
        String sourceRevisionValue = properties.get(KafkaStreamIdentity.KAFKA_SOURCE_REVISION_PROPERTY);
        if (topicName == null || topicName.isBlank()
                || topicIdValue == null || topicIdValue.isBlank()
                || sourceRevisionValue == null || sourceRevisionValue.isBlank()) {
            return null;
        }
        Uuid topicId;
        long sourceRevision;
        try {
            topicId = Uuid.fromString(topicIdValue);
            sourceRevision = Long.parseLong(sourceRevisionValue);
        } catch (IllegalArgumentException error) {
            return null;
        }
        if (Uuid.ZERO_UUID.equals(topicId)
                || sourceRevision < 0
                || !KafkaStreamIdentity.streamIdentifier(topicName, topicId).equals(identifier)) {
            return null;
        }
        return new ManagedTopic(topicName, topicId, sourceRevision);
    }

    /**
     * Closes the catalog and then the producer-state Oxia client, collecting failures. A resource
     * that failed to close is retried by a later call.
     */
    @Override
    public synchronized void close() throws Exception {
        List<Exception> failures = new ArrayList<>();
        if (!catalogClosed) {
            try {
                catalog.close();
                catalogClosed = true;
            } catch (Exception e) {
                failures.add(e);
            }
        }
        if (!producerStateClientClosed) {
            try {
                producerStateClient.close();
                producerStateClientClosed = true;
            } catch (Exception e) {
                failures.add(e);
            }
        }
        if (failures.isEmpty()) {
            return;
        }
        Exception failure = failures.get(0);
        failures.stream().skip(1).forEach(failure::addSuppressed);
        throw failure;
    }
}
