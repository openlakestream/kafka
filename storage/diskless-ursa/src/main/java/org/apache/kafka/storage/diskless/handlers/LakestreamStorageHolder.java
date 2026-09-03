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
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.storage.diskless.DisklessFutures;
import org.apache.kafka.storage.diskless.OxiaServiceUrl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import io.lakestream.api.Log;
import io.lakestream.api.Partitioning;
import io.lakestream.api.PartitioningStrategy;
import io.lakestream.api.SchemaConfig;
import io.lakestream.api.StreamCatalog;
import io.lakestream.api.StreamCatalogLoader;
import io.lakestream.api.StreamConfig;
import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.exception.AlreadyExistsException;
import io.lakestream.api.exception.NoSuchStreamException;
import io.lakestream.api.exception.StreamPermanentlyDeletedException;
import io.oxia.client.api.AsyncOxiaClient;

/**
 * Holds a {@link StreamCatalog} instance for Kafka diskless storage.
 *
 * <p>Partitions are opened with create-if-absent: the first broker to open a partition of a topic
 * creates the stream, and the first broker to open a partition beyond the committed layout grows
 * it. Topic deletion is owned by the active controller; this holder only remembers locally which
 * topic incarnations were deleted so a request racing the deletion cannot reopen their logs.
 */
final class LakestreamStorageHolder implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(LakestreamStorageHolder.class);
    private static final long OXIA_CONNECT_TIMEOUT_SECONDS = 10;
    private static final String OXIA_STORAGE_URL_PROP = "oxiaStorageUrl";
    /** Deleted topic IDs remembered per broker; the oldest entry is dropped past this bound. */
    private static final int MAX_DELETED_TOPICS = 10_000;
    /** One open, then one more after the stream has been created or grown. */
    private static final int OPEN_ATTEMPTS = 2;

    private final StreamCatalog catalog;
    private final AsyncOxiaClient producerStateOxiaClient;
    private final Map<Uuid, Boolean> deletedTopics;
    private boolean catalogClosed;
    private boolean producerStateOxiaClientClosed;

    LakestreamStorageHolder(
            StreamCatalog catalog,
            AsyncOxiaClient producerStateOxiaClient) {
        this(catalog, producerStateOxiaClient, MAX_DELETED_TOPICS);
    }

    LakestreamStorageHolder(
            StreamCatalog catalog,
            AsyncOxiaClient producerStateOxiaClient,
            int maxDeletedTopics) {
        if (maxDeletedTopics <= 0) {
            throw new IllegalArgumentException("maxDeletedTopics must be positive");
        }
        this.catalog = catalog;
        this.producerStateOxiaClient = producerStateOxiaClient;
        this.deletedTopics = Collections.synchronizedMap(
                new LinkedHashMap<Uuid, Boolean>(16, 0.75f, false) {
                    @Override
                    protected boolean removeEldestEntry(Map.Entry<Uuid, Boolean> eldest) {
                        return size() > maxDeletedTopics;
                    }
                });
    }

    StreamCatalog catalog() {
        return catalog;
    }

    AsyncOxiaClient oxiaClient() {
        return producerStateOxiaClient;
    }

    /**
     * Opens the log of {@code tp.partition()}, creating the stream with {@code partitionCount} logs
     * (Kafka identity plus {@code topicConfig} properties) when it does not exist yet, or growing it
     * when the partition is not part of the committed layout yet.
     *
     * <p>A stream this broker creates is stamped with {@code sourceRevision}, the last metadata
     * offset this broker applied. That offset is at or after the record that created the topic, so
     * the active controller's orphan sweep, which only deletes state at or below the offset of its
     * own image, cannot delete a topic created after the image it is sweeping against.
     *
     * <p>The returned future fails with {@link StreamPermanentlyDeletedException} for a tombstoned
     * topic incarnation, which must never be recreated, and with
     * {@link NotLeaderOrFollowerException} for a topic this broker has already seen deleted.
     */
    CompletableFuture<Log> openPartition(
            TopicIdPartition tp,
            int partitionCount,
            Map<String, String> topicConfig,
            long sourceRevision) {
        if (isTopicDeleted(tp.topicId())) {
            return CompletableFuture.failedFuture(
                    new NotLeaderOrFollowerException("Topic " + tp.topic() + " was deleted"));
        }
        StreamIdentifier id = KafkaStreamIdentity.streamIdentifier(tp.topic(), tp.topicId());
        return openOrProvision(id, tp, partitionCount, topicConfig, sourceRevision, OPEN_ATTEMPTS);
    }

    /** Fences further opens of {@code topicId} on this broker. */
    void markTopicDeleted(Uuid topicId) {
        if (topicId == null) {
            return;
        }
        deletedTopics.put(topicId, Boolean.TRUE);
    }

    boolean isTopicDeleted(Uuid topicId) {
        return topicId != null && deletedTopics.containsKey(topicId);
    }

    /**
     * @param attemptsLeft open attempts left, counting the one this call makes. Provisioning only
     *                     happens while another attempt is left, so the recursion is bounded.
     */
    private CompletableFuture<Log> openOrProvision(
            StreamIdentifier id,
            TopicIdPartition tp,
            int partitionCount,
            Map<String, String> topicConfig,
            long sourceRevision,
            int attemptsLeft) {
        return openLog(id, tp.partition()).handle((openedLog, error) -> {
            if (error == null) {
                return openedLog == null
                        ? CompletableFuture.<Log>failedFuture(openedNull(id))
                        : CompletableFuture.completedFuture(openedLog);
            }
            Throwable cause = DisklessFutures.unwrap(error);
            // A tombstoned identifier can never be created again, so it is never provisioned.
            if (attemptsLeft <= 1 || cause instanceof StreamPermanentlyDeletedException) {
                return CompletableFuture.<Log>failedFuture(cause);
            }
            CompletableFuture<?> provision;
            if (cause instanceof NoSuchStreamException) {
                provision = createStream(id, tp, partitionCount, topicConfig, sourceRevision);
            } else if (cause instanceof IllegalArgumentException && partitionCount > tp.partition()) {
                provision = catalog.increasePartitions(id, partitionCount);
            } else {
                return CompletableFuture.<Log>failedFuture(cause);
            }
            return provision.thenCompose(ignored ->
                    openOrProvision(id, tp, partitionCount, topicConfig, sourceRevision, attemptsLeft - 1));
        }).thenCompose(future -> future);
    }

    /**
     * Never throws on the calling thread, which opens a partition while holding the broker's
     * partition lifecycle lock: an open that fails synchronously, or that breaks the catalog
     * contract by answering with null, fails the returned future instead.
     */
    private CompletableFuture<Log> openLog(StreamIdentifier id, int partitionIndex) {
        CompletableFuture<Log> open;
        try {
            open = catalog.openLog(id, partitionIndex);
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
        return open == null ? CompletableFuture.failedFuture(openedNull(id)) : open;
    }

    private static IllegalStateException openedNull(StreamIdentifier id) {
        return new IllegalStateException("StreamCatalog.openLog returned null for " + id.fullName());
    }

    private CompletableFuture<?> createStream(
            StreamIdentifier id,
            TopicIdPartition tp,
            int partitionCount,
            Map<String, String> topicConfig,
            long sourceRevision) {
        Partitioning partitioning = new Partitioning(
                PartitioningStrategy.INDEXED,
                Map.of("numPartitions", String.valueOf(Math.max(partitionCount, tp.partition() + 1))));
        Map<String, String> properties = KafkaStreamIdentity.streamProperties(
                tp.topic(), tp.topicId(), topicConfig, sourceRevision);
        return catalog.createStream(id, new StreamConfig(), partitioning, new SchemaConfig(), properties)
                .handle((metadata, error) -> {
                    if (error == null) {
                        return null;
                    }
                    Throwable cause = DisklessFutures.unwrap(error);
                    if (cause instanceof AlreadyExistsException) {
                        // Another broker created the same stream first; open what it committed.
                        log.debug("Stream {} was created concurrently", id.fullName());
                        return null;
                    }
                    throw new CompletionException(cause);
                });
    }

    static LakestreamStorageHolder create(UrsaStorageConfig config) throws Exception {
        StreamCatalog catalog = openCatalog(config);
        AsyncOxiaClient producerStateOxiaClient = null;
        try {
            // connectProducerStateOxiaClient re-sets the interrupt flag itself, so this catch
            // must not do it a second time.
            producerStateOxiaClient = connectProducerStateOxiaClient(config);
            return new LakestreamStorageHolder(catalog, producerStateOxiaClient);
        } catch (Exception e) {
            try {
                catalog.close();
            } catch (Exception ignored) {
            }
            closeOxiaClientAfterFailedCreate(producerStateOxiaClient, null, e);
            throw e;
        }
    }

    private static StreamCatalog openCatalog(UrsaStorageConfig config) throws Exception {
        try {
            return StreamCatalogLoader.open(config.getCatalogOxiaServiceUrl(), buildStorageProperties(config));
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw e;
        }
    }

    /**
     * Connects the Oxia client that Kafka owns for producer-state snapshots, waiting at most
     * {@link #OXIA_CONNECT_TIMEOUT_SECONDS}. A client that only connects after that deadline is
     * closed rather than leaked.
     */
    static AsyncOxiaClient connectProducerStateOxiaClient(UrsaStorageConfig config) throws Exception {
        CompletableFuture<AsyncOxiaClient> pending =
                new OxiaServiceUrl(config.getUrsaOxiaServiceUrl()).client();
        try {
            return pending.get(OXIA_CONNECT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (Exception e) {
            closeOxiaClientAfterFailedCreate(null, pending, e);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw e;
        }
    }

    static void closeOxiaClientAfterFailedCreate(
            AsyncOxiaClient oxiaClient,
            CompletableFuture<AsyncOxiaClient> oxiaClientFuture,
            Exception createFailure) {
        if (oxiaClient != null) {
            try {
                oxiaClient.close();
            } catch (Exception closeFailure) {
                createFailure.addSuppressed(closeFailure);
            }
        } else if (oxiaClientFuture != null) {
            oxiaClientFuture.thenAccept(client -> {
                try {
                    client.close();
                } catch (Exception ignored) {
                }
            });
        }
    }

    static Properties buildStorageProperties(UrsaStorageConfig config) {
        Properties properties = new Properties();
        String normalizedBackendType = normalizeBackendType(config.getBackendType());
        properties.setProperty("backendStorageType", normalizedBackendType);
        properties.setProperty("storagePath", config.getStoragePath());
        properties.setProperty(OXIA_STORAGE_URL_PROP, config.getUrsaOxiaServiceUrl());
        properties.setProperty("writeBufferFlushIntervalMs", String.valueOf(config.getWriteBufferFlushIntervalMs()));
        properties.setProperty("writeBufferSize", String.valueOf(config.getWriteBufferSize()));
        properties.setProperty("writeBufferFlushSize", String.valueOf(config.getWriteBufferFlushSize()));

        if (isRemoteBackend(normalizedBackendType)) {
            setIfNotEmpty(config.getS3Endpoint(), v -> properties.setProperty("cloudStorageEndpoint", v));
            setIfNotEmpty(config.getS3Bucket(), v -> properties.setProperty("bucket", v));
            setIfNotEmpty(config.getStoragePath(), v -> properties.setProperty("prefix", v));
            setIfNotEmpty(config.getS3Region(), v -> properties.setProperty("region", v));
        }

        if (isS3Backend(normalizedBackendType)) {
            setIfNotEmpty(config.getS3AccessKey(), v -> properties.setProperty("s3AccessKeyId", v));
            setIfNotEmpty(config.getS3SecretKey(), v -> properties.setProperty("s3SecretAccessKey", v));
            setIfNotEmpty(config.getS3SessionToken(), v -> properties.setProperty("s3SessionToken", v));
            if (config.getS3PathStyleAccess() != null) {
                properties.setProperty("s3PathStyleAccess", String.valueOf(config.getS3PathStyleAccess()));
            }
            // Deprecated fields, keep for compatibility with older configs.
            setIfNotEmpty(config.getS3Bucket(), v -> properties.setProperty("s3Bucket", v));
            setIfNotEmpty(config.getStoragePath(), v -> properties.setProperty("s3Prefix", v));
            setIfNotEmpty(config.getS3Region(), v -> properties.setProperty("s3Region", v));
        }

        setIfNotEmpty(config.getCompactionBucket(), v -> properties.setProperty("compactionBucket", v));
        setIfNotEmpty(config.getCompactionPrefix(), v -> properties.setProperty("compactionPrefix", v));
        return properties;
    }

    private static boolean isRemoteBackend(String normalizedBackendType) {
        return !"LOCAL".equals(normalizedBackendType);
    }

    private static boolean isS3Backend(String normalizedBackendType) {
        return "S3".equals(normalizedBackendType);
    }

    private static String normalizeBackendType(String backendType) {
        String normalizedBackendType = backendType.toUpperCase(Locale.ROOT);
        if ("AZURE_BLOB".equals(normalizedBackendType) || "AZUREBLOB".equals(normalizedBackendType)) {
            return "AZUREBLOB";
        }
        return normalizedBackendType;
    }

    private static void setIfNotEmpty(String value, Consumer<String> setter) {
        if (value != null && !value.isEmpty()) {
            setter.accept(value);
        }
    }

    /**
     * Closes the catalog and then the producer-state Oxia client, collecting failures. A resource
     * that failed to close is retried by a later call.
     */
    @Override
    public synchronized void close() throws IOException {
        List<Exception> failures = new ArrayList<>();
        if (!catalogClosed) {
            try {
                if (catalog != null) {
                    catalog.close();
                }
                catalogClosed = true;
            } catch (Exception e) {
                failures.add(e);
            }
        }
        if (!producerStateOxiaClientClosed) {
            try {
                if (producerStateOxiaClient != null) {
                    producerStateOxiaClient.close();
                }
                producerStateOxiaClientClosed = true;
            } catch (Exception e) {
                failures.add(e);
            }
        }
        if (failures.isEmpty()) {
            return;
        }
        IOException failure = new IOException(failures.get(0));
        failures.stream().skip(1).forEach(failure::addSuppressed);
        throw failure;
    }
}
