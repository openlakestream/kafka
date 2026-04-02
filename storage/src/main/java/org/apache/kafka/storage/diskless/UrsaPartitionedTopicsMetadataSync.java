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

import org.apache.kafka.storage.diskless.handlers.KafkaManagedLedgerNaming;
import org.apache.kafka.storage.diskless.idempotent.ProducerStateSnapshotKeys;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * Best-effort synchronizer that mirrors diskless topic lifecycle into Oxia keys used by ursa-storage.
 *
 * <p>This class is intentionally free of Kafka metadata module dependencies, to avoid build-time cycles.
 */
public final class UrsaPartitionedTopicsMetadataSync implements AutoCloseable {

    private static final String PARTITIONED_TOPIC_PREFIX = "/admin/partitioned-topics/";

    private final BiConsumer<String, Throwable> faultHandler;
    private final ObjectMapper objectMapper;
    private final OxiaStore store;
    private CompletableFuture<Void> lastOp = CompletableFuture.completedFuture(null);

    public UrsaPartitionedTopicsMetadataSync(
            BiConsumer<String, Throwable> faultHandler,
            OxiaStore store) {
        this(faultHandler, new ObjectMapper(), store);
    }

    public UrsaPartitionedTopicsMetadataSync(
            BiConsumer<String, Throwable> faultHandler,
            ObjectMapper objectMapper,
            OxiaStore store) {
        this.faultHandler = Objects.requireNonNull(faultHandler, "faultHandler must not be null");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
        this.store = Objects.requireNonNull(store, "store must not be null");
    }

    /**
     * Delete the partitioned topic metadata entry and any producer-state snapshot entries if partitions >= 0.
     *
     * <p>Managed ledger metadata and the underlying Ursa stream are deleted by the broker-side reconciliation path,
     * which still has enough context to perform a complete delete through the managed ledger factory.
     *
     * <p>Producer-state snapshot keys are only deleted when topicId is present.
     */
    public void deleteTopicMetadata(String topicName, String topicId, int partitions, String context) {
        String partitionedKey = partitionedTopicMetadataPath(topicName);
        enqueue("delete " + partitionedKey, context, () -> store.delete(partitionedKey));
        if (partitions >= 0) {
            for (int partition = 0; partition < partitions; partition++) {
                if (topicId != null && !topicId.isBlank()) {
                    String producerStateKey = ProducerStateSnapshotKeys.snapshotKey(topicId, partition);
                    enqueue("delete " + producerStateKey, context, () -> store.delete(producerStateKey));
                }
            }
        }
    }

    /**
     * Synchronously upsert partitioned topic metadata to Oxia.
     * Used by the pre-commit handler to ensure Oxia write succeeds before KRaft commit.
     */
    public void upsertPartitionedTopicMetadataSync(String topicName, int partitions,
            Map<String, String> properties, long timeoutMs) throws Exception {
        byte[] payload = serializePartitionedTopicMetadata(partitions, properties);
        String key = partitionedTopicMetadataPath(topicName);
        store.put(key, payload).get(timeoutMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() throws Exception {
        store.close();
    }

    private void enqueue(String opName, String context, Supplier<CompletableFuture<Void>> op) {
        synchronized (this) {
            lastOp = lastOp.handle((ignored, ignoredErr) -> null)
                    .thenCompose(ignored -> op.get()
                            .exceptionally(t -> {
                                faultHandler.accept("Failed: " + opName + " in " + context, t);
                                return null;
                            }));
        }
    }

    private byte[] serializePartitionedTopicMetadata(int partitions, Map<String, String> properties) throws Exception {
        Map<String, Object> root = new HashMap<>();
        root.put("partitions", partitions);
        root.put("properties", properties != null ? properties : Map.of());
        return objectMapper.writeValueAsBytes(root);
    }

    static String partitionedTopicMetadataPath(String topicName) {
        return PARTITIONED_TOPIC_PREFIX
                + KafkaManagedLedgerNaming.TENANT + "/"
                + KafkaManagedLedgerNaming.NAMESPACE + "/"
                + KafkaManagedLedgerNaming.DOMAIN + "/"
                + topicName;
    }
}
