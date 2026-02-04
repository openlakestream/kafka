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

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.OxiaClientBuilder;

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

    private final Object lock = new Object();
    private CompletableFuture<Void> lastOp = CompletableFuture.completedFuture(null);

    public UrsaPartitionedTopicsMetadataSync(
            String oxiaServiceUrl,
            String namespace,
            BiConsumer<String, Throwable> faultHandler) {
        this(
                faultHandler,
                new ObjectMapper(),
                () -> new DefaultOxiaStore(oxiaServiceUrl, namespace)
        );
    }

    UrsaPartitionedTopicsMetadataSync(
            BiConsumer<String, Throwable> faultHandler,
            ObjectMapper objectMapper,
            Supplier<OxiaStore> storeSupplier) {
        this.faultHandler = Objects.requireNonNull(faultHandler, "faultHandler must not be null");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
        Objects.requireNonNull(storeSupplier, "storeSupplier must not be null");

        OxiaStore createdStore;
        try {
            createdStore = storeSupplier.get();
        } catch (Throwable t) {
            createdStore = null;
            faultHandler.accept("Error initializing Oxia client for UrsaPartitionedTopicsMetadataSync", t);
        }
        this.store = createdStore;
    }

    public void upsertPartitionedTopicMetadata(String topicName, int partitions, Map<String, String> properties, String context) {
        if (store == null) {
            return;
        }
        byte[] payload;
        try {
            payload = serializePartitionedTopicMetadata(partitions, properties);
        } catch (Exception e) {
            faultHandler.accept("Error serializing partitioned topic metadata for " + topicName + " in " + context, e);
            return;
        }
        String key = partitionedTopicMetadataPath(topicName);
        enqueue("put " + key, context, () -> store.put(key, payload));
    }

    /**
     * Delete /admin/partitioned-topics entry, and optionally /managed-ledgers entries if partitions >= 0.
     */
    public void deleteTopicMetadata(String topicName, int partitions, String context) {
        if (store == null) {
            return;
        }

        String partitionedKey = partitionedTopicMetadataPath(topicName);
        enqueue("delete " + partitionedKey, context, () -> store.delete(partitionedKey));

        if (partitions >= 0) {
            for (int partition = 0; partition < partitions; partition++) {
                String mlKey = managedLedgerMetadataPath(topicName, partition);
                enqueue("delete " + mlKey, context, () -> store.delete(mlKey));
            }
        }
    }

    @Override
    public void close() throws Exception {
        if (store != null) {
            store.close();
        }
    }

    private void enqueue(String opName, String context, Supplier<CompletableFuture<Void>> op) {
        synchronized (lock) {
            lastOp = lastOp.handle((ignored, ignoredErr) -> null)
                    .thenCompose(ignored -> op.get())
                    .exceptionally(t -> {
                        faultHandler.accept("Error executing " + opName + " in " + context, t);
                        return null;
                    });
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

    static String managedLedgerMetadataPath(String topicName, int partition) {
        return KafkaManagedLedgerNaming.MANAGED_LEDGER_PREFIX
                + KafkaManagedLedgerNaming.TENANT + "/"
                + KafkaManagedLedgerNaming.NAMESPACE + "/"
                + KafkaManagedLedgerNaming.DOMAIN + "/"
                + topicName + "-partition-" + partition;
    }

    interface OxiaStore extends AutoCloseable {
        CompletableFuture<Void> put(String key, byte[] value);

        CompletableFuture<Void> delete(String key);

        @Override
        void close() throws Exception;
    }

    static final class DefaultOxiaStore implements OxiaStore {
        private final AsyncOxiaClient client;

        DefaultOxiaStore(String oxiaServiceUrl, String namespace) {
            this.client = OxiaClientBuilder.create(oxiaServiceUrl)
                    .namespace(namespace)
                    .asyncClient()
                    .join();
        }

        @Override
        public CompletableFuture<Void> put(String key, byte[] value) {
            return client.put(key, value).thenApply(ignored -> null);
        }

        @Override
        public CompletableFuture<Void> delete(String key) {
            return client.delete(key).thenApply(ignored -> null);
        }

        @Override
        public void close() throws Exception {
            client.close();
        }
    }
}
