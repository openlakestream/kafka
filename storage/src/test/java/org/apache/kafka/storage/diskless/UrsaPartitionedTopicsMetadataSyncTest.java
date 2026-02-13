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

import org.apache.kafka.storage.diskless.idempotent.ProducerStateSnapshot;
import org.apache.kafka.test.TestUtils;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UrsaPartitionedTopicsMetadataSyncTest {

    private static final class RecordingStore implements UrsaPartitionedTopicsMetadataSync.OxiaStore {
        private final List<String> putKeys = new CopyOnWriteArrayList<>();
        private final List<byte[]> putValues = new CopyOnWriteArrayList<>();
        private final List<String> deleteKeys = new CopyOnWriteArrayList<>();

        @Override
        public CompletableFuture<Void> put(String key, byte[] value) {
            putKeys.add(key);
            putValues.add(value);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<Void> delete(String key) {
            deleteKeys.add(key);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void close() {
        }
    }

    @Test
    void deleteTopicMetadataDeletesPartitionedManagedLedgersAndProducerSnapshots() throws Exception {
        RecordingStore store = new RecordingStore();
        List<String> faults = new CopyOnWriteArrayList<>();
        UrsaPartitionedTopicsMetadataSync sync = new UrsaPartitionedTopicsMetadataSync(
                (message, cause) -> faults.add(message),
                new ObjectMapper(),
                store
        );

        String topicName = "test-topic";
        String topicId = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
        int partitions = 3;
        sync.deleteTopicMetadata(topicName, topicId, partitions, "test");

        Set<String> expectedKeys = new HashSet<>();
        expectedKeys.add("/admin/partitioned-topics/public/default/persistent/" + topicName);
        for (int partition = 0; partition < partitions; partition++) {
            expectedKeys.add("/managed-ledgers/public/default/persistent/" + topicName + "-partition-" + partition);
            expectedKeys.add(ProducerStateSnapshot.generateSnapshotKey(topicId, partition));
        }

        TestUtils.waitForCondition(() -> store.deleteKeys.size() == expectedKeys.size()
                        && expectedKeys.equals(new HashSet<>(store.deleteKeys)),
                5_000,
                "Timed out waiting for delete operations to be enqueued. expected=" + expectedKeys + ", actual=" + store.deleteKeys);
        assertTrue(faults.isEmpty(), "Unexpected faults: " + faults);
    }

    @Test
    void deleteTopicMetadataWithoutTopicIdDoesNotDeleteProducerSnapshots() throws Exception {
        RecordingStore store = new RecordingStore();
        UrsaPartitionedTopicsMetadataSync sync = new UrsaPartitionedTopicsMetadataSync(
                (message, cause) -> {
                },
                new ObjectMapper(),
                store
        );

        String topicName = "test-topic";
        int partitions = 2;
        sync.deleteTopicMetadata(topicName, null, partitions, "test");

        TestUtils.waitForCondition(() -> store.deleteKeys.size() == 1 + partitions,
                5_000,
                "Timed out waiting for delete operations. actual=" + store.deleteKeys);
        assertFalse(store.deleteKeys.stream().anyMatch(k -> k.startsWith("producer-state-snapshot/")),
                "Producer snapshot keys should not be deleted when topicId is missing. deleteKeys=" + store.deleteKeys);
    }

    @Test
    void upsertPartitionedTopicMetadataSyncWritesToStore() throws Exception {
        RecordingStore store = new RecordingStore();
        ObjectMapper objectMapper = new ObjectMapper();
        UrsaPartitionedTopicsMetadataSync sync = new UrsaPartitionedTopicsMetadataSync(
                (message, cause) -> { },
                objectMapper,
                store
        );

        sync.upsertPartitionedTopicMetadataSync("sync-topic", 3, Map.of("key", "val"), 5000);

        String expectedKey = "/admin/partitioned-topics/public/default/persistent/sync-topic";
        assertEquals(1, store.putKeys.size());
        assertEquals(expectedKey, store.putKeys.get(0));

        Map<?, ?> payload = objectMapper.readValue(store.putValues.get(0), Map.class);
        assertEquals(Map.of("partitions", 3, "properties", Map.of("key", "val")), payload);
        sync.close();
    }

    @Test
    void constructorThrowsWhenStoreIsNull() {
        assertThrows(NullPointerException.class,
                () -> new UrsaPartitionedTopicsMetadataSync(
                        (message, cause) -> { },
                        new ObjectMapper(),
                        null
                ));
    }

}
