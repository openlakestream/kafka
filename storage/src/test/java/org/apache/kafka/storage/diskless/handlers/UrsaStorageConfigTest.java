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

import org.apache.kafka.server.config.ServerLogConfigs;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class UrsaStorageConfigTest {

    @Test
    void testBackendTypeOverrides() throws Exception {
        assertEquals("GCS", backendType("GCS"));
        assertEquals("AZURE_BLOB", backendType("AZURE_BLOB"));
        assertEquals("AZUREBLOB", backendType("AZUREBLOB"));
    }

    @Test
    void testSnapshotConfigDefaults() throws Exception {
        UrsaStorageConfig config = UrsaStorageConfig.fromConfigs(Map.of());

        assertEquals(ServerLogConfigs.URSA_STORAGE_PRODUCER_STATE_SNAPSHOT_INTERVAL_MS_DEFAULT,
            config.getProducerStateSnapshotIntervalMs());
        assertEquals(ServerLogConfigs.URSA_STORAGE_PRODUCER_STATE_SNAPSHOT_RECORD_THRESHOLD_DEFAULT,
            config.getProducerStateSnapshotRecordThreshold());
    }

    @Test
    void testSnapshotConfigOverrides() throws Exception {
        UrsaStorageConfig config = UrsaStorageConfig.fromConfigs(Map.of(
            ServerLogConfigs.URSA_STORAGE_PRODUCER_STATE_SNAPSHOT_INTERVAL_MS_CONFIG, "1234",
            ServerLogConfigs.URSA_STORAGE_PRODUCER_STATE_SNAPSHOT_RECORD_THRESHOLD_CONFIG, "5678"
        ));

        assertEquals(1234L, config.getProducerStateSnapshotIntervalMs());
        assertEquals(5678, config.getProducerStateSnapshotRecordThreshold());
    }

    private static String backendType(String backendType) throws Exception {
        UrsaStorageConfig config = UrsaStorageConfig.fromConfigs(Map.of(
            ServerLogConfigs.URSA_STORAGE_BACKEND_TYPE_CONFIG, backendType
        ));
        return config.getBackendType();
    }
}
