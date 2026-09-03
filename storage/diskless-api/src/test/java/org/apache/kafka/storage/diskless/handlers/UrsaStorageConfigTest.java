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

import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.server.config.ServerLogConfigs;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Covers {@link UrsaStorageConfig}, including the consolidated Oxia URL configuration surface:
 * {@code ursa.catalog.oxia.service.url} and {@code ursa.oxia.service.url} are the only two knobs
 * left after {@code ursa.storage.oxia.service.url} and {@code ursa.storage.namespace} were removed.
 */
class UrsaStorageConfigTest {

    @Test
    void testCatalogOxiaServiceUrlDefault() throws Exception {
        UrsaStorageConfig config = UrsaStorageConfig.fromConfigs(Map.of());

        assertEquals(ServerLogConfigs.URSA_CATALOG_OXIA_SERVICE_URL_DEFAULT, config.getCatalogOxiaServiceUrl());
    }

    @Test
    void testUrsaOxiaServiceUrlDefault() throws Exception {
        UrsaStorageConfig config = UrsaStorageConfig.fromConfigs(Map.of());

        assertEquals(ServerLogConfigs.URSA_OXIA_SERVICE_URL_DEFAULT, config.getUrsaOxiaServiceUrl());
    }

    @Test
    void testCatalogOxiaServiceUrlOverride() throws Exception {
        UrsaStorageConfig config = UrsaStorageConfig.fromConfigs(Map.of(
                ServerLogConfigs.URSA_CATALOG_OXIA_SERVICE_URL_CONFIG, "oxia://catalog:6648/kafka"
        ));

        assertEquals("oxia://catalog:6648/kafka", config.getCatalogOxiaServiceUrl());
    }

    @Test
    void testUrsaOxiaServiceUrlOverride() throws Exception {
        UrsaStorageConfig config = UrsaStorageConfig.fromConfigs(Map.of(
                ServerLogConfigs.URSA_OXIA_SERVICE_URL_CONFIG, "oxia://storage:6648/kafka"
        ));

        assertEquals("oxia://storage:6648/kafka", config.getUrsaOxiaServiceUrl());
    }

    @Test
    void testRemovedOxiaServiceUrlAndNamespaceKeysAreIgnored() throws Exception {
        // ursa.storage.oxia.service.url and ursa.storage.namespace were removed in favor of
        // ursa.catalog.oxia.service.url / ursa.oxia.service.url. Confirm stray legacy keys no
        // longer influence the resolved defaults (referenced here as raw strings since the
        // config constants themselves no longer exist).
        UrsaStorageConfig config = UrsaStorageConfig.fromConfigs(Map.of(
                "ursa.storage.oxia.service.url", "legacy-host:9999",
                "ursa.storage.namespace", "legacy-namespace"
        ));

        assertEquals(ServerLogConfigs.URSA_CATALOG_OXIA_SERVICE_URL_DEFAULT, config.getCatalogOxiaServiceUrl());
        assertEquals(ServerLogConfigs.URSA_OXIA_SERVICE_URL_DEFAULT, config.getUrsaOxiaServiceUrl());
    }

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

    @Test
    void testOptionalS3OptionsDefaultToNull() throws Exception {
        UrsaStorageConfig config = UrsaStorageConfig.fromConfigs(Map.of());

        assertNull(config.getS3SessionToken());
        assertNull(config.getS3PathStyleAccess());
    }

    @Test
    void testS3ReaderOptionsPreserveExplicitValues() throws Exception {
        UrsaStorageConfig config = UrsaStorageConfig.fromConfigs(Map.of(
                ServerLogConfigs.URSA_STORAGE_S3_SESSION_TOKEN_CONFIG, new Password("session-token"),
                ServerLogConfigs.URSA_STORAGE_S3_PATH_STYLE_ACCESS_CONFIG, "true"
        ));

        assertEquals("session-token", config.getS3SessionToken());
        assertEquals(Boolean.TRUE, config.getS3PathStyleAccess());
    }

    private static String backendType(String backendType) throws Exception {
        UrsaStorageConfig config = UrsaStorageConfig.fromConfigs(Map.of(
                ServerLogConfigs.URSA_STORAGE_BACKEND_TYPE_CONFIG, backendType
        ));
        return config.getBackendType();
    }
}
