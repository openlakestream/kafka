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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class KafkaManagedLedgerFactoryHolderTest {

    private static final String SYSTEM_EXTERNAL_READER_FACTORY_CLASS_PROP = "ursa.externalReaderFactoryClass";
    private static final String EXTERNAL_READER_FACTORY_CLASS_PROP = "externalReaderFactoryClass";
    private static final String NOOP_EXTERNAL_READER_FACTORY_CLASS =
            "io.streamnative.ursa.mledger.reader.NoopExternalReaderFactory";
    private static final String SYSTEM_KOP_SCHEMA_REGISTRY_URL_PROP = "kopSchemaRegistryUrl";
    private static final String SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_PROP =
            "kopSchemaRegistryHttpHeaderAuthorization";
    private static final String SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_FILE_PROP =
            "kopSchemaRegistryHttpHeaderAuthorizationFile";

    @BeforeEach
    void setUp() {
        clearSystemProperties();
    }

    @AfterEach
    void tearDown() {
        clearSystemProperties();
    }

    private static void clearSystemProperties() {
        System.clearProperty(SYSTEM_EXTERNAL_READER_FACTORY_CLASS_PROP);
        System.clearProperty(SYSTEM_KOP_SCHEMA_REGISTRY_URL_PROP);
        System.clearProperty(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_PROP);
        System.clearProperty(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_FILE_PROP);
    }

    @Test
    void testDoesNotInjectSchemaRegistryConfigWhenExternalReaderFactoryIsNoop() {
        UrsaStorageConfig config = UrsaStorageConfig.builder().build();

        System.setProperty(SYSTEM_KOP_SCHEMA_REGISTRY_URL_PROP, "http://example:8001");
        System.setProperty(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_PROP, "Bearer token");
        System.setProperty(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_FILE_PROP, "/mnt/secrets/token");

        Properties properties = KafkaManagedLedgerFactoryHolder.buildManagedLedgerProperties(
                config, KafkaManagedLedgerFactoryHolder.formatOxiaUrl(config));

        assertEquals(NOOP_EXTERNAL_READER_FACTORY_CLASS, properties.getProperty(EXTERNAL_READER_FACTORY_CLASS_PROP));
        assertFalse(properties.containsKey(SYSTEM_KOP_SCHEMA_REGISTRY_URL_PROP));
        assertFalse(properties.containsKey(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_PROP));
        assertFalse(properties.containsKey(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_FILE_PROP));
    }

    @Test
    void testInjectsSchemaRegistryAuthorizationWhenExternalReaderEnabled() {
        UrsaStorageConfig config = UrsaStorageConfig.builder().build();

        System.setProperty(SYSTEM_EXTERNAL_READER_FACTORY_CLASS_PROP,
                "io.streamnative.ursa.lakehouse.reader.LakehouseReaderFactory");
        System.setProperty(SYSTEM_KOP_SCHEMA_REGISTRY_URL_PROP, "http://example:8001");
        System.setProperty(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_PROP, "Bearer token");

        Properties properties = KafkaManagedLedgerFactoryHolder.buildManagedLedgerProperties(
                config, KafkaManagedLedgerFactoryHolder.formatOxiaUrl(config));

        assertEquals(
                "io.streamnative.ursa.lakehouse.reader.LakehouseReaderFactory",
                properties.getProperty(EXTERNAL_READER_FACTORY_CLASS_PROP));
        assertEquals("http://example:8001", properties.getProperty(SYSTEM_KOP_SCHEMA_REGISTRY_URL_PROP));
        assertEquals("Bearer token", properties.getProperty(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_PROP));
        assertFalse(properties.containsKey(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_FILE_PROP));
    }

    @Test
    void testAuthorizationFileTakesPrecedenceOverAuthorizationValue() {
        UrsaStorageConfig config = UrsaStorageConfig.builder().build();

        System.setProperty(SYSTEM_EXTERNAL_READER_FACTORY_CLASS_PROP,
                "io.streamnative.ursa.lakehouse.reader.LakehouseReaderFactory");
        System.setProperty(SYSTEM_KOP_SCHEMA_REGISTRY_URL_PROP, "http://example:8001");
        System.setProperty(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_PROP, "Bearer token");
        System.setProperty(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_FILE_PROP, "/mnt/secrets/token");

        Properties properties = KafkaManagedLedgerFactoryHolder.buildManagedLedgerProperties(
                config, KafkaManagedLedgerFactoryHolder.formatOxiaUrl(config));

        assertEquals("http://example:8001", properties.getProperty(SYSTEM_KOP_SCHEMA_REGISTRY_URL_PROP));
        assertEquals(
                "/mnt/secrets/token",
                properties.getProperty(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_FILE_PROP));
        assertFalse(properties.containsKey(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_PROP));
    }

    @Test
    void testBlankAuthorizationIsIgnored() {
        UrsaStorageConfig config = UrsaStorageConfig.builder().build();

        System.setProperty(SYSTEM_EXTERNAL_READER_FACTORY_CLASS_PROP,
                "io.streamnative.ursa.lakehouse.reader.LakehouseReaderFactory");
        System.setProperty(SYSTEM_KOP_SCHEMA_REGISTRY_URL_PROP, "http://example:8001");
        System.setProperty(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_PROP, "   ");

        Properties properties = KafkaManagedLedgerFactoryHolder.buildManagedLedgerProperties(
                config, KafkaManagedLedgerFactoryHolder.formatOxiaUrl(config));

        assertEquals("http://example:8001", properties.getProperty(SYSTEM_KOP_SCHEMA_REGISTRY_URL_PROP));
        assertFalse(properties.containsKey(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_PROP));
        assertFalse(properties.containsKey(SYSTEM_KOP_SCHEMA_REGISTRY_HTTP_HEADER_AUTHORIZATION_FILE_PROP));
    }
}
