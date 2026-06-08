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
package org.apache.kafka.common.utils;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaPluginClassLoaderUtilsTest {

    @ParameterizedTest
    @ValueSource(strings = {
        "java.lang.String",
        "javax.net.ssl.SSLContext",
        "jdk.internal.misc.Unsafe",
        "sun.misc.Signal",
        "kafka.server.KafkaConfig",
        "org.apache.kafka.common.KafkaException",
        "com.yammer.metrics.core.MetricsRegistry",
        "org.slf4j.Logger",
        "org.apache.logging.log4j.Logger",
        "org.apache.log4j.Logger",
        "scala.Option",
        "com.typesafe.scalalogging.Logger"
    })
    void testPlatformClassesAreNotLoadedInIsolation(String className) {
        assertFalse(KafkaPluginClassLoaderUtils.shouldLoadInIsolation(className));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "io.oxia.client.api.OxiaClientBuilder",
        "software.amazon.awssdk.services.s3.S3Client",
        "org.apache.bookkeeper.mledger.ManagedLedgerFactory",
        "io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdkBuilder"
    })
    void testPluginDependencyClassesAreLoadedInIsolation(String className) {
        assertTrue(KafkaPluginClassLoaderUtils.shouldLoadInIsolation(className));
    }
}
