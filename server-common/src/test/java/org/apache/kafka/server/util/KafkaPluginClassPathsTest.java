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
package org.apache.kafka.server.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaPluginClassPathsTest {

    @TempDir
    Path tempDir;

    @Test
    void testConfiguredClassPathTakesPrecedence() {
        assertEquals(
                "/tmp/custom-plugin",
                KafkaPluginClassPaths.configuredOrDefault(
                        "/tmp/custom-plugin",
                        "ursa-storage",
                        KafkaPluginClassPathsTest.class));
    }

    @Test
    void testBlankClassPathUsesKafkaHomePluginDirectory() throws Exception {
        Path kafkaHome = Files.createDirectory(tempDir.resolve("kafka-home"));
        String originalKafkaHome = System.getProperty("kafka.home");
        System.setProperty("kafka.home", kafkaHome.toString());
        try {
            assertSamePath(
                    new File(kafkaHome.toFile(), "ursa-storage" + File.separator + "*").getPath(),
                    KafkaPluginClassPaths.configuredOrDefault(
                            "",
                            "ursa-storage",
                            KafkaPluginClassPathsTest.class));
        } finally {
            restoreKafkaHome(originalKafkaHome);
        }
    }

    private void assertSamePath(String expected, String actual) throws Exception {
        assertEquals(
                new File(expected).getCanonicalFile(),
                new File(actual).getCanonicalFile());
    }

    private void restoreKafkaHome(String originalKafkaHome) {
        if (originalKafkaHome == null) {
            System.clearProperty("kafka.home");
        } else {
            System.setProperty("kafka.home", originalKafkaHome);
        }
    }
}
