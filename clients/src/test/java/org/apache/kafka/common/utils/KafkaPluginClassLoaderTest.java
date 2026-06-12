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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KafkaPluginClassLoaderTest {

    private static final String ISOLATED_SERVICE_RESOURCE =
            "META-INF/services/io.opentelemetry.sdk.autoconfigure.spi.ResourceProvider";
    private static final String PLATFORM_SERVICE_RESOURCE =
            "META-INF/services/org.apache.kafka.common.config.provider.ConfigProvider";

    @TempDir
    Path tempDir;

    @Test
    void testIsolatedServiceResourcesDoNotIncludeParentResources() throws Exception {
        Path childDir = Files.createDirectory(tempDir.resolve("child"));
        Path parentDir = Files.createDirectory(tempDir.resolve("parent"));
        writeResource(childDir, ISOLATED_SERVICE_RESOURCE, "child-provider");
        writeResource(parentDir, ISOLATED_SERVICE_RESOURCE, "parent-provider");

        try (URLClassLoader parent = parentClassLoader(parentDir);
             KafkaPluginClassLoader loader = new KafkaPluginClassLoader(childDir.toString(), parent)) {
            assertEquals(List.of("child-provider"), readResources(loader, ISOLATED_SERVICE_RESOURCE));
        }
    }

    @Test
    void testPlatformServiceResourcesIncludeParentResources() throws Exception {
        Path childDir = Files.createDirectory(tempDir.resolve("child"));
        Path parentDir = Files.createDirectory(tempDir.resolve("parent"));
        writeResource(childDir, PLATFORM_SERVICE_RESOURCE, "child-provider");
        writeResource(parentDir, PLATFORM_SERVICE_RESOURCE, "parent-provider");

        try (URLClassLoader parent = parentClassLoader(parentDir);
             KafkaPluginClassLoader loader = new KafkaPluginClassLoader(childDir.toString(), parent)) {
            assertEquals(List.of("child-provider", "parent-provider"), readResources(loader, PLATFORM_SERVICE_RESOURCE));
        }
    }

    private static URLClassLoader parentClassLoader(Path directory) throws Exception {
        return new URLClassLoader(new URL[] {directory.toUri().toURL()}, null);
    }

    private static void writeResource(Path root, String resourceName, String contents) throws IOException {
        Path resource = root.resolve(resourceName);
        Files.createDirectories(resource.getParent());
        Files.writeString(resource, contents, StandardCharsets.UTF_8);
    }

    private static List<String> readResources(ClassLoader classLoader, String resourceName) throws IOException {
        Enumeration<URL> resources = classLoader.getResources(resourceName);
        List<String> contents = new ArrayList<>();
        while (resources.hasMoreElements()) {
            URL resource = resources.nextElement();
            try (var input = resource.openStream()) {
                contents.add(new String(input.readAllBytes(), StandardCharsets.UTF_8));
            }
        }
        return contents;
    }
}
