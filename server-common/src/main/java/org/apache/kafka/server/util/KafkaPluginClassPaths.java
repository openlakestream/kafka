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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public final class KafkaPluginClassPaths {

    private KafkaPluginClassPaths() {
    }

    public static String configuredOrDefault(
            String configuredClassPath,
            String defaultPluginDirectory,
            Class<?> anchorClass) {
        return configuredClassPath == null || configuredClassPath.isBlank()
                ? defaultClassPath(defaultPluginDirectory, anchorClass)
                : configuredClassPath;
    }

    public static URL[] toUrls(String classPath) throws IOException {
        List<URL> urls = new ArrayList<>();
        for (String entry : classPath.split(File.pathSeparator)) {
            if (entry == null || entry.isBlank()) {
                continue;
            }
            addUrls(urls, entry.trim());
        }
        return urls.toArray(new URL[0]);
    }

    private static String defaultClassPath(String defaultPluginDirectory, Class<?> anchorClass) {
        String kafkaHome = System.getProperty("kafka.home");
        if (kafkaHome == null || kafkaHome.isBlank()) {
            kafkaHome = System.getenv("KAFKA_HOME");
        }
        if (kafkaHome == null || kafkaHome.isBlank()) {
            kafkaHome = kafkaHomeFromCodeSource(anchorClass);
        }
        if (kafkaHome == null || kafkaHome.isBlank()) {
            kafkaHome = ".";
        }
        return new File(kafkaHome, defaultPluginDirectory + File.separator + "*").getPath();
    }

    private static String kafkaHomeFromCodeSource(Class<?> anchorClass) {
        try {
            URL location = anchorClass
                    .getProtectionDomain()
                    .getCodeSource()
                    .getLocation();
            File file = new File(location.toURI());
            File directory = file.isDirectory() ? file : file.getParentFile();
            if (directory != null && "libs".equals(directory.getName())) {
                File kafkaHome = directory.getParentFile();
                if (kafkaHome != null) {
                    return kafkaHome.getAbsolutePath();
                }
            }
        } catch (Exception ignored) {
            // Fall back to the current working directory below.
        }
        return null;
    }

    private static void addUrls(List<URL> urls, String entry) throws IOException {
        if (entry.endsWith("*")) {
            File dir = new File(entry.substring(0, entry.length() - 1));
            File[] jars = dir.listFiles(file -> file.isFile() && file.getName().endsWith(".jar"));
            if (jars != null) {
                Arrays.sort(jars, Comparator.comparing(File::getName));
                for (File jar : jars) {
                    urls.add(jar.toURI().toURL());
                }
            }
        } else {
            urls.add(new File(entry).toURI().toURL());
        }
    }
}
