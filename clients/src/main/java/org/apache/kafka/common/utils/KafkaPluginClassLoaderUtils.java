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

import java.util.regex.Pattern;

/**
 * Utility methods for Kafka plugin class loader isolation.
 */
public final class KafkaPluginClassLoaderUtils {

    private static final Pattern EXCLUDE = Pattern.compile("^(?:"
            + "java"
            + "|javax"
            + "|jdk"
            + "|sun"
            + "|kafka"
            + "|org\\.apache\\.kafka"
            + "|com\\.yammer\\.metrics"
            + "|org\\.slf4j"
            + "|org\\.apache\\.logging\\.log4j"
            + "|org\\.apache\\.log4j"
            + "|scala"
            + "|com\\.typesafe\\.scalalogging"
            + ")\\..*$");

    private KafkaPluginClassLoaderUtils() {
    }

    /**
     * Return whether the class with the given name should be loaded in isolation using a plugin
     * classloader.
     *
     * @param name the fully qualified name of the class.
     * @return true if this class should be loaded in isolation, false otherwise.
     */
    public static boolean shouldLoadInIsolation(String name) {
        return !EXCLUDE.matcher(name).matches();
    }
}
