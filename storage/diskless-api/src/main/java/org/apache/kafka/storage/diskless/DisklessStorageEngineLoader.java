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

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.util.KafkaPluginClassPaths;
import org.apache.kafka.storage.diskless.handlers.UrsaStorageConfig;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import java.lang.reflect.Constructor;
import java.net.URL;
import java.util.Map;
import java.util.function.Function;

public final class DisklessStorageEngineLoader {

    private static final String DEFAULT_URSA_STORAGE_DIR = "ursa-storage";
    private static final String URSA_ENGINE_CLASS =
            "org.apache.kafka.storage.diskless.handlers.UrsaStorageEngineImpl";

    private DisklessStorageEngineLoader() {
    }

    public static DisklessStorageEngine load(
            Time time,
            int brokerId,
            UrsaStorageConfig ursaConfig,
            BrokerTopicStats brokerTopicStats,
            Map<String, Object> logConfigDefaults,
            Function<String, Map<String, String>> topicConfigSupplier) {
        return load(
                time,
                brokerId,
                ursaConfig,
                brokerTopicStats,
                logConfigDefaults,
                topicConfigSupplier,
                URSA_ENGINE_CLASS);
    }

    static DisklessStorageEngine load(
            Time time,
            int brokerId,
            UrsaStorageConfig ursaConfig,
            BrokerTopicStats brokerTopicStats,
            Map<String, Object> logConfigDefaults,
            Function<String, Map<String, String>> topicConfigSupplier,
            String engineClassName) {
        try {
            ClassLoader parent = DisklessStorageEngineLoader.class.getClassLoader();
            URL[] urls = classPathUrls(ursaConfig.getClassPath());
            DisklessClassLoaderRegistry.Lease classLoaderLease = DisklessClassLoaderRegistry.acquire(urls, parent);
            try {
                Object engine = DisklessClassLoaderContext.call(classLoaderLease.classLoader(), () -> {
                    Class<?> engineClass = Class.forName(engineClassName, true, classLoaderLease.classLoader());
                    Constructor<?> constructor = engineClass.getConstructor(
                            Time.class,
                            int.class,
                            UrsaStorageConfig.class,
                            BrokerTopicStats.class,
                            Map.class,
                            Function.class
                    );
                    return constructor.newInstance(
                            time,
                            brokerId,
                            ursaConfig,
                            brokerTopicStats,
                            logConfigDefaults,
                            topicConfigSupplier
                    );
                });
                return new LeasedDisklessStorageEngine((DisklessStorageEngine) engine, classLoaderLease);
            } catch (Throwable t) {
                DisklessClassLoaderRegistry.closeLeaseOnFailure(classLoaderLease, t);
                throw rethrow(t);
            }
        } catch (Exception e) {
            throw new KafkaException("Failed to load Ursa diskless storage engine", e);
        }
    }

    static URL[] classPathUrls(String configuredClassPath) throws Exception {
        String classPath = KafkaPluginClassPaths.configuredOrDefault(
                configuredClassPath,
                DEFAULT_URSA_STORAGE_DIR,
                DisklessStorageEngineLoader.class);
        return KafkaPluginClassPaths.toUrls(classPath);
    }

    private static RuntimeException rethrow(Throwable t) throws Exception {
        if (t instanceof Exception exception) {
            throw exception;
        }
        if (t instanceof Error error) {
            throw error;
        }
        return new KafkaException("Failed to load Ursa diskless storage engine", t);
    }
}
