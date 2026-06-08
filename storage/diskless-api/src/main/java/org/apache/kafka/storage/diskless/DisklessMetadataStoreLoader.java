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
import org.apache.kafka.server.util.KafkaPluginClassPaths;

import java.lang.reflect.Constructor;
import java.net.URL;

public final class DisklessMetadataStoreLoader {

    private static final String DEFAULT_URSA_STORAGE_DIR = "ursa-storage";
    private static final String OXIA_STORE_CLASS =
            "org.apache.kafka.storage.diskless.OxiaDisklessMetadataStore";

    private DisklessMetadataStoreLoader() {
    }

    public static DisklessMetadataStore load(String url, String classPath) {
        return load(url, classPath, OXIA_STORE_CLASS);
    }

    static DisklessMetadataStore load(String url, String classPath, String storeClassName) {
        try {
            ClassLoader parent = DisklessMetadataStoreLoader.class.getClassLoader();
            URL[] urls = classPathUrls(classPath);
            DisklessClassLoaderRegistry.Lease classLoaderLease = DisklessClassLoaderRegistry.acquire(urls, parent);
            try {
                DisklessMetadataStore store = DisklessClassLoaderContext.call(classLoaderLease.classLoader(), () -> {
                    Class<?> storeClass = Class.forName(storeClassName, true, classLoaderLease.classLoader());
                    Constructor<?> constructor = storeClass.getConstructor(String.class);
                    return (DisklessMetadataStore) constructor.newInstance(url);
                });
                return new LeasedDisklessMetadataStore(store, classLoaderLease);
            } catch (Throwable t) {
                DisklessClassLoaderRegistry.closeLeaseOnFailure(classLoaderLease, t);
                throw rethrow(t);
            }
        } catch (Exception e) {
            throw new KafkaException("Failed to load Oxia store", e);
        }
    }

    static URL[] classPathUrls(String configuredClassPath) throws Exception {
        String classPath = KafkaPluginClassPaths.configuredOrDefault(
                configuredClassPath,
                DEFAULT_URSA_STORAGE_DIR,
                DisklessMetadataStoreLoader.class);
        return KafkaPluginClassPaths.toUrls(classPath);
    }

    private static RuntimeException rethrow(Throwable t) throws Exception {
        if (t instanceof Exception exception) {
            throw exception;
        }
        if (t instanceof Error error) {
            throw error;
        }
        return new KafkaException("Failed to load Oxia store", t);
    }
}
