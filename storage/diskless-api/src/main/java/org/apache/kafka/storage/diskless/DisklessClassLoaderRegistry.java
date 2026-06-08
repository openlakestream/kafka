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

import org.apache.kafka.common.utils.KafkaPluginClassLoader;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

final class DisklessClassLoaderRegistry {
    private static final Map<RuntimeKey, SharedRuntime> RUNTIMES = new HashMap<>();

    private DisklessClassLoaderRegistry() {
    }

    static Lease acquire(URL[] urls, ClassLoader parent) throws IOException {
        if (urls.length == 0) {
            return new Lease(parent, null);
        }

        RuntimeKey key = RuntimeKey.from(urls, parent);
        synchronized (RUNTIMES) {
            SharedRuntime runtime = RUNTIMES.get(key);
            if (runtime == null) {
                runtime = new SharedRuntime(key, new KafkaPluginClassLoader(urls, parent));
                RUNTIMES.put(key, runtime);
            }
            runtime.retain();
            return new Lease(runtime.classLoader, runtime);
        }
    }

    private static void release(SharedRuntime runtime) {
        KafkaPluginClassLoader classLoaderToClose = null;
        synchronized (RUNTIMES) {
            if (runtime.release()) {
                RUNTIMES.remove(runtime.key, runtime);
                classLoaderToClose = runtime.classLoader;
            }
        }
        if (classLoaderToClose != null) {
            try {
                classLoaderToClose.close();
            } catch (IOException ignored) {
                // Closing a URLClassLoader only releases local resources. Startup/shutdown should not fail here.
            }
        }
    }

    static void closeLeaseOnFailure(Lease lease, Throwable failure) {
        try {
            lease.close();
        } catch (Throwable closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }

    static final class Lease implements AutoCloseable {
        private final ClassLoader classLoader;
        private final SharedRuntime runtime;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        private Lease(ClassLoader classLoader, SharedRuntime runtime) {
            this.classLoader = classLoader;
            this.runtime = runtime;
        }

        ClassLoader classLoader() {
            return classLoader;
        }

        @Override
        public void close() {
            if (runtime != null && closed.compareAndSet(false, true)) {
                DisklessClassLoaderRegistry.release(runtime);
            }
        }
    }

    private static final class SharedRuntime {
        private final RuntimeKey key;
        private final KafkaPluginClassLoader classLoader;
        private int references;

        private SharedRuntime(RuntimeKey key, KafkaPluginClassLoader classLoader) {
            this.key = key;
            this.classLoader = classLoader;
        }

        private void retain() {
            references++;
        }

        private boolean release() {
            references--;
            return references == 0;
        }
    }

    private record RuntimeKey(ClassLoader parent, List<String> urls) {
        private static RuntimeKey from(URL[] urls, ClassLoader parent) {
            return new RuntimeKey(
                    parent,
                    Arrays.stream(urls)
                            .map(RuntimeKey::canonicalUrl)
                            .toList()
            );
        }

        private static String canonicalUrl(URL url) {
            if ("file".equals(url.getProtocol())) {
                try {
                    return new File(url.toURI()).getCanonicalFile().toURI().toURL().toExternalForm();
                } catch (Exception ignored) {
                    return url.toExternalForm();
                }
            }
            return url.toExternalForm();
        }
    }
}
