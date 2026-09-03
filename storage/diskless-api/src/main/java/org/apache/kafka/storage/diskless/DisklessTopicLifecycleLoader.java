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

import org.apache.kafka.storage.diskless.handlers.UrsaStorageConfig;

import java.util.Objects;

/** Loads the diskless topic lifecycle implementation from the isolated storage runtime. */
public final class DisklessTopicLifecycleLoader {

    private DisklessTopicLifecycleLoader() {
    }

    public static DisklessTopicLifecycle load(UrsaStorageConfig config) {
        return DisklessStorageProviderLoader.load(
                config,
                "topic lifecycle",
                provider -> provider.createTopicLifecycle(config),
                DisklessTopicLifecycle.class);
    }

    /**
     * Returns a non-blocking facade that loads the isolated provider on its first operation.
     * Initialization failures are surfaced through that operation's future and retried by a later
     * operation, allowing the active controller reconciler to supervise provider availability.
     */
    public static DisklessTopicLifecycle loadLazily(UrsaStorageConfig config) {
        Objects.requireNonNull(config, "config must not be null");
        return new LazyDisklessTopicLifecycle(() -> load(config));
    }
}
