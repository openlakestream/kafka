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

import java.util.concurrent.CompletableFuture;

final class LeasedDisklessMetadataStore implements DisklessMetadataStore {
    private final DisklessMetadataStore delegate;
    private final DisklessClassLoaderRegistry.Lease classLoaderLease;

    LeasedDisklessMetadataStore(
            DisklessMetadataStore delegate,
            DisklessClassLoaderRegistry.Lease classLoaderLease) {
        this.delegate = delegate;
        this.classLoaderLease = classLoaderLease;
    }

    @Override
    public CompletableFuture<Void> put(String key, byte[] value) {
        return callWithClassLoader(() -> delegate.put(key, value));
    }

    @Override
    public CompletableFuture<Void> delete(String key) {
        return callWithClassLoader(() -> delegate.delete(key));
    }

    @Override
    public void close() throws Exception {
        try {
            DisklessClassLoaderContext.call(classLoaderLease.classLoader(), () -> {
                delegate.close();
                return null;
            });
        } finally {
            classLoaderLease.close();
        }
    }

    private <T> T callWithClassLoader(DisklessClassLoaderContext.Action<T> action) {
        try {
            return DisklessClassLoaderContext.call(classLoaderLease.classLoader(), action);
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Exception e) {
            throw new KafkaException("Failed to invoke Ursa diskless metadata store", e);
        }
    }
}
