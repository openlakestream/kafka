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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.oxia.client.api.AsyncOxiaClient;

public final class DefaultOxiaStore implements OxiaStore {
    private static final long CONNECT_TIMEOUT_SECONDS = 10;
    private final AsyncOxiaClient client;

    /**
     * Create the Oxia based metadata store.
     *
     * @param oxiaServiceUrl the format of "oxia://host:port[/namespace]". If namespace is not provided, "default" will
     *                       be used.
     * @throws IllegalArgumentException if the URL format is invalid.
     */
    public DefaultOxiaStore(OxiaServiceUrl url) throws ExecutionException, InterruptedException, TimeoutException {
        this.client = url.client().get(CONNECT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @Override
    public CompletableFuture<Void> put(String key, byte[] value) {
        return client.put(key, value).thenApply(ignored -> null);
    }

    @Override
    public CompletableFuture<Void> delete(String key) {
        return client.delete(key).thenApply(ignored -> null);
    }

    @Override
    public void close() throws Exception {
        client.close();
    }
}
