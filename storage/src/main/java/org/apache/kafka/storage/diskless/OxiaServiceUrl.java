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

import org.apache.commons.lang3.tuple.Pair;

import java.util.concurrent.CompletableFuture;

import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.OxiaClientBuilder;
import io.streamnative.ursa.storage.UrsaStorage;

public class OxiaServiceUrl {

    private final String url;
    private final Pair<String, String> addressAndNamespace;

    public OxiaServiceUrl(String url) throws Exception {
        this.url = url;
        this.addressAndNamespace = UrsaStorage.validateOxiaUrl(url);
    }

    public CompletableFuture<AsyncOxiaClient> client() {
        return OxiaClientBuilder.create(addressAndNamespace.getLeft())
                .namespace(addressAndNamespace.getRight())
                .asyncClient();
    }

    @Override
    public String toString() {
        return url;
    }
}
