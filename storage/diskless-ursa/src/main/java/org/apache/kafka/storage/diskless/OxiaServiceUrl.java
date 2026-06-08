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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.CompletableFuture;

import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.OxiaClientBuilder;

public class OxiaServiceUrl {

    private final String url;
    private final String address;
    private final String namespace;

    public OxiaServiceUrl(String url) throws Exception {
        this.url = url;
        URI uri = parse(url);
        this.address = uri.getAuthority();
        String path = uri.getPath();
        this.namespace = path == null || path.isBlank() || "/".equals(path)
                ? "default"
                : path.substring(1);
    }

    public CompletableFuture<AsyncOxiaClient> client() {
        return OxiaClientBuilder.create(address)
                .namespace(namespace)
                .asyncClient();
    }

    private static URI parse(String url) throws URISyntaxException {
        URI uri = new URI(url);
        if (!"oxia".equals(uri.getScheme()) || uri.getAuthority() == null || uri.getAuthority().isBlank()) {
            throw new IllegalArgumentException(
                    "Invalid Oxia service URL '" + url + "'. Expected format is oxia://host:port[/namespace]");
        }
        return uri;
    }

    @Override
    public String toString() {
        return url;
    }
}
