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

import org.apache.kafka.common.Node;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.utils.Utils;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.function.Function;
import java.util.stream.StreamSupport;

public class DisklessBrokerSelector {

    private final Function<ListenerName, Iterable<Node>> aliveBrokerNodesSupplier;
    private final ListenerName selectionListener;

    public DisklessBrokerSelector(
            Function<ListenerName, Iterable<Node>> aliveBrokerNodesSupplier,
            ListenerName selectionListener
    ) {
        this.aliveBrokerNodesSupplier = Objects.requireNonNull(
                aliveBrokerNodesSupplier, "aliveBrokerNodesSupplier cannot be null");
        this.selectionListener = Objects.requireNonNull(
                selectionListener, "selectionListener cannot be null");
    }

    public OptionalInt selectBroker(Uuid topicId, int partitionIndex) {
        Objects.requireNonNull(topicId, "topicId cannot be null");

        if (Uuid.ZERO_UUID.equals(topicId)) {
            return OptionalInt.empty();
        }

        List<Node> brokers = allAliveBrokers();
        if (brokers.isEmpty()) {
            return OptionalInt.empty();
        }

        byte[] input = (topicId + "-" + partitionIndex).getBytes(StandardCharsets.UTF_8);
        int hash = Utils.murmur2(input);
        int idx = Math.floorMod(hash, brokers.size());
        return OptionalInt.of(brokers.get(idx).id());
    }

    private List<Node> allAliveBrokers() {
        List<Node> result = new ArrayList<>();
        Iterable<Node> nodes = aliveBrokerNodesSupplier.apply(selectionListener);
        if (nodes == null) {
            return result;
        }

        StreamSupport.stream(nodes.spliterator(), false)
                .sorted(Comparator.comparing(Node::id))
                .forEach(result::add);
        return result;
    }
}
