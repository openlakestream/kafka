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

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.OptionalInt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DisklessBrokerSelectorTest {

    private static final ListenerName LISTENER = new ListenerName("INTERNAL");

    @Test
    void testSelectBrokerUsesClientZoneWhenRackMatches() {
        List<Node> aliveBrokers = List.of(
                new Node(0, "host0", 9092, "zone-a"),
                new Node(1, "host1", 9092, "zone-b"),
                new Node(2, "host2", 9092, "zone-a")
        );
        DisklessBrokerSelector selector = new DisklessBrokerSelector(ignored -> aliveBrokers, LISTENER);

        String zone = selector.effectiveZone("producer,zone_id=zone-a");
        OptionalInt selected = selector.selectBrokerForZone(Uuid.randomUuid(), 0, zone);

        assertTrue(selected.isPresent());
        assertTrue(selected.getAsInt() == 0 || selected.getAsInt() == 2);
    }

    @Test
    void testSelectBrokerFallsBackToAllBrokersWhenZoneMissing() {
        List<Node> aliveBrokers = List.of(
                new Node(0, "host0", 9092, "zone-a"),
                new Node(1, "host1", 9092, "zone-b")
        );
        DisklessBrokerSelector selector = new DisklessBrokerSelector(ignored -> aliveBrokers, LISTENER);
        Uuid topicId = Uuid.randomUuid();

        assertEquals(
                selector.selectBrokerForZone(topicId, 0, DisklessClientZone.NO_ZONE),
                selector.selectBrokerForZone(topicId, 0, selector.effectiveZone("producer"))
        );
    }

    @Test
    void testSelectBrokerFallsBackToAllBrokersWhenZoneUnknown() {
        List<Node> aliveBrokers = List.of(
                new Node(0, "host0", 9092, "zone-a"),
                new Node(1, "host1", 9092, "zone-b")
        );
        DisklessBrokerSelector selector = new DisklessBrokerSelector(ignored -> aliveBrokers, LISTENER);
        Uuid topicId = Uuid.randomUuid();

        assertEquals(
                selector.selectBrokerForZone(topicId, 0, DisklessClientZone.NO_ZONE),
                selector.selectBrokerForZone(topicId, 0, selector.effectiveZone("producer,zone_id=zone-c"))
        );
    }
}
