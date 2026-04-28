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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
class DisklessClientZoneTest {

    @Test
    void testReturnsNoZoneWhenClientIdIsMissing() {
        assertEquals(DisklessClientZone.NO_ZONE, DisklessClientZone.get(null));
        assertEquals(DisklessClientZone.NO_ZONE, DisklessClientZone.get("producer"));
    }

    @Test
    void testExtractsZoneIdFromBeginning() {
        assertEquals("zone-a", DisklessClientZone.get("zone_id=zone-a"));
    }

    @Test
    void testExtractsZoneIdFromMiddleAndTrimsWhitespace() {
        assertEquals("zone-b", DisklessClientZone.get("client=a, zone_id=zone-b ,tenant=t1"));
    }

    @Test
    void testReturnsNoZoneForEmptyZoneId() {
        assertEquals(DisklessClientZone.NO_ZONE, DisklessClientZone.get("zone_id=   "));
    }

    @Test
    void testReturnsNoZoneForInvalidZoneId() {
        assertEquals(DisklessClientZone.NO_ZONE, DisklessClientZone.get("xzone_id=zone-a"));
    }
}
