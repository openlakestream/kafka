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
package org.apache.kafka.storage.diskless.idempotent;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProducerStateSerDesTest {

    @Test
    void testSerializeDeserializeRoundTrip() throws Exception {
        Map<Long, ProducerStateEntry> producers = new LinkedHashMap<>();
        ProducerStateEntry p1 = new ProducerStateEntry((short) 3, 1000L);
        p1.appendBatch((short) 3, new BatchMetadata(0, 9, CompletableFuture.completedFuture(123L), 1000L));
        p1.appendBatch((short) 3, new BatchMetadata(10, 19, CompletableFuture.completedFuture(133L), 1100L));
        p1.appendBatch((short) 3, new BatchMetadata(20, 29, CompletableFuture.completedFuture(143L), 1200L));
        ProducerStateEntry p2 = new ProducerStateEntry((short) 7, 2000L);
        p2.appendBatch((short) 7, new BatchMetadata(30, 39, CompletableFuture.completedFuture(456L), 2000L));
        p2.appendBatch((short) 7, new BatchMetadata(40, 49, CompletableFuture.completedFuture(466L), 2100L));
        p2.appendBatch((short) 7, new BatchMetadata(50, 59, CompletableFuture.completedFuture(476L), 2200L));
        producers.put(1001L, p1);
        producers.put(1002L, p2);

        ProducerStateSerDes.SerializationResult result = ProducerStateSerDes.serialize(producers, 1024 * 1024);
        byte[] payload = result.bytes();
        assertEquals(0x1, payload[0]);
        Map<Long, ProducerStateEntry> snapshot = ProducerStateSerDes.deserialize(payload);
        assertEquals(producers.keySet(), snapshot.keySet());
        for (Map.Entry<Long, ProducerStateEntry> entry : producers.entrySet()) {
            ProducerStateEntry expected = entry.getValue();
            ProducerStateEntry actual = snapshot.get(entry.getKey());
            assertEquals(expected.producerEpoch(), actual.producerEpoch());
            assertEquals(expected.lastTimestamp(), actual.lastTimestamp());
            assertEquals(new ArrayList<>(expected.batchMetadata()), new ArrayList<>(actual.batchMetadata()));
        }
    }

    @Test
    void testDeserializeRejectsPayloadWithInvalidMagic() {
        byte[] payload = new byte[]{42, 1, 2, 3};
        assertThrows(Exception.class, () -> ProducerStateSerDes.deserialize(payload));
    }
}
