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
package org.apache.kafka.storage.diskless.handlers;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.diskless.idempotent.ProducerStateStore;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class UrsaStorageStateTest {

    @Test
    void testCleanupPartitionClosesManagedLedgerAndClearsProducerState() throws Exception {
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("test-topic", 0));

        ProducerStateStore producerStateStore = mock(ProducerStateStore.class);
        when(producerStateStore.clearPartition(tp)).thenReturn(CompletableFuture.completedFuture(null));

        ManagedLedgerFactory managedLedgerFactory = mock(ManagedLedgerFactory.class);
        ManagedLedger ledger1 = mock(ManagedLedger.class);
        ManagedLedger ledger2 = mock(ManagedLedger.class);
        CountDownLatch closeLatch = new CountDownLatch(1);
        doAnswer(invocation -> {
            closeLatch.countDown();
            return null;
        }).when(ledger1).close();

        AtomicInteger openCount = new AtomicInteger(0);
        doAnswer(invocation -> {
            AsyncCallbacks.OpenLedgerCallback callback = invocation.getArgument(2);
            int attempt = openCount.getAndIncrement();
            callback.openLedgerComplete(attempt == 0 ? ledger1 : ledger2, invocation.getArgument(3));
            return null;
        }).when(managedLedgerFactory).asyncOpen(
                anyString(),
                any(ManagedLedgerConfig.class),
                any(AsyncCallbacks.OpenLedgerCallback.class),
                any(),
                any()
        );

        UrsaStorageState state = new UrsaStorageState(
                Time.SYSTEM,
                1,
                mock(UrsaStorageConfig.class),
                mock(BrokerTopicStats.class),
                producerStateStore,
                managedLedgerFactory);

        state.getOrCreateManagedLedger(tp).get(5, TimeUnit.SECONDS);
        state.getPartitionState(tp);

        assertTrue(state.cleanupPartition(tp));

        verify(producerStateStore, times(1)).clearPartition(tp);
        verify(managedLedgerFactory, times(1)).asyncOpen(anyString(), any(), any(), any(), any());

        assertTrue(closeLatch.await(5, TimeUnit.SECONDS));

        state.getOrCreateManagedLedger(tp).get(5, TimeUnit.SECONDS);
        verify(managedLedgerFactory, times(2)).asyncOpen(anyString(), any(), any(), any(), any());
    }

    @Test
    void testCleanupPartitionNoopWhenNoState() {
        TopicIdPartition tp = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("test-topic", 0));

        ProducerStateStore producerStateStore = mock(ProducerStateStore.class);
        when(producerStateStore.clearPartition(tp)).thenReturn(CompletableFuture.completedFuture(null));
        ManagedLedgerFactory managedLedgerFactory = mock(ManagedLedgerFactory.class);

        UrsaStorageState state = new UrsaStorageState(
                Time.SYSTEM,
                1,
                mock(UrsaStorageConfig.class),
                mock(BrokerTopicStats.class),
                producerStateStore,
                managedLedgerFactory);

        assertFalse(state.cleanupPartition(tp));

        verify(producerStateStore, times(1)).clearPartition(tp);
        verify(managedLedgerFactory, never()).asyncOpen(anyString(), any(), any(), any(), any());
    }
}
