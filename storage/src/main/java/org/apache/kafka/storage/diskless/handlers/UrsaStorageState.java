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
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.diskless.idempotent.ProducerStateStore;
import org.apache.kafka.storage.diskless.idempotent.UrsaProducerStateStore;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Shared state container for Ursa storage components.
 * Manages ManagedLedger instances and partition state for UrsaManagedLedgerReader and UrsaManagedLedgerWriter.
 */
public class UrsaStorageState implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(UrsaStorageState.class);

    private final Time time;
    private final int brokerId;
    private final UrsaStorageConfig config;
    private final BrokerTopicStats brokerTopicStats;

    private final ConcurrentHashMap<TopicIdPartition, PartitionState> partitionStates = new ConcurrentHashMap<>();

    private final ProducerStateStore producerStateStore;

    private final KafkaManagedLedgerFactoryHolder managedLedgerFactoryHolder;
    private final ManagedLedgerFactory managedLedgerFactory;
    private final ConcurrentHashMap<TopicIdPartition, CompletableFuture<ManagedLedger>> managedLedgerCache =
            new ConcurrentHashMap<>();

    /**
     * Creates UrsaStorageState for production use.
     */
    public UrsaStorageState(
            Time time,
            int brokerId,
            UrsaStorageConfig config,
            BrokerTopicStats brokerTopicStats) {
        this(time, brokerId, config, brokerTopicStats, null);
    }

    /**
     * Creates UrsaStorageState with a custom ProducerStateStore.
     * Useful for testing idempotent validation logic.
     */
    public UrsaStorageState(
            Time time,
            int brokerId,
            UrsaStorageConfig config,
            BrokerTopicStats brokerTopicStats,
            ProducerStateStore producerStateStore) {
        this.time = time;
        this.brokerId = brokerId;
        this.config = config;
        this.brokerTopicStats = brokerTopicStats;

        KafkaManagedLedgerFactoryHolder holder;
        try {
            holder = KafkaManagedLedgerFactoryHolder.create(config);
            log.info("Initialized ManagedLedgerFactory for Kafka diskless storage");
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize ManagedLedgerFactory", e);
        }
        this.managedLedgerFactoryHolder = holder;
        this.managedLedgerFactory = holder.factory();

        this.producerStateStore = producerStateStore != null 
                ? producerStateStore 
                : new UrsaProducerStateStore(holder::oxiaClient, time);
    }

    public Time time() {
        return time;
    }

    public int brokerId() {
        return brokerId;
    }

    public UrsaStorageConfig config() {
        return config;
    }

    public BrokerTopicStats brokerTopicStats() {
        return brokerTopicStats;
    }

    public ProducerStateStore producerStateStore() {
        return producerStateStore;
    }

    public CompletableFuture<ManagedLedger> getOrCreateManagedLedger(TopicIdPartition tp) {
        CompletableFuture<ManagedLedger> existing = managedLedgerCache.get(tp);
        if (existing != null) {
            return existing;
        }

        String name = KafkaManagedLedgerNaming.managedLedgerName(tp);
        ManagedLedgerConfig mlConfig = new ManagedLedgerConfig().setCreateIfMissing(true);

        CompletableFuture<ManagedLedger> openFuture = new CompletableFuture<>();
        managedLedgerFactory.asyncOpen(name, mlConfig, new AsyncCallbacks.OpenLedgerCallback() {
            @Override
            public void openLedgerComplete(ManagedLedger ledger, Object ctx) {
                openFuture.complete(ledger);
            }

            @Override
            public void openLedgerFailed(ManagedLedgerException exception, Object ctx) {
                openFuture.completeExceptionally(exception);
            }
        }, null, null);

        // Evict from cache on failure to allow retry after transient errors
        openFuture.whenComplete((ledger, error) -> {
            if (error != null) {
                managedLedgerCache.remove(tp, openFuture);
                log.warn("Failed to open ManagedLedger for partition {}, evicting from cache", tp, error);
            }
        });

        CompletableFuture<ManagedLedger> raced = managedLedgerCache.putIfAbsent(tp, openFuture);
        return raced != null ? raced : openFuture;
    }


    /**
     * Gets the partition state, creating one if necessary.
     */
    public PartitionState getPartitionState(TopicIdPartition tp) {
        return partitionStates.computeIfAbsent(tp, k -> new PartitionState());
    }

    /**
     * Gets the next offset for a partition.
     */
    public long getNextOffset(TopicIdPartition tp) {
        PartitionState state = partitionStates.get(tp);
        return state != null ? state.getNextOffset() : 0;
    }

    /**
     * Increments the offset for a partition and returns the base offset.
     */
    public long incrementOffset(TopicIdPartition tp, int count) {
        return getPartitionState(tp).incrementOffset(count);
    }

    @Override
    public void close() throws IOException {
        partitionStates.clear();
        managedLedgerCache.clear();
        producerStateStore.close();
        if (managedLedgerFactoryHolder != null) {
            managedLedgerFactoryHolder.close();
        }
    }

    /**
     * Tracks per-partition state for offset and cumulative size management.
     */
    public static class PartitionState {
        private final AtomicLong nextOffset = new AtomicLong(0);
        private final AtomicLong cumulativeSize = new AtomicLong(0);

        public long incrementOffset(int count) {
            return nextOffset.getAndAdd(count);
        }

        public long incrementCumulativeSize(long size) {
            return cumulativeSize.getAndAdd(size);
        }

        public long getNextOffset() {
            return nextOffset.get();
        }

        public long getCumulativeSize() {
            return cumulativeSize.get();
        }

        public void setNextOffset(long offset) {
            nextOffset.set(offset);
        }

        public void setCumulativeSize(long size) {
            cumulativeSize.set(size);
        }
    }
}
