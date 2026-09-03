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

import org.apache.kafka.common.Uuid;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Lazy controller-side facade for the isolated topic lifecycle provider.
 *
 * <p>The provider is loaded off the metadata publisher thread on the first operation and memoized
 * afterwards. A failed load is not memoized, so the controller reconciler retries it with its next
 * operation.
 */
final class LazyDisklessTopicLifecycle implements DisklessTopicLifecycle {

    private static final Logger log = LoggerFactory.getLogger(LazyDisklessTopicLifecycle.class);

    private final Supplier<DisklessTopicLifecycle> loader;
    private final Object lock = new Object();
    private CompletableFuture<DisklessTopicLifecycle> loading;
    private boolean closed;
    private boolean delegateClosed;

    LazyDisklessTopicLifecycle(Supplier<DisklessTopicLifecycle> loader) {
        this.loader = Objects.requireNonNull(loader, "loader must not be null");
    }

    @Override
    public CompletableFuture<Void> ensureTopic(
            String topicName,
            Uuid topicId,
            int partitions,
            Map<String, String> configs,
            long sourceRevision) {
        return call(lifecycle -> lifecycle.ensureTopic(topicName, topicId, partitions, configs, sourceRevision));
    }

    @Override
    public CompletableFuture<Void> deleteTopic(String topicName, Uuid topicId) {
        return call(lifecycle -> lifecycle.deleteTopic(topicName, topicId));
    }

    @Override
    public CompletableFuture<List<ManagedTopic>> listManagedTopics() {
        return call(DisklessTopicLifecycle::listManagedTopics);
    }

    @Override
    public CompletableFuture<Void> sweepOrphans(Set<Uuid> liveTopicIds, long imageOffset) {
        return call(lifecycle -> lifecycle.sweepOrphans(liveTopicIds, imageOffset));
    }

    private <R> CompletableFuture<R> call(Function<DisklessTopicLifecycle, CompletableFuture<R>> operation) {
        CompletableFuture<DisklessTopicLifecycle> current;
        synchronized (lock) {
            if (closed) {
                return CompletableFuture.failedFuture(
                        new IllegalStateException("diskless topic lifecycle is closed"));
            }
            if (loading == null) {
                CompletableFuture<DisklessTopicLifecycle> started = CompletableFuture.supplyAsync(
                        loader, task -> {
                            Thread thread = new Thread(task, "diskless-topic-lifecycle-loader");
                            thread.setDaemon(true);
                            thread.start();
                        });
                loading = started;
                // A load that already failed completes this callback inline, which clears the
                // memoized future, so this call must keep using the future it started.
                current = started;
                started.whenComplete((loaded, error) -> onLoaded(started, loaded, error));
            } else {
                current = loading;
            }
        }
        return current.thenCompose(lifecycle -> {
            synchronized (lock) {
                // close() may have run while the provider was loading, or between two operations:
                // the delegate is then already closed and its lease released.
                if (closed) {
                    return CompletableFuture.<R>failedFuture(
                            new IllegalStateException("diskless topic lifecycle is closed"));
                }
            }
            return operation.apply(lifecycle);
        });
    }

    private void onLoaded(
            CompletableFuture<DisklessTopicLifecycle> started,
            DisklessTopicLifecycle loaded,
            Throwable error) {
        boolean closeLoaded;
        synchronized (lock) {
            if (error != null) {
                if (loading == started) {
                    // A failed load is not memoized: the next operation retries it.
                    loading = null;
                }
                return;
            }
            closeLoaded = claimDelegateCloseLocked();
        }
        if (closeLoaded) {
            closeQuietly(loaded);
        }
    }

    /**
     * Claims the right to close the loaded delegate, once the facade is closed.
     *
     * <p>{@link #close} and {@link #onLoaded} can both observe a loaded, unclosed delegate: a load
     * that completes after {@code close()} has published {@code closed} but before it inspects the
     * loading future is visible to both. Exactly one of them wins this claim, so the delegate is
     * closed once.
     */
    private boolean claimDelegateCloseLocked() {
        assert Thread.holdsLock(lock) : "claimDelegateCloseLocked must be called with the lock held";
        if (!closed || delegateClosed) {
            return false;
        }
        delegateClosed = true;
        return true;
    }

    @Override
    public void close() throws Exception {
        CompletableFuture<DisklessTopicLifecycle> current;
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            current = loading;
        }
        if (current == null || !current.isDone() || current.isCompletedExceptionally()) {
            // An in-flight load closes its result in onLoaded.
            return;
        }
        DisklessTopicLifecycle loaded = current.join();
        boolean closeLoaded;
        synchronized (lock) {
            closeLoaded = claimDelegateCloseLocked();
        }
        if (closeLoaded) {
            loaded.close();
        }
    }

    private static void closeQuietly(DisklessTopicLifecycle lifecycle) {
        try {
            lifecycle.close();
        } catch (Exception e) {
            log.warn("Failed to close lazily loaded diskless topic lifecycle", e);
        }
    }
}
