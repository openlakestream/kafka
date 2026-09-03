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
import org.apache.kafka.storage.diskless.DisklessFutures;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

import io.lakestream.api.Log;
import io.lakestream.api.exception.LogFencedException;

/**
 * The retention side of one diskless partition: it turns retention settings into soft trims.
 *
 * <p>Requests are coalesced rather than queued. At most one run is in flight; a request that arrives
 * while a run is in flight only records the latest settings, so a burst of config updates and
 * periodic checks costs one extra trim, not one per request. Every run resolves the settings against
 * the log itself ({@code getLastOffset} to {@code computeRetentionTrimOffset} to {@code getFirstOffset}
 * to {@code softTrim}), so the newest settings always win.
 *
 * <p>Nothing here blocks: each stage is chained onto the storage future that precedes it, which is
 * what lets the shared diskless timer start a run without waiting for one.
 *
 * <p>A trim that lands is reported to {@code onTrimmed}: it moved the log's first offset, so the
 * offset window {@link PartitionReader} caches no longer describes the log and has to be dropped.
 */
final class PartitionRetention {

    private static final Logger log = LoggerFactory.getLogger(PartitionRetention.class);

    private final TopicIdPartition topicIdPartition;
    private final Supplier<CompletableFuture<Log>> logSupplier;
    private final Consumer<Throwable> onFenced;
    /** Told after each trim that landed, so the read side stops answering from a window it moved. */
    private final Runnable onTrimmed;
    /** The settings of the newest request that has not started running yet, if any. */
    private final AtomicReference<RetentionRequest> pending = new AtomicReference<>();
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();

    PartitionRetention(
            TopicIdPartition topicIdPartition,
            Supplier<CompletableFuture<Log>> logSupplier,
            Consumer<Throwable> onFenced,
            Runnable onTrimmed) {
        this.topicIdPartition = Objects.requireNonNull(topicIdPartition, "topicIdPartition must not be null");
        this.logSupplier = Objects.requireNonNull(logSupplier, "logSupplier must not be null");
        this.onFenced = Objects.requireNonNull(onFenced, "onFenced must not be null");
        this.onTrimmed = Objects.requireNonNull(onTrimmed, "onTrimmed must not be null");
    }

    /** Coalesces requests: at most one trim in flight, the latest request wins. */
    void request(long retentionMs, long retentionBytes) {
        if (closed.get()) {
            return;
        }
        pending.set(new RetentionRequest(retentionMs, retentionBytes));
        startIfIdle();
    }

    /** Stops accepting requests. The returned future completes once the run in flight has settled. */
    CompletableFuture<Void> close() {
        closed.set(true);
        pending.set(null);
        completeCloseIfIdle();
        return closeFuture;
    }

    private void startIfIdle() {
        if (running.compareAndSet(false, true)) {
            runNext();
        }
    }

    private void runNext() {
        RetentionRequest request = closed.get() ? null : pending.getAndSet(null);
        if (request == null) {
            running.set(false);
            if (closed.get()) {
                completeCloseIfIdle();
                return;
            }
            // Closes the race where a request arrived after the read above but before the worker
            // published that it had stopped.
            if (pending.get() != null) {
                startIfIdle();
            }
            return;
        }

        CompletableFuture<Void> run;
        try {
            run = applyRetention(request.retentionMs(), request.retentionBytes());
        } catch (Throwable error) {
            run = CompletableFuture.failedFuture(error);
        }
        run.whenComplete((ignored, error) -> {
            if (error != null) {
                reportFailure(error);
            }
            runNext();
        });
    }

    private void reportFailure(Throwable error) {
        Throwable cause = DisklessFutures.unwrap(error);
        if (UrsaPartitionLog.hasCause(cause, LogFencedException.class)) {
            onFenced.accept(cause);
        } else if (!closed.get()) {
            // A run that loses its race with close() fails against a retired handle; that is the
            // expected outcome of closing, not something to warn about.
            log.warn("Failed to apply retention for {}", topicIdPartition, error);
        }
    }

    private void completeCloseIfIdle() {
        if (closed.get() && !running.get()) {
            closeFuture.complete(null);
        }
    }

    private CompletableFuture<Void> applyRetention(long retentionMs, long retentionBytes) {
        if (retentionMs < 0 && retentionBytes < 0) {
            return CompletableFuture.completedFuture(null);
        }
        return logSupplier.get().thenCompose(logInstance -> logInstance.getLastOffset()
                .thenCompose(lastOffset -> lastOffset == null || lastOffset.offset() < 0
                        ? CompletableFuture.completedFuture(-1L)
                        : logInstance.computeRetentionTrimOffset(
                                lastOffset.offset(), retentionMs, retentionBytes))
                .thenCompose(trimOffset -> trimOffset == null || trimOffset < 0
                        ? CompletableFuture.completedFuture(null)
                        : trimFromStart(logInstance, trimOffset)));
    }

    /** Trims only what the log still holds: a computed offset below the start is already gone. */
    private CompletableFuture<Void> trimFromStart(Log logInstance, long trimOffset) {
        return logInstance.getFirstOffset().thenCompose(firstOffset -> {
            if (firstOffset == null || firstOffset.offset() < 0 || trimOffset < firstOffset.offset()) {
                return CompletableFuture.completedFuture(null);
            }
            if (closed.get()) {
                // The partition was retired while the trim offset was being computed.
                return CompletableFuture.completedFuture(null);
            }
            CompletableFuture<Long> trim = logInstance.softTrim(trimOffset);
            if (trim == null) {
                return CompletableFuture.failedFuture(new IllegalStateException(
                        "Log.softTrim returned a null future for " + topicIdPartition));
            }
            return trim.thenApply(ignored -> {
                // The log now starts past what the read side last saw, so its window must go.
                onTrimmed.run();
                return null;
            });
        });
    }

    private record RetentionRequest(long retentionMs, long retentionBytes) {
    }
}
