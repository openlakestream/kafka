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
package kafka.server.metadata;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.image.ConfigurationsDelta;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.TopicsDelta;
import org.apache.kafka.image.loader.LoaderManifest;
import org.apache.kafka.image.publisher.MetadataPublisher;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.server.metrics.KafkaMetricsGroup;
import org.apache.kafka.storage.diskless.DisklessFutures;
import org.apache.kafka.storage.diskless.DisklessTopicLifecycle;
import org.apache.kafka.storage.diskless.DisklessTopics;

import com.yammer.metrics.core.Meter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;

/**
 * Drives diskless storage lifecycle operations from the active controller's metadata image.
 *
 * <p>Every KRaft metadata update names a desired state for each diskless topic: present with a
 * partition count and a configuration, or absent. The reconciler records that desired state,
 * stamped with the metadata offset it came from, and drives {@link DisklessTopicLifecycle} towards
 * it. Operations are idempotent, so a failure is simply retried with exponential backoff until it
 * succeeds or the desired state changes underneath it.
 *
 * <p>Concurrency rules:
 * <ul>
 *   <li>Lifecycle operations are never started on the thread that delivered the metadata update:
 *       that thread records the desired state and posts a drain to this reconciler's executor, so
 *       a lifecycle implementation that is slow to hand back its future cannot stall the
 *       controller's metadata pipeline.</li>
 *   <li>At most one operation is in flight per topic; a desired-state change that arrives while an
 *       operation runs is applied when that operation completes. An attempt that exceeds
 *       {@code operationTimeoutMs} is cancelled best effort and retried, so it may still be running
 *       inside the storage layer when the next attempt starts -- which is why every lifecycle
 *       operation must be idempotent.</li>
 *   <li>At most {@code maxConcurrentOperations} operations are in flight across all topics.</li>
 *   <li>Leadership changes bump a generation counter. Results of operations started in an earlier
 *       generation are discarded without touching any state, and every retry and sweep scheduled by
 *       that generation is cancelled.</li>
 *   <li>A periodic sweep deletes storage for topics that vanished from the metadata image while
 *       this node was not the active controller.</li>
 * </ul>
 */
public final class DisklessTopicLifecycleReconciler implements MetadataPublisher {

    private static final Logger log = LoggerFactory.getLogger(DisklessTopicLifecycleReconciler.class);

    private static final long DEFAULT_INITIAL_RETRY_MS = 1_000L;
    private static final long DEFAULT_MAX_RETRY_MS = 30_000L;
    private static final long DEFAULT_OPERATION_TIMEOUT_MS = 60_000L;
    private static final int DEFAULT_MAX_CONCURRENT_OPERATIONS = 16;

    private static final String PENDING_OPERATIONS_METRIC = "PendingOperations";
    private static final String FAILED_OPERATIONS_METRIC = "FailedOperations";
    private static final String ERRORS_METRIC = "LifecycleOperationErrorsPerSec";

    /**
     * The state one topic must reach, as of the metadata image that last changed it. {@link #remember}
     * stores a new instance whenever the target changes and never mutates one. Reference identity
     * therefore answers "is this still the state we set out to reach?".
     */
    private record Desired(String name, Uuid id, boolean present, int partitions,
                           Map<String, String> configs, long sourceRevision) {

        /** True when {@code other} asks for the same end state, ignoring where each came from. */
        boolean sameTargetAs(Desired other) {
            return other != null
                && present == other.present
                && partitions == other.partitions
                && name.equals(other.name)
                && configs.equals(other.configs);
        }
    }

    private final int nodeId;
    private final DisklessTopicLifecycle lifecycle;
    private final BiConsumer<String, Throwable> faultHandler;
    private final long sweepIntervalMs;
    private final long operationTimeoutMs;
    private final int maxConcurrentOperations;
    private final ExponentialBackoff backoff;
    private final ScheduledExecutorService executor;
    private final KafkaMetricsGroup metricsGroup =
        new KafkaMetricsGroup("kafka.server", "DisklessTopicLifecycleReconciler");
    private final Meter errors = metricsGroup.newMeter(ERRORS_METRIC, "errors", TimeUnit.SECONDS);

    // All mutable state below is guarded by this.
    private final Map<Uuid, Desired> desired = new HashMap<>();
    private final Map<Uuid, Runner> runners = new HashMap<>();
    private final Deque<Runner> waitingForPermit = new ArrayDeque<>();
    private MetadataImage latestImage;
    private boolean active;
    private boolean closed;
    private boolean fullReconcilePending;
    private long generation;
    private int inFlight;
    private ScheduledFuture<?> sweepTask;
    private boolean sweepInFlight;

    public DisklessTopicLifecycleReconciler(int nodeId, DisklessTopicLifecycle lifecycle,
                                            BiConsumer<String, Throwable> faultHandler, long sweepIntervalMs) {
        this(nodeId, lifecycle, faultHandler, sweepIntervalMs, DEFAULT_INITIAL_RETRY_MS, DEFAULT_MAX_RETRY_MS,
            DEFAULT_OPERATION_TIMEOUT_MS, DEFAULT_MAX_CONCURRENT_OPERATIONS,
            Executors.newSingleThreadScheduledExecutor(runnable -> {
                Thread thread = new Thread(runnable, "diskless-lifecycle-reconciler-" + nodeId);
                thread.setDaemon(true);
                return thread;
            }));
    }

    // Visible for testing.
    DisklessTopicLifecycleReconciler(int nodeId, DisklessTopicLifecycle lifecycle,
                                     BiConsumer<String, Throwable> faultHandler, long sweepIntervalMs,
                                     long initialRetryMs, long maxRetryMs, long operationTimeoutMs,
                                     int maxConcurrentOperations, ScheduledExecutorService executor) {
        this.nodeId = nodeId;
        this.lifecycle = Objects.requireNonNull(lifecycle, "lifecycle must not be null");
        this.faultHandler = Objects.requireNonNull(faultHandler, "faultHandler must not be null");
        this.executor = Objects.requireNonNull(executor, "executor must not be null");
        if (sweepIntervalMs <= 0 || operationTimeoutMs <= 0 || maxConcurrentOperations <= 0) {
            throw new IllegalArgumentException("the sweep interval, operation timeout and concurrency limit "
                + "must all be positive");
        }
        this.sweepIntervalMs = sweepIntervalMs;
        this.operationTimeoutMs = operationTimeoutMs;
        this.maxConcurrentOperations = maxConcurrentOperations;
        this.backoff = new ExponentialBackoff(initialRetryMs, 2, maxRetryMs, 0.2);
        metricsGroup.newGauge(PENDING_OPERATIONS_METRIC, this::pendingOperations);
        metricsGroup.newGauge(FAILED_OPERATIONS_METRIC, this::failedOperations);
    }

    @Override
    public String name() {
        return "DisklessTopicLifecycleReconciler id=" + nodeId;
    }

    @Override
    public void onControllerChange(LeaderAndEpoch newLeaderAndEpoch) {
        MetadataImage image;
        boolean nowActive;
        synchronized (this) {
            generation++;
            cancelAllLocked();
            active = !closed && newLeaderAndEpoch.isLeader(nodeId);
            fullReconcilePending = active;
            nowActive = active;
            image = latestImage;
        }
        if (nowActive && image != null) {
            reconcileFullImage(image);
        }
    }

    @Override
    public void onMetadataUpdate(MetadataDelta delta, MetadataImage newImage, LoaderManifest manifest) {
        boolean full;
        synchronized (this) {
            latestImage = newImage;
            if (!active) {
                return;
            }
            full = fullReconcilePending;
        }
        if (full) {
            reconcileFullImage(newImage);
            return;
        }
        long sourceRevision = newImage.highestOffsetAndEpoch().offset();
        if (delta.topicsDelta() != null) {
            applyTopicsDelta(delta.topicsDelta(), delta.image(), newImage, sourceRevision);
        }
        if (delta.configsDelta() != null) {
            applyConfigsDelta(delta.configsDelta(), newImage, sourceRevision);
        }
    }

    /** Records the desired state of every topic this delta created, deleted or repartitioned. */
    private void applyTopicsDelta(TopicsDelta topics, MetadataImage oldImage, MetadataImage newImage,
                                  long sourceRevision) {
        for (Uuid id : topics.deletedTopicIds()) {
            // The deleted topic is only in the image the delta was built from, and so is its config.
            TopicImage old = oldImage.topics().getTopic(id);
            if (old != null && DisklessTopics.isDiskless(old.name(), topicConfigs(oldImage, old.name()))) {
                remember(new Desired(old.name(), id, false, 0, Map.of(), sourceRevision));
            }
        }
        for (Uuid id : topics.createdTopicIds()) {
            rememberPresent(newImage, id, sourceRevision);
        }
        for (Uuid id : topics.changedTopics().keySet()) {
            TopicImage old = oldImage.topics().getTopic(id);
            TopicImage current = newImage.topics().getTopic(id);
            if (old != null && current != null && old.partitions().size() != current.partitions().size()) {
                rememberPresent(newImage, id, sourceRevision);
            }
        }
    }

    /**
     * Records the desired state of every topic whose configuration changed, which also covers a
     * topic that just became -- or stopped being -- diskless.
     */
    private void applyConfigsDelta(ConfigurationsDelta configs, MetadataImage newImage, long sourceRevision) {
        for (ConfigResource resource : configs.changes().keySet()) {
            if (resource.type() != ConfigResource.Type.TOPIC) {
                continue;
            }
            TopicImage topic = newImage.topics().getTopic(resource.name());
            if (topic != null) {
                rememberPresent(newImage, topic.id(), sourceRevision);
            }
        }
    }

    /**
     * Reconciles every diskless topic in {@code image}, then sweeps storage this controller no
     * longer recognises. Called once per leadership acquisition, so it must not assume that any
     * earlier desired state survived.
     */
    private void reconcileFullImage(MetadataImage image) {
        long sourceRevision = image.highestOffsetAndEpoch().offset();
        for (TopicImage topic : image.topics().topicsById().values()) {
            Map<String, String> configs = topicConfigs(image, topic.name());
            if (DisklessTopics.isDiskless(topic.name(), configs)) {
                remember(new Desired(topic.name(), topic.id(), true, topic.partitions().size(),
                    configs, sourceRevision));
            }
        }
        synchronized (this) {
            fullReconcilePending = false;
        }
        // Nothing needs retiring here: onControllerChange cleared the desired state before asking
        // for this reconcile, so everything still desired was just remembered from this image.
        // Storage for topics that vanished while this node was not the active controller is retired
        // by the sweep below -- against the image just reconciled rather than whatever arrives next,
        // so the offset handed to the storage layer is never newer than the state it came from. It
        // is posted like every other lifecycle call, because this runs on the metadata thread.
        schedule(() -> startSweep(image), 0);
    }

    private void rememberPresent(MetadataImage image, Uuid id, long sourceRevision) {
        TopicImage topic = image.topics().getTopic(id);
        if (topic == null) {
            return;
        }
        Map<String, String> configs = topicConfigs(image, topic.name());
        if (DisklessTopics.isDiskless(topic.name(), configs)) {
            remember(new Desired(topic.name(), id, true, topic.partitions().size(), configs, sourceRevision));
        } else {
            forget(id);
        }
    }

    /**
     * Stops tracking a topic that is no longer diskless, so that turning the config back on
     * reconciles it from scratch. Its storage is left alone: {@code deleteTopic} may only be used
     * once the topic has left Kafka metadata for good, and the sweep protects every topic the image
     * still holds. Both retire it once the topic itself is deleted.
     */
    private void forget(Uuid id) {
        synchronized (this) {
            if (desired.remove(id) == null) {
                return;
            }
            Runner runner = runners.get(id);
            if (runner == null) {
                return;
            }
            runner.wake();      // startNextLocked drops a runner that has no desired state left
        }
        postDrain();
    }

    /** Records {@code next} as the topic's desired state and, if it changed anything, drives it. */
    private void remember(Desired next) {
        synchronized (this) {
            if (!active || closed || next.sameTargetAs(desired.get(next.id()))) {
                return;
            }
            desired.put(next.id(), next);
            runners.computeIfAbsent(next.id(), Runner::new).wake();
        }
        postDrain();
    }

    /** Hands the next drain to {@link #executor}: the caller may be the metadata-loader thread. */
    private void postDrain() {
        schedule(this::drain, 0);
    }

    /** Starts whatever work is queued. Always runs on {@link #executor}. */
    private void drain() {
        synchronized (this) {
            startNextLocked();
        }
    }

    /** Serializes operations for one topic; at most one attempt is in flight per topic. */
    private final class Runner {
        private final Uuid id;
        private long attempt;
        private long generationSeen;
        private boolean running;
        private boolean queued;
        private ScheduledFuture<?> retry;

        Runner(Uuid id) {
            this.id = id;
        }

        /**
         * Queues this runner for a permit unless it already holds one. Called with the monitor held.
         * Queueing only: the caller drains, either inline when it already runs on {@link #executor}
         * or by posting one.
         *
         * <p>Every caller has just changed this topic's desired state, so a backoff still waiting to
         * retry the previous one no longer applies and is cancelled: the new state runs now. Repeated
         * failures of the same desired state go through {@link #retry} instead and keep backing off.
         */
        void wake() {
            if (running || queued) {
                return;
            }
            if (retry != null) {
                retry.cancel(false);
                retry = null;
            }
            queued = true;
            waitingForPermit.add(this);
        }
    }

    /**
     * Starts queued operations while permits remain. Called with the monitor held and always on
     * {@link #executor}, so the lifecycle call runs there rather than on the metadata-loader
     * thread; its completion is handed back to the executor so that an already-completed future
     * cannot re-enter this method on the same thread.
     */
    private void startNextLocked() {
        while (inFlight < maxConcurrentOperations && !waitingForPermit.isEmpty()) {
            Runner runner = waitingForPermit.poll();
            runner.queued = false;
            Desired target = desired.get(runner.id);
            if (target == null) {
                runners.remove(runner.id, runner);
                continue;
            }
            runner.running = true;
            runner.generationSeen = generation;
            inFlight++;
            CompletableFuture<Void> operation;
            try {
                operation = target.present()
                    ? lifecycle.ensureTopic(target.name(), target.id(), target.partitions(),
                        target.configs(), target.sourceRevision())
                    : lifecycle.deleteTopic(target.name(), target.id());
                if (operation == null) {
                    throw new IllegalStateException("the diskless topic lifecycle returned a null future");
                }
            } catch (Throwable t) {
                operation = CompletableFuture.failedFuture(t);
            }
            withTimeout(operation)
                .whenComplete((ignored, error) -> schedule(() -> complete(runner, target, error), 0));
        }
    }

    /** Applies the outcome of one operation. Always runs on {@link #executor}. */
    private void complete(Runner runner, Desired target, Throwable error) {
        long delay = -1;
        long attempt = 0;
        synchronized (this) {
            if (runner.generationSeen != generation || closed || !active) {
                // A leadership change already discarded this runner and reset the permit count;
                // touching any shared state here would corrupt the generation that replaced it.
                return;
            }
            inFlight--;
            runner.running = false;
            if (error == null) {
                runner.attempt = 0;
                Desired latest = desired.get(runner.id);
                if (latest == target) {
                    if (!latest.present()) {
                        desired.remove(runner.id);          // deletion is terminal for this topic id
                    }
                    runners.remove(runner.id, runner);
                } else {
                    runner.wake();                          // the desired state moved on meanwhile
                }
            } else {
                runner.attempt++;
                attempt = runner.attempt;
                delay = backoff.backoff(attempt - 1);
                runner.retry = schedule(() -> retry(runner), delay);
            }
            startNextLocked();
        }
        if (error != null) {
            errors.mark();
            String description = (target.present() ? "ensure diskless topic " : "delete diskless topic ")
                + target.name() + " (" + target.id() + ")";
            faultHandler.accept("Failed to " + description
                + " (attempt " + attempt + ", retrying in " + delay + " ms)", DisklessFutures.unwrap(error));
        }
    }

    /** Re-queues a runner whose backoff elapsed. Always runs on {@link #executor}. */
    private void retry(Runner runner) {
        synchronized (this) {
            if (runner.queued || runner.running) {
                // wake() cancelled this backoff for a newer desired state and re-queued the runner
                // while this task was already past the point of cancellation.
                return;
            }
            runner.retry = null;
            if (!active || closed || runner.generationSeen != generation || runners.get(runner.id) != runner) {
                return;
            }
            runner.queued = true;
            waitingForPermit.add(runner);
            startNextLocked();
        }
    }

    /** The periodic sweep entry point. Always runs on {@link #executor}. */
    private void sweep() {
        MetadataImage image;
        synchronized (this) {
            sweepTask = null;
            if (!active || closed || sweepInFlight) {
                // Either a sweep is already running -- and will reschedule the next one when it
                // finishes -- or this node stopped being the active controller.
                return;
            }
            if (latestImage == null) {
                // No image to sweep against yet. Keep the periodic chain alive rather than ending
                // it here, which would leave this controller with no sweep at all.
                sweepTask = schedule(this::sweep, sweepIntervalMs);
                return;
            }
            image = latestImage;
        }
        startSweep(image);
    }

    /**
     * Deletes storage for every managed topic missing from {@code image}. At most one sweep runs at
     * a time, and each one reschedules the next when it finishes.
     */
    private void startSweep(MetadataImage image) {
        long sweepGeneration;
        synchronized (this) {
            if (!active || closed || sweepInFlight) {
                return;
            }
            if (sweepTask != null) {
                sweepTask.cancel(false);
                sweepTask = null;
            }
            sweepInFlight = true;
            sweepGeneration = generation;
        }
        long imageOffset = image.highestOffsetAndEpoch().offset();
        // Every topic in the image, diskless or not: the sweep deletes storage permanently, so a
        // topic id that Kafka metadata still knows must never be swept, whatever its configuration
        // says now or said when this image was built.
        Set<Uuid> liveTopicIds = new HashSet<>(image.topics().topicsById().keySet());
        log.debug("Sweeping diskless storage for topics missing from metadata image at offset {}", imageOffset);
        CompletableFuture<Void> result;
        try {
            result = lifecycle.sweepOrphans(liveTopicIds, imageOffset);
            if (result == null) {
                throw new IllegalStateException("the diskless topic lifecycle returned a null future");
            }
        } catch (Throwable t) {
            result = CompletableFuture.failedFuture(t);
        }
        withTimeout(result)
            .whenComplete((ignored, error) -> completeSweep(sweepGeneration, error));
    }

    private void completeSweep(long sweepGeneration, Throwable error) {
        synchronized (this) {
            if (sweepGeneration != generation || closed || !active) {
                // A newer generation already reset sweepInFlight and may have started its own sweep.
                return;
            }
            sweepInFlight = false;
            sweepTask = schedule(this::sweep, sweepIntervalMs);
        }
        if (error != null) {
            errors.mark();
            faultHandler.accept("Failed to sweep orphaned diskless topic storage", DisklessFutures.unwrap(error));
        }
    }

    /** Drops every runner, retry and sweep of the generation that just ended. Monitor held. */
    private void cancelAllLocked() {
        for (Runner runner : runners.values()) {
            if (runner.retry != null) {
                runner.retry.cancel(false);
            }
        }
        runners.clear();
        waitingForPermit.clear();
        desired.clear();
        // Operations still running for the old generation return through complete(), which discards
        // them without decrementing, so the permit count belongs entirely to the new generation.
        inFlight = 0;
        if (sweepTask != null) {
            sweepTask.cancel(false);
            sweepTask = null;
        }
        sweepInFlight = false;
    }

    @Override
    public void close() {
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            active = false;
            generation++;
            cancelAllLocked();
        }
        executor.shutdownNow();
        metricsGroup.removeMetric(PENDING_OPERATIONS_METRIC);
        metricsGroup.removeMetric(FAILED_OPERATIONS_METRIC);
        metricsGroup.removeMetric(ERRORS_METRIC);
    }

    /**
     * A view of {@code operation} that fails with a {@link TimeoutException} after
     * {@code operationTimeoutMs}.
     *
     * <p>{@link CompletableFuture#orTimeout} completes the future it is called on, so it is applied
     * to a copy: the storage layer's own future is never force-completed behind its back, and its
     * eventual result still reaches whatever the storage layer chained onto it. When the timeout
     * fires the original is cancelled, but that is best effort -- a {@link CompletableFuture} cannot
     * interrupt the work behind it, so the abandoned attempt may still complete inside the storage
     * layer after the reconciler has released its permit and started the next one. Lifecycle
     * operations are idempotent, which is what makes that safe.
     */
    private CompletableFuture<Void> withTimeout(CompletableFuture<Void> operation) {
        CompletableFuture<Void> timed = operation.copy().orTimeout(operationTimeoutMs, TimeUnit.MILLISECONDS);
        timed.whenComplete((ignored, error) -> {
            if (DisklessFutures.unwrap(error) instanceof TimeoutException) {
                operation.cancel(true);
            }
        });
        return timed;
    }

    /** Runs {@code task} on {@link #executor} after {@code delayMs}, or not at all once closed. */
    private ScheduledFuture<?> schedule(Runnable task, long delayMs) {
        try {
            return executor.schedule(task, delayMs, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            log.debug("Dropping a diskless lifecycle task; the reconciler is shutting down", e);
            return null;
        }
    }

    private static Map<String, String> topicConfigs(MetadataImage image, String topicName) {
        return Map.copyOf(image.configs().configMapForResource(new ConfigResource(ConfigResource.Type.TOPIC, topicName)));
    }

    private synchronized int pendingOperations() {
        return runners.size();
    }

    private synchronized int failedOperations() {
        return (int) runners.values().stream().filter(runner -> runner.attempt > 0).count();
    }

    // Visible for testing.
    int pendingOperationsForTesting() {
        return pendingOperations();
    }
}
