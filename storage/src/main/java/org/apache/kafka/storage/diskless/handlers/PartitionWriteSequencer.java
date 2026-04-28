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

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Supplier;

final class PartitionWriteSequencer {

    private static final CompletableFuture<Void> COMPLETED = CompletableFuture.completedFuture(null);
    private static final Executor SEQUENCED_WRITE_EXECUTOR = ForkJoinPool.commonPool();

    private final String ownerName;
    private CompletableFuture<Void> tail;

    PartitionWriteSequencer(String ownerName) {
        this.ownerName = Objects.requireNonNull(ownerName, "ownerName must not be null");
        this.tail = COMPLETED;
    }

    <T> CompletableFuture<T> submit(Supplier<WriteTask<T>> taskSupplier) {
        Objects.requireNonNull(taskSupplier, "taskSupplier must not be null");
        CompletableFuture<Void> newTail = new CompletableFuture<>();
        CompletableFuture<Void> previousTail;
        synchronized (this) {
            previousTail = tail;
            tail = newTail;
        }

        CompletableFuture<T> result = new CompletableFuture<>();
        previousTail.whenCompleteAsync((ignored, previousError) -> {
            WriteTask<T> task;
            try {
                task = taskSupplier.get();
            } catch (Throwable t) {
                result.completeExceptionally(t);
                completeTail(newTail);
                return;
            }

            CompletableFuture<Void> submittedFuture = task.submittedFuture();
            if (submittedFuture == null) {
                submittedFuture = COMPLETED;
            }
            submittedFuture.whenComplete((submittedValue, submittedError) -> completeTail(newTail));

            CompletableFuture<T> taskFuture = task.resultFuture();
            if (taskFuture == null) {
                result.completeExceptionally(new IllegalStateException("Missing write result future for " + ownerName));
                return;
            }

            taskFuture.whenComplete((value, error) -> {
                if (error != null) {
                    result.completeExceptionally(error);
                } else {
                    result.complete(value);
                }
            });
        }, SEQUENCED_WRITE_EXECUTOR);

        return result;
    }

    synchronized void reset() {
        tail = COMPLETED;
    }

    private void completeTail(CompletableFuture<Void> newTail) {
        newTail.complete(null);
        synchronized (this) {
            if (tail == newTail) {
                tail = COMPLETED;
            }
        }
    }

    static final class WriteTask<T> {
        private final CompletableFuture<Void> submittedFuture;
        private final CompletableFuture<T> resultFuture;

        WriteTask(CompletableFuture<Void> submittedFuture, CompletableFuture<T> resultFuture) {
            this.submittedFuture = submittedFuture;
            this.resultFuture = resultFuture;
        }

        CompletableFuture<Void> submittedFuture() {
            return submittedFuture;
        }

        CompletableFuture<T> resultFuture() {
            return resultFuture;
        }
    }
}
