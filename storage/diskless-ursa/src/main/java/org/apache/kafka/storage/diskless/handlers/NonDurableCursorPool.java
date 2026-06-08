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

import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A bounded pool for reusing non-durable cursors against a single {@link ManagedLedger}.
 *
 * <p>Each acquired cursor is exclusive to the caller until the lease is closed. When the pool is exhausted,
 * callers are queued (FIFO) until a cursor is released.
 *
 * <p>The pool intentionally uses a fixed set of cursor names to avoid unbounded cursor creation on the
 * managed-ledger implementation which caches cursors by name.
 */
final class NonDurableCursorPool implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(NonDurableCursorPool.class);

    private static final ManagedLedgerException POOL_CLOSED_EXCEPTION =
            new ManagedLedgerException("Non-durable cursor pool is closed");

    private final ManagedLedger managedLedger;
    private final String cursorNamePrefix;
    private final int maxSize;
    private final ArrayDeque<Slot> idle = new ArrayDeque<>();
    private final ArrayDeque<Waiter> waiters = new ArrayDeque<>();

    private boolean closed = false;

    NonDurableCursorPool(ManagedLedger managedLedger, String cursorNamePrefix, int maxSize) {
        if (managedLedger == null) {
            throw new IllegalArgumentException("managedLedger must not be null");
        }
        if (cursorNamePrefix == null || cursorNamePrefix.isBlank()) {
            throw new IllegalArgumentException("cursorNamePrefix must not be null or blank");
        }
        if (maxSize <= 0) {
            throw new IllegalArgumentException("maxSize must be > 0");
        }
        this.managedLedger = managedLedger;
        this.cursorNamePrefix = cursorNamePrefix;
        this.maxSize = maxSize;
        for (int i = 0; i < maxSize; i++) {
            idle.add(new Slot(i));
        }
    }

    CompletableFuture<Lease> acquire(Position startPosition) {
        Slot slot;
        synchronized (this) {
            if (closed) {
                return CompletableFuture.failedFuture(POOL_CLOSED_EXCEPTION);
            }

            slot = idle.poll();
            if (slot == null) {
                Waiter waiter = new Waiter(startPosition);
                waiters.add(waiter);
                return waiter.future;
            }
        }

        return prepareLease(slot, startPosition)
                .whenComplete((lease, error) -> {
                    if (error != null) {
                        releaseSlot(slot);
                    }
                });
    }

    @Override
    public void close() {
        List<Waiter> toFail;
        List<ManagedCursor> toClose;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            toFail = new ArrayList<>(waiters);
            waiters.clear();
            toClose = new ArrayList<>(idle.size());
            for (Slot slot : idle) {
                if (slot.cursor != null) {
                    toClose.add(slot.cursor);
                }
                slot.cursor = null;
            }
            idle.clear();
        }

        for (Waiter waiter : toFail) {
            waiter.future.completeExceptionally(POOL_CLOSED_EXCEPTION);
        }

        for (ManagedCursor cursor : toClose) {
            try {
                cursor.close();
            } catch (Exception e) {
                log.warn("Failed to close pooled cursor for ledger {}", managedLedger.getName(), e);
            }
        }
    }

    private CompletableFuture<Lease> prepareLease(Slot slot, Position startPosition) {
        ManagedCursor cursor = slot.cursor;
        if (cursor == null) {
            try {
                cursor = managedLedger.newNonDurableCursor(startPosition, cursorName(slot.index));
            } catch (ManagedLedgerException e) {
                return CompletableFuture.failedFuture(e);
            }
            slot.cursor = cursor;
            return CompletableFuture.completedFuture(new Lease(this, slot));
        }

        if (isCursorAtPosition(cursor, startPosition)) {
            return CompletableFuture.completedFuture(new Lease(this, slot));
        }

        return resetCursor(cursor, startPosition)
                .thenApply(ignored -> new Lease(this, slot));
    }

    private boolean isCursorAtPosition(ManagedCursor cursor, Position startPosition) {
        try {
            Position readPosition = cursor.getReadPosition();
            return readPosition != null && readPosition.compareTo(startPosition) == 0;
        } catch (Throwable t) {
            return false;
        }
    }

    private CompletableFuture<Void> resetCursor(ManagedCursor cursor, Position startPosition) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            cursor.asyncResetCursor(startPosition, false, new AsyncCallbacks.ResetCursorCallback() {
                @Override
                public void resetComplete(Object ctx) {
                    future.complete(null);
                }

                @Override
                public void resetFailed(ManagedLedgerException exception, Object ctx) {
                    future.completeExceptionally(exception);
                }
            });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    private void release(Lease lease) {
        if (!lease.released.compareAndSet(false, true)) {
            return;
        }
        releaseSlot(lease.slot);
    }

    private void releaseSlot(Slot slot) {
        if (slot == null) {
            return;
        }

        Waiter waiter;
        synchronized (this) {
            if (closed) {
                ManagedCursor cursor = slot.cursor;
                slot.cursor = null;
                if (cursor != null) {
                    try {
                        cursor.close();
                    } catch (Exception e) {
                        log.warn("Failed to close pooled cursor for ledger {}", managedLedger.getName(), e);
                    }
                }
                return;
            }

            waiter = pollNextWaiterLocked();
            if (waiter == null) {
                idle.add(slot);
                return;
            }
        }

        prepareLease(slot, waiter.startPosition)
                .whenComplete((lease, error) -> {
                    if (error != null) {
                        waiter.future.completeExceptionally(error);
                        releaseSlot(slot);
                    } else {
                        waiter.future.complete(lease);
                    }
                });
    }

    private Waiter pollNextWaiterLocked() {
        while (true) {
            Waiter waiter = waiters.poll();
            if (waiter == null) {
                return null;
            }
            if (!waiter.future.isDone()) {
                return waiter;
            }
        }
    }

    private String cursorName(int slotIndex) {
        return cursorNamePrefix + "-" + slotIndex;
    }

    private static final class Slot {
        private final int index;
        private ManagedCursor cursor;

        private Slot(int index) {
            this.index = index;
        }
    }

    private static final class Waiter {
        private final Position startPosition;
        private final CompletableFuture<Lease> future = new CompletableFuture<>();

        private Waiter(Position startPosition) {
            this.startPosition = startPosition;
        }
    }

    static final class Lease implements AutoCloseable {
        private final NonDurableCursorPool pool;
        private final Slot slot;
        private final AtomicBoolean released = new AtomicBoolean(false);

        private Lease(NonDurableCursorPool pool, Slot slot) {
            this.pool = pool;
            this.slot = slot;
        }

        ManagedCursor cursor() {
            return slot.cursor;
        }

        @Override
        public void close() {
            pool.release(this);
        }
    }
}
