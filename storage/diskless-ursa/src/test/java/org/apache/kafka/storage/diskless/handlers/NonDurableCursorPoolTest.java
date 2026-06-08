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
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class NonDurableCursorPoolTest {

    @Test
    void testAcquireSkipsResetWhenCursorAlreadyAtPosition() throws Exception {
        ManagedLedger ledger = mock(ManagedLedger.class);
        ManagedCursor cursor = mock(ManagedCursor.class);

        Position startPosition = PositionFactory.create(/* ledgerId */ 1L, /* entryId */ 7L);
        when(ledger.newNonDurableCursor(eq(startPosition), anyString())).thenReturn(cursor);
        when(cursor.getReadPosition()).thenReturn(startPosition);

        AtomicInteger resetCalls = new AtomicInteger(0);
        doAnswer(invocation -> {
            resetCalls.incrementAndGet();
            AsyncCallbacks.ResetCursorCallback callback = invocation.getArgument(2);
            callback.resetComplete(null);
            return null;
        }).when(cursor).asyncResetCursor(any(Position.class), eq(false), any(AsyncCallbacks.ResetCursorCallback.class));

        NonDurableCursorPool pool = new NonDurableCursorPool(ledger, "test-cursor", 1);

        try (NonDurableCursorPool.Lease lease = pool.acquire(startPosition).get()) {
            assertNotNull(lease.cursor());
        }

        try (NonDurableCursorPool.Lease lease = pool.acquire(startPosition).get()) {
            assertNotNull(lease.cursor());
        }

        verify(ledger, times(1)).newNonDurableCursor(eq(startPosition), anyString());
        verify(cursor, times(0)).asyncResetCursor(any(Position.class), eq(false), any(AsyncCallbacks.ResetCursorCallback.class));
    }
}
