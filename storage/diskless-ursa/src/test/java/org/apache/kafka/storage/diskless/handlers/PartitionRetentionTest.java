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

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.lakestream.api.Log;
import io.lakestream.api.LogOffset;
import io.lakestream.api.exception.LogFencedException;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class PartitionRetentionTest {

    private static final TopicIdPartition TP =
            new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("retention-topic", 0));

    @Test
    void retentionRequestsAreCoalesced() throws Exception {
        Log log = mock(Log.class);
        CompletableFuture<Long> firstTrim = new CompletableFuture<>();
        when(log.getLastOffset()).thenReturn(completedFuture(new LogOffset(10L, 1, 1L)));
        when(log.computeRetentionTrimOffset(10L, 1000L, -1L)).thenReturn(completedFuture(3L));
        when(log.computeRetentionTrimOffset(10L, 500L, -1L)).thenReturn(completedFuture(6L));
        when(log.getFirstOffset()).thenReturn(completedFuture(new LogOffset(0L, 1, 1L)));
        when(log.softTrim(3L)).thenReturn(firstTrim);
        when(log.softTrim(6L)).thenReturn(completedFuture(7L));
        PartitionRetention retention = new PartitionRetention(TP, () -> completedFuture(log), e -> { }, () -> { });

        retention.request(1000L, -1L);
        retention.request(700L, -1L);   // superseded before the first trim finishes
        retention.request(500L, -1L);
        firstTrim.complete(4L);

        verify(log, timeout(5000)).softTrim(6L);
        verify(log, never()).computeRetentionTrimOffset(10L, 700L, -1L);
        retention.close().get(5, TimeUnit.SECONDS);
    }

    @Test
    void disabledRetentionDoesNotInspectTheLog() throws Exception {
        Log log = mock(Log.class);
        PartitionRetention retention = new PartitionRetention(TP, () -> completedFuture(log), e -> { }, () -> { });

        retention.request(-1L, -1L);
        retention.close().get(5, TimeUnit.SECONDS);

        verifyNoInteractions(log);
    }

    @Test
    void trimIsAppliedAtTheComputedOffset() throws Exception {
        Log log = mock(Log.class);
        when(log.getLastOffset()).thenReturn(completedFuture(new LogOffset(42L, 1, 1L)));
        when(log.computeRetentionTrimOffset(42L, 120_000L, 2L * 1024 * 1024))
                .thenReturn(completedFuture(10L));
        when(log.getFirstOffset()).thenReturn(completedFuture(new LogOffset(5L, 1, 1L)));
        when(log.softTrim(10L)).thenReturn(completedFuture(11L));
        PartitionRetention retention = new PartitionRetention(TP, () -> completedFuture(log), e -> { }, () -> { });

        retention.request(120_000L, 2L * 1024 * 1024);
        retention.close().get(5, TimeUnit.SECONDS);

        verify(log).softTrim(10L);
    }

    @Test
    void trimBeforeTheCurrentStartIsSkipped() throws Exception {
        Log log = mock(Log.class);
        when(log.getLastOffset()).thenReturn(completedFuture(new LogOffset(42L, 1, 1L)));
        when(log.computeRetentionTrimOffset(42L, 120_000L, -1L)).thenReturn(completedFuture(4L));
        when(log.getFirstOffset()).thenReturn(completedFuture(new LogOffset(5L, 1, 1L)));
        PartitionRetention retention = new PartitionRetention(TP, () -> completedFuture(log), e -> { }, () -> { });

        retention.request(120_000L, -1L);
        retention.close().get(5, TimeUnit.SECONDS);

        verify(log, never()).softTrim(anyLong());
    }

    @Test
    void anEmptyLogSkipsTheRetentionComputation() throws Exception {
        Log log = mock(Log.class);
        when(log.getLastOffset()).thenReturn(completedFuture(LogOffset.NOT_FOUND));
        PartitionRetention retention = new PartitionRetention(TP, () -> completedFuture(log), e -> { }, () -> { });

        retention.request(120_000L, -1L);
        retention.close().get(5, TimeUnit.SECONDS);

        verify(log, never()).computeRetentionTrimOffset(anyLong(), anyLong(), anyLong());
        verify(log, never()).getFirstOffset();
        verify(log, never()).softTrim(anyLong());
    }

    @Test
    void aMissingFirstOffsetSkipsTheTrim() throws Exception {
        Log log = mock(Log.class);
        when(log.getLastOffset()).thenReturn(completedFuture(new LogOffset(42L, 1, 1L)));
        when(log.computeRetentionTrimOffset(42L, 120_000L, -1L)).thenReturn(completedFuture(10L));
        when(log.getFirstOffset()).thenReturn(completedFuture(null));
        PartitionRetention retention = new PartitionRetention(TP, () -> completedFuture(log), e -> { }, () -> { });

        retention.request(120_000L, -1L);
        retention.close().get(5, TimeUnit.SECONDS);

        verify(log, never()).softTrim(anyLong());
    }

    @Test
    void aFailedRetentionRunIsSwallowedAndDoesNotStopLaterRuns() throws Exception {
        Log log = mock(Log.class);
        when(log.getLastOffset())
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("boom")))
                .thenReturn(completedFuture(new LogOffset(42L, 1, 1L)));
        when(log.computeRetentionTrimOffset(42L, 120_000L, -1L)).thenReturn(completedFuture(10L));
        when(log.getFirstOffset()).thenReturn(completedFuture(new LogOffset(5L, 1, 1L)));
        when(log.softTrim(10L)).thenReturn(completedFuture(11L));
        PartitionRetention retention = new PartitionRetention(TP, () -> completedFuture(log), e -> { }, () -> { });

        retention.request(120_000L, -1L);
        retention.request(120_000L, -1L);

        verify(log, timeout(5000)).softTrim(10L);
        retention.close().get(5, TimeUnit.SECONDS);
    }

    @Test
    void aCompletedTrimDropsTheReadersCachedOffsetWindow() throws Exception {
        Log log = mock(Log.class);
        AtomicInteger trims = new AtomicInteger();
        when(log.getLastOffset()).thenReturn(completedFuture(new LogOffset(42L, 1, 1L)));
        when(log.computeRetentionTrimOffset(42L, 120_000L, -1L)).thenReturn(completedFuture(10L));
        when(log.getFirstOffset()).thenReturn(completedFuture(new LogOffset(5L, 1, 1L)));
        when(log.softTrim(10L)).thenReturn(completedFuture(11L));
        PartitionRetention retention = new PartitionRetention(
                TP, () -> completedFuture(log), e -> { }, trims::incrementAndGet);

        retention.request(120_000L, -1L);
        retention.close().get(5, TimeUnit.SECONDS);

        // The trim moved the log's first offset, so a window cached before it is now wrong.
        verify(log).softTrim(10L);
        assertEquals(1, trims.get());
    }

    @Test
    void aTrimThatNeverRanLeavesTheCachedOffsetWindowAlone() throws Exception {
        Log log = mock(Log.class);
        AtomicInteger trims = new AtomicInteger();
        when(log.getLastOffset()).thenReturn(completedFuture(new LogOffset(42L, 1, 1L)));
        when(log.computeRetentionTrimOffset(42L, 120_000L, -1L)).thenReturn(completedFuture(4L));
        when(log.getFirstOffset()).thenReturn(completedFuture(new LogOffset(5L, 1, 1L)));
        PartitionRetention retention = new PartitionRetention(
                TP, () -> completedFuture(log), e -> { }, trims::incrementAndGet);

        retention.request(120_000L, -1L);
        retention.close().get(5, TimeUnit.SECONDS);

        verify(log, never()).softTrim(anyLong());
        assertEquals(0, trims.get());
    }

    @Test
    void aFencedLogIsReportedToTheFenceCallback() throws Exception {
        Log log = mock(Log.class);
        AtomicReference<Throwable> fenced = new AtomicReference<>();
        when(log.getLastOffset())
                .thenReturn(CompletableFuture.failedFuture(new LogFencedException("fenced")));
        PartitionRetention retention = new PartitionRetention(TP, () -> completedFuture(log), fenced::set, () -> { });

        retention.request(120_000L, -1L);
        retention.close().get(5, TimeUnit.SECONDS);

        assertInstanceOf(LogFencedException.class, fenced.get());
    }

    @Test
    void aRunFencedAfterCloseStillReachesTheFenceCallback() throws Exception {
        Log log = mock(Log.class);
        AtomicReference<Throwable> fenced = new AtomicReference<>();
        CompletableFuture<LogOffset> lastOffset = new CompletableFuture<>();
        when(log.getLastOffset()).thenReturn(lastOffset);
        PartitionRetention retention = new PartitionRetention(TP, () -> completedFuture(log), fenced::set, () -> { });

        retention.request(120_000L, -1L);
        CompletableFuture<Void> closeFuture = retention.close();
        assertFalse(closeFuture.isDone(), "close must not complete while a run is in flight");

        // A run that loses its race with close() is not warned about, but a fence still has to
        // reach the callback: it is how the partition log learns it lost ownership.
        lastOffset.completeExceptionally(new LogFencedException("fenced"));

        closeFuture.get(5, TimeUnit.SECONDS);
        assertInstanceOf(LogFencedException.class, fenced.get());
    }

    @Test
    void closeWaitsForTheInFlightTrimAndDropsLaterTrims() throws Exception {
        Log log = mock(Log.class);
        CompletableFuture<Long> trimOffset = new CompletableFuture<>();
        when(log.getLastOffset()).thenReturn(completedFuture(new LogOffset(42L, 1, 1L)));
        when(log.computeRetentionTrimOffset(42L, 120_000L, -1L)).thenReturn(trimOffset);
        when(log.getFirstOffset()).thenReturn(completedFuture(new LogOffset(5L, 1, 1L)));
        PartitionRetention retention = new PartitionRetention(TP, () -> completedFuture(log), e -> { }, () -> { });

        retention.request(120_000L, -1L);
        verify(log).computeRetentionTrimOffset(42L, 120_000L, -1L);

        CompletableFuture<Void> closeFuture = retention.close();
        assertFalse(closeFuture.isDone(), "close must not complete while a run is in flight");

        trimOffset.complete(10L);
        closeFuture.get(5, TimeUnit.SECONDS);
        verify(log, never()).softTrim(anyLong());

        retention.request(120_000L, -1L);
        verify(log, never()).softTrim(anyLong());
    }
}
