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

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * Metadata for one producer batch.
 */
public final class BatchMetadata {

    private final int firstSeq;
    private final int lastSeq;
    private final CompletableFuture<Long> baseOffsetFuture;
    private volatile long timestamp;

    public BatchMetadata(int firstSeq, int lastSeq, CompletableFuture<Long> baseOffsetFuture, long timestamp) {
        this.firstSeq = firstSeq;
        this.lastSeq = lastSeq;
        this.baseOffsetFuture = Objects.requireNonNull(baseOffsetFuture, "baseOffsetFuture must not be null");
        this.timestamp = timestamp;
    }

    public int firstSeq() {
        return firstSeq;
    }

    public int lastSeq() {
        return lastSeq;
    }

    public CompletableFuture<Long> baseOffsetFuture() {
        return baseOffsetFuture;
    }

    public long timestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public int numMessages() {
        return numMessages(firstSeq, lastSeq);
    }

    public static int numMessages(int firstSeq, int lastSeq) {
        if (firstSeq <= lastSeq) {
            return lastSeq - firstSeq + 1;
        }
        return lastSeq + (Integer.MAX_VALUE - firstSeq) + 2;
    }

    @Override
    public String toString() {
        return "{firstSeq=" + firstSeq
            + ", lastSeq=" + lastSeq
            + ", timestamp=" + timestamp
            + ", offset=" + offsetOrDefault(-1L)
            + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BatchMetadata that)) {
            return false;
        }
        return firstSeq == that.firstSeq
            && lastSeq == that.lastSeq
            && timestamp == that.timestamp
            && offsetOrDefault(-1L) == that.offsetOrDefault(-1L);
    }

    @Override
    public int hashCode() {
        return Objects.hash(firstSeq, lastSeq, offsetOrDefault(-1L), timestamp);
    }

    private long offsetOrDefault(long defaultValue) {
        if (!baseOffsetFuture.isDone() || baseOffsetFuture.isCompletedExceptionally()) {
            return defaultValue;
        }
        return baseOffsetFuture.getNow(defaultValue);
    }
}
