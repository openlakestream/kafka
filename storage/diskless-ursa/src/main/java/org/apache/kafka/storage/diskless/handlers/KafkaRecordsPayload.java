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

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MutableRecordBatch;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.record.internal.RecordBatch;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import io.lakestream.api.LogEntry;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Utilities for storing and reading raw Kafka {@link MemoryRecords} payloads.
 *
 * <p>Validation and rebasing here only ever inspect record <em>batch headers</em>: the
 * control/transactional batch flags, the v2 base offset, and the declared record count. Individual
 * records are never decoded, so a compressed batch is never decompressed and a record's CRC is
 * never recomputed on this path. The storage entry header (offset and record count) is the
 * authoritative source of truth for what was durably appended.
 */
public final class KafkaRecordsPayload {

    private KafkaRecordsPayload() {
    }

    /**
     * Copies records into a reference-counted payload owned by the caller.
     *
     * <p>The copy is required because a Lakestream append is asynchronous and must not borrow the
     * request buffer after the request lifecycle has ended.
     */
    public static ByteBuf copyForAppend(MemoryRecords records) {
        return Unpooled.copiedBuffer(records.buffer().duplicate());
    }

    /** Validates that records can be stored as one complete raw payload with the supplied record count. */
    public static void validateForAppend(MemoryRecords records, int expectedRecordCount) {
        int payloadSize = records.buffer().remaining();
        if (payloadSize == 0) {
            throw new IllegalArgumentException("Kafka append contains no MemoryRecords payload");
        }
        validateHeaders(records, payloadSize, expectedRecordCount);
    }

    /**
     * Copies a storage entry's payload into an independently owned buffer, then validates its
     * batch headers and rebases its batches to the entry's storage offset.
     *
     * <p>The copy is required because {@link LogEntry#payload()} is a read-only view: rebasing
     * writes through {@link MutableRecordBatch#setLastOffset}, which only works on a buffer this
     * method owns. Validation is still header-only (see the class javadoc), so the copy is the
     * only added cost — no record is decoded or its CRC recomputed. For a whole batch of entries,
     * prefer {@link #assemble}, which copies the batch into one shared buffer instead of one
     * buffer per entry.
     */
    public static MemoryRecords readableRecords(ByteBuf payload, long baseOffset, int expectedRecordCount) {
        if (payload == null || !payload.isReadable()) {
            throw new IllegalArgumentException("Kafka storage entry contains no MemoryRecords payload");
        }
        if (expectedRecordCount <= 0) {
            throw new IllegalArgumentException(
                    "Kafka storage entry must contain at least one record, but expected " + expectedRecordCount);
        }
        int size = payload.readableBytes();
        ByteBuffer copy = copyPayload(payload, size);
        MemoryRecords records = MemoryRecords.readableRecords(copy);
        List<Integer> counts = validateHeaders(records, size, expectedRecordCount);
        rebase(records, counts, baseOffset);
        return records;
    }

    private static ByteBuffer copyPayload(ByteBuf payload, int payloadSize) {
        int readerIndex = payload.readerIndex();
        ByteBuffer copy = ByteBuffer.allocate(payloadSize);
        copy.put(payload.nioBuffer(readerIndex, payloadSize).duplicate());
        copy.flip();
        return copy;
    }

    /**
     * Concatenates a sequence of storage entries into one independently owned {@link MemoryRecords},
     * copying each entry's payload exactly once and rebasing its batches to that entry's offset.
     */
    public static MemoryRecords assemble(List<LogEntry> entries) {
        if (entries == null || entries.isEmpty()) {
            return MemoryRecords.EMPTY;
        }
        // Each entry's payload is resolved once: LogEntry.payload() is not required to be cheap or
        // to hand back the same buffer on every call.
        ByteBuf[] payloads = new ByteBuf[entries.size()];
        int total = 0;
        int index = 0;
        for (LogEntry entry : entries) {
            ByteBuf payload = entry.payload();
            payloads[index++] = payload;
            total = Math.addExact(total, payload.readableBytes());
        }
        if (total == 0) {
            return MemoryRecords.EMPTY;
        }

        ByteBuffer combined = ByteBuffer.allocate(total);
        index = 0;
        for (LogEntry entry : entries) {
            ByteBuf payload = payloads[index++];
            int start = combined.position();
            int size = payload.readableBytes();

            ByteBuffer target = combined.duplicate();
            target.position(start);
            target.limit(start + size);
            payload.getBytes(payload.readerIndex(), target);
            combined.position(start + size);

            ByteBuffer entrySlice = combined.duplicate();
            entrySlice.position(start);
            entrySlice.limit(start + size);
            MemoryRecords decoded = MemoryRecords.readableRecords(entrySlice.slice());
            List<Integer> counts = validateHeaders(decoded, size, entry.numberOfRecords());
            rebase(decoded, counts, entry.offset());
        }
        combined.flip();
        return MemoryRecords.readableRecords(combined);
    }

    /** Stamps every batch with the broker's log-append timestamp, in place. */
    public static void setLogAppendTime(MemoryRecords records, long timestamp) {
        for (MutableRecordBatch batch : records.batches()) {
            batch.setMaxTimestamp(TimestampType.LOG_APPEND_TIME, timestamp);
        }
    }

    /**
     * Header-only validation: rejects control and transactional batches, requires v2 batches to
     * declare base offset 0 with a record count consistent with their offset range, and requires
     * the decoded batches to exactly cover {@code payloadSize} bytes and {@code expectedRecordCount}
     * records overall. Never iterates individual records of a v2 batch.
     *
     * @return each batch's record count, in batch order
     */
    private static List<Integer> validateHeaders(MemoryRecords records, int payloadSize, int expectedRecordCount) {
        List<Integer> counts = new ArrayList<>();
        int validBytes = 0;
        int totalRecords = 0;
        for (MutableRecordBatch batch : records.batches()) {
            validateBatchType(batch);
            int count = batch.magic() >= RecordBatch.MAGIC_VALUE_V2 ? validateV2Batch(batch) : countLegacyBatch(batch);
            counts.add(count);
            validBytes = Math.addExact(validBytes, batch.sizeInBytes());
            totalRecords = Math.addExact(totalRecords, count);
        }
        if (counts.isEmpty()) {
            throw new IllegalArgumentException("Kafka MemoryRecords contains no complete record batch");
        }
        if (validBytes != payloadSize) {
            throw new IllegalArgumentException(
                    "Kafka MemoryRecords contains trailing or incomplete bytes: valid="
                            + validBytes + ", payload=" + payloadSize);
        }
        if (totalRecords != expectedRecordCount) {
            throw new IllegalArgumentException(
                    "Kafka record count does not match the storage header: decoded="
                            + totalRecords + ", expected=" + expectedRecordCount);
        }
        return counts;
    }

    private static void validateBatchType(MutableRecordBatch batch) {
        if (batch.isControlBatch()) {
            throw new IllegalArgumentException("Kafka MemoryRecords contains a control batch");
        }
        if (batch.isTransactional()) {
            throw new IllegalArgumentException("Kafka MemoryRecords contains a transactional batch");
        }
    }

    /** Validates a v2+ batch header and returns its declared record count. */
    private static int validateV2Batch(MutableRecordBatch batch) {
        if (batch.baseOffset() != 0L) {
            throw new IllegalArgumentException(
                    "Kafka producer record batch must have base offset 0, but was " + batch.baseOffset());
        }
        Integer declared = batch.countOrNull();
        if (declared == null || declared <= 0) {
            throw new IllegalArgumentException("Kafka record batch declares no records");
        }
        long fromOffsets = batch.lastOffset() - batch.baseOffset() + 1L;
        if (fromOffsets != declared) {
            throw new IllegalArgumentException(
                    "Kafka record batch offset range does not match its count: range="
                            + fromOffsets + ", declared=" + declared);
        }
        return declared;
    }

    /** v0/v1 batches carry no record count in the header, so this is the one path that decodes records. */
    private static int countLegacyBatch(MutableRecordBatch batch) {
        int count = 0;
        for (Iterator<Record> it = batch.iterator(); it.hasNext(); it.next()) {
            count++;
        }
        if (count == 0) {
            throw new IllegalArgumentException("Kafka MemoryRecords contains an empty record batch");
        }
        return count;
    }

    /** Rebases each batch's offsets sequentially from {@code baseOffset}, using each batch's record count. */
    private static void rebase(MemoryRecords records, List<Integer> counts, long baseOffset) {
        long next = baseOffset;
        int i = 0;
        for (MutableRecordBatch batch : records.batches()) {
            long last = Math.addExact(next, counts.get(i++) - 1L);
            batch.setLastOffset(last);
            next = Math.addExact(last, 1L);
        }
    }
}
