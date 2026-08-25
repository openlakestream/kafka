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

import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.record.internal.CompressionType;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MemoryRecordsBuilder;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.storage.diskless.handlers.RecordAnalyzer.RecordAnalysisResult;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RecordAnalyzerTest {

    private static final TopicPartition TEST_PARTITION = new TopicPartition("test-topic", 0);

    @Test
    void testAnalyzeValidRecords() {
        long timestamp = System.currentTimeMillis();
        MemoryRecords records = MemoryRecords.withRecords(
                Compression.NONE,
                new SimpleRecord(timestamp, "key".getBytes(StandardCharsets.UTF_8),
                        "value".getBytes(StandardCharsets.UTF_8))
        );

        RecordAnalysisResult result = RecordAnalyzer.analyzeAndValidateRecords(
                records, TEST_PARTITION, 0);

        assertNotNull(result);
        assertEquals(0L, result.firstOffset());
        assertEquals(0L, result.lastOffset());
        assertTrue(result.validBytes() > 0);
        assertEquals(CompressionType.NONE, result.sourceCompression());
        assertTrue(result.maxTimestamp() > 0);
        assertEquals(1, result.recordCount());
    }

    @Test
    void testAnalyzeMultipleRecords() {
        long timestamp = System.currentTimeMillis();
        MemoryRecords records = MemoryRecords.withRecords(
                Compression.NONE,
                new SimpleRecord(timestamp, "key1".getBytes(), "value1".getBytes()),
                new SimpleRecord(timestamp + 1, "key2".getBytes(), "value2".getBytes()),
                new SimpleRecord(timestamp + 2, "key3".getBytes(), "value3".getBytes())
        );

        RecordAnalysisResult result = RecordAnalyzer.analyzeAndValidateRecords(
                records, TEST_PARTITION, 0);

        assertNotNull(result);
        assertEquals(0L, result.firstOffset());
        assertEquals(2L, result.lastOffset());
        assertEquals(3, result.numMessages());
        assertEquals(3, result.recordCount());
        assertEquals(timestamp + 2, result.maxTimestamp());
    }

    @Test
    void testAnalyzeEmptyRecords() {
        MemoryRecords records = MemoryRecords.EMPTY;

        RecordAnalysisResult result = RecordAnalyzer.analyzeAndValidateRecords(
                records, TEST_PARTITION, 0);

        assertNotNull(result);
        assertEquals(-1L, result.firstOffset());
        assertEquals(-1L, result.lastOffset());
        assertEquals(0, result.validBytes());
        assertEquals(0, result.numMessages());
        assertEquals(0, result.recordCount());
    }

    @Test
    void testAnalyzeCompressedRecords() {
        long timestamp = System.currentTimeMillis();
        MemoryRecords records = MemoryRecords.withRecords(
                Compression.gzip().build(),
                new SimpleRecord(timestamp, "key".getBytes(), "value".getBytes())
        );

        RecordAnalysisResult result = RecordAnalyzer.analyzeAndValidateRecords(
                records, TEST_PARTITION, 0);

        assertNotNull(result);
        assertEquals(CompressionType.GZIP, result.sourceCompression());
        assertTrue(result.validBytes() > 0);
    }

    @Test
    void testRejectsRecordTooLarge() {
        long timestamp = System.currentTimeMillis();
        byte[] largeValue = new byte[1024];
        MemoryRecords records = MemoryRecords.withRecords(
                Compression.NONE,
                new SimpleRecord(timestamp, "key".getBytes(), largeValue)
        );

        // Set maxMessageSize to a small value
        assertThrows(RecordTooLargeException.class, () ->
                RecordAnalyzer.analyzeAndValidateRecords(records, TEST_PARTITION, 100));
    }

    @Test
    void testSkipsSizeCheckWhenMaxMessageSizeIsZero() {
        long timestamp = System.currentTimeMillis();
        byte[] largeValue = new byte[1024];
        MemoryRecords records = MemoryRecords.withRecords(
                Compression.NONE,
                new SimpleRecord(timestamp, "key".getBytes(), largeValue)
        );

        // maxMessageSize = 0 should skip size check
        RecordAnalysisResult result = RecordAnalyzer.analyzeAndValidateRecords(
                records, TEST_PARTITION, 0);

        assertNotNull(result);
        assertTrue(result.validBytes() > 0);
    }

    @Test
    void testRejectsNonZeroBaseOffsetForV2Batch() {
        // Create a batch with non-zero baseOffset
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        MemoryRecordsBuilder builder = MemoryRecords.builder(
                buffer,
                RecordBatch.MAGIC_VALUE_V2,
                Compression.NONE,
                TimestampType.CREATE_TIME,
                100L, // non-zero baseOffset
                System.currentTimeMillis(),
                RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH,
                RecordBatch.NO_SEQUENCE
        );
        builder.append(new SimpleRecord(System.currentTimeMillis(), "key".getBytes(), "value".getBytes()));
        MemoryRecords records = builder.build();

        assertThrows(InvalidRecordException.class, () ->
                RecordAnalyzer.analyzeAndValidateRecords(records, TEST_PARTITION, 0));
    }

    @Test
    void testAcceptsZeroBaseOffsetForV2Batch() {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        MemoryRecordsBuilder builder = MemoryRecords.builder(
                buffer,
                RecordBatch.MAGIC_VALUE_V2,
                Compression.NONE,
                TimestampType.CREATE_TIME,
                0L, // zero baseOffset
                System.currentTimeMillis(),
                RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH,
                RecordBatch.NO_SEQUENCE
        );
        builder.append(new SimpleRecord(System.currentTimeMillis(), "key".getBytes(), "value".getBytes()));
        MemoryRecords records = builder.build();

        RecordAnalysisResult result = RecordAnalyzer.analyzeAndValidateRecords(
                records, TEST_PARTITION, 0);

        assertNotNull(result);
        assertEquals(0L, result.firstOffset());
    }

    @Test
    void testRecordAnalysisResultToString() {
        long timestamp = System.currentTimeMillis();
        MemoryRecords records = MemoryRecords.withRecords(
                Compression.NONE,
                new SimpleRecord(timestamp, "key".getBytes(), "value".getBytes())
        );

        RecordAnalysisResult result = RecordAnalyzer.analyzeAndValidateRecords(
                records, TEST_PARTITION, 0);

        String toString = result.toString();
        assertTrue(toString.contains("RecordAnalysisResult"));
        assertTrue(toString.contains("firstOffset=0"));
        assertTrue(toString.contains("lastOffset=0"));
        assertTrue(toString.contains("validBytes="));
    }

    @Test
    void testFirstOrLastOffsetOfFirstBatch() {
        long timestamp = System.currentTimeMillis();
        MemoryRecords records = MemoryRecords.withRecords(
                Compression.NONE,
                new SimpleRecord(timestamp, "key".getBytes(), "value".getBytes())
        );

        RecordAnalysisResult result = RecordAnalyzer.analyzeAndValidateRecords(
                records, TEST_PARTITION, 0);

        // For V2 batches, firstOffset should be set
        assertEquals(0L, result.firstOrLastOffsetOfFirstBatch());
    }

    @Test
    void testIdempotentRecordsAnalysis() {
        long timestamp = System.currentTimeMillis();
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        MemoryRecordsBuilder builder = MemoryRecords.builder(
                buffer,
                RecordBatch.MAGIC_VALUE_V2,
                Compression.NONE,
                TimestampType.CREATE_TIME,
                0L,
                timestamp,
                1000L, // producerId
                (short) 0, // epoch
                0 // baseSequence
        );
        builder.append(new SimpleRecord(timestamp, "key".getBytes(), "value".getBytes()));
        MemoryRecords records = builder.build();

        RecordAnalysisResult result = RecordAnalyzer.analyzeAndValidateRecords(
                records, TEST_PARTITION, 0);

        assertNotNull(result);
        assertEquals(0L, result.firstOffset());
        assertEquals(0L, result.lastOffset());
        assertTrue(result.validBytes() > 0);
    }

    @Test
    void testMultipleBatchesAnalysis() {
        long timestamp = System.currentTimeMillis();

        // Create first batch
        ByteBuffer buffer1 = ByteBuffer.allocate(512);
        MemoryRecordsBuilder builder1 = MemoryRecords.builder(
                buffer1,
                RecordBatch.MAGIC_VALUE_V2,
                Compression.NONE,
                TimestampType.CREATE_TIME,
                0L,
                timestamp,
                RecordBatch.NO_PRODUCER_ID,
                RecordBatch.NO_PRODUCER_EPOCH,
                RecordBatch.NO_SEQUENCE
        );
        builder1.append(new SimpleRecord(timestamp, "key1".getBytes(), "value1".getBytes()));
        builder1.append(new SimpleRecord(timestamp, "key2".getBytes(), "value2".getBytes()));
        MemoryRecords batch1 = builder1.build();

        RecordAnalysisResult result = RecordAnalyzer.analyzeAndValidateRecords(
                batch1, TEST_PARTITION, 0);

        assertNotNull(result);
        assertEquals(0L, result.firstOffset());
        assertEquals(1L, result.lastOffset());
        assertEquals(2, result.numMessages());
    }
}
