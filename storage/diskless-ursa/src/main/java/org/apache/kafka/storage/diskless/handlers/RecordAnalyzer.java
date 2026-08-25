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
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.record.internal.CompressionType;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.MutableRecordBatch;
import org.apache.kafka.common.record.internal.RecordBatch;

import java.util.Optional;

/**
 * Utility class for analyzing and validating records before appending to Ursa storage.
 * This mirrors the validation logic from UnifiedLog.analyzeAndValidateRecords but adapted
 * for the Ursa diskless storage path.
 */
public final class RecordAnalyzer {

    private static final long UNKNOWN_OFFSET = -1L;

    private RecordAnalyzer() {
    }

    /**
     * Result of analyzing and validating records.
     * This is a lightweight alternative to LogAppendInfo for the diskless storage path.
     */
    public static final class RecordAnalysisResult {
        private final long firstOffset;
        private final long lastOffset;
        private final Optional<Integer> lastLeaderEpoch;
        private final long maxTimestamp;
        private final CompressionType sourceCompression;
        private final int validBytes;
        private final long lastOffsetOfFirstBatch;
        private final int recordCount;

        public RecordAnalysisResult(
                long firstOffset,
                long lastOffset,
                Optional<Integer> lastLeaderEpoch,
                long maxTimestamp,
                CompressionType sourceCompression,
                int validBytes,
                long lastOffsetOfFirstBatch,
                int recordCount) {
            this.firstOffset = firstOffset;
            this.lastOffset = lastOffset;
            this.lastLeaderEpoch = lastLeaderEpoch;
            this.maxTimestamp = maxTimestamp;
            this.sourceCompression = sourceCompression;
            this.validBytes = validBytes;
            this.lastOffsetOfFirstBatch = lastOffsetOfFirstBatch;
            this.recordCount = recordCount;
        }

        public long firstOffset() {
            return firstOffset;
        }

        public long lastOffset() {
            return lastOffset;
        }

        public Optional<Integer> lastLeaderEpoch() {
            return lastLeaderEpoch;
        }

        public long maxTimestamp() {
            return maxTimestamp;
        }

        public CompressionType sourceCompression() {
            return sourceCompression;
        }

        public int validBytes() {
            return validBytes;
        }

        public long lastOffsetOfFirstBatch() {
            return lastOffsetOfFirstBatch;
        }

        /**
         * Get the total number of records across all batches.
         */
        public int recordCount() {
            return recordCount;
        }

        /**
         * Get the first offset if it exists, else get the last offset of the first batch.
         */
        public long firstOrLastOffsetOfFirstBatch() {
            return firstOffset >= 0 ? firstOffset : lastOffsetOfFirstBatch;
        }

        /**
         * Get the (maximum) number of messages described by this result.
         */
        public long numMessages() {
            if (firstOffset >= 0 && lastOffset >= 0) {
                return lastOffset - firstOffset + 1;
            }
            return 0;
        }

        @Override
        public String toString() {
            return "RecordAnalysisResult(" +
                    "firstOffset=" + firstOffset +
                    ", lastOffset=" + lastOffset +
                    ", lastLeaderEpoch=" + lastLeaderEpoch +
                    ", maxTimestamp=" + maxTimestamp +
                    ", sourceCompression=" + sourceCompression +
                    ", validBytes=" + validBytes +
                    ", lastOffsetOfFirstBatch=" + lastOffsetOfFirstBatch +
                    ", recordCount=" + recordCount +
                    ')';
        }
    }

    /**
     * Analyze and validate records for appending to storage.
     * <p>
     * Validates the following:
     * <ol>
     * <li>Each message matches its CRC</li>
     * <li>Each message size is valid (if maxMessageSize > 0)</li>
     * <li>Client requests have baseOffset == 0 for V2+ batches</li>
     * </ol>
     * <p>
     * Also computes:
     * <ol>
     * <li>First offset in the message set</li>
     * <li>Last offset in the message set</li>
     * <li>Number of valid bytes</li>
     * <li>Maximum timestamp</li>
     * <li>Source compression type</li>
     * <li>Total record count</li>
     * </ol>
     *
     * @param records        The records to analyze
     * @param topicPartition The topic partition (for error messages)
     * @param maxMessageSize Maximum allowed message size (0 or negative to skip size check)
     * @return RecordAnalysisResult containing validation results and computed metadata
     * @throws CorruptRecordException  if CRC validation fails
     * @throws RecordTooLargeException if a batch exceeds maxMessageSize
     * @throws InvalidRecordException  if baseOffset validation fails for client requests
     */
    public static RecordAnalysisResult analyzeAndValidateRecords(
            MemoryRecords records,
            TopicPartition topicPartition,
            int maxMessageSize) {

        AnalysisState state = new AnalysisState();

        for (MutableRecordBatch batch : records.batches()) {
            validateBatch(batch, topicPartition, maxMessageSize);
            state.processBatch(batch);
        }

        return state.buildResult();
    }

    private static void validateBatch(MutableRecordBatch batch, TopicPartition topicPartition, int maxMessageSize) {
        validateBaseOffset(batch, topicPartition);
        validateMessageSize(batch, topicPartition, maxMessageSize);
        validateCrc(batch, topicPartition);
    }

    private static void validateBaseOffset(MutableRecordBatch batch, TopicPartition topicPartition) {
        if (batch.magic() >= RecordBatch.MAGIC_VALUE_V2 && batch.baseOffset() != 0) {
            throw new InvalidRecordException("The baseOffset of the record batch in the append to "
                    + topicPartition + " should be 0, but it is " + batch.baseOffset());
        }
    }

    private static void validateMessageSize(MutableRecordBatch batch, TopicPartition topicPartition, int maxMessageSize) {
        int batchSize = batch.sizeInBytes();
        if (maxMessageSize > 0 && batchSize > maxMessageSize) {
            throw new RecordTooLargeException("The record batch size in the append to " + topicPartition
                    + " is " + batchSize + " bytes which exceeds the maximum configured value of "
                    + maxMessageSize + ").");
        }
    }

    private static void validateCrc(MutableRecordBatch batch, TopicPartition topicPartition) {
        if (!batch.isValid()) {
            throw new CorruptRecordException("Record is corrupt (stored crc = " + batch.checksum()
                    + ") in topic partition " + topicPartition + ".");
        }
    }

    /**
     * Mutable state holder for batch analysis to reduce NPath complexity.
     */
    private static final class AnalysisState {
        private int validBytesCount = 0;
        private int recordCount = 0;
        private long firstOffset = UNKNOWN_OFFSET;
        private long lastOffset = -1L;
        private int lastLeaderEpoch = RecordBatch.NO_PARTITION_LEADER_EPOCH;
        private CompressionType sourceCompression = CompressionType.NONE;
        private long maxTimestamp = RecordBatch.NO_TIMESTAMP;
        private boolean readFirstMessage = false;
        private long lastOffsetOfFirstBatch = -1L;

        void processBatch(MutableRecordBatch batch) {
            trackFirstBatch(batch);
            updateLastOffset(batch);
            updateMaxTimestamp(batch);
            updateCompression(batch);
            updateRecordCount(batch);
            validBytesCount += batch.sizeInBytes();
        }

        private void trackFirstBatch(MutableRecordBatch batch) {
            if (!readFirstMessage) {
                if (batch.magic() >= RecordBatch.MAGIC_VALUE_V2) {
                    firstOffset = batch.baseOffset();
                }
                lastOffsetOfFirstBatch = batch.lastOffset();
                readFirstMessage = true;
            }
        }

        private void updateLastOffset(MutableRecordBatch batch) {
            lastOffset = batch.lastOffset();
            lastLeaderEpoch = batch.partitionLeaderEpoch();
        }

        private void updateMaxTimestamp(MutableRecordBatch batch) {
            if (batch.maxTimestamp() > maxTimestamp) {
                maxTimestamp = batch.maxTimestamp();
            }
        }

        private void updateCompression(MutableRecordBatch batch) {
            if (batch.compressionType() != CompressionType.NONE) {
                sourceCompression = batch.compressionType();
            }
        }

        private void updateRecordCount(MutableRecordBatch batch) {
            Integer batchRecordCount = batch.countOrNull();
            if (batchRecordCount != null) {
                recordCount += batchRecordCount;
            }
        }

        RecordAnalysisResult buildResult() {
            Optional<Integer> leaderEpoch = lastLeaderEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH
                    ? Optional.empty()
                    : Optional.of(lastLeaderEpoch);

            return new RecordAnalysisResult(
                    firstOffset,
                    lastOffset,
                    leaderEpoch,
                    maxTimestamp,
                    sourceCompression,
                    validBytesCount,
                    lastOffsetOfFirstBatch,
                    recordCount
            );
        }
    }
}
