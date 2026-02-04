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

import org.apache.kafka.common.record.ControlRecordType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.storage.diskless.handlers.RecordAnalyzer.RecordAnalysisResult;

import org.apache.pulsar.common.api.proto.MarkerType;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * Utility class for encoding and decoding Kafka records with Pulsar MessageMetadata.
 * This enables Ursa storage compaction to work with Kafka records by providing
 * the necessary metadata (producer info, sequence numbers, etc.).
 */
public final class KafkaEntryFormatter {

    private static final Logger log = LoggerFactory.getLogger(KafkaEntryFormatter.class);

    // Property key-value to identify entries as Kafka format
    public static final String ENTRY_FORMAT_KEY = "entry.format";
    public static final String ENTRY_FORMAT_VALUE = "kafka";

    private KafkaEntryFormatter() {
    }

    /**
     * Encodes Kafka MemoryRecords with MessageMetadata for Ursa storage.
     * The encoded format is: [MessageMetadata][Kafka Records]
     *
     * @param records the Kafka records to encode
     * @param analysisResult the analysis result containing metadata from the records
     * @return ByteBuf containing the encoded entry (caller must release)
     */
    public static ByteBuf encode(MemoryRecords records, RecordAnalysisResult analysisResult) {
        ByteBuf recordsWrapper = Unpooled.wrappedBuffer(records.buffer());

        MessageMetadata msgMetadata = buildMessageMetadata(records, analysisResult);

        ByteBuf encoded = Commands.serializeMetadataAndPayload(
                Commands.ChecksumType.None,
                msgMetadata,
                recordsWrapper);

        recordsWrapper.release();

        return encoded;
    }

    /**
     * Decodes an entry to extract the Kafka records payload.
     * The entry format is: [MessageMetadata][Kafka Records]
     *
     * @param entry the ByteBuf containing the encoded entry
     * @return ByteBuffer containing the Kafka records (the entry buffer should not be released by caller)
     */
    public static ByteBuffer decode(ByteBuf entry) {
        // Parse and skip the metadata, returning the payload
        MessageMetadata metadata = Commands.parseMessageMetadata(entry);
        if (log.isTraceEnabled()) {
            log.trace("Decoded entry with metadata: numMessages={}, producerName={}, sequenceId={}",
                    metadata.getNumMessagesInBatch(),
                    metadata.hasProducerName() ? metadata.getProducerName() : "none",
                    metadata.hasSequenceId() ? metadata.getSequenceId() : -1);
        }

        // The entry's reader index now points to the payload (Kafka records)
        return entry.nioBuffer();
    }

    /**
     * Builds MessageMetadata from Kafka records and analysis result.
     */
    private static MessageMetadata buildMessageMetadata(MemoryRecords records, RecordAnalysisResult analysisResult) {
        MessageMetadata metadata = new MessageMetadata();

        // Mark as Kafka entry format
        metadata.addProperty()
                .setKey(ENTRY_FORMAT_KEY)
                .setValue(ENTRY_FORMAT_VALUE);

        // Set publish time (required field). Prefer Kafka max timestamp to align publishTime-based
        // searches (ListOffsets/offsetsForTimes) with Kafka semantics; fall back to wall clock.
        long maxTimestamp = analysisResult.maxTimestamp();
        metadata.setPublishTime(maxTimestamp >= 0 ? maxTimestamp : System.currentTimeMillis());

        // Set number of messages
        metadata.setNumMessagesInBatch(analysisResult.recordCount());

        // Extract producer info from the first batch
        RecordBatch firstBatch = records.firstBatch();
        if (firstBatch != null) {
            setProducerInfo(metadata, firstBatch);
            attachTransactionInfo(metadata, records, firstBatch);
        } else {
            // Default values for required fields when no batch is present
            metadata.setProducerName("kafka-producer");
            metadata.setSequenceId(0);
        }

        return metadata;
    }

    /**
     * Sets producer information in the metadata from a record batch.
     */
    private static void setProducerInfo(MessageMetadata metadata, RecordBatch batch) {
        if (batch.hasProducerId()) {
            // Use producer ID as producer name for compaction
            metadata.setProducerName(String.valueOf(batch.producerId()));

            // Set sequence IDs for idempotent/transactional producers
            if (batch.baseSequence() >= 0) {
                metadata.setSequenceId(batch.baseSequence());
            } else {
                metadata.setSequenceId(0);
            }
            if (batch.lastSequence() >= 0) {
                metadata.setHighestSequenceId(batch.lastSequence());
            }
        } else {
            // Non-idempotent producer - use a placeholder
            metadata.setProducerName("kafka-producer");
            metadata.setSequenceId(0);
        }
    }

    /**
     * Attaches transaction information to the metadata if applicable.
     */
    private static void attachTransactionInfo(MessageMetadata metadata, MemoryRecords records, RecordBatch firstBatch) {
        if (firstBatch.isControlBatch()) {
            // Control batch - extract control record type
            Record record = firstBatch.iterator().next();
            ControlRecordType type = ControlRecordType.parse(record.key());

            // Set marker type for transaction markers
            if (type == ControlRecordType.COMMIT || type == ControlRecordType.ABORT) {
                metadata.setMarkerType(MarkerType.TXN_COMMIT_VALUE);
                metadata.setTxnidMostBits(0L);
                metadata.setTxnidLeastBits(0L);
            }

            // Store transaction metadata
            metadata.addProperty()
                    .setKey("txn.producerId")
                    .setValue(String.valueOf(firstBatch.producerId()));
            metadata.addProperty()
                    .setKey("txn.producerEpoch")
                    .setValue(String.valueOf(firstBatch.producerEpoch()));
            metadata.addProperty()
                    .setKey("txn.controlType")
                    .setValue(type.name());

        } else if (firstBatch.isTransactional()) {
            // Transactional data batch (not control)
            metadata.addProperty()
                    .setKey("txn.producerId")
                    .setValue(String.valueOf(firstBatch.producerId()));
            metadata.addProperty()
                    .setKey("txn.producerEpoch")
                    .setValue(String.valueOf(firstBatch.producerEpoch()));
        }
    }
}
