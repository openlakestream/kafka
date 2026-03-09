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
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.storage.diskless.handlers.RecordAnalyzer.RecordAnalysisResult;

import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

import io.netty.buffer.ByteBuf;

/**
 * Writer implementation using ManagedLedger for storage.
 */
public class UrsaManagedLedgerWriter extends AbstractUrsaStorageWriter {

    private static final Logger log = LoggerFactory.getLogger(UrsaManagedLedgerWriter.class);

    public UrsaManagedLedgerWriter(UrsaStorageState state) {
        super(state);
    }

    @Override
    protected String writerName() {
        return "ManagedLedger";
    }

    @Override
    protected CompletableFuture<PartitionResponse> performIdempotentAppend(
            TopicIdPartition tp,
            MemoryRecords records,
            RecordAnalysisResult analysisResult) {

        return state.getOrCreateManagedLedger(tp)
                .thenCompose(managedLedger -> {
                    ByteBuf data = KafkaEntryFormatter.encode(records, analysisResult);
                    int dataSize = data.readableBytes();

                    log.debug("Appending {} records ({} bytes) to managed ledger {} for partition {}, analysisResult: {}",
                            analysisResult.recordCount(), dataSize, managedLedger.getName(), tp, analysisResult);

                    CompletableFuture<Position> addFuture;
                    try {
                        addFuture = asyncAddEntry(managedLedger, data, analysisResult.recordCount());
                    } catch (Throwable t) {
                        data.release();
                        return CompletableFuture.failedFuture(t);
                    }

                    return addFuture.whenComplete((ignored, error) -> data.release())
                            .thenCompose(position -> {
                                long baseOffset = position.getEntryId();
                                long logAppendTime = state.time().milliseconds();

                                log.debug("Append completed for partition {} with baseOffset {}", tp, baseOffset);

                                return updateStateAfterWrite(tp, records, baseOffset, logAppendTime)
                                        .thenApply(ignored -> new PartitionResponse(Errors.NONE, baseOffset, logAppendTime, 0L));
                            });
                });
    }

    private CompletableFuture<Position> asyncAddEntry(ManagedLedger managedLedger, ByteBuf data, int numberOfMessages) {
        CompletableFuture<Position> future = new CompletableFuture<>();
        managedLedger.asyncAddEntry(data, numberOfMessages, new AsyncCallbacks.AddEntryCallback() {
            @Override
            public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                future.complete(position);
            }

            @Override
            public void addFailed(ManagedLedgerException exception, Object ctx) {
                future.completeExceptionally(exception);
            }
        }, null);
        return future;
    }
}
