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
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.server.config.ServerLogConfigs;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import io.lakestream.api.Log;
import io.lakestream.api.Partitioning;
import io.lakestream.api.StreamCatalog;
import io.lakestream.api.StreamIdentifier;
import io.lakestream.api.StreamMetadata;
import io.lakestream.api.exception.AlreadyExistsException;
import io.lakestream.api.exception.NoSuchStreamException;
import io.lakestream.api.exception.StreamPermanentlyDeletedException;
import io.oxia.client.api.AsyncOxiaClient;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LakestreamStorageHolderTest {

    private static final Uuid TOPIC_ID = Uuid.randomUuid();

    @Test
    void openPartitionCreatesStreamWhenAbsent() throws Exception {
        StreamCatalog catalog = mock(StreamCatalog.class);
        Log log = mock(Log.class);
        StreamIdentifier id = KafkaStreamIdentity.streamIdentifier("orders", TOPIC_ID);
        when(catalog.openLog(id, 1))
            .thenReturn(failedFuture(new NoSuchStreamException(id)))
            .thenReturn(completedFuture(log));
        when(catalog.createStream(eq(id), any(), any(), any(), anyMap()))
            .thenReturn(completedFuture(mock(StreamMetadata.class)));
        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, mock(AsyncOxiaClient.class));

        assertSame(log, holder.openPartition(tp("orders", 1), 3, Map.of("retention.ms", "1"), 77L).get());

        ArgumentCaptor<Partitioning> partitioning = ArgumentCaptor.forClass(Partitioning.class);
        ArgumentCaptor<Map<String, String>> properties = ArgumentCaptor.forClass(Map.class);
        verify(catalog).createStream(eq(id), any(), partitioning.capture(), any(), properties.capture());
        assertEquals(3, partitioning.getValue().numPartitions());
        assertEquals("true", properties.getValue().get(KafkaStreamIdentity.KAFKA_MANAGED_PROPERTY));
        assertEquals("orders", properties.getValue().get(KafkaStreamIdentity.SOURCE_LOGICAL_NAME_PROPERTY));
        assertEquals("orders", properties.getValue().get(KafkaStreamIdentity.KAFKA_TOPIC_NAME_PROPERTY));
        assertEquals("1", properties.getValue().get("retention.ms"));
        // The broker's own metadata offset, not 0, so the controller's sweep cannot treat a topic
        // created after its image as an orphan.
        assertEquals("77", properties.getValue().get(KafkaStreamIdentity.KAFKA_SOURCE_REVISION_PROPERTY));
    }

    @Test
    void openPartitionGrowsLayoutWhenIndexIsMissing() throws Exception {
        StreamCatalog catalog = mock(StreamCatalog.class);
        Log log = mock(Log.class);
        StreamIdentifier id = KafkaStreamIdentity.streamIdentifier("orders", TOPIC_ID);
        when(catalog.openLog(id, 5))
            .thenReturn(failedFuture(new IllegalArgumentException("Partition 5 is not in the committed layout")))
            .thenReturn(completedFuture(log));
        when(catalog.increasePartitions(id, 6)).thenReturn(completedFuture(mock(StreamMetadata.class)));
        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, mock(AsyncOxiaClient.class));

        assertSame(log, holder.openPartition(tp("orders", 5), 6, Map.of(), 12L).get());
        verify(catalog, never()).createStream(any(), any(), any(), any(), anyMap());
    }

    @Test
    void concurrentCreateToleratesAlreadyExists() throws Exception {
        StreamCatalog catalog = mock(StreamCatalog.class);
        Log log = mock(Log.class);
        StreamIdentifier id = KafkaStreamIdentity.streamIdentifier("orders", TOPIC_ID);
        when(catalog.openLog(id, 0))
            .thenReturn(failedFuture(new NoSuchStreamException(id)))
            .thenReturn(completedFuture(log));
        when(catalog.createStream(eq(id), any(), any(), any(), anyMap()))
            .thenReturn(failedFuture(new AlreadyExistsException("exists")));
        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, mock(AsyncOxiaClient.class));

        assertSame(log, holder.openPartition(tp("orders", 0), 1, Map.of(), 12L).get());
    }

    @Test
    void permanentlyDeletedStreamIsNotRecreated() {
        StreamCatalog catalog = mock(StreamCatalog.class);
        StreamIdentifier id = KafkaStreamIdentity.streamIdentifier("orders", TOPIC_ID);
        when(catalog.openLog(id, 0)).thenReturn(failedFuture(new StreamPermanentlyDeletedException(id)));
        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, mock(AsyncOxiaClient.class));

        ExecutionException failure = assertThrows(
            ExecutionException.class,
            () -> holder.openPartition(tp("orders", 0), 1, Map.of(), 12L).get());

        assertInstanceOf(StreamPermanentlyDeletedException.class, failure.getCause());
        verify(catalog, never()).createStream(any(), any(), any(), any(), anyMap());
    }

    @Test
    void deletedTopicsAreRejectedAndTheSetIsBounded() {
        LakestreamStorageHolder holder = new LakestreamStorageHolder(
            mock(StreamCatalog.class), mock(AsyncOxiaClient.class), 2);
        Uuid a = Uuid.randomUuid();
        Uuid b = Uuid.randomUuid();
        Uuid c = Uuid.randomUuid();

        holder.markTopicDeleted(a);
        holder.markTopicDeleted(b);
        holder.markTopicDeleted(c);

        assertFalse(holder.isTopicDeleted(a));
        assertTrue(holder.isTopicDeleted(b));
        assertTrue(holder.isTopicDeleted(c));
    }

    @Test
    void deletedTopicIsFencedWithoutTouchingTheCatalog() {
        StreamCatalog catalog = mock(StreamCatalog.class);
        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, mock(AsyncOxiaClient.class));

        holder.markTopicDeleted(TOPIC_ID);

        ExecutionException failure = assertThrows(
            ExecutionException.class,
            () -> holder.openPartition(tp("orders", 0), 1, Map.of(), 12L).get());

        assertInstanceOf(NotLeaderOrFollowerException.class, failure.getCause());
        verify(catalog, never()).openLog(any(), anyInt());
        verify(catalog, never()).createStream(any(), any(), any(), any(), anyMap());
    }

    @Test
    void testUsesNeutralStoragePropertiesWithoutReaderImplementationDetails() throws Exception {
        UrsaStorageConfig config = UrsaStorageConfig.fromConfigs(Map.of(
                ServerLogConfigs.URSA_OXIA_SERVICE_URL_CONFIG, "oxia://storage:6648/kafka",
                ServerLogConfigs.URSA_STORAGE_BACKEND_TYPE_CONFIG, "GCS",
                ServerLogConfigs.URSA_STORAGE_PATH_CONFIG, "gcs-prefix",
                ServerLogConfigs.URSA_STORAGE_S3_ENDPOINT_CONFIG, "http://fake-gcs:4443",
                ServerLogConfigs.URSA_STORAGE_S3_BUCKET_CONFIG, "gcs-bucket",
                ServerLogConfigs.URSA_STORAGE_S3_REGION_CONFIG, "us-central1",
                ServerLogConfigs.URSA_STORAGE_COMPACTION_BUCKET_CONFIG, "compacted-bucket",
                ServerLogConfigs.URSA_STORAGE_COMPACTION_PREFIX_CONFIG, "compacted-prefix"
        ));

        Properties properties = LakestreamStorageHolder.buildStorageProperties(config);

        assertEquals("oxia://storage:6648/kafka", properties.getProperty("oxiaStorageUrl"));
        assertEquals("GCS", properties.getProperty("backendStorageType"));
        assertEquals("gcs-bucket", properties.getProperty("bucket"));
        assertEquals("gcs-prefix", properties.getProperty("prefix"));
        assertEquals("us-central1", properties.getProperty("region"));
        assertEquals("compacted-bucket", properties.getProperty("compactionBucket"));
        assertEquals("compacted-prefix", properties.getProperty("compactionPrefix"));
        assertFalse(properties.containsKey("externalReaderFactoryClass"));
        assertFalse(properties.containsKey("compactedObjectReaderFactoryClass"));
        assertFalse(properties.containsKey("compactionBackendStorageType"));
        assertFalse(properties.containsKey("compactionBucketRegion"));
        assertFalse(properties.containsKey("storageTier"));
    }

    @Test
    void testAzureBackendAliasesNormalizeWithoutDerivingReaderBackend() throws Exception {
        verifyAzureBackendAlias("AZURE_BLOB");
        verifyAzureBackendAlias("AZUREBLOB");
    }

    @Test
    void testS3BackendStillIncludesS3SpecificCredentials() throws Exception {
        UrsaStorageConfig config = UrsaStorageConfig.fromConfigs(Map.of(
                ServerLogConfigs.URSA_STORAGE_BACKEND_TYPE_CONFIG, "S3",
                ServerLogConfigs.URSA_STORAGE_PATH_CONFIG, "s3-prefix",
                ServerLogConfigs.URSA_STORAGE_S3_ENDPOINT_CONFIG, "http://localstack:4566",
                ServerLogConfigs.URSA_STORAGE_S3_BUCKET_CONFIG, "s3-bucket",
                ServerLogConfigs.URSA_STORAGE_S3_REGION_CONFIG, "us-east-1",
                ServerLogConfigs.URSA_STORAGE_S3_ACCESS_KEY_CONFIG, "access",
                ServerLogConfigs.URSA_STORAGE_S3_SECRET_KEY_CONFIG, "secret",
                ServerLogConfigs.URSA_STORAGE_S3_SESSION_TOKEN_CONFIG, "session",
                ServerLogConfigs.URSA_STORAGE_S3_PATH_STYLE_ACCESS_CONFIG, "false"
        ));

        Properties properties = LakestreamStorageHolder.buildStorageProperties(config);

        assertEquals("S3", properties.getProperty("backendStorageType"));
        assertEquals("access", properties.getProperty("s3AccessKeyId"));
        assertEquals("secret", properties.getProperty("s3SecretAccessKey"));
        assertEquals("session", properties.getProperty("s3SessionToken"));
        assertEquals("false", properties.getProperty("s3PathStyleAccess"));
        assertEquals("s3-bucket", properties.getProperty("s3Bucket"));
        assertEquals("s3-prefix", properties.getProperty("s3Prefix"));
        assertEquals("us-east-1", properties.getProperty("s3Region"));
    }

    @Test
    void testCloseOwnsCatalogAndSeparateProducerStateOxiaClient() throws Exception {
        StreamCatalog catalog = mock(StreamCatalog.class);
        AsyncOxiaClient producerStateOxiaClient = mock(AsyncOxiaClient.class);
        LakestreamStorageHolder holder = new LakestreamStorageHolder(catalog, producerStateOxiaClient);

        assertSame(catalog, holder.catalog());
        assertSame(producerStateOxiaClient, holder.oxiaClient());

        holder.close();

        verify(catalog).close();
        verify(producerStateOxiaClient).close();
    }

    @Test
    void testLateProducerStateOxiaClientIsClosedAfterCreateFailure() throws Exception {
        CompletableFuture<AsyncOxiaClient> clientFuture = new CompletableFuture<>();
        AsyncOxiaClient client = mock(AsyncOxiaClient.class);

        LakestreamStorageHolder.closeOxiaClientAfterFailedCreate(
                null, clientFuture, new Exception("creation failed"));
        clientFuture.complete(client);

        verify(client).close();
    }

    private static void verifyAzureBackendAlias(String backendType) throws Exception {
        UrsaStorageConfig config = UrsaStorageConfig.fromConfigs(Map.of(
                ServerLogConfigs.URSA_STORAGE_BACKEND_TYPE_CONFIG, backendType,
                ServerLogConfigs.URSA_STORAGE_PATH_CONFIG, "azure-prefix",
                ServerLogConfigs.URSA_STORAGE_S3_ENDPOINT_CONFIG, "https://account.blob.core.windows.net",
                ServerLogConfigs.URSA_STORAGE_S3_BUCKET_CONFIG, "account@container",
                ServerLogConfigs.URSA_STORAGE_S3_REGION_CONFIG, "unused-region"
        ));

        Properties properties = LakestreamStorageHolder.buildStorageProperties(config);

        assertEquals("AZUREBLOB", properties.getProperty("backendStorageType"));
        assertEquals("account@container", properties.getProperty("bucket"));
        assertEquals("azure-prefix", properties.getProperty("prefix"));
        assertEquals("unused-region", properties.getProperty("region"));
        assertFalse(properties.containsKey("compactionBackendStorageType"));
        assertFalse(properties.containsKey("compactionBucketRegion"));
    }

    private static TopicIdPartition tp(String topic, int partition) {
        return new TopicIdPartition(TOPIC_ID, new TopicPartition(topic, partition));
    }
}
