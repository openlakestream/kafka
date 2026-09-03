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
package org.apache.kafka.server.ursa.integration;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import io.oxia.testcontainers.OxiaContainer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * E2E test verifying that diskless topic metadata remains available when Oxia becomes unavailable.
 *
 * <p>Starts a real cluster with Oxia, then pauses the Oxia container to simulate unavailability.
 * Kafka topic creation must commit without waiting for Oxia, and the asynchronous diskless
 * lifecycle reconciler must reconcile the committed topic into Lakestream after Oxia recovers.
 */
@Timeout(value = 180, unit = TimeUnit.SECONDS)
@Tag("integration")
public class UrsaStorageOxiaFailureE2ETest extends UrsaStorageE2ETestBase {

    private static final int DEFAULT_TIMEOUT_SECONDS = 30;

    @TempDir
    static Path baseDir;

    private static OxiaContainer oxiaContainer;
    private static KafkaClusterTestKit cluster;

    @BeforeAll
    static void startCluster() throws Exception {
        oxiaContainer = new OxiaContainer(DockerImageName.parse("oxia/oxia:main"));
        oxiaContainer.start();

        cluster = enableBrokerRequestPipelining(new KafkaClusterTestKit.Builder(
                new TestKitNodes.Builder()
                        .setNumBrokerNodes(1)
                        .setNumControllerNodes(1)
                        .build()))
                .setConfigProp(ServerLogConfigs.URSA_STORAGE_ENABLE_CONFIG, "true")
                .setConfigProp(ServerLogConfigs.URSA_CATALOG_OXIA_SERVICE_URL_CONFIG,
                        "oxia://" + oxiaContainer.getServiceAddress() + "/default")
                .setConfigProp(ServerLogConfigs.URSA_OXIA_SERVICE_URL_CONFIG,
                        "oxia://" + oxiaContainer.getServiceAddress() + "/default")
                .setConfigProp(ServerLogConfigs.URSA_STORAGE_BACKEND_TYPE_CONFIG, "LOCAL")
                .setConfigProp(ServerLogConfigs.URSA_STORAGE_PATH_CONFIG, baseDir.toString())
                .setConfigProp("offsets.topic.replication.factor", "1")
                .setConfigProp("transaction.state.log.replication.factor", "1")
                .setConfigProp("transaction.state.log.min.isr", "1")
                .build();
        cluster.format();
        cluster.startup();

        // Verify cluster is healthy by creating a topic before pausing Oxia
        createAndVerifyTopic(cluster);
    }

    @AfterAll
    static void stopCluster() {
        if (cluster != null) {
            try {
                cluster.close();
            } catch (Exception e) {
                // ignore
            }
        }
        if (oxiaContainer != null) {
            oxiaContainer.stop();
        }
    }

    @Test
    @DisplayName("Diskless topic creation commits while Oxia is paused and reconciles after recovery")
    void testDisklessTopicCreationReconcilesAfterOxiaRecovers() throws Exception {
        String topicName = "oxia-paused-diskless-" + System.currentTimeMillis();
        int partitions = 3;
        Uuid topicId;

        // Pause Oxia container to simulate network unavailability
        oxiaContainer.getDockerClient()
                .pauseContainerCmd(oxiaContainer.getContainerId()).exec();
        try {
            try (Admin admin = cluster.admin()) {
                NewTopic newTopic = new NewTopic(topicName, partitions, (short) 1)
                        .configs(Map.of(TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true"));

                assertDoesNotThrow(
                        () -> admin.createTopics(Collections.singleton(newTopic))
                                .all().get(10, TimeUnit.SECONDS),
                        "KRaft topic creation should not wait for the unavailable Lakestream catalog");

                waitForTopicReady(admin, topicName, partitions);
                TopicDescription description = admin.describeTopics(Set.of(topicName))
                        .allTopicNames()
                        .get(10, TimeUnit.SECONDS)
                        .get(topicName);
                assertNotNull(description, "Committed topic should be visible in Kafka metadata");
                assertEquals(partitions, description.partitions().size());
                topicId = description.topicId();
                assertNotEquals(Uuid.ZERO_UUID, topicId, "Committed topic should have a stable topic ID");
            }
        } finally {
            // Always unpause so AfterAll can stop the container cleanly
            oxiaContainer.getDockerClient()
                    .unpauseContainerCmd(oxiaContainer.getContainerId()).exec();
        }

        assertCatalogStreamReady(topicName, topicId, partitions);
    }

    @Test
    @DisplayName("Non-diskless topic creation succeeds when Oxia is paused")
    void testNonDisklessTopicCreationSucceedsWhenOxiaPaused() throws Exception {
        oxiaContainer.getDockerClient()
                .pauseContainerCmd(oxiaContainer.getContainerId()).exec();
        try {
            String topicName = "oxia-paused-normal-" + System.currentTimeMillis();
            try (Admin admin = cluster.admin()) {
                NewTopic newTopic = new NewTopic(topicName, 1, (short) 1)
                        .configs(Map.of(TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "false"));

                assertDoesNotThrow(
                        () -> admin.createTopics(Collections.singleton(newTopic))
                                .all().get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS),
                        "Non-diskless topic creation should succeed when Oxia is paused");
            }
        } finally {
            oxiaContainer.getDockerClient()
                    .unpauseContainerCmd(oxiaContainer.getContainerId()).exec();
        }
    }

    private static void createAndVerifyTopic(KafkaClusterTestKit kit) throws Exception {
        String warmupTopic = "oxia-warmup-" + System.currentTimeMillis();
        Uuid topicId;
        try (Admin admin = kit.admin()) {
            NewTopic newTopic = new NewTopic(warmupTopic, 1, (short) 1)
                    .configs(Map.of(TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true"));
            admin.createTopics(Collections.singleton(newTopic))
                    .all().get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            topicId = admin.describeTopics(Set.of(warmupTopic))
                    .allTopicNames()
                    .get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .get(warmupTopic)
                    .topicId();
        }
        assertCatalogStreamReady(warmupTopic, topicId, 1);
    }

    private static void assertCatalogStreamReady(
            String topicName,
            Uuid topicId,
            int expectedPartitions
    ) throws Exception {
        String streamName = IsolatedUrsaCatalogInspector.streamName(cluster, topicName, topicId);
        String namespace = IsolatedUrsaCatalogInspector.namespace(cluster);

        try (IsolatedUrsaCatalogInspector catalog = createCatalogInspector()) {
            TestUtils.waitForCondition(() -> {
                if (!catalog.isStreamListed(
                        namespace, streamName, 10, TimeUnit.SECONDS)) {
                    return false;
                }
                return catalog.partitionCount(
                        namespace, streamName, 10, TimeUnit.SECONDS) == expectedPartitions;
            }, 30_000, 100,
                    () -> "Timed out waiting for catalog stream reconciliation: "
                            + namespace + "/" + streamName);
        }
    }

    private static IsolatedUrsaCatalogInspector createCatalogInspector() throws Exception {
        String catalogUri = "oxia://" + oxiaContainer.getServiceAddress() + "/default";
        Properties properties = new Properties();
        properties.setProperty("backendStorageType", "LOCAL");
        properties.setProperty("storagePath", baseDir.toString());
        properties.setProperty("oxiaStorageUrl", catalogUri);
        return IsolatedUrsaCatalogInspector.open(cluster, catalogUri, properties);
    }
}
