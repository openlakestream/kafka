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
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.server.config.ServerLogConfigs;

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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import io.streamnative.oxia.testcontainers.OxiaContainer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * E2E test verifying that diskless topic creation fails gracefully when Oxia becomes unavailable.
 *
 * <p>Starts a real cluster with Oxia, then pauses the Oxia container to simulate unavailability.
 * Diskless topic creation must return an error to the client, while non-diskless
 * topic creation must remain unaffected.
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

        cluster = new KafkaClusterTestKit.Builder(
                new TestKitNodes.Builder()
                        .setNumBrokerNodes(1)
                        .setNumControllerNodes(1)
                        .build())
                .setConfigProp(ServerLogConfigs.URSA_STORAGE_ENABLE_CONFIG, "true")
                .setConfigProp(ServerLogConfigs.URSA_STORAGE_OXIA_SERVICE_URL_CONFIG,
                        oxiaContainer.getServiceAddress())
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
        // PLACEHOLDER_AFTER_ALL
        if (oxiaContainer != null) {
            oxiaContainer.stop();
        }
    }

    @Test
    @DisplayName("Diskless topic creation fails when Oxia is paused")
    void testDisklessTopicCreationFailsWhenOxiaPaused() throws Exception {
        // Pause Oxia container to simulate network unavailability
        oxiaContainer.getDockerClient()
                .pauseContainerCmd(oxiaContainer.getContainerId()).exec();
        try {
            String topicName = "oxia-paused-diskless-" + System.currentTimeMillis();
            try (Admin admin = Admin.create(cluster.clientProperties())) {
                NewTopic newTopic = new NewTopic(topicName, 1, (short) 1)
                        .configs(Map.of(TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true"));

                ExecutionException ex = assertThrows(ExecutionException.class,
                        () -> admin.createTopics(Collections.singleton(newTopic))
                                .all().get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS));

                assertInstanceOf(UnknownServerException.class, ex.getCause(),
                        "Expected UnknownServerException, got: " + ex.getCause());
                assertTrue(ex.getCause().getMessage().contains("Oxia"),
                        "Error message should mention Oxia, got: " + ex.getCause().getMessage());
            }
        } finally {
            // Always unpause so AfterAll can stop the container cleanly
            oxiaContainer.getDockerClient()
                    .unpauseContainerCmd(oxiaContainer.getContainerId()).exec();
        }
    }

    @Test
    @DisplayName("Non-diskless topic creation succeeds when Oxia is paused")
    void testNonDisklessTopicCreationSucceedsWhenOxiaPaused() throws Exception {
        oxiaContainer.getDockerClient()
                .pauseContainerCmd(oxiaContainer.getContainerId()).exec();
        try {
            String topicName = "oxia-paused-normal-" + System.currentTimeMillis();
            try (Admin admin = Admin.create(cluster.clientProperties())) {
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
        try (Admin admin = Admin.create(kit.clientProperties())) {
            NewTopic newTopic = new NewTopic(warmupTopic, 1, (short) 1)
                    .configs(Map.of(TopicConfig.URSA_STORAGE_ENABLE_CONFIG, "true"));
            admin.createTopics(Collections.singleton(newTopic))
                    .all().get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
    }
}
