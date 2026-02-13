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
package org.apache.kafka.controller;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult;
import org.apache.kafka.common.metadata.FeatureLevelRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.metadata.BrokerHeartbeatReply;
import org.apache.kafka.metadata.FakeKafkaConfigSchema;
import org.apache.kafka.metadata.RecordTestUtils;
import org.apache.kafka.metadata.placement.StripedReplicaPlacer;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.EligibleLeaderReplicasVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.util.MockRandom;
import org.apache.kafka.timeline.SnapshotRegistry;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.common.protocol.Errors.NONE;
import static org.apache.kafka.controller.ControllerRequestContextUtil.anonymousContextFor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Timeout(40)
public class DisklessTopicDefaultsTest {
    private static final int BROKER_SESSION_TIMEOUT_MS = 1000;

    @Test
    public void testCreateDisklessTopicDefaultEnabled() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().
            setDisklessStorageSystemEnabled(true).
            setStaticConfig("ursa.storage.topic.default.enable", "true").
            build();
        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1, 2);

        CreateTopicsRequestData request = new CreateTopicsRequestData();
        CreatableTopic topic = new CreatableTopic().setName("diskless-topic-default").
            setNumPartitions(2).setReplicationFactor((short) -1);
        request.topics().add(topic);

        ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.CREATE_TOPICS);
        CreatableTopicResult topicResult = ctx.replicationControl.createTopics(requestContext, request,
            Set.of("diskless-topic-default")).response().topics().find("diskless-topic-default");
        assertNotNull(topicResult);
        assertEquals(NONE.code(), topicResult.errorCode());
        assertEquals(1, topicResult.replicationFactor());
    }

    @Test
    public void testCreateDisklessTopicDefaultEnabledWithExplicitReplicationFactorBackfillsConfig() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().
            setDisklessStorageSystemEnabled(true).
            setStaticConfig("ursa.storage.topic.default.enable", "true").
            build();
        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1, 2);

        // Simulate auto-topic-creation requests where broker fills in default.replication.factor.
        CreateTopicsRequestData request = new CreateTopicsRequestData();
        CreatableTopic topic = new CreatableTopic().setName("diskless-topic-default-rf-3").
            setNumPartitions(2).setReplicationFactor((short) 3);
        request.topics().add(topic);

        ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.CREATE_TOPICS);
        ControllerResult<CreateTopicsResponseData> result = ctx.replicationControl.createTopics(requestContext, request,
            Set.of("diskless-topic-default-rf-3"));
        CreatableTopicResult topicResult = result.response().topics().find("diskless-topic-default-rf-3");
        assertNotNull(topicResult);
        assertEquals(NONE.code(), topicResult.errorCode());
        assertEquals(1, topicResult.replicationFactor());

        ctx.replay(result.records());
        assertEquals("true", ctx.configurationControl.getConfigs(
            new ConfigResource(ConfigResource.Type.TOPIC, "diskless-topic-default-rf-3"))
            .get(TopicConfig.URSA_STORAGE_ENABLE_CONFIG));
    }

    // The following code is copied from `ReplicationControlManagerTest`. Don't reuse that class to avoid conflicts
    // when merging code from the upstream Apache Kafka code base.
    private static final class ReplicationControlTestContext {
        private static final class Builder {
            private MetadataVersion metadataVersion = MetadataVersion.latestTesting();
            private MockTime mockTime = new MockTime();
            private boolean disklessStorageSystemEnabled = false;
            private final Map<String, Object> staticConfig = new HashMap<>();

            Builder setDisklessStorageSystemEnabled(boolean disklessStorageSystemEnabled) {
                this.disklessStorageSystemEnabled = disklessStorageSystemEnabled;
                return this;
            }

            Builder setStaticConfig(String key, Object value) {
                this.staticConfig.put(key, value);
                return this;
            }

            ReplicationControlTestContext build() {
                return new ReplicationControlTestContext(metadataVersion,
                    mockTime,
                    disklessStorageSystemEnabled,
                    staticConfig);
            }
        }

        final SnapshotRegistry snapshotRegistry = new SnapshotRegistry(new LogContext());
        final LogContext logContext = new LogContext();
        final MockTime time;
        final MockRandom random = new MockRandom();
        final FeatureControlManager featureControl;
        final ClusterControlManager clusterControl;
        final ConfigurationControlManager configurationControl;
        final ReplicationControlManager replicationControl;

        void replay(List<ApiMessageAndVersion> records) {
            RecordTestUtils.replayAll(clusterControl, records);
            RecordTestUtils.replayAll(configurationControl, records);
            RecordTestUtils.replayAll(replicationControl, records);
        }

        private ReplicationControlTestContext(
            MetadataVersion metadataVersion,
            MockTime time,
            boolean disklessStorageSystemEnabled,
            Map<String, Object> staticConfig
        ) {
            this.time = time;
            this.featureControl = new FeatureControlManager.Builder().
                setSnapshotRegistry(snapshotRegistry).
                setQuorumFeatures(new QuorumFeatures(0,
                    QuorumFeatures.defaultSupportedFeatureMap(true),
                    List.of(0))).
                build();
            this.featureControl.replay(new FeatureLevelRecord().
                setName(MetadataVersion.FEATURE_NAME).
                setFeatureLevel(metadataVersion.featureLevel()));
            featureControl.replay(new FeatureLevelRecord()
                .setName(EligibleLeaderReplicasVersion.FEATURE_NAME)
                .setFeatureLevel(EligibleLeaderReplicasVersion.ELRV_0.featureLevel())
            );
            this.clusterControl = new ClusterControlManager.Builder().
                setLogContext(logContext).
                setTime(time).
                setSnapshotRegistry(snapshotRegistry).
                setSessionTimeoutNs(TimeUnit.MILLISECONDS.convert(BROKER_SESSION_TIMEOUT_MS, TimeUnit.NANOSECONDS)).
                setReplicaPlacer(new StripedReplicaPlacer(random)).
                setFeatureControlManager(featureControl).
                setBrokerShutdownHandler(this::handleBrokerShutdown).
                build();
            this.configurationControl = new ConfigurationControlManager.Builder().
                setSnapshotRegistry(snapshotRegistry).
                setFeatureControl(featureControl).
                setStaticConfig(staticConfig).
                setKafkaConfigSchema(FakeKafkaConfigSchema.INSTANCE).
                build();
            this.replicationControl = new ReplicationControlManager.Builder().
                setSnapshotRegistry(snapshotRegistry).
                setLogContext(logContext).
                setMaxElectionsPerImbalance(Integer.MAX_VALUE).
                setConfigurationControl(configurationControl).
                setClusterControl(clusterControl).
                setCreateTopicPolicy(java.util.Optional.empty()).
                setFeatureControl(featureControl).
                setDisklessStorageSystemEnabled(disklessStorageSystemEnabled).
                build();
            clusterControl.activate();
        }

        void handleBrokerShutdown(int brokerId, boolean isCleanShutdown, List<ApiMessageAndVersion> records) {
            replicationControl.handleBrokerShutdown(brokerId, isCleanShutdown, records);
        }

        void registerBrokers(Integer... brokerIds) {
            Object[] brokersAndDirs = new Object[brokerIds.length * 2];
            for (int i = 0; i < brokerIds.length; i++) {
                brokersAndDirs[i * 2] = brokerIds[i];
                brokersAndDirs[i * 2 + 1] = List.of(
                    Uuid.fromString("TESTBROKER" + Integer.toString(100000 + brokerIds[i]).substring(1) + "DIRAAAA")
                );
            }
            registerBrokersWithDirs(brokersAndDirs);
        }

        @SuppressWarnings("unchecked")
        void registerBrokersWithDirs(Object... brokerIdsAndDirs) {
            if (brokerIdsAndDirs.length % 2 != 0) {
                throw new IllegalArgumentException("uneven number of arguments");
            }
            for (int i = 0; i < brokerIdsAndDirs.length / 2; i++) {
                int brokerId = (int) brokerIdsAndDirs[i * 2];
                List<Uuid> logDirs = (List<Uuid>) brokerIdsAndDirs[i * 2 + 1];
                RegisterBrokerRecord brokerRecord = new RegisterBrokerRecord().
                    setBrokerEpoch(defaultBrokerEpoch(brokerId)).setBrokerId(brokerId).
                    setRack(null).setLogDirs(logDirs);
                brokerRecord.endPoints().add(new RegisterBrokerRecord.BrokerEndpoint().
                    setSecurityProtocol(SecurityProtocol.PLAINTEXT.id).
                    setPort((short) 9092 + brokerId).
                    setName("PLAINTEXT").
                    setHost("localhost"));
                replay(List.of(new ApiMessageAndVersion(brokerRecord, (short) 3)));
            }
        }

        void unfenceBrokers(Integer... brokerIds) {
            for (int brokerId : brokerIds) {
                clusterControl.trackBrokerHeartbeat(brokerId, defaultBrokerEpoch(brokerId));
                ControllerResult<BrokerHeartbeatReply> result = replicationControl.
                    processBrokerHeartbeat(new BrokerHeartbeatRequestData().
                        setBrokerId(brokerId).setBrokerEpoch(defaultBrokerEpoch(brokerId)).
                        setCurrentMetadataOffset(1).
                        setWantFence(false).setWantShutDown(false), 0);
                replay(result.records());
            }
        }
    }

    private static Long defaultBrokerEpoch(int brokerId) {
        return brokerId + 100L;
    }
}
