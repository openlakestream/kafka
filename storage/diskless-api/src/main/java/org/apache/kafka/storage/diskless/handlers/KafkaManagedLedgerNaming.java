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

/**
 * Naming convention for Kafka diskless topics when backed by Pulsar ManagedLedger metadata layout.
 *
 * <p>Note: ursa-for-kafka does not have tenant/namespace concepts today. We use a fixed
 * {@code public/default/persistent} naming to ensure Oxia topic discovery via {@code /managed-ledgers}.
 */
public final class KafkaManagedLedgerNaming {

    public static final String TENANT = "public";
    public static final String NAMESPACE = "default";
    public static final String DOMAIN = "persistent";

    public static final String MANAGED_LEDGER_PREFIX = "/managed-ledgers/";

    private KafkaManagedLedgerNaming() {
    }

    /**
     * ManagedLedger name passed into {@code StorageWalManagedLedgerFactory.open(name, ...)}.
     *
     * <p>The Oxia metadata key will be {@code /managed-ledgers/ + name}.
     */
    public static String managedLedgerName(TopicIdPartition tp) {
        return TENANT + "/" + NAMESPACE + "/" + DOMAIN + "/" + tp.topic() + "-partition-" + tp.partition();
    }

    /**
     * Oxia metadata key for the managed ledger.
     */
    public static String managedLedgerMetadataPath(TopicIdPartition tp) {
        return MANAGED_LEDGER_PREFIX + managedLedgerName(tp);
    }
}
