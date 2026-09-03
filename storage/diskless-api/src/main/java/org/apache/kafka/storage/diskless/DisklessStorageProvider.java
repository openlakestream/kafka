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
package org.apache.kafka.storage.diskless;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.diskless.handlers.UrsaStorageConfig;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import java.util.Map;
import java.util.OptionalInt;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * Entry point implemented by an isolated diskless-storage runtime.
 *
 * <p>Kafka discovers exactly one provider through {@link java.util.ServiceLoader}. Keeping the
 * implementation selection behind this SPI prevents broker and controller code from depending on
 * implementation class names or constructing implementation-specific components independently.
 */
public interface DisklessStorageProvider {

    /** Creates the broker data-plane engine. */
    DisklessStorageEngine createStorageEngine(StorageEngineContext context) throws Exception;

    /** Creates the controller-side topic lifecycle. */
    DisklessTopicLifecycle createTopicLifecycle(UrsaStorageConfig config) throws Exception;

    /** Broker dependencies supplied to the isolated storage engine. */
    record StorageEngineContext(
            Time time,
            int brokerId,
            UrsaStorageConfig config,
            BrokerTopicStats brokerTopicStats,
            Map<String, Object> logConfigDefaults,
            Function<String, Map<String, String>> topicConfigSupplier,
            Function<String, OptionalInt> partitionCountSupplier,
            LongSupplier imageOffsetSupplier) {
    }
}
