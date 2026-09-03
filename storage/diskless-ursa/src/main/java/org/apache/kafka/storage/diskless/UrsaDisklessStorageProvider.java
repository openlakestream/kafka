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

import org.apache.kafka.storage.diskless.handlers.UrsaDisklessTopicLifecycle;
import org.apache.kafka.storage.diskless.handlers.UrsaStorageConfig;
import org.apache.kafka.storage.diskless.handlers.UrsaStorageEngineImpl;

/** Diskless-storage provider backed by Ursa and Lakestream. */
public final class UrsaDisklessStorageProvider implements DisklessStorageProvider {

    @Override
    public DisklessStorageEngine createStorageEngine(StorageEngineContext context) {
        return new UrsaStorageEngineImpl(
                context.time(),
                context.brokerId(),
                context.config(),
                context.brokerTopicStats(),
                context.logConfigDefaults(),
                context.topicConfigSupplier(),
                context.partitionCountSupplier(),
                context.imageOffsetSupplier());
    }

    @Override
    public DisklessTopicLifecycle createTopicLifecycle(UrsaStorageConfig config) throws Exception {
        return new UrsaDisklessTopicLifecycle(config);
    }
}
