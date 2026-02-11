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

import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class UrsaStorageStateRetentionTest {

    @Test
    void testManagedLedgerConfigUpdatedWhenTopicConfigChanges() {
        ManagedLedgerConfig initialConfig = new ManagedLedgerConfig();
        ManagedLedger managedLedger = mock(ManagedLedger.class);

        ManagedLedgerConfig[] current = new ManagedLedgerConfig[]{initialConfig};
        when(managedLedger.getConfig()).thenAnswer(invocation -> current[0]);
        doAnswer(invocation -> {
            ManagedLedgerConfig updated = invocation.getArgument(0);
            current[0] = updated;
            return null;
        }).when(managedLedger).setConfig(any(ManagedLedgerConfig.class));

        long retentionMs = 120_000L;
        long retentionBytes = 2L * 1024 * 1024;

        UrsaStorageState.maybeUpdateRetentionConfig(managedLedger, retentionMs, retentionBytes);

        ManagedLedgerConfig afterUpdate = current[0];
        assertTrue(afterUpdate != initialConfig, "Expected a new config instance to be applied");
        assertEquals(retentionMs, afterUpdate.getRetentionTimeMillis());
        assertEquals(2L, afterUpdate.getRetentionSizeInMB());

        ManagedLedgerConfig previous = current[0];
        UrsaStorageState.maybeUpdateRetentionConfig(managedLedger, retentionMs, retentionBytes);
        assertSame(previous, current[0], "Expected config to remain unchanged when retention is unchanged");
    }
}
