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

import java.util.List;

import io.lakestream.api.LogEntry;

/** Utilities for releasing Lakestream read results owned by the caller. */
public final class LogEntryUtils {

    private LogEntryUtils() {
    }

    /**
     * Closes every non-null entry and returns the resulting failure, if any.
     *
     * <p>Cleanup continues after a close failure. When {@code precedingError} is non-null, close
     * failures are attached to it as suppressed exceptions. Otherwise the first close failure is
     * returned and subsequent failures are suppressed on it.
     */
    public static Throwable closeAll(List<LogEntry> entries, Throwable precedingError) {
        Throwable failure = precedingError;
        if (entries == null) {
            return failure;
        }
        for (LogEntry entry : entries) {
            if (entry == null) {
                continue;
            }
            try {
                entry.close();
            } catch (Throwable closeError) {
                if (failure == null) {
                    failure = closeError;
                } else if (failure != closeError) {
                    failure.addSuppressed(closeError);
                }
            }
        }
        return failure;
    }
}
