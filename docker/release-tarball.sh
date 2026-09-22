#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#
# Build the Kafka release tarball and print its path on stdout. Everything else
# (Gradle output, warnings) goes to stderr so callers can capture the path:
#
#   TARBALL=$(docker/release-tarball.sh)
#
# Environment:
#   GRADLE_ARGS   Extra arguments for the release build, e.g. GRADLE_ARGS=--offline
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${PROJECT_ROOT}"

GRADLE_BUILD_ARGS=(./gradlew releaseTarGz --no-daemon -q)
if [[ -n "${GRADLE_ARGS:-}" ]]; then
    # Word splitting is intended: GRADLE_ARGS may carry several flags.
    # shellcheck disable=SC2206
    GRADLE_BUILD_ARGS+=(${GRADLE_ARGS})
fi

# The tarball's doc generators each run in a forked JVM whose classpath carries
# slf4j-api without a binding, so every one of them prints the same three-line
# "StaticLoggerBinder" warning. Gradle's -q silences its own output but not a
# child JVM's, so filter out just those lines. Real build failures stay intact.
set +e
"${GRADLE_BUILD_ARGS[@]}" 2>&1 | grep -v '^SLF4J: ' >&2
gradle_status=${PIPESTATUS[0]}
set -e
if [[ ${gradle_status} -ne 0 ]]; then
    exit "${gradle_status}"
fi

# Several versions may sit side by side after a -Pversion build; take the newest.
TARBALL=$(find "${PROJECT_ROOT}/core/build/distributions" -name "kafka_2.13-*.tgz" -not -name "*-site-docs.tgz" -print0 \
    | xargs -0 ls -t 2>/dev/null | head -1)
if [[ -z "${TARBALL}" ]]; then
    echo "ERROR: Could not find kafka tarball in core/build/distributions/" >&2
    exit 1
fi

echo "${TARBALL}"
