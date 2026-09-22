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
# Check that a Strimzi-compatible image built by build-image.sh has the layout
# the Strimzi cluster operator and this fork expect. Runs entirely inside the
# image; no Kubernetes needed.
#
# Usage:
#   ./verify-image.sh IMAGE_NAME:TAG
#

set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "Usage: $0 IMAGE_NAME:TAG" >&2
    exit 1
fi
IMAGE="$1"

echo "Verifying ${IMAGE} ..."
docker run --rm --entrypoint bash "${IMAGE}" -c '
set -euo pipefail
fail() { echo "FAIL: $*" >&2; exit 1; }
ok() { echo "ok: $*"; }

[[ "$(id -u)" == "1001" && "$(id -g)" == "0" ]] || fail "expected uid 1001 gid 0, got $(id -u):$(id -g)"
ok "runs as uid 1001 in group 0"

[[ "${KAFKA_HOME:-}" == "/opt/kafka" ]] || fail "KAFKA_HOME is ${KAFKA_HOME:-unset}, expected /opt/kafka"
[[ -n "${KAFKA_VERSION:-}" ]] || fail "KAFKA_VERSION is unset"
ok "KAFKA_HOME=/opt/kafka KAFKA_VERSION=${KAFKA_VERSION}"

for f in kafka_run.sh kafka_readiness.sh kafka_liveness.sh dynamic_resources.sh bin/kafka-server-start.sh bin/kafka-storage.sh; do
    [[ -x "/opt/kafka/${f}" ]] || fail "missing executable /opt/kafka/${f}"
done
# Sourced by kafka_run.sh rather than executed, so only their presence matters.
for f in set_kafka_jmx_options.sh set_kafka_gc_options.sh to_bytes.gawk; do
    [[ -f "/opt/kafka/${f}" ]] || fail "missing /opt/kafka/${f}"
done
ok "Strimzi entrypoints and Kafka scripts present"

for d in /opt/cruise-control /opt/kafka-exporter /opt/prometheus-jmx-exporter /opt/kafka/plugins; do
    [[ -d "${d}" ]] || fail "missing ${d}"
done
ok "Cruise Control, exporters and plugins directory present"

compgen -G "/opt/kafka/libs/kafka-agent-*.jar" >/dev/null || fail "kafka-agent jar missing from /opt/kafka/libs"
compgen -G "/opt/kafka/libs/tracing-agent-*.jar" >/dev/null || fail "tracing-agent jar missing from /opt/kafka/libs"
ok "Strimzi kafka-agent and tracing-agent kept"

kafka_jars=$(ls /opt/kafka/libs/kafka_2.13-*.jar)
[[ "$(echo "${kafka_jars}" | wc -l)" -eq 1 ]] || fail "expected exactly one kafka_2.13 jar, found: ${kafka_jars}"
[[ "${kafka_jars}" == "/opt/kafka/libs/kafka_2.13-${KAFKA_VERSION}.jar" ]] || fail "expected kafka_2.13-${KAFKA_VERSION}.jar, found ${kafka_jars}"
ok "single Kafka core jar: $(basename "${kafka_jars}")"

# One version per artifact. Native classifier jars (netty-*-linux-x86_64 and
# friends) legitimately share a name and version, so compare (name, version).
dups=$(for jar in /opt/kafka/libs/*.jar; do
    name=$(basename "${jar}")
    key=$(echo "${name}" | sed -E "s/-[0-9][0-9A-Za-z.+_-]*\.jar$//")
    rest="${name#"${key}-"}"
    rest="${rest%.jar}"
    echo "${key} ${rest%%-*}"
done | sort -u | awk "{print \$1}" | uniq -d)
[[ -z "${dups}" ]] || fail "artifacts present in more than one version under /opt/kafka/libs: ${dups}"
ok "no duplicate artifact versions in /opt/kafka/libs ($(ls /opt/kafka/libs/*.jar | wc -l | tr -d " ") jars)"

compgen -G "/opt/kafka/ursa-storage/kafka-storage-diskless-ursa-*.jar" >/dev/null || fail "isolated Ursa runtime missing from /opt/kafka/ursa-storage"
ok "isolated Ursa runtime present ($(ls /opt/kafka/ursa-storage/*.jar | wc -l | tr -d " ") jars)"

if ls /opt/kafka/libs | grep -q -E "^ursa-storage-(core|common|lakestream|kafka-runtime)-"; then
    fail "Ursa runtime jars leaked into /opt/kafka/libs"
fi
ok "Ursa runtime stays out of the main classpath"

uuid=$(/opt/kafka/bin/kafka-storage.sh random-uuid 2>&1) || fail "kafka-storage.sh random-uuid failed: ${uuid}"
[[ "${uuid}" =~ ^[A-Za-z0-9_-]{22}$ ]] || fail "unexpected kafka-storage.sh output: ${uuid}"
ok "Kafka tools start on the merged classpath"
'
echo "Image ${IMAGE} passed all checks."
