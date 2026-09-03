#!/usr/bin/env bash
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

set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
compose_file="$here/docker-compose.yml"
project="${COMPOSE_PROJECT_NAME:-kafka-ursa}"
kafka_image="${IMAGE:-lakestream/kafka:latest}"
compactor_image="${COMPACTOR_IMAGE:-lakestream/compactor:latest}"
num_records="${NUM_RECORDS:-100}"
passed=false

# The brokers default to general-purpose write-path settings. This verifier
# wants every record on its way to the compactor immediately, so it trades
# produce latency for a short feedback loop.
export URSA_WRITE_BUFFER_FLUSH_INTERVAL_MS="${URSA_WRITE_BUFFER_FLUSH_INTERVAL_MS:-100}"
export URSA_WRITE_BUFFER_FLUSH_SIZE="${URSA_WRITE_BUFFER_FLUSH_SIZE:-4096}"

# The compaction/Iceberg services live in the `lakehouse` profile and the demo
# workload in `lakehouse-demo`; this verifier needs both.
compose() {
  docker compose --project-name "$project" -f "$compose_file" \
    --profile lakehouse --profile lakehouse-demo "$@"
}

# `ps`/`down` only see services in the enabled profiles, so the project guard and
# the cleanup have to name every profile.
# Keep this profile list in sync with ALL_PROFILES in Makefile and in
# committer-tools/docker_cluster_smoke.py.
compose_all() {
  docker compose --project-name "$project" -f "$compose_file" \
    --profile lakehouse --profile demo --profile share-demo \
    --profile lakehouse-demo --profile tools "$@"
}

for image in "$kafka_image" "$compactor_image"; do
  if ! docker image inspect "$image" >/dev/null 2>&1; then
    echo "Required local image is missing: $image" >&2
    echo "Run URSA_STORAGE_DIR=/path/to/ursa-storage ./build-images.sh first." >&2
    exit 2
  fi
done

existing_containers="$(compose_all ps -aq)"
existing_volumes="$(docker volume ls \
  --filter "label=com.docker.compose.project=$project" --quiet)"
if [[ -n "$existing_containers" || -n "$existing_volumes" ]]; then
  echo "Compose project '$project' already has containers or volumes." >&2
  echo "Run 'make destroy' before starting a fresh E2E." >&2
  exit 2
fi

cleanup() {
  if [[ "$passed" == "true" && "${KEEP_RUNNING:-false}" != "true" ]]; then
    compose_all down -v --remove-orphans >/dev/null
  elif [[ "$passed" != "true" ]]; then
    echo "E2E failed; containers were left running for inspection." >&2
    echo "Logs: docker compose -f $compose_file logs compactor polaris" >&2
  fi
}
trap cleanup EXIT

compose up -d \
  oxia minio minio-init polaris polaris-setup compactor kafka-1 kafka-2 kafka-3

compose run --rm kafka-consumer-check

# The stack is already up and raw-producer has completed; --no-deps keeps the
# one-shot producer from running a second time.
compose run --rm --no-deps wait-for-parquet

attempt=1
last_query_output=""
while [[ "$attempt" -le 120 ]]; do
  if last_query_output="$(compose run --rm --no-deps duckdb-query-check 2>&1)"; then
    echo "$last_query_output"
    break
  fi
  echo "Iceberg table is not queryable yet; waiting... ($attempt/120)"
  attempt=$((attempt + 1))
  sleep 2
done

if [[ "$attempt" -gt 120 ]]; then
  echo "Timed out waiting for DuckDB to read the expected Iceberg rows." >&2
  echo "$last_query_output" >&2
  exit 1
fi

compactor_logs="$(compose logs --no-color compactor)"
materialization_count="$(printf '%s\n' "$compactor_logs" \
  | grep -F -c "Materializing [0,$num_records) of stream" || true)"
if [[ "$materialization_count" != "1" ]]; then
  echo "Expected exactly one materialization for [0,$num_records), found $materialization_count." >&2
  exit 1
fi
if grep -F -q "During compact error" <<< "$compactor_logs"; then
  echo "The compactor logged a task failure; inspect its logs before retrying." >&2
  exit 1
fi
if grep -F -q "Internal compaction task publisher is disabled" <<< "$compactor_logs"; then
  echo "The compactor unexpectedly disabled its internal task publisher." >&2
  exit 1
fi

passed=true
echo "E2E passed: Kafka -> Ursa WAL -> compaction -> Polaris/Iceberg -> DuckDB."
if [[ "${KEEP_RUNNING:-false}" == "true" ]]; then
  echo "The stack is still running under Compose project '$project'."
fi
