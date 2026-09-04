#!/bin/sh
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

# Reads DEMO_TOPIC from the beginning through the Kafka REST proxy's Avro
# consumer, which decodes every record with the schema registry, proving the
# Avro frames round-trip through Ursa storage before the lakehouse check runs.

set -eu

: "${DEMO_TOPIC:?DEMO_TOPIC is required}"
: "${NUM_RECORDS:?NUM_RECORDS is required}"
: "${KAFKA_REST_URL:?KAFKA_REST_URL is required}"
timeout_seconds="${CONSUME_TIMEOUT_SECONDS:-120}"

# A fresh group each run, so a retained stack never replays committed offsets.
group="${DEMO_TOPIC}-check-$(date +%s)"
instance=check
consumer="${KAFKA_REST_URL}/consumers/${group}/instances/${instance}"
output=/tmp/kafka-consumed.json

cleanup() {
  curl -sS -X DELETE -H 'Accept: application/vnd.kafka.v2+json' "$consumer" >/dev/null 2>&1 || true
}
trap cleanup EXIT

curl -sS --fail-with-body -X POST \
  -H 'Content-Type: application/vnd.kafka.v2+json' \
  --data "{\"name\": \"${instance}\", \"format\": \"avro\", \"auto.offset.reset\": \"earliest\"}" \
  "${KAFKA_REST_URL}/consumers/${group}" >/dev/null
curl -sS --fail-with-body -X POST \
  -H 'Content-Type: application/vnd.kafka.v2+json' \
  --data "{\"topics\": [\"${DEMO_TOPIC}\"]}" \
  "${consumer}/subscription"

# The first poll after subscribing usually returns [] while the partition is
# being assigned, so keep polling until every record is in or time runs out.
: > "$output"
count=0
deadline=$(( $(date +%s) + timeout_seconds ))
while [ "$count" -lt "$NUM_RECORDS" ] && [ "$(date +%s)" -lt "$deadline" ]; do
  curl -sS --fail-with-body -H 'Accept: application/vnd.kafka.avro.v2+json' \
    "${consumer}/records?timeout=5000" >> "$output"
  echo >> "$output"
  count="$(grep -o '"order_id"' "$output" | wc -l | tr -d ' ')"
done

if [ "$count" != "$NUM_RECORDS" ]; then
  echo "Kafka read back ${count} of ${NUM_RECORDS} records from ${DEMO_TOPIC}." >&2
  exit 1
fi

echo "Kafka read back ${count} Avro records from Ursa storage. First decoded record:"
grep -o '"value": *{[^}]*}' "$output" | head -n 1 | sed 's/^"value": *//'
