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

# Registers the Order schema under the `<topic>-value` subject and produces
# NUM_RECORDS Avro orders to DEMO_TOPIC through the Kafka REST proxy, which
# writes Confluent wire-format values (magic byte, schema id, Avro binary) —
# the same bytes a Kafka client with the Confluent Avro serializer would send.
#
# `<topic>-value` is the subject the Ursa compactor resolves when it
# materializes the topic into Iceberg. Register before the compactor sees the
# first record: it caches a missing subject per topic and keeps treating the
# topic as raw bytes until it is restarted.

set -eu

: "${DEMO_TOPIC:?DEMO_TOPIC is required}"
: "${NUM_RECORDS:?NUM_RECORDS is required}"
: "${SCHEMA_REGISTRY_URL:?SCHEMA_REGISTRY_URL is required}"
: "${KAFKA_REST_URL:?KAFKA_REST_URL is required}"
subject="${DEMO_TOPIC}-value"
batch_size=200

# Every field is required; the compactor maps them to the Iceberg columns
# order_id BIGINT, customer VARCHAR, region VARCHAR, quantity INTEGER,
# amount DOUBLE, order_ts_ms BIGINT. The timestamp is a plain long of epoch
# milliseconds rather than an Avro `timestamp-millis`: the REST proxy validates
# logical types against native datetime values, which JSON cannot carry. (The
# compactor itself maps `timestamp-millis` to an Iceberg TIMESTAMP; a Kafka
# client using the Avro serializer can use it.)
value_schema='{
  "type": "record",
  "name": "Order",
  "namespace": "io.lakestream.demo",
  "fields": [
    {"name": "order_id", "type": "long"},
    {"name": "customer", "type": "string"},
    {"name": "region", "type": "string"},
    {"name": "quantity", "type": "int"},
    {"name": "amount", "type": "double"},
    {"name": "order_ts_ms", "type": "long"}
  ]
}'

# The registry API takes the schema as a JSON string, so escape the quotes.
schema_as_json_string="$(printf '%s' "$value_schema" | tr -d '\n' | sed 's/"/\\"/g')"

# Registering the same schema twice returns the same id, so this is idempotent.
# The registry answers its health check a little before it has finished the
# primary election that registrations need, hence the retry.
attempt=1
while :; do
  if response="$(curl -sS --fail-with-body -X POST \
      -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
      --data "{\"schema\": \"${schema_as_json_string}\"}" \
      "${SCHEMA_REGISTRY_URL}/subjects/${subject}/versions")"; then
    break
  fi
  if [ "$attempt" -ge 30 ]; then
    echo "Schema registration failed: ${response}" >&2
    exit 1
  fi
  echo "Schema registry is not accepting registrations yet, retrying... (${attempt}/30)"
  attempt=$((attempt + 1))
  sleep 2
done
schema_id="$(printf '%s' "$response" \
  | sed -n 's/.*"id"[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p')"
if [ -z "$schema_id" ]; then
  echo "Could not parse a schema id from: ${response}" >&2
  exit 1
fi
echo "Registered schema id ${schema_id} as subject ${subject}."

# One POST per batch. The proxy answers 422 when any record fails, which
# --fail-with-body turns into a non-zero exit.
produce_batch() {
  if ! response="$(curl -sS --fail-with-body -X POST \
      -H 'Content-Type: application/vnd.kafka.avro.v2+json' \
      --data "{\"value_schema_id\": ${schema_id}, \"records\": [$1]}" \
      "${KAFKA_REST_URL}/topics/${DEMO_TOPIC}")"; then
    echo "Produce request failed: ${response}" >&2
    exit 1
  fi
  case "$response" in
    *error_code*)
      echo "Produce request reported a record error: ${response}" >&2
      exit 1
      ;;
  esac
}

# Deterministic content, so the DuckDB check can assert on the decoded values
# and not only on the row count: order_id runs 1..N, so sum(order_id) must be
# N * (N + 1) / 2. Orders rotate over three regions and 1..5 units at 19.99.
echo "Producing ${NUM_RECORDS} Avro orders to ${DEMO_TOPIC} via ${KAFKA_REST_URL}..."
now_ms=$(( $(date +%s) * 1000 ))
records=""
in_batch=0
sent=0
i=1
while [ "$i" -le "$NUM_RECORDS" ]; do
  case $(( (i - 1) % 3 )) in
    0) region=AMER ;;
    1) region=EMEA ;;
    *) region=APAC ;;
  esac
  quantity=$(( (i - 1) % 5 + 1 ))
  cents=$(( quantity * 1999 ))
  record="$(printf '{"value":{"order_id":%d,"customer":"customer-%03d","region":"%s","quantity":%d,"amount":%d.%02d,"order_ts_ms":%d}}' \
    "$i" "$(( (i - 1) % 20 + 1 ))" "$region" "$quantity" \
    "$(( cents / 100 ))" "$(( cents % 100 ))" "$(( now_ms - (NUM_RECORDS - i) * 1000 ))")"
  records="${records}${records:+,}${record}"
  in_batch=$((in_batch + 1))
  if [ "$in_batch" -ge "$batch_size" ] || [ "$i" -eq "$NUM_RECORDS" ]; then
    produce_batch "$records"
    sent=$((sent + in_batch))
    records=""
    in_batch=0
  fi
  i=$((i + 1))
done
echo "Produced ${sent} records. Sample: ${record}"
