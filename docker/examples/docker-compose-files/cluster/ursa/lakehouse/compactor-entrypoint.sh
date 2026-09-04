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

set -eu

config_file=/tmp/ursa-storage.properties
: > "$config_file"

property() {
  printf '%s=%s\n' "$1" "$2" >> "$config_file"
}

# Lakestream catalog, WAL metadata, and compaction task coordination.
property metadataStoreUrl "$URSA_METADATA_STORE_URL"
property oxiaStorageUrl "$URSA_OXIA_STORAGE_URL"
# WAL entries contain Kafka MemoryRecords, which the compaction layer decodes
# before writing compacted rows.

# Ursa WAL in MinIO.
property backendStorageType S3
property bucket "$URSA_WAL_BUCKET"
property prefix "$URSA_WAL_PREFIX"
property cloudStorageEndpoint "$URSA_S3_ENDPOINT"
property region "$AWS_REGION"
property s3AccessKeyId "$AWS_ACCESS_KEY_ID"
property s3SecretAccessKey "$AWS_SECRET_ACCESS_KEY"
property s3PathStyleAccess true

# Managed compacted objects used by Kafka fetch after compaction.
property compactionBackendStorageType S3
property compactionBucket "$URSA_COMPACTION_BUCKET"
property compactionPrefix "$URSA_COMPACTION_PREFIX"
property compactionBucketRegion "$AWS_REGION"
property hadoop.fs.s3a.endpoint "$URSA_S3_ENDPOINT"
property hadoop.fs.s3a.path.style.access true
property hadoop.fs.s3a.connection.ssl.enabled false

# Keep the local feedback loop short.
property compactedFileSizeLimit "${URSA_COMPACTED_FILE_SIZE_LIMIT:-1}"
property tailCompactDataVisibilityIntervalInSeconds "${URSA_TAIL_VISIBILITY_INTERVAL_SECONDS:-2}"
property refreshLocalTopicInternalInSeconds "${URSA_REFRESH_LOG_INTERVAL_SECONDS:-1}"
property refreshLocalTaskIntervalInSeconds "${URSA_REFRESH_TASK_INTERVAL_SECONDS:-1}"
property compactedThreadNum "${URSA_COMPACTED_THREAD_NUM:-2}"
property publishThreadNum "${URSA_PUBLISH_THREAD_NUM:-2}"
property commitThreadNum "${URSA_COMMIT_THREAD_NUM:-2}"
property maxCommitIntervalInSeconds "${URSA_MAX_COMMIT_INTERVAL_SECONDS:-2}"
property metastoreRequestRateLimitPerSecond "${URSA_METASTORE_RATE_LIMIT:-500}"

# Diskless compaction-task ownership stays inside Ursa. The compactor discovers
# Lakestream logs and publishes each range before materializing it.
property internalCompactionTaskPublisherEnabled true

# Record a per-file ManagedTableFileIndex in each compacted entry index. The
# brokers' Kafka-only reader serves fetches of compacted ranges from that index
# and refuses entries without it, so without this flag consumers stop seeing
# data as soon as a range is compacted.
property managedTableSchemaEvolutionEnabled true

# Everything above is what compaction needs on its own: the WAL is compacted into
# the managed objects Kafka fetch reads, which is also what advances the WAL
# delete watermark so retention can free storage. That runs with no catalog.
#
# Materialization is the second sink: the same WAL read is additionally written
# to an external Iceberg table registered in Polaris. It needs the `lakehouse`
# profile to be up, so it stays off unless the caller asks for it.
if [ "${URSA_MATERIALIZATION_ENABLED:-false}" = "true" ]; then
  property materializationEnabled true
  property materializationDefaultNamespace default
  property clusterSdtEnabled true
  property clusterSbtEnabled true
  property lakehouseType ICEBERG
  property streamTableMode EXTERNAL
  property catalog.name polaris

  # Kafka records its logical topic name as system-owned stream metadata. Ursa uses it as the
  # default external table name while keeping the UUID-qualified stream identity for managed data.
  # A topic recreated under the same name therefore appends to the existing external table.

  # Schema registry speaking the Confluent REST API (Karapace here; Confluent Schema Registry
  # or Apicurio's ccompat API work the same). The compactor looks up the `<topic>-value`
  # subject: a topic whose records carry an Avro, JSON Schema or Protobuf schema id is decoded
  # into typed Iceberg columns, a topic without a subject lands as a single `payload` binary
  # column. Only the Iceberg sink reads schemas; the managed objects Kafka fetch reads stay
  # raw batches, so the brokers never need this URL.
  if [ -n "${URSA_SCHEMA_REGISTRY_URL:-}" ]; then
    property schemaRegistryUrl "$URSA_SCHEMA_REGISTRY_URL"
  fi

  # Iceberg REST catalog and S3FileIO configuration. Polaris authentication is
  # deliberately static in this local-only stack; credential vending is not
  # needed to validate Kafka -> Ursa -> Iceberg -> DuckDB.
  property iceberg.catalog.polaris.type rest
  property iceberg.catalog.polaris.uri "$POLARIS_CATALOG_URI"
  property iceberg.catalog.polaris.warehouse "$POLARIS_CATALOG_NAME"
  property iceberg.catalog.polaris.credential "$POLARIS_CLIENT_ID:$POLARIS_CLIENT_SECRET"
  property iceberg.catalog.polaris.scope PRINCIPAL_ROLE:ALL
  property iceberg.catalog.polaris.oauth2-server-uri "$POLARIS_OAUTH2_URI"
  property iceberg.catalog.polaris.catalog-backend POLARIS
  property iceberg.catalog.polaris.io-impl org.apache.iceberg.aws.s3.S3FileIO
  property iceberg.catalog.polaris.client.region "$AWS_REGION"
  property iceberg.catalog.polaris.s3.endpoint "$URSA_S3_ENDPOINT"
  property iceberg.catalog.polaris.s3.path-style-access true
  property iceberg.catalog.polaris.s3.access-key-id "$AWS_ACCESS_KEY_ID"
  property iceberg.catalog.polaris.s3.secret-access-key "$AWS_SECRET_ACCESS_KEY"
else
  property materializationEnabled false
fi

exec java ${URSA_JAVA_OPTS:--Xmx1024M -XX:+UseZGC} \
  -Dio.netty.tryReflectionSetAccessible=true \
  -Djava.net.preferIPv4Stack=true \
  -Dlog4j.configurationFile=/opt/ursa-demo/log4j2.properties \
  -cp '/opt/ursa/ursa-storage-compact.jar:/opt/ursa/lib/*' \
  io.lakestream.ursa.compact.CompactionMain --conf "$config_file"
