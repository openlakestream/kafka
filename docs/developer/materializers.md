<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Materializers and UFK

A *materializer* turns a stream into a table in another system, such as Apache Iceberg or Delta Lake.
Materializers run in the Ursa *compactor*, the service that rewrites the write-ahead log
(WAL) into larger, columnar objects in the background. The compactor reads each range of a stream
once, and hands the entries to the materializer along the way. Materializers never run in the Kafka
brokers.

The materializer SPI, and the Iceberg and Delta materializers, live in
[openlakestream/ursa](https://github.com/openlakestream/ursa). To write a new one, follow Ursa's
[Write a materializer](https://github.com/openlakestream/ursa/blob/main/docs/developer/materializer-guide.md)
guide. This page covers what is specific to Ursa for Apache Kafka (UFK):

- [What UFK writes](#what-ufk-writes): the streams, entries and properties a materializer gets from a
  Kafka topic
- [What UFK doesn't do](#what-ufk-doesnt-do)

## What UFK writes

Every diskless topic is backed by one Lakestream stream. A materializer reads these streams like any
other. This is what's specific to the streams UFK writes.

**One stream per topic, one log per partition.** The stream is named
`default/<topic>-topic-id-<topic ID>`, in the `default` namespace. The topic ID keeps a deleted topic
apart from a new topic created under the same name. Each partition of the topic is one log of the
stream, in partition order.

**Entries are the producer's record batches.** Each WAL entry holds the `MemoryRecords` that a
producer sent, one or more record batches, with the producer's compression. The offsets inside the
batches aren't Kafka offsets. Producer batches are stored with base offset 0, and the entry's own
offset is where they start. Ursa's `KafkaStorageEntryDecoder` rebases them for you. When a topic uses
`LogAppendTime`, the broker has already stamped the time into each batch. Transactional and control
batches are never written, because diskless topics don't support transactions.

**Stream properties name the topic.** UFK sets these properties on every stream, and discards any
value a caller tries to set for them:

| Property | Value |
|---|---|
| `lakestream.source.logical.name` | The topic name |
| `lakestream.kafka.topic.name` | The topic name |
| `lakestream.kafka.topic.id` | The topic ID |
| `lakestream.kafka.managed` | `true` |
| `lakestream.kafka.source.revision` | The offset in Kafka's metadata log that the properties were read from. UFK uses it to ignore out-of-date updates. |

The stream properties also carry the topic's explicit configuration overrides, such as `retention.ms`.
Broker defaults aren't copied.

**Name things by the logical name, not the stream name.** A topic that is deleted and created again
under the same name gets a new stream with the same logical name. Look up schemas and name tables by
the logical name, which Ursa's `KafkaSourceMetadata.topicName` returns, so that the recreated topic
keeps writing to the same table. Never parse the topic out of the stream name.

**Schemas come from a schema registry, not from Kafka.** UFK attaches no schema to a stream. The
compactor looks up the `<topic>-value` subject in the registry that `schemaRegistryUrl` points to,
and decodes Avro, JSON Schema and Protobuf values. What happens to a value without a registered
subject depends on the materializer. The Iceberg materializer, for example, writes it to a single
`payload` column. Register the subject before the first record reaches the compactor: the compactor
caches a missing subject, and treats the topic as raw bytes until it restarts.

The code behind this section is
[`KafkaStreamIdentity`](../../storage/diskless-ursa/src/main/java/org/apache/kafka/storage/diskless/handlers/KafkaStreamIdentity.java),
[`PartitionWriter`](../../storage/diskless-ursa/src/main/java/org/apache/kafka/storage/diskless/handlers/PartitionWriter.java)
and
[`KafkaRecordsPayload`](../../storage/diskless-ursa/src/main/java/org/apache/kafka/storage/diskless/handlers/KafkaRecordsPayload.java).
Changing any of it changes what every materializer receives, so it needs a LIP in
[openlakestream/lips](https://github.com/openlakestream/lips/blob/main/CONTRIBUTING.md).

## What UFK doesn't do

- **There are no per-topic materialization settings.** `ursa.storage.enable` is the only Ursa topic
  configuration, and Kafka rejects topic configurations it doesn't know. Configure
  materialization in the compactor instead. `materializationDefaultNamespace=default` applies one
  policy to every diskless topic. For a policy per stream, use the
  `StreamCatalog` API, as described in Ursa's
  [Materialize a stream to a table](https://github.com/openlakestream/ursa/blob/main/docs/user/table-materialization.md).
- **Brokers don't load materializers.** A materializer's jars go on the compactor's classpath only.
- **Kafka reads don't go through the materializer.** Consumers read the WAL, or, after compaction,
  the compacted objects the compactor writes for Kafka.
- **Compaction doesn't skip a failing materializer.** The compactor commits a range only after the
  materializer commits it. While a destination is down, compaction of that stream stops, and so does
  the storage that retention would free.

## What a materializer pull request here looks like

Usually, there isn't one. A new materializer lives in its own repository, with a LIP and a small
registration pull request in Ursa. Pull requests here are for:

- this documentation
- changes to what UFK writes for materializers: the stream identity, the stream properties, or the
  entry format. These need a LIP in [openlakestream/lips](https://github.com/openlakestream/lips),
  because Ursa's Kafka codecs and every materializer depend on them.

## Getting help

Ask about running materializers against UFK in
[Discussions: Q&A](https://github.com/openlakestream/kafka/discussions/categories/q-a). Questions
about the SPI, or about a materializer you're writing, belong in
[Ursa's Discussions](https://github.com/openlakestream/ursa/discussions).
