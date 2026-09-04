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

# LIP-001: Diskless Storage with Ursa Integration

- *Author(s)*: Kai Wang
- *Proposal time*: 2026-01
- *Implemented*: YES
- *Released*: NO
- *Repository*: https://github.com/lakestream-io/kafka
- *Discussion Link*: N/A

## TL;DR

This LIP introduces **Diskless Storage** mode for Apache Kafka, replacing traditional local log persistence with remote stream-based storage via **Ursa**. When enabled, Kafka brokers become stateless with respect to message data, offloading durability to Ursa's distributed storage layer and producer state management to **Oxia** (a distributed key-value store). This architectural shift enables rapid broker failover, eliminates ISR-based replication overhead for diskless topics, and provides a foundation for truly elastic Kafka deployments.

## Background Knowledge

### Traditional Kafka Storage Model

In standard Apache Kafka, each broker maintains local log segments on disk:
- **Log Segments**: Messages are appended to `.log` files organized by partition
- **Index Files**: `.index` and `.timeindex` files enable efficient offset lookups
- **Producer State**: `.snapshot` files track idempotent producer sequences for exactly-once semantics
- **ISR Replication**: Followers continuously fetch from leaders to maintain in-sync replicas

This model tightly couples brokers to their local storage, making failover expensive (data must be re-replicated) and scaling inflexible.

### Ursa Storage

Ursa is a distributed storage engine designed for streaming workloads:
- **Stream-based API**: Data is organized as streams with append-only semantics
- **Built-in Replication**: Ursa handles durability internally, eliminating the need for Kafka-level ISR
- **Backend Flexibility**: Supports LOCAL, S3, GCS, and Azure Blob storage backends
- **High Throughput**: Optimized for streaming append and sequential read patterns

### Oxia

Oxia is a distributed key-value store used for metadata and state management:
- **Producer State Persistence**: Replaces local `.snapshot` files for idempotent producer tracking
- **Catalog Persistence**: The Ursa StreamCatalog provider may persist layouts and lifecycle state in Oxia; Kafka never reads or writes those private keys
- **Fast Recovery**: Enables rapid broker failover without local state re-hydration

### KIP-405 Tiered Storage (Context)

Apache Kafka's KIP-405 introduced tiered storage, where older log segments are moved to remote storage while recent data remains local. **Diskless storage differs fundamentally**: it eliminates local storage entirely for designated topics, treating the remote system as the primary (and only) storage layer.

## Motivation

### Current Limitations

1. **Expensive Failover**: When a broker fails, a new broker must replicate all partition data before becoming fully available, leading to prolonged recovery times.

2. **Storage-Compute Coupling**: Brokers are tightly bound to their local disks, preventing true elastic scaling. Adding brokers requires data rebalancing; removing brokers requires data migration.

3. **ISR Replication Overhead**: For every message produced, followers must fetch and persist the data, consuming network bandwidth and disk I/O.

4. **Stateful Brokers**: Local state (logs, snapshots, indexes) makes brokers stateful, complicating container orchestration and cloud deployments.

### Why Diskless Storage

- **Instant Failover**: Since data resides in Ursa, any broker can immediately serve a partition after leader election without data synchronization.
- **Elastic Scaling**: Brokers become stateless workers; scale up/down based on CPU and network, not storage.
- **Reduced Operational Complexity**: No local disk management, simplified backup/restore, cloud-native deployment patterns.
- **Cost Optimization**: Leverage object storage (S3) pricing instead of attached block storage.

## Goals

### In Scope

1. **Topic-Level Diskless Mode**: Enable diskless storage on a per-topic basis via `ursa.storage.enable=true` topic configuration.

2. **Transparent Client Compatibility**: Existing Kafka producers and consumers work without modification; the diskless nature is transparent to clients.

3. **Idempotent Producer Support**: Maintain exactly-once semantics by persisting producer state in Oxia instead of local files.

4. **Multiple Backend Support**: Support LOCAL, S3, GCS, and Azure Blob storage backends for Ursa.

5. **Async I/O Model**: Implement fully asynchronous storage operations to avoid blocking request handler threads.

### Out of Scope

1. **Transactional Producers**: Initial implementation rejects transactional produce requests for diskless topics (future enhancement).

2. **Internal Topics**: System topics like `__consumer_offsets` and `__transaction_state` remain on traditional local storage.

3. **Compacted Topics**: Kafka key-based log compaction semantics are not supported for diskless topics in this phase. Only external WAL-to-Parquet style compaction in the Ursa storage layer (for storage/analytics) is available.

4. **Consumer Group Offset Storage**: Consumer offsets continue to use the traditional `__consumer_offsets` topic.

## High Level Design

The diskless storage architecture introduces a **bypass layer** that intercepts storage operations in the `ReplicaManager` and routes them to Ursa instead of local logs.

In this LIP, diskless topics are handled by the Lakestream-backed implementations
`UrsaLakestreamWriter` / `UrsaLakestreamReader`.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Kafka Broker                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   ┌─────────────────┐                                                       │
│   │  Kafka Clients  │  (Producers / Consumers / Admin)                      │
│   └────────┬────────┘                                                       │
│            │                                                                │
│            ▼                                                                │
│   ┌─────────────────┐                                                       │
│   │  KafkaApis      │  Request Handler                                      │
│   └────────┬────────┘                                                       │
│            │                                                                │
│            ▼                                                                │
│   ┌─────────────────────────────────────────────────────────────────────┐   │
│   │                       ReplicaManager                                │   │
│   │  ┌─────────────────────────────────────────────────────────────┐    │   │
│   │  │         DisklessStorageReplicaManagerSupport                │    │   │
│   │  │  ┌─────────────────┐    ┌──────────────────┐                │    │   │
│   │  │  │ partitionEntries│───▶│ isDisklessTopic? │                │    │   │
│   │  │  └─────────────────┘    └────────┬─────────┘                │    │   │
│   │  │                                  │                          │    │   │
│   │  │              ┌───────────────────┼───────────────────┐      │    │   │
│   │  │              │                   │                   │      │    │   │
│   │  │              ▼                   ▼                   ▼      │    │   │
│   │  │    ┌─────────────────┐ ┌─────────────────┐ ┌────────────┐   │    │   │
│   │  │    │Lakestream Writer│ │Lakestream Reader│ │ Classic Log│   │    │   │
│   │  │    └────────┬────────┘ └────────┬────────┘ └─────┬──────┘   │    │   │
│   │  └─────────────┼───────────────────┼────────────────┼──────────┘    │   │
│   └────────────────┼───────────────────┼────────────────┼───────────────┘   │
│                    │                   │                │                   │
└────────────────────┼───────────────────┼────────────────┼───────────────────┘
                     │                   │                │
                     ▼                   ▼                ▼
          ┌─────────────────┐  ┌─────────────────┐  ┌──────────────┐
          │  Ursa Storage   │  │  Ursa Storage   │  │  Local Disk  │
          │  (Remote)       │  │  (Remote)       │  │  (.log files)│
          └────────┬────────┘  └────────┬────────┘  └──────────────┘
                   │                    │
                   ▼                    ▼
          ┌─────────────────────────────────────┐
          │          Storage Backend            │
          │ (LOCAL / S3 / GCS / Azure Blob)     │
          └─────────────────────────────────────┘
                            │
                            ▼
          ┌─────────────────────────────────────┐
          │              Oxia                   │
          │      Producer State / Catalog       │
          └─────────────────────────────────────┘
```

### Key Design Decisions

1. **Topic-Level Granularity**: Diskless mode is enabled per-topic, allowing mixed deployments where some topics use traditional storage and others use Ursa.

2. **Replication Factor = 1**: Since Ursa handles durability, Kafka-level replication is bypassed. Diskless topics are created with RF=1 (or the controller enforces this).

3. **Async-First**: All Ursa operations return `CompletableFuture`, ensuring request handler threads are never blocked on remote I/O.

4. **Producer State in Oxia**: Idempotent producer sequences are validated and persisted asynchronously to Oxia, enabling stateless broker failover.

## Detailed Design

### Design & Implementation Details

#### Core Components

| Component | Responsibility |
|-----------|----------------|
| `DisklessStorageReplicaManagerSupport` | Entry point; partitions requests between diskless and classic paths |
| `UrsaLakestreamWriter` | Write path for diskless topics; appends records to a Lakestream log |
| `UrsaLakestreamReader` | Read path for diskless topics; fans Fetch and ListOffsets out over partitions and decides the request-level long poll |
| `UrsaStorageState` | Manages partition LogId handles, offset tracking, and shared state |
| `UrsaPartitionLog` | One diskless partition: the Lakestream log handle, its writer, reader and retention, and the storage-error mapping |
| `PartitionWriter` | Per-partition write path: validation, per-partition ordering, payload ownership, the append, and the long-poll wake-up |
| `PartitionReader` | Per-partition read path: the cached fetch cursor, the cached offset window, and the ListOffsets lookups |
| `PartitionRetention` | Per-partition retention: coalesces trim requests and drives them against the log |
| `ProducerStateManager` | Validates idempotent producer sequences and persists snapshots to Oxia |
| `DisklessTopicLifecycle` | Controller-side SPI for one topic's storage: `ensureTopic`, `deleteTopic`, `listManagedTopics`, `sweepOrphans` |
| `DisklessTopicLifecycleReconciler` | Active-controller reconciler: drives `DisklessTopicLifecycle` towards the metadata image and sweeps orphans on a configurable interval |
| `UrsaStorageConfig` | Configuration holder for Ursa settings |
| `MetadataCacheDisklessStorageView` | Determines if a topic is diskless based on topic config |
| `ListOffsetsPartitionRequest` | Request DTO for diskless ListOffsets operations |
| `ListOffsetsPartitionResponse` | Response DTO for diskless ListOffsets operations |

#### Topic Lifecycle Ownership

| Actor | Responsibility |
|---|---|
| Broker | Opens a partition with create-if-absent: `openLog(id, p)`; on `NoSuchStreamException` calls `createStream` with the topic's partition count and identity properties, on "partition not in layout" calls `increasePartitions`, then opens again. Retries once on `AlreadyExistsException`. |
| Active controller | Reconciles properties, partition growth, deletion, and orphan cleanup through one `DisklessTopicLifecycle`. The reconciler records each topic's desired state from the metadata image and drives it on its own executor, retrying with backoff until it succeeds or the desired state changes; operations are therefore idempotent and safe to overlap. |
| Ursa provider | Fans a lifecycle call out to the catalog and to producer-state cleanup. The controller never sees two stores. |

**Producer State**: Producer state keeps only `producer-state-snapshot/<topicId>-<partition>[/<zone>]` keys and a `producer-state-snapshot-deleted/<topicId>` fence. Deletion writes the fence first, so a broker claiming a snapshot cannot recreate one behind the delete; then a range delete over the unzoned prefix; then a per-key delete of the zoned snapshots enumerated through the `producer-state-snapshot-topic` secondary index, because Oxia compares keys one path segment at a time and no fixed range bound covers a zone of arbitrary depth. A periodic sweep on the active controller removes whatever a claim recreated inside its own fence-read window.

#### Write Path (Produce)

```
Producer Request
       │
       ▼
ReplicaManager.appendRecords()
       │
       ├──▶ disklessStorageSupport.partitionAppendEntries()
       │         │
       │         ├──▶ directEntries (diskless topics)
       │         │         │
       │         │         ▼
       │         │    UrsaLakestreamWriter.write()
       │         │         │
       │         │         ├──▶ ProducerStateManager.validate()   ◄── Oxia
       │         │         │         (sequence validation)
       │         │         │
       │         │         ├──▶ UrsaStorageState.getOrCreatePartitionLog()
       │         │         │
       │         │         ├──▶ Log.append(numberOfMessages, data)
       │         │         │
       │         │         └──▶ ProducerStateManager.updateAfterWrite()
       │         │
       │         └──▶ classicEntries (traditional topics)
       │                   │
       │                   ▼
       │              appendRecordsToLeader() (local log)
       │
       ▼
   Combine Results & Respond
```

**Key Async Changes**:
- `UrsaLakestreamWriter.write()` returns `CompletableFuture<Map<TopicIdPartition, PartitionResponse>>`
- Lakestream append operations are non-blocking (`Log.append`)
- Producer state validation and updates are async operations to Oxia
- Callback composition using `thenCompose` and `whenComplete`

#### Performance Note: Why `flush.interval=250ms` Can Become `~1000ms+` Produce Latency

Ursa's write buffer is flushed periodically (`ursa.storage.write.buffer.flush.interval.ms`). In isolation, a single append typically completes within:
- **best case**: just after a flush (near-0 additional wait)
- **worst case**: just before a flush (up to ~`flush.interval` additional wait)
- **average**: ~`flush.interval / 2` additional wait (assuming uniform arrival within the cycle)

However, Kafka’s broker network layer historically enforces a **single in-flight request per TCP connection**:
- After the broker reads one request from a connection, it **mutes** that connection and does not read another request from it until the current request’s response is fully sent.
- This behavior simplifies response ordering and bounds per-connection buffering, but it also turns the broker into a **strict per-connection queue**.

For diskless topics, the `ProduceRequest` completion is gated by remote durability (Ursa flush + Oxia producer state update for idempotent producers). If the client (producer) has `N` in-flight produce requests on one connection, and the broker processes them strictly one-by-one, the end-to-end latency becomes approximately:
- **worst case**: `N × flush.interval` (each request misses a flush cycle)
- **average**: `(N + 1) / 2 × flush.interval`

Example: with `flush.interval=250ms` and typical `max.in.flight.requests.per.connection=5`, the average can drift toward `~3 × 250ms = 750ms` and p99 can approach `~5 × 250ms = 1250ms`, before accounting for any additional scheduling, serialization, or storage overhead. This explains “250ms flush, but ~1000ms+ produce latency”.

This amplification is not caused by “out-of-order writes”; it is caused by **head-of-line blocking** due to broker-side connection muting.

#### Fix: Move the “Waiting Point” from “Per Request” to “Per Flush Cycle”

The main goal is to let the broker continue to read and parse new requests even while prior produce requests are waiting on Ursa durability, so multiple produces can be “covered” by the same Ursa flush cycle instead of waiting a full flush cycle each.

We make two complementary changes:

1. **Broker Request Pipelining (SocketServer)**:
   - Allow multiple requests to be in-flight concurrently on the same connection (no mute on request received).
   - Preserve correctness by enforcing **in-order responses per connection** (buffer completed responses until earlier responses are sent).
   - Keep throttling semantics: throttled requests still apply backpressure via mute/unmute during the throttle window.

2. **Per-partition Sequenced Writes for Non-idempotent Appends**:
   - Non-idempotent producers reuse the broker's per-partition sequenced write queue instead of a dedicated pipeline object.
   - Each write is validated before append submission, then handed off to `Log.append()`.
   - Once the append is submitted to the Lakestream log, the next queued write for that partition may be submitted, which preserves submission order without a separate configurable pipeline.

#### Threading Model (What Runs Where)

This matters because “async storage” alone does not guarantee low end-to-end latency if the broker stops reading the connection.

- **Network I/O**: `SocketServer` processor threads read from the TCP connection, parse requests, and place them into the request queue.
- **Request handling**: request handler threads execute `KafkaApis`/`ReplicaManager` and start diskless write work.
- **Storage + state I/O**: Ursa/Oxia operations complete on their own async executors/IO threads.
- **Callback completion**: diskless produce completion is posted back onto the request handler context before generating the response.
- **Response send**: the network layer sends responses back to the client; with pipelining enabled, it still sends strictly in-order per connection.

Without broker request pipelining, the network layer’s mute/unmute behavior makes the connection itself the bottleneck (head-of-line blocking), regardless of how asynchronous the storage layer is.

#### Storage Entry Payload

Each Lakestream entry payload is exactly the Kafka `MemoryRecords` bytes received by the broker. There is no
additional storage envelope or protocol-specific metadata prefix. Lakestream's entry header is authoritative for the
durable base offset and record count; readers validate the complete Kafka batch payload and rebase every batch from
that entry metadata before returning it to clients.

Readers intentionally do not accept the former length-prefixed payload. This is a hard format cutover: deployments
must isolate or migrate the WAL entries, compacted Parquet objects, and their indexes before switching. Reusing an old
storage namespace is unsafe because the legacy Parquet objects retain the same serde identifier and cannot be
distinguished from raw `MemoryRecords` through metadata alone.

#### Read Path (Fetch)

```
Fetch Request
       │
       ▼
ReplicaManager.fetchMessages()
       │
       ├──▶ disklessStorageSupport.partitionFetchInfos()
       │         │
       │         ├──▶ directFetches (diskless topics)
       │         │         │
       │         │         ▼
       │         │    UrsaLakestreamReader.fetch()
       │         │         │
       │         │         ├──▶ UrsaStorageState.getOrCreatePartitionLog()
       │         │         │
       │         │         ├──▶ PartitionReader offset window
       │         │         │      (cached ≤ 100 ms; else Log.getFirstOffset + getLastOffset,
       │         │         │       always re-read before OFFSET_OUT_OF_RANGE)
       │         │         │
       │         │         ├──▶ LogCursor.readEntries()
       │         │         │
       │         │         └──▶ Convert entries to MemoryRecords
       │         │               (rebase batches from LogEntry offset metadata)
       │         │
       │         └──▶ classicFetches (traditional topics)
       │                   │
       │                   ▼
       │              Log.read() (local log)
       │
       ▼
   Combine Results & Respond
```

**Offset Handling**:
- Lakestream entries use their log offset as the base offset (and may contain multiple Kafka records)
- `UrsaLakestreamReader` patches the `baseOffset` of fetched record batches to match the log-entry offset
- This ensures consumers see consistent offsets regardless of storage backend

**Offset Window Caching**: Every fetch and every ListOffsets needs the log's offset window --
`Log.getFirstOffset()` and `Log.getLastOffset()`, two index reads against Ursa's metadata store.
`PartitionReader` reads that window at most once every **100 ms** per partition
(`OFFSET_RANGE_REFRESH_MS`, a constant rather than a config) and answers from the cached one in
between; concurrent requests share one in-flight read rather than each starting their own. The
exception is `ListOffsets(-2/-4)`, which always reads it fresh (see **ListOffsets Path**). Because
a Lakestream log has one writer per zone-aware owner, the window is deliberately a bounded-staleness
view rather than an exact one:

- **This broker's own appends are visible immediately.** `PartitionWriter` reports every entry that
  lands to `PartitionReader.observeAppend()` before it wakes any long-polling fetch, which widens
  the cached window with no read at all. The window only ever moves forward.
- **Another owner's appends are visible within the refresh interval or one fetch wait, whichever is
  longer.** A caught-up consumer long-polls for `fetch.max.wait.ms` before it re-reads, which is
  already longer than 100 ms, so the interval alone is what refreshes the window it reads through.
- **`OFFSET_OUT_OF_RANGE` is never answered from a window read before the request arrived.** A fetch
  offset the cached window does not cover forces a read from storage, and that read must have been
  *issued after the fetch arrived* -- joining one that was already running could let a window
  fetched before another owner's append condemn an offset that does exist and reset the consumer.
- **This broker's own retention trim drops the window**, because the trim is what moves the first
  offset; closing the partition drops it too. A trim run by *another* owner of the log is not
  signalled here, so it lands in the cached window only when the window is next read -- bounded by
  the refresh interval, exactly like that owner's appends. What a client is told about the first
  offset is not bounded that way: `ListOffsets(-2/-4)` reads the window fresh, so it reports every
  trim, whoever ran it.
- **The window is timed against the monotonic clock**, so a wall-clock step backwards cannot make a
  stale window look young and leave its staleness unbounded.

#### ListOffsets Path

```
ListOffsets Request
       │
       ▼
ReplicaManager.fetchOffset()
       │
       ├──▶ disklessStorageSupport.isDisklessStorageTopic()
       │         │
       │         ├──▶ YES: Validation (duplicates, unsupported timestamps)
       │         │         │
       │         │         ├──▶ Duplicate partition? → INVALID_REQUEST
       │         │         │
       │         │         ├──▶ Unsupported timestamp? → UNSUPPORTED_VERSION
       │         │         │
       │         │         └──▶ Valid request:
       │         │                   │
       │         │                   ▼
       │         │              DisklessStorageReplicaManagerSupport.handleListOffsets()
       │         │                   │
       │         │                   ▼
       │         │              UrsaLakestreamReader.listOffsets()
       │         │                   │
       │         │                   ├──▶ EARLIEST (-2): offset window start, always read fresh
       │         │                   │
       │         │                   ├──▶ LATEST (-1): cached offset window HWM
       │         │                   │
       │         │                   ├──▶ MAX_TIMESTAMP (-3): last entry header (offset + write time)
       │         │                   │
       │         │                   ├──▶ LATEST_TIERED (-5): return -1 (N/A)
       │         │                   │
       │         │                   └──▶ timestamp >= 0: binary search entry headers, then scan forward
       │         │
       │         └──▶ NO: fetchOffsetClassic() (local log)
       │
       ▼
   Combine Results & Respond
```

**Supported Timestamp Queries**:

| Timestamp Value | Meaning | Diskless Behavior |
|-----------------|---------|-------------------|
| `-2` (EARLIEST) | First available offset | Returns the first available Lakestream log offset, from a window read issued after the request arrived rather than from the cache |
| `-1` (LATEST) | High watermark | Returns the end offset derived from the last Lakestream log offset and record count, from the cached offset window |
| `-3` (MAX_TIMESTAMP) | Offset with highest timestamp | Answers from the last entry's header alone: its base offset and its write timestamp (see **Limitations**) |
| `-4` (EARLIEST_LOCAL) | First local offset | Same as EARLIEST for diskless (no local/remote distinction), including the fresh read |
| `-5` (LATEST_TIERED) | End of tiered storage | Returns -1 (not applicable for diskless) |
| `>= 0` | First offset with timestamp >= value | Binary searches entry headers, then reads forward over at most 256 entries |

**Key Implementation Details**:

1. **Validation in ReplicaManager**: Before forwarding to Ursa, `ReplicaManager.fetchOffset()` applies the same validation as the classic path:
   - Duplicate partition check → `INVALID_REQUEST`
   - Unsupported timestamp for protocol version → `UNSUPPORTED_VERSION`

2. **EARLIEST is read fresh; LATEST follows the cached window**: both are served from the Lakestream log's first and last offsets, but they treat the ≤ 100 ms cached offset window (see **Read Path (Fetch)** → **Offset Window Caching**) differently. LATEST and MAX_TIMESTAMP answer from the cache and may therefore lag another owner's append by up to the refresh interval. EARLIEST and EARLIEST_LOCAL never do: they are answered from a window read *issued after the request arrived*, reusing the gate the fetch path uses before it reports `OFFSET_OUT_OF_RANGE`, so a concurrent in-flight read is shared and a request costs at most one extra pair of index reads. Only a retention trim moves the log's start, ListOffsets is rare next to fetch, and a client asking where the log begins is usually about to reset to that offset -- so exactness is worth the read. The freshly read window is cached like any other refresh.

3. **Timestamp Search**: Binary searches the Lakestream entry headers for the last entry whose write time predates the target, then decodes entries forward from it until one holds a record at or after the target. A record's create time is never newer than the write time of the entry carrying it, which is what lets the binary search skip everything before that point; past it the records can lag their headers by any amount, so the forward scan is bounded at 256 entries. Exhausting the bound answers with the base offset and write time of the earliest scanned entry written at or after the target -- at or before the true answer, never past it, so a consumer seeking there re-reads a little rather than skipping records. No cursor is opened, and the broker does not depend on a provider-specific publish-time index.

4. **Async Execution**: All ListOffsets operations return `CompletableFuture` to avoid blocking request handler threads.

#### ISR Bypass

For diskless topics:
1. `DisklessTopicMetadataTransformer` reports ISR = [leader] only
2. `ReplicaFetcherThread` skips fetching for diskless partitions
3. No delayed produce waiting for follower acks (Ursa ack is sufficient)

#### Producer State Management

```
┌─────────────────────────────────────────────────────────────┐
│                    ProducerStateManager                     │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  In-Memory Cache                                            │
│  ┌─────────────────────────────────────────────────────┐    │
│  │  Map<TopicIdPartition, Map<ProducerId, ProducerState>>│    │
│  └─────────────────────────────────────────────────────┘    │
│                          │                                  │
│                          │ periodic snapshot                │
│                          ▼                                  │
│  ┌─────────────────────────────────────────────────────┐    │
│  │                     Oxia                            │    │
│  │  Key: producer-state-snapshot/{topicId}-{partition} │    │
│  │  Value: Serialized ProducerStateSnapshot            │    │
│  └─────────────────────────────────────────────────────┘    │
│                                                             │
│  Recovery Flow:                                             │
│  1. Load snapshot from Oxia                                 │
│  2. Replay entries from Ursa after snapshot offset          │
│  3. Rebuild in-memory producer state                        │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Public-facing Changes

#### Public API

No changes to the Kafka protocol. Existing `ProduceRequest`, `FetchRequest`, and `ListOffsetsRequest` work transparently with diskless topics.

**Behavioral Differences**:
- Transactional produce requests are rejected with `INVALID_REQUEST` for diskless topics
- Long-polling fetch waits inside the diskless read path rather than in the fetch purgatory: a request wakes on the first append to any of its caught-up partitions, or on `maxWaitMs`. `minBytes` is honoured only through that wait -- a fetch never waits when any of its partitions already has data.
- `ListOffsets` by timestamp can answer with an offset at or before the exact match on a topic whose record timestamps lag their entries' write times by more than 256 entries (see **Limitations**)
- The high watermark a fetch or `ListOffsets(-1)` reports can lag another owner's append by up to the refresh interval or one fetch wait, whichever is longer (see **Limitations**); this broker's own appends never lag, and neither `OFFSET_OUT_OF_RANGE` nor `ListOffsets(-2/-4)` is ever answered from a stale window

#### Binary Protocol

No protocol changes required.

#### Configuration

**Broker Configuration** (`server.properties`):

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `ursa.storage.enable` | boolean | `false` | Master toggle for Ursa storage mode |
| `ursa.storage.topic.default.enable` | boolean | `false` | Enable diskless storage for topics by default |
| `ursa.catalog.oxia.service.url` | string | `oxia://localhost:6648/default` | Oxia metadata store URL for the Ursa log catalog (format: `oxia://host:port/[namespace]`) |
| `ursa.oxia.service.url` | string | `oxia://localhost:6648/default` | Oxia metadata store URL for Ursa storage metadata and producer state (format: `oxia://host:port/[namespace]`) |
| `ursa.storage.backend.type` | string | `LOCAL` | Storage backend: `LOCAL`, `S3`, `GCS`, `AZURE_BLOB` (`AZUREBLOB` is also accepted for compatibility). Azure compaction requires an HNS-enabled account because compacted files use ABFS. |
| `ursa.storage.path` | string | `/tmp/ursa-data` | Local storage path for `LOCAL`, or the remote object prefix for `S3`/`GCS`/Azure Blob |
| `ursa.storage.compaction.prefix` | string | `/tmp/compaction-data` | Compaction output prefix for remote object storage backends |
| `ursa.storage.write.buffer.flush.interval.ms` | long | `250` | Write buffer flush interval |
| `ursa.storage.lifecycle.sweep.interval.ms` | long | `600000` | Active-controller periodic interval (ms) to sweep orphaned diskless topic storage. Minimum 1000ms. |
| `ursa.storage.write.buffer.size` | int | `4194304` (4MB) | Size of each WAL write buffer segment |
| `ursa.storage.write.buffer.flush.size` | long | `268435456` (256MB) | Write buffer flush size threshold |
| `ursa.storage.producer.state.snapshot.interval.ms` | long | `30000` | Periodic interval (ms) for producer-state snapshot. Set `<= 0` to disable time-based snapshot. |
| `ursa.storage.producer.state.snapshot.record.threshold` | int | `10000` | Number of appended records that triggers producer-state snapshot. Set `<= 0` to disable threshold-based snapshot. |
| `ursa.storage.s3.endpoint` | string | `""` | Remote object storage endpoint URL. Reused as an endpoint override for GCS/Azure-compatible deployments |
| `ursa.storage.s3.bucket` | string | `kafka-ursa-storage` | Remote object storage bucket or container name. Reused for GCS/Azure backends |
| `ursa.storage.compaction.bucket` | string | `kafka-ursa-storage` | Remote object storage bucket or container name for compaction output |
| `ursa.storage.s3.region` | string | `us-east-1` | Remote object storage region when the selected backend uses one |
| `ursa.storage.s3.access.key` | string | `""` | S3 access key |
| `ursa.storage.s3.secret.key` | string | `""` | S3 secret key |
| `ursa.storage.s3.session.token` | password | `""` | Optional session token for temporary S3 credentials |
| `ursa.storage.s3.path.style.access` | boolean | unset | Explicit S3 path-style override for compacted-object reads; custom endpoints default to path-style access |
| `socket.server.enable.request.pipelining` | boolean | `false` | Allow multiple in-flight requests per connection; preserves response order but reduces latency amplification for diskless produces |

**Diskless Performance Recommendation**:
- Set `socket.server.enable.request.pipelining=true` when diskless topics are enabled and `ursa.storage.write.buffer.flush.interval.ms` is non-trivial (e.g., 50–250ms). This prevents per-connection head-of-line blocking from amplifying produce latency across multiple in-flight requests.
- Keep it `false` by default for conservative memory behavior; enabling pipelining can increase buffering if a single connection issues multiple large-response requests (e.g., unusually large fetches).

**Topic Configuration**:

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `ursa.storage.enable` | boolean | `false` | Enable diskless mode for this topic |

#### CLI

Standard `kafka-topics.sh` is used to create diskless topics:

```bash
# Create a diskless topic
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create \
  --topic my-diskless-topic \
  --partitions 10 \
  --config ursa.storage.enable=true

# Verify topic configuration
bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe \
  --topic my-diskless-topic
```

**Constraints**:
- Replication factor must be 1 for diskless topics
- Internal topics cannot be diskless

## Security Considerations

### Authentication & Authorization

- Diskless storage inherits Kafka's existing ACL model
- Topic-level authorization applies regardless of storage backend
- S3 credentials are stored in broker configuration (recommend using IAM roles in cloud deployments)

### Data Security

- Data in transit to Ursa/S3 should use TLS
- Data at rest encryption depends on the storage backend configuration
- Oxia connections should be secured with appropriate authentication

### Multi-tenancy

- Diskless topics respect Kafka's multi-tenancy model
- Namespace isolation in Ursa provides additional separation
- Producer state in Oxia is keyed by topic ID, preventing cross-tenant access

## Backward & Forward Compatibility

### Revert

To revert from diskless storage:

1. **Create new traditional topic** with same schema
2. **Mirror data** using MirrorMaker 2 or similar tool
3. **Update consumers** to use new topic
4. **Delete diskless topic**

**Note**: Direct conversion from diskless to traditional storage is not supported. Data must be migrated.

### Upgrade

**Prerequisites**:
1. Deploy Ursa storage backend
2. Deploy Oxia cluster
3. Configure broker with Ursa settings

**Upgrade Steps**:
1. Rolling restart brokers with new configuration
2. Create new topics with `ursa.storage.enable=true`
3. Existing topics remain on traditional storage unless recreated

**Rollback**:
- Disable `ursa.storage.enable` and restart brokers
- Diskless topics will become unavailable until re-enabled

## How Will This Be Made Available?

### Fully-Managed Product: Hosted / BYOC Cloud

Diskless storage is designed for cloud-native deployments:
- **Hosted**: Automatically configured with managed Ursa/Oxia backends
- **BYOC**: Customer configures S3 bucket and Oxia endpoint
- **Console Integration**: Topic creation UI includes diskless toggle

### Self-Managed Product: Platform / Private Cloud

**Requirements**:
- Ursa storage deployment (or S3-compatible object storage)
- Oxia cluster for state management
- Network connectivity between brokers and storage backends

**Configuration**:
See Configuration section above for required properties.

## Alternatives

### Alternative 1: Extend KIP-405 Tiered Storage

**Approach**: Use tiered storage with aggressive local retention (e.g., 0 bytes).

**Rejected Because**:
- Tiered storage still requires local log segments for active data
- Recovery still requires downloading from remote storage
- Doesn't fully eliminate ISR replication overhead

### Alternative 2: Shared-Nothing with Remote Log Manager

**Approach**: Replace local log with pluggable remote log manager.

**Rejected Because**:
- More invasive changes to core Kafka code
- Ursa integration provides optimized streaming semantics
- Oxia provides better state management than generic KV stores

## General Notes

### Why Async Implementation is Required

Traditional Kafka storage operates synchronously:
1. Append to local log (fast local I/O)
2. Wait for ISR replication (network + remote I/O)
3. Respond to client

With remote primary storage, synchronous operations would block request handler threads on network I/O, drastically reducing throughput. The async model:
1. Initiates append to Ursa (non-blocking)
2. Registers completion callback
3. Request handler thread immediately available for other requests
4. Callback executes when Ursa acknowledges

### Key Async Patterns

**CompletableFuture Composition**:
```java
// Write path example
return validateBeforeWrite(tp, records)
    .thenCompose(validationError -> {
        if (validationError != null) {
            return CompletableFuture.completedFuture(errorResponse);
        }
        return state.getOrCreatePartitionLog(tp)
            .write(records, zone, writerName);
    })
    .exceptionally(e -> errorResponse);
```

**Parallel Partition Processing**:
```java
List<CompletableFuture<Entry>> futures = partitions.stream()
    .map(this::processPartition)
    .toList();

return CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]))
    .thenApply(v -> futures.stream()
        .map(CompletableFuture::join)
        .collect(toMap(Entry::getKey, Entry::getValue)));
```

### Purgatory Bypass

Traditional Kafka uses `DelayedOperationPurgatory` for:
- `DelayedProduce`: Wait for ISR acks
- `DelayedFetch`: Wait for data to satisfy min bytes

For diskless topics:
- **DelayedProduce**: Not needed; Ursa ack is sufficient
- **DelayedFetch**: Replaced by a long poll inside the diskless read path. Every requested partition registers an append waiter before the first read, so an append landing during that read still wakes the request; a request that already has records, an error, or nothing that waiting could change answers at once. Otherwise it waits for the first append on any of its caught-up partitions, or for the single timeout the request schedules for all of them, and re-reads exactly those partitions once.

### Limitations

1. **No Transactions**: Transactional produce is rejected
2. **No K/V Compaction**: K/V Log compaction is not supported
3. **RF=1 Only**: Multi-replica diskless topics not supported
4. **No Internal Topics**: System topics use traditional storage
5. **MAX_TIMESTAMP is approximate**: `ListOffsets(-3)` answers with the last entry's base offset and its write timestamp instead of scanning records for the true maximum create time. Entries are written in write-timestamp order, so this is exact whenever record timestamps follow write order; where they do not, the answer names the last entry rather than the record with the highest timestamp. The exact answer would cost one index lookup per record.
6. **The reported high watermark is bounded-stale across owners**: the offset window behind fetch, `ListOffsets(-1/-3)` and the high watermark is read from storage at most once every 100 ms per partition. Records appended by this broker are folded into it immediately, so a producer and consumer on the same broker see no delay; records appended by *another* owner of the same log become visible within the refresh interval or one fetch wait, whichever is longer -- a caught-up consumer long-polls for `fetch.max.wait.ms`, which is longer than the interval, so the interval is what governs. Two answers are exempt, both because they are the ones a client acts on by moving its offset: `OFFSET_OUT_OF_RANGE` and `ListOffsets(-2/-4)` are only ever answered from a window whose read was issued after the request arrived. The cached window's own first offset is *not* exempt -- only *this* broker's retention trim drops it, so a trim run by another owner reaches it within the refresh interval, like that owner's appends -- but nothing a client is told depends on that: the log start it reads comes from `ListOffsets(-2)`, and the start reported alongside a fetch is only ever too low, never past a record the fetch could still return.
7. **Timestamp lookups scan a bounded window**: `ListOffsets(t)` reads at most 256 entries past its binary search. Beyond that it answers with the first entry written at or after `t`, which is at or before the true offset and never past it. If none of the scanned entries was written at or after `t`, the lookup reports no offset at all, even though a later entry past the window may hold one. That exhaustion answer's timestamp is always the entry's write time, not a record's create time.

### Future Enhancements

1. **Transaction Support**: Integrate with Ursa's transaction capabilities
2. **Multi-Region**: Cross-region replication via Ursa

### Compaction Support

Diskless topics support **external compaction** via the Ursa compactor. In this mode, the compactor runs as a separate container and performs offline batch processing of WAL data, materializing it into Parquet files for efficient storage and analytical querying.

This external compaction is **not** Kafka key/value log compaction and does **not** change consumer semantics for diskless topics. Kafka-side K/V log compaction remains unsupported for diskless topics (see **Limitations** → **No K/V Compaction**).

Compaction-task ownership follows the storage boundary. For diskless topics, the standalone Ursa compactor discovers LogIds from committed Lakestream layouts and publishes its own tasks; Kafka brokers do not run a compaction-task publisher. The compactor reads the WAL through the Storage API and decodes the Kafka `MemoryRecords` carried by each entry. Classic-topic lakehouse ingestion is outside this integration and will use a separate StreamCatalog-based source-reader design.

The Ursa storage runtime reads the V2 `KAFKA_BATCHED_RAW_PARQUET` format behind the Lakestream API. Kafka broker code neither selects nor instantiates the compacted-object reader implementation. The runtime fails fast for legacy V1 lakehouse indexes.

**Architecture**:
```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Kafka Broker   │────▶│   Ursa WAL      │────▶│  Ursa Compactor │
│  (writes data)  │     │   (S3/Local)    │     │ (tasks + data)  │
└─────────────────┘     └─────────────────┘     └────────┬────────┘
                                                         │
                                                         ▼
                                               ┌─────────────────┐
                                               │  Parquet Files  │
                                               │  (S3/Local)     │
                                               └─────────────────┘
```

**Docker Demo**:

The repository includes a single Docker Compose stack whose `lakehouse-demo`
profile exercises compaction end to end:

```bash
# Run the demo. It starts the stack itself -- Polaris plus Karapace as schema
# registry and Kafka REST proxy live in the `lakehouse` profile, which the core
# cluster does not enable -- then creates a topic, produces Avro records whose
# schema is registered as `<topic>-value`, reads them back before and after
# Parquet compaction, and queries the typed Iceberg table from DuckDB (row
# count, sum of a column, sample rows and an aggregate). It requires a clean
# project, and on success it removes its own containers and volumes.
cd docker/examples/docker-compose-files/cluster/ursa
make lakehouse-demo

# Cleanup after a failed or retained run (KEEP_RUNNING=true)
make destroy
```

**Requirements**:
- Standalone Ursa compactor Maven package (built from the `ursa-storage` repository)
- MinIO or real S3 for storage backend

### Lakestream identity compatibility

Every Lakestream stream is scoped to the Kafka topic incarnation, not only the reusable topic name.
For topic ID `<uuid>`, Kafka derives the topic-level `StreamIdentifier`
`default/<topic>-topic-id-<uuid>`. The stream's committed `StreamLayout` contains one opaque `LogId`
per Kafka partition, in partition-index order. Kafka opens data-plane logs only through
`StreamCatalog.openLog(StreamIdentifier, LogId)`; it does not derive partition-log names.

Kafka also stores the stable logical topic name in system-owned stream properties
`lakestream.source.logical.name` and `lakestream.kafka.topic.name`. Ursa uses that value for Schema
Registry subjects and as the default SDT table name; users do not configure either property or a
`tableNameTemplate`. Kafka discards caller-supplied source-metadata and internal materialization
values and writes the authoritative source metadata itself. Materialization must not infer the
logical topic from the UUID-qualified StreamIdentifier
name. An explicit SDT `tableIdentifier` or naming template still takes precedence. This SDT name
never replaces the incarnation-qualified identity used for SBT Compacted Objects, partition layout,
or Oxia indexes; when SBT and SDT are both enabled they are independent outputs from one source read.

The Ursa compactor uses these incarnation-qualified StreamIdentifiers and their committed layout
LogIds; there is no Kafka-side compaction-task publisher or cache. The catalog's Oxia keys, stream-ID mappings,
tombstones, task records, and object paths are private Ursa implementation details and are not part
of Kafka's integration contract. If deletion cleanup fails, old storage state can remain orphaned,
but a recreated topic receives a distinct StreamIdentifier and cannot attach to the orphan.

In-place rolling upgrades from earlier experimental name-only diskless-storage metadata layouts are
not supported. Deploy this version with an empty Oxia namespace, or perform an offline metadata and
object-store migration before starting the brokers. The runtime intentionally does not retain a
legacy dual-read or name-only fallback path.
