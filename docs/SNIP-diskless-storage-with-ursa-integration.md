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

# SNIP-001: Diskless Storage with Ursa Integration

- *Author(s)*: Kai Wang
- *Proposal time*: 2026-01
- *Implemented*: YES
- *Released*: NO
- *Repository*: https://github.com/ursaio/ursa-for-kafka
- *Discussion Link*: N/A

## TL;DR

This SNIP introduces **Diskless Storage** mode for Apache Kafka, replacing traditional local log persistence with remote stream-based storage via **StreamNative Ursa**. When enabled, Kafka brokers become stateless with respect to message data, offloading durability to Ursa's distributed storage layer and producer state management to **Oxia** (a distributed key-value store). This architectural shift enables rapid broker failover, eliminates ISR-based replication overhead for diskless topics, and provides a foundation for truly elastic Kafka deployments.

## Background Knowledge

### Traditional Kafka Storage Model

In standard Apache Kafka, each broker maintains local log segments on disk:
- **Log Segments**: Messages are appended to `.log` files organized by partition
- **Index Files**: `.index` and `.timeindex` files enable efficient offset lookups
- **Producer State**: `.snapshot` files track idempotent producer sequences for exactly-once semantics
- **ISR Replication**: Followers continuously fetch from leaders to maintain in-sync replicas

This model tightly couples brokers to their local storage, making failover expensive (data must be re-replicated) and scaling inflexible.

### StreamNative Ursa Storage

Ursa is StreamNative's distributed storage engine designed for streaming workloads:
- **Stream-based API**: Data is organized as streams with append-only semantics
- **Built-in Replication**: Ursa handles durability internally, eliminating the need for Kafka-level ISR
- **Backend Flexibility**: Supports LOCAL, S3, GCS, and Azure Blob storage backends
- **High Throughput**: Optimized for streaming append and sequential read patterns

### Oxia

Oxia is a distributed key-value store used for metadata and state management:
- **Producer State Persistence**: Replaces local `.snapshot` files for idempotent producer tracking
- **Stream Metadata**: Manages partition-to-stream mappings and offsets
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

In this SNIP, diskless topics are handled by the ManagedLedger-based implementations `UrsaManagedLedgerWriter` / `UrsaManagedLedgerReader`.

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
│   │  │    │UrsaML Writer    │ │UrsaML Reader    │ │ Classic Log│   │    │   │
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
          │  (Producer State / Stream Metadata) │
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
| `UrsaManagedLedgerWriter` | Write path for diskless topics; appends records via ManagedLedger |
| `UrsaManagedLedgerReader` | Read path for diskless topics; handles Fetch and ListOffsets via ManagedLedger |
| `UrsaStorageState` | Manages stream IDs, offset tracking, and shared state |
| `UrsaProducerStateStore` | Persists producer state to Oxia for idempotent semantics |
| `NonIdempotentPartitionAppendPipeline` | Per-partition append pipelining for non-idempotent writes (bounded concurrency, preserves order) |
| `UrsaStorageConfig` | Configuration holder for Ursa settings |
| `MetadataCacheDisklessStorageView` | Determines if a topic is diskless based on topic config |
| `ListOffsetsPartitionRequest` | Request DTO for diskless ListOffsets operations |
| `ListOffsetsPartitionResponse` | Response DTO for diskless ListOffsets operations |

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
       │         │    UrsaManagedLedgerWriter.write()
       │         │         │
       │         │         ├──▶ ProducerStateStore.validate()     ◄── Oxia
       │         │         │         (sequence validation)
       │         │         │
       │         │         ├──▶ UrsaStorageState.getOrCreateManagedLedger()
       │         │         │
       │         │         ├──▶ ManagedLedger.asyncAddEntry(data, numberOfMessages)
       │         │         │
       │         │         └──▶ ProducerStateStore.updateAfterWrite()
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
- `UrsaManagedLedgerWriter.write()` returns `CompletableFuture<Map<TopicIdPartition, PartitionResponse>>`
- ManagedLedger append operations are non-blocking (`asyncAddEntry`)
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

2. **Per-partition Non-idempotent Append Pipelining (NonIdempotentPartitionAppendPipeline)**:
   - For non-idempotent producers, allow multiple `ManagedLedger.asyncAddEntry()` calls in-flight per partition (bounded concurrency).
   - Rely on ManagedLedger's guarantee: append responses complete **in invocation order**, which preserves Kafka partition ordering.
   - For idempotent producers, keep per-partition serialization around sequence validation/state updates.

#### Threading Model (What Runs Where)

This matters because “async storage” alone does not guarantee low end-to-end latency if the broker stops reading the connection.

- **Network I/O**: `SocketServer` processor threads read from the TCP connection, parse requests, and place them into the request queue.
- **Request handling**: request handler threads execute `KafkaApis`/`ReplicaManager` and start diskless write work.
- **Storage + state I/O**: Ursa/Oxia operations complete on their own async executors/IO threads.
- **Callback completion**: diskless produce completion is posted back onto the request handler context before generating the response.
- **Response send**: the network layer sends responses back to the client; with pipelining enabled, it still sends strictly in-order per connection.

Without broker request pipelining, the network layer’s mute/unmute behavior makes the connection itself the bottleneck (head-of-line blocking), regardless of how asynchronous the storage layer is.

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
       │         │    UrsaManagedLedgerReader.fetch()
       │         │         │
       │         │         ├──▶ UrsaStorageState.getOrCreateManagedLedger()
       │         │         │
       │         │         ├──▶ ManagedCursor.asyncReadEntries()
       │         │         │
       │         │         └──▶ Convert entries to MemoryRecords
       │         │               (patch baseOffset = entryId)
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
- ManagedLedger entries use `entryId` as the base offset (and may contain multiple Kafka records)
- `UrsaManagedLedgerReader` patches the `baseOffset` of fetched record batches to match `entryId`
- This ensures consumers see consistent offsets regardless of storage backend

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
       │         │              UrsaManagedLedgerReader.listOffsets()
       │         │                   │
       │         │                   ├──▶ EARLIEST (-2): getFirstPosition()
       │         │                   │
       │         │                   ├──▶ LATEST (-1): getLastConfirmedEntry() → HWM
       │         │                   │
       │         │                   ├──▶ MAX_TIMESTAMP (-3): lastEntry publishTime → last offset
       │         │                   │
       │         │                   ├──▶ LATEST_TIERED (-5): return -1 (N/A)
       │         │                   │
       │         │                   └──▶ timestamp >= 0: publishTime binary search + record scan
       │         │
       │         └──▶ NO: fetchOffsetClassic() (local log)
       │
       ▼
   Combine Results & Respond
```

**Supported Timestamp Queries**:

| Timestamp Value | Meaning | Diskless Behavior |
|-----------------|---------|-------------------|
| `-2` (EARLIEST) | First available offset | Returns earliest ManagedLedger position (offset) |
| `-1` (LATEST) | High watermark | Returns high watermark derived from the last confirmed entry |
| `-3` (MAX_TIMESTAMP) | Offset with highest timestamp | Returns last message offset and publishTime (if available) |
| `-4` (EARLIEST_LOCAL) | First local offset | Same as EARLIEST for diskless (no local/remote distinction) |
| `-5` (LATEST_TIERED) | End of tiered storage | Returns -1 (not applicable for diskless) |
| `>= 0` | First offset with timestamp >= value | PublishTime binary search to narrow the start entry, then scan records |

**Key Implementation Details**:

1. **Validation in ReplicaManager**: Before forwarding to Ursa, `ReplicaManager.fetchOffset()` applies the same validation as the classic path:
   - Duplicate partition check → `INVALID_REQUEST`
   - Unsupported timestamp for protocol version → `UNSUPPORTED_VERSION`

2. **EARLIEST/LATEST Optimization**: EARLIEST/LATEST are served from ManagedLedger positions (first position and last confirmed entry).

3. **Timestamp Search**: Uses publishTime to narrow the candidate start entry (binary search), then scans Kafka records to compare actual record timestamps.

4. **Async Execution**: All ListOffsets operations return `CompletableFuture` to avoid blocking request handler threads.

#### ISR Bypass

For diskless topics:
1. `DisklessTopicMetadataTransformer` reports ISR = [leader] only
2. `ReplicaFetcherThread` skips fetching for diskless partitions
3. No delayed produce waiting for follower acks (Ursa ack is sufficient)

#### Producer State Management

```
┌─────────────────────────────────────────────────────────────┐
│                   UrsaProducerStateStore                    │
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
- Long-polling fetch behavior may return earlier for diskless topics (no purgatory waiting)

#### Binary Protocol

No protocol changes required.

#### Configuration

**Broker Configuration** (`server.properties`):

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `ursa.storage.enable` | boolean | `false` | Master toggle for Ursa storage mode |
| `ursa.storage.topic.default.enable` | boolean | `false` | Enable diskless storage for topics by default |
| `ursa.storage.oxia.service.url` | string | `localhost:6648` | Oxia service URL for metadata |
| `pulsar.oxia.service.url` | string | `oxia://localhost:6648/default` | Oxia metadata store URL for Pulsar-managed ledger metadata (format: `oxia://host:port/[namespace]`) |
| `ursa.oxia.service.url` | string | `oxia://localhost:6648/default` | Oxia metadata store URL for Ursa storage metadata (format: `oxia://host:port/[namespace]`) |
| `ursa.storage.backend.type` | string | `LOCAL` | Storage backend: `LOCAL`, `S3`, `GCS`, `AZURE_BLOB` (`AZUREBLOB` is also accepted for compatibility) |
| `ursa.storage.path` | string | `/tmp/ursa-data` | Local storage path for `LOCAL`, or the remote object prefix for `S3`/`GCS`/Azure Blob |
| `ursa.storage.compaction.prefix` | string | `/tmp/compaction-data` | Compaction output prefix for remote object storage backends |
| `ursa.storage.namespace` | string | `default` | Namespace for Ursa streams |
| `ursa.storage.wal.directory` | string | `/tmp/ursa-wal` | Write-ahead log directory |
| `ursa.storage.write.buffer.flush.interval.ms` | long | `250` | Write buffer flush interval |
| `ursa.storage.write.buffer.size` | int | `4194304` (4MB) | Size of each WAL write buffer segment |
| `ursa.storage.write.buffer.flush.size` | long | `268435456` (256MB) | Write buffer flush size threshold |
| `ursa.storage.producer.state.snapshot.interval.ms` | long | `30000` | Periodic interval (ms) for producer-state snapshot. Set `<= 0` to disable time-based snapshot. |
| `ursa.storage.producer.state.snapshot.record.threshold` | int | `10000` | Number of appended records that triggers producer-state snapshot. Set `<= 0` to disable threshold-based snapshot. |
| `ursa.storage.non.idempotent.max.in.flight.appends.per.partition` | int | `16` | Maximum in-flight non-idempotent appends per partition |
| `ursa.storage.non.idempotent.max.in.flight.bytes.per.partition` | long | `-1` | Maximum bytes of in-flight non-idempotent appends per partition (-1 disables) |
| `ursa.storage.s3.endpoint` | string | `""` | Remote object storage endpoint URL. Reused as an endpoint override for GCS/Azure-compatible deployments |
| `ursa.storage.s3.bucket` | string | `kafka-ursa-storage` | Remote object storage bucket or container name. Reused for GCS/Azure backends |
| `ursa.storage.compaction.bucket` | string | `kafka-ursa-storage` | Remote object storage bucket or container name for compaction output |
| `ursa.storage.s3.region` | string | `us-east-1` | Remote object storage region when the selected backend uses one |
| `ursa.storage.s3.access.key` | string | `""` | S3 access key |
| `ursa.storage.s3.secret.key` | string | `""` | S3 secret key |
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
        return state.getOrCreateManagedLedger(tp)
            .thenCompose(ledger -> addEntryAsync(ledger, data, numberOfMessages))
            .thenCompose(position -> updateStateAfterWrite(tp, records, position.getEntryId(), logAppendTime));
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
- **DelayedFetch**: Currently returns immediately; future enhancement may add Ursa-aware waiting

### Limitations

1. **No Transactions**: Transactional produce is rejected
2. **No K/V Compaction**: K/V Log compaction is not supported
3. **RF=1 Only**: Multi-replica diskless topics not supported
4. **No Internal Topics**: System topics use traditional storage

### Future Enhancements

1. **Transaction Support**: Integrate with Ursa's transaction capabilities
2. **Multi-Region**: Cross-region replication via Ursa
3. **Long-Poll Fetch**: Implement Ursa-aware fetch waiting

### Compaction Support

Diskless topics support **external compaction** via the Ursa compactor. In this mode, the compactor runs as a separate container and performs offline batch processing of WAL data, materializing it into Parquet files for efficient storage and analytical querying.

This external compaction is **not** Kafka key/value log compaction and does **not** change consumer semantics for diskless topics. Kafka-side K/V log compaction remains unsupported for diskless topics (see **Limitations** → **No K/V Compaction**).
**Architecture**:
```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Kafka Broker   │────▶│   Ursa WAL      │────▶│  Ursa Compactor │
│  (writes data)  │     │   (S3/Local)    │     │  (external)     │
└─────────────────┘     └─────────────────┘     └────────┬────────┘
                                                         │
                                                         ▼
                                               ┌─────────────────┐
                                               │  Parquet Files  │
                                               │  (S3/Local)     │
                                               └─────────────────┘
```

**Docker Demo**:

The repository includes Docker Compose files for running a compaction demo:

```bash
# Start the cluster with compaction support
cd docker/examples/docker-compose-files/cluster/ursa
docker compose -f docker-compose-localstack-compaction.yml up -d

# Run the demo (creates topic, produces Avro records, waits for Parquet compaction)
docker compose -f docker-compose-localstack-compaction.yml \
  -f docker-compose-localstack-compaction.demo.yml \
  run --rm avro-consumer

# Cleanup
docker compose -f docker-compose-localstack-compaction.yml down -v --remove-orphans
```

Or use the helper script:
```bash
bash ./run-localstack-compaction-demo.sh
```

**Requirements**:
- Ursa compactor image (built from `ursa-storage` repository)
- Schema Registry (for Avro serialization in demo)
- LocalStack or real S3 for storage backend
