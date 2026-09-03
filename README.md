# Diskless Kafka with a Built-in Lakehouse

[![CI](https://github.com/lakestream-io/kafka/actions/workflows/ci.yml/badge.svg?branch=4.3-ursa&event=push)](https://github.com/lakestream-io/kafka/actions/workflows/ci.yml?query=event%3Apush+branch%3A4.3-ursa)

A fork of [Apache Kafka](https://github.com/apache/kafka) with two additions:

- **Diskless Kafka.** Topics whose durability comes from object storage instead of broker disks. Brokers hold
  no partition data, so failover moves no bytes, scaling is a CPU and network decision, and a zone-aware
  client never pays for cross-AZ replication.
- **Data in the lakehouse.** The records those topics write are compacted into Parquet and registered as an
  Iceberg table, queryable from DuckDB, Trino or Spark. No copy job, no Connect sink, no second retention
  policy — Kafka consumers and SQL engines read the same bytes out of your own bucket.

Everything Apache Kafka does, this fork still does. Diskless storage is an additional, per-topic option, so a
single cluster can serve latency-sensitive workloads on classic topics and cost-sensitive workloads on
diskless topics at the same time.

## One cluster, two storage modes

|                          | Classic topic (Apache Kafka)                     | Diskless topic (`ursa.storage.enable=true`)                            |
| ------------------------ | ------------------------------------------------ | ---------------------------------------------------------------------- |
| Optimized for            | Latency                                          | Cost                                                                   |
| Durability               | Local log segments + ISR replication (RF ≥ 2)    | Object storage (S3 / GCS / Azure Blob) through Ursa, Kafka RF = 1      |
| Cross-AZ traffic         | Producer → leader, plus leader → followers       | None in steady state with [zone-aware routing](#zone-aware-routing): clients reach an in-zone broker and that broker writes straight to object storage |
| Broker state             | Stateful: partition data lives on the broker     | Stateless for record data                                              |
| Failover                 | New leader must catch up on replicated data      | Any live broker can serve the partition immediately, no data movement  |
| Elasticity               | Adding/removing brokers moves partition data     | Scale on CPU and network alone                                         |
| Lakehouse                | –                                                | The same compacted Parquet can be registered as an Iceberg table       |

Both modes share one controller quorum, one metadata log, one set of ACLs, one consumer-group coordinator,
and the same client protocol. Consumer offsets stay in `__consumer_offsets`, which is always a classic topic.

## Drop-in replacement

- **Nothing changes until you turn it on.** `ursa.storage.enable` defaults to `false` on both the broker and
  the topic, so a stock configuration behaves exactly like the Apache Kafka release it is built from
  (currently 4.3).
- **No protocol changes.** Existing producers, consumers, Kafka Streams, Kafka Connect, admin clients, and the
  `bin/` tools work unmodified against diskless topics. Clients cannot tell the difference: the broker still
  reports a leader, still serves `Fetch` and `ListOffsets`, still enforces offsets and idempotent producer
  semantics.
- **Per-topic, not per-cluster.** Diskless is a topic config. Migrating means creating new topics with
  `ursa.storage.enable=true`; existing topics are untouched.
- **Upstream-compatible codebase.** Diskless logic sits behind `isDisklessTopic()` checks; upstream code paths
  are unchanged when diskless is disabled.

## How it works

Diskless storage is a bypass layer in `ReplicaManager` that routes storage operations for enabled topics to
Ursa instead of local logs.

```
          Producers / Consumers
                    │
                    ▼
            ┌───────────────┐
            │   KafkaApis   │
            └───────┬───────┘
                    ▼
          ┌───────────────────┐
          │  ReplicaManager   │
          └─────────┬─────────┘
                    │  DisklessStorageReplicaManagerSupport
          ┌─────────┴──────────┐
      diskless              classic
          │                    │
          ▼                    ▼
   ┌──────────────┐    ┌──────────────────┐
   │ Ursa writer  │    │    Local log     │
   │  and reader  │    │  (.log + ISR)    │
   └──────┬───────┘    └──────────────────┘
          │
   ┌──────┴────────────┬──────────────────┐
   ▼                   ▼                  ▼
Object storage       Oxia            Compaction
(WAL + Parquet)   (catalog +      (WAL → Parquet
                 producer state)   → Iceberg)
```

- **Writes** go into an Ursa write-ahead log in object storage. The write buffer flushes on an interval
  (`ursa.storage.write.buffer.flush.interval.ms`, default 250 ms) or once
  `ursa.storage.write.buffer.flush.size` bytes accumulate — the interval is the produce-latency floor for an
  idle topic, throughput is the bound for a busy one.
- **Reads** are served from the same storage, transparently spanning the WAL and the compacted objects that
  compaction has already written.
- **Compaction** consolidates the WAL into those compacted objects. It runs as a separate service, and it is
  required: Kafka retention on a diskless topic issues a *soft* trim, and WAL objects are only physically
  deleted up to the oldest un-compacted position across every stream. See
  [Compaction is required](#compaction-is-required).
- **Producer state** for idempotent producers is snapshotted into [Oxia](https://github.com/oxia-db/oxia)
  instead of local `.snapshot` files, so a broker restart does not lose idempotence guarantees.
- **Topic lifecycle** — creation, partition growth, deletion, and orphan cleanup — is reconciled by the active
  controller against the stream catalog.
- All storage calls are asynchronous; request-handler threads are never blocked on object storage.

## From topic to lakehouse

Records written to a diskless topic are compacted out of the WAL into Parquet and registered in an Iceberg
REST catalog, so the same data is queryable by engines like DuckDB, Trino, or Spark without a copy job, a
Connect sink, or a second retention policy.

```
Kafka producer ──▶ broker ──▶ Ursa WAL (object storage)
                                       │
                                       ▼
                              compaction (Parquet)
                                  │          │
                                  ▼          ▼
                          compacted     Iceberg table
                           objects      (REST catalog)
                              │               │
                              ▼               ▼
                      Kafka consumers    SQL engines
```

Compaction runs as a separate service (the Ursa compactor) rather than inside the broker, so it never
competes with the request path. Kafka consumers and query engines read the same bytes: one copy of the data,
one retention policy, in your own bucket.

### Compaction is required

Compaction is not a lakehouse add-on — it is what makes diskless storage reclaimable:

1. Kafka retention on a diskless topic issues a **soft trim**, which moves the log's first offset. Soft-trimmed
   entries are logically gone but still occupy object storage.
2. WAL objects are physically deleted by a cleaner that runs inside the broker's storage runtime.
3. That cleaner's delete watermark is the **oldest un-compacted position across every stream**, and only
   compaction advances it.

So a deployment with no compaction service has a WAL that only grows: `retention.ms` and `retention.bytes`
hide records from consumers without freeing a byte. Because the watermark is a global minimum, one stream that
never gets compacted also pins reclamation for every other stream.

Registering an Iceberg table is a second, optional sink on the same compaction pass. You can run compaction
without any catalog at all.

## Quick start

The fastest way to see it running is the Docker Compose stack — three brokers, Oxia, MinIO, and optionally the
compactor plus an Iceberg catalog:

```bash
cd docker/examples/docker-compose-files/cluster/ursa

# Both images: the broker, and the compactor built from a local ursa-storage checkout
URSA_STORAGE_DIR=/path/to/ursa-storage ./build-images.sh

make up                 # Oxia + MinIO + 3 brokers + compactor
make create-topic       # create a diskless topic
make demo               # producer/consumer perf demo
make destroy            # tear everything down
```

Adding the Iceberg catalog is one more command — it starts Polaris and has the compactor write the external
table alongside the compacted objects it already writes:

```bash
make lakehouse-demo     # Kafka -> Parquet -> Iceberg -> DuckDB, asserted end to end
```

See [the stack's README](docker/examples/docker-compose-files/cluster/ursa/README.md) for the full set of
profiles, ports, and knobs.

## Enabling diskless storage on your own cluster

You need an object storage bucket, an [Oxia](https://github.com/oxia-db/oxia) service, the Ursa runtime jars
in `$KAFKA_HOME/ursa-storage/` (the release tarball built by `./gradlew releaseTarGz` puts them there), and a
running [compaction service](#compaction-is-required). An Iceberg catalog is optional on top of that.

Broker/controller configuration:

```properties
# Turn the diskless storage system on for this cluster
ursa.storage.enable=true

# Metadata and producer state
ursa.catalog.oxia.service.url=oxia://oxia:6648/default
ursa.oxia.service.url=oxia://oxia:6648/default

# Record storage
ursa.storage.backend.type=S3
ursa.storage.path=ursa/wal
ursa.storage.s3.bucket=my-kafka-bucket
ursa.storage.s3.region=us-east-1
ursa.storage.s3.endpoint=https://s3.us-east-1.amazonaws.com
ursa.storage.s3.access.key=...
ursa.storage.s3.secret.key=...

# Compacted objects
ursa.storage.compaction.bucket=my-kafka-bucket
ursa.storage.compaction.prefix=ursa/compacted
```

Then create a diskless topic:

```bash
bin/kafka-topics.sh --create \
  --topic events \
  --partitions 12 \
  --replication-factor 1 \
  --config ursa.storage.enable=true \
  --bootstrap-server localhost:9092
```

Every other topic in the cluster stays classic. To flip the default for new, non-internal topics, set
`ursa.storage.topic.default.enable=true` on the brokers.

`ursa.storage.enable` is immutable after topic creation — changing a topic between modes means deleting and
recreating it.

### Key configuration

| Config | Default | Purpose |
| ------ | ------- | ------- |
| `ursa.storage.enable` | `false` | Broker: enable the diskless storage system. Topic: make this topic diskless. |
| `ursa.storage.topic.default.enable` | `false` | Make new non-internal topics diskless unless overridden. |
| `ursa.storage.backend.type` | `LOCAL` | `LOCAL`, `S3`, `GCS`, or `AZURE_BLOB`. |
| `ursa.storage.path` | `/tmp/ursa-data` | Object prefix for the WAL (a local path for the `LOCAL` backend). |
| `ursa.storage.s3.bucket` / `.region` / `.endpoint` / `.access.key` / `.secret.key` | – | Object storage connection. |
| `ursa.storage.compaction.bucket` / `.prefix` | `kafka-ursa-storage` / `/tmp/compaction-data` | Where compacted objects live. |
| `ursa.catalog.oxia.service.url` | `oxia://localhost:6648/default` | Oxia URL for the stream catalog. |
| `ursa.oxia.service.url` | `oxia://localhost:6648/default` | Oxia URL for storage metadata and producer-state snapshots. |
| `ursa.storage.write.buffer.flush.interval.ms` | `250` | Produce-latency floor; lower for latency, raise to batch more per PUT. |
| `ursa.storage.write.buffer.flush.size` | `256 MiB` | Size that triggers an early flush. |
| `ursa.storage.class.path` | `$KAFKA_HOME/ursa-storage/*` | Classpath for the isolated Ursa runtime. |
| `ursa.storage.lifecycle.sweep.interval.ms` | `600000` | Controller sweep for storage left behind by deleted topics. |
| `socket.server.enable.request.pipelining` | `false` | Process multiple produce requests per connection concurrently; reduces latency amplification against a flush-cycle storage backend. |

Run `bin/kafka-configs.sh` or read
[`ServerLogConfigs`](server-common/src/main/java/org/apache/kafka/server/config/ServerLogConfigs.java) for the
complete list.

### Zone-aware routing

To keep diskless traffic inside an availability zone, set `broker.rack` on each broker and put a `zone_id`
token in the client's `client.id`:

```properties
client.id=orders-service,zone_id=us-east-1a
```

The broker then picks an owner in the client's zone and reports it as the partition leader, falling back to
the full set of live brokers when no in-zone broker is available. Clients need no code changes beyond the
`client.id`. See [LIP-002](docs/LIP-ursa-zone-aware-owner-selection.md).

## Limitations of diskless topics

- **No transactional producers.** Transactional produce requests are rejected; idempotent producers are
  supported.
- **No key-based log compaction.** `cleanup.policy=compact` semantics are not implemented for diskless topics;
  only WAL-to-Parquet compaction in the storage layer.
- **Replication factor must be 1.** Durability comes from object storage, so Kafka-level ISR replication is
  bypassed. `CreateTopics` rejects any other value.
- **Internal topics are always classic.** `__consumer_offsets` and `__transaction_state` stay on local logs.
- **Mode is fixed at creation.** `ursa.storage.enable` cannot be altered afterwards.
- **Retention needs compaction to free storage.** Without a compaction service running, `retention.ms` and
  `retention.bytes` remove records logically but reclaim nothing.

## Building from source

You need JDK 17 and the `io.lakestream:ursa-storage` artifacts in your local Maven repository (build them from
[lakestream-io/ursa-storage](https://github.com/lakestream-io/ursa-storage) with `mvn -DskipTests install`).

```bash
./gradlew jar                                  # build
./gradlew releaseTarGz                         # binary tarball, including ./ursa-storage/ runtime jars
./gradlew test -Pkafka.ci.isolated.tests=exclude   # main test suite
./gradlew test -Pkafka.ci.isolated.tests=only      # diskless integration tests
```

The standard Apache Kafka build, test, IDE, and code-quality instructions all still apply — see the
[upstream README](https://github.com/apache/kafka/blob/trunk/README.md). Fork-specific conventions live in
[AGENTS.md](AGENTS.md).

## Documentation

- [LIP-001: Diskless Storage with Ursa Integration](docs/LIP-diskless-storage-with-ursa-integration.md)
- [LIP-002: Ursa Zone-Aware Owner Selection](docs/LIP-ursa-zone-aware-owner-selection.md)
- [Docker Compose stack](docker/examples/docker-compose-files/cluster/ursa/README.md)
- [AGENTS.md](AGENTS.md) — architecture, module layout, and build conventions
- [Apache Kafka documentation](https://kafka.apache.org/documentation/) for everything inherited from upstream

## Relationship to upstream

This repository tracks Apache Kafka and keeps divergence minimal: changes are additive where possible, and the
upstream test suite is expected to pass with diskless storage disabled. Bugs in Kafka itself belong upstream;
issues with diskless storage or the lakehouse path belong here.

## License

Apache License 2.0, the same as Apache Kafka. See [LICENSE](LICENSE).
