Diskless Storage Docker Setup
==============================

This directory contains the Docker Compose stack for running Kafka with **Diskless/Ursa Storage**. For enabled topics, Ursa is the primary storage layer rather than a tier that sits behind Kafka's local log.

Everything lives in a single `docker-compose.yml`. The default `docker compose up` starts the core cluster — Oxia, MinIO, three brokers and the Ursa compactor — which needs both locally built images. The Iceberg catalog sits behind the `lakehouse` profile, and every demo workload behind a profile of its own, so none of them start on their own.

The compactor is part of the core stack rather than the `lakehouse` profile because compaction is what makes storage reclaimable, not just what feeds the lakehouse. Kafka retention on a diskless topic issues a *soft* trim; WAL objects are physically deleted by a cleaner whose watermark is the oldest un-compacted position across every stream, and only compaction advances that watermark. With no compactor running, `retention.ms` and `retention.bytes` hide records logically and free nothing.

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Kafka-1   │     │   Kafka-2   │     │   Kafka-3   │
│  (broker +  │     │  (broker +  │     │  (broker +  │
│ controller) │     │ controller) │     │ controller) │
└──────┬──────┘     └──────┬──────┘     └──────┬──────┘
       │                   │                   │
       └───────────────────┼───────────────────┘
                           │
              ┌────────────┴────────────┐
              │                         │
       ┌──────▼──────┐          ┌───────▼───────┐
       │    Oxia     │          │     MinIO     │
       │  (metadata) │          │  (S3 storage) │
       └──────┬──────┘          └───────┬───────┘
              │                         │
              └───────────┬─────────────┘
                          │
                 ┌────────▼────────┐      ┌──────────┐
                 │  Ursa compactor │─────▶│ Polaris  │
                 │ (tasks + data)  │      │ (Iceberg)│
                 └─────────────────┘      └────┬─────┘
                                               │
                                          ┌────▼────┐
                                          │ DuckDB  │
                                          └─────────┘
```

Core cluster (started by default):

- **Oxia**: Lakestream catalog, WAL indexes, compaction tasks, and producer state
- **MinIO**: S3-compatible object storage for the Ursa WAL, the managed compacted objects, and the Iceberg warehouse
- **Kafka brokers**: 3-node KRaft cluster with diskless storage enabled
- **Ursa compactor**: standalone container that publishes compaction tasks, decodes the Kafka `MemoryRecords` in the WAL, and writes the managed compacted objects Kafka fetch reads. This is also what lets the WAL be reclaimed, so it runs with or without the lakehouse.

`lakehouse` profile (opt in):

- **Polaris**: Iceberg REST catalog
- **Ursa compactor, with materialization on**: `URSA_MATERIALIZATION_ENABLED=true` adds a second sink to the same WAL read, registering an external Iceberg table in Polaris, named after the Kafka topic. Compose recreates the compactor when that variable changes.
- **DuckDB** (`tools` profile): on-demand SQL engine used for queries and for the end-to-end assertion

```text
Kafka producer -> Kafka broker -> Ursa WAL (MinIO)
                                     |
                                     v
                     Ursa compactor (publishes tasks)
                              |           |
                              v           v
                    managed objects   Iceberg table      <- lakehouse profile,
                                      |     |               materialization on
                                      v     v
                                  Polaris  MinIO
                                      |
                                      v
                                   DuckDB
```

The left branch is the core stack: it is what Kafka fetch reads after compaction and what lets the WAL be reclaimed. The right branch is the opt-in lakehouse.

Classic local-log ingestion is not part of this diskless demo and will use a separate StreamCatalog-based integration.

## Prerequisites

- Docker >= 20.10.4
- Docker Compose >= 2.20 (the stack uses `depends_on: ... required: false`)
- Python >= 3.7 (for building the Kafka image)
- Java >= 17 (for building Kafka)
- A local `ursa-storage` checkout (for the compactor image, which the core stack needs)

## Build the images

Two images are built locally, and the core stack needs both. Everything else is pulled from a public registry at a pinned version.

```bash
cd docker/examples/docker-compose-files/cluster/ursa

# Both images -> lakestream/kafka:latest and lakestream/compactor:latest
URSA_STORAGE_DIR=/path/to/ursa-storage ./build-images.sh
# or: make build-images URSA_STORAGE_DIR=/path/to/ursa-storage

# Kafka broker image only -> lakestream/kafka:latest
# Enough to rebuild the brokers against an existing compactor image.
./build-image.sh
```

Useful options:

```bash
./build-image.sh --amd64                 # linux/amd64 image (useful on Apple Silicon)
./build-image.sh myrepo/kafka:v1         # custom image name
GRADLE_ARGS=--offline ./build-image.sh   # extra Gradle arguments
MAVEN_ARGS=-o ./build-images.sh          # extra Maven arguments for the compactor
SKIP_KAFKA_BUILD=true ./build-images.sh  # reuse the existing Kafka image
SKIP_URSA_BUILD=true ./build-images.sh   # reuse the existing compactor package
```

## Quick start

```bash
# Start the core cluster: Oxia, MinIO, 3 brokers, and the compactor
make up

# Create a diskless topic
make create-topic
make create-topic TOPIC=my-topic PARTITIONS=6

# Produce and consume with the perf tools
make produce
make consume

# Stop, or stop and remove the volumes
make down
make destroy
```

To add the Iceberg catalog on top, enable the `lakehouse` profile and turn materialization on. Compose sees the changed environment and recreates the compactor with the catalog configured:

```bash
URSA_MATERIALIZATION_ENABLED=true docker compose --profile lakehouse up -d
```

Without that variable the `lakehouse` profile starts Polaris but the compactor keeps writing only the managed compacted objects, so no Iceberg table appears. The `make` targets below set it for you.

## Demos

Each demo is a Compose profile of one-shot containers. They never run during a plain `docker compose up`.

| Profile | Services | What it does |
|---------|----------|--------------|
| `lakehouse` | `polaris`, `polaris-setup` | Adds the Iceberg REST catalog (long-running, not one-shot). Pair with `URSA_MATERIALIZATION_ENABLED=true` so the already-running compactor also writes the external table. |
| `demo` | `kafka-ready`, `create-topic`, `producer-1`, `producer-2`, `consumer` | Creates `test-diskless` (`PARTITIONS`, default 12) and runs two producers plus a consumer perf test |
| `share-demo` | `kafka-ready`, `create-share-topic`, `configure-share-group`, `share-producer`, `share-consumer`, `describe-share-group` | Share-group consumption on a single-partition diskless topic |
| `lakehouse-demo` | `kafka-ready`, `lakehouse-create-topic`, `raw-producer`, `kafka-consumer-check`, `wait-for-parquet`, `duckdb-query-check` | Kafka -> Ursa WAL -> compaction -> Iceberg -> DuckDB assertions (combine with `lakehouse`) |
| `tools` | `duckdb` | On-demand SQL shell against the local Polaris catalog |

```bash
make demo             # perf demo, torn down on exit (including on Ctrl+C)
make share-demo       # share-group demo, torn down on exit
make lakehouse-demo   # end-to-end lakehouse assertion
make duckdb           # DuckDB shell

# Or drive the profiles directly
docker compose --profile demo up
URSA_MATERIALIZATION_ENABLED=true docker compose --profile lakehouse --profile lakehouse-demo up
URSA_MATERIALIZATION_ENABLED=true docker compose --profile lakehouse --profile tools run --rm duckdb
```

`make lakehouse-demo` and `make duckdb` enable the `lakehouse` profile and materialization for you. `make compaction-logs` needs neither, because the compactor is part of the core stack.

### Lakehouse end-to-end

`make lakehouse-demo` runs `run-lakehouse-demo.sh`, which needs both local images. It creates a diskless topic, writes 100 raw Kafka records, reads all 100 back through Kafka, waits for the compactor to write Parquet into MinIO, polls DuckDB until the external Iceberg table holds the same row count, and asserts that the range was materialized exactly once without compactor errors. On success it removes the demo containers and volumes.

```bash
KEEP_RUNNING=true ./run-lakehouse-demo.sh   # keep the stack for inspection
NUM_RECORDS=500 ./run-lakehouse-demo.sh     # change the record count
```

It also lowers the brokers' WAL write buffer to a 100 ms interval and 4096
bytes (`URSA_WRITE_BUFFER_FLUSH_INTERVAL_MS`, `URSA_WRITE_BUFFER_FLUSH_SIZE`)
so its 100 records reach the compactor immediately. Those are verifier
settings, not perf-demo settings.

The verifier requires a fresh Compose project so a prior topic or Iceberg snapshot cannot make an assertion pass accidentally. After a retained or failed run, use `make destroy` before retrying.

Inside DuckDB (`make duckdb`), the table is named after the Kafka topic:

```sql
SHOW ALL TABLES;
SELECT count(*) FROM lakehouse.default."ursa-lakehouse-e2e";
```

The stream behind it is named `<topic>-topic-id-<uuid>`, because Kafka lets a deleted topic be
recreated under the same name and the two incarnations must not share a log. The table drops that
suffix — `tableNameTemplate` in the compactor's config names it from the topic instead — so queries,
views and dashboards keep working across a topic's lifetimes. The flip side is that a topic recreated
under the same name appends to the existing table; drop the table first when that is not wanted.

Polaris intentionally uses its in-memory development metastore and static MinIO credentials. It suits a reproducible local run, not a durable or production catalog. DuckDB is on demand rather than a resident server; use Trino instead when a shared JDBC/HTTP query endpoint is a requirement. All published ports bind to `127.0.0.1` because the stack uses fixed development credentials.

## Verify services

```bash
make ps                # running services
make logs              # follow kafka-1
make compaction-logs   # follow the compactor
make list-topics
```

## Console producer/consumer

```bash
make console-producer
make console-consumer
```

## Advanced performance testing

`ursa.storage.write.buffer.flush.interval.ms` is the produce-latency floor: a
produce waits up to one flush interval, plus the S3 PUT, before it is
acknowledged. The buffer also flushes early once
`ursa.storage.write.buffer.flush.size` bytes accumulate, so a busy topic is
bounded by throughput rather than by the interval. Lower the interval for
latency, raise it to batch more per PUT.

```bash
./bin/kafka-producer-perf-test.sh \
  --topic test-diskless \
  --num-records 20000 \
  --record-size 1024 \
  --throughput -1 \
  --producer-props \
    bootstrap.servers=localhost:29092 \
    acks=1 \
    linger.ms=25 \
    batch.size=102400 \
    buffer.memory=128000000 \
    max.request.size=64000000 \
    max.in.flight.requests.per.connection=100000 \
    enable.idempotence=false
```

## Available make commands

| Command | Description |
|---------|-------------|
| `make help` | List the available targets |
| `make build-images` | Build the Kafka and Ursa compactor images |
| `make up` | Start the core cluster, compactor included |
| `make down` | Stop all services, every profile included |
| `make destroy` | Stop all services and remove volumes, every profile included |
| `make logs` | Follow kafka-1 logs |
| `make compaction-logs` | Follow compactor logs |
| `make ps` | Show running services |
| `make create-topic` | Create a diskless topic (`TOPIC`, `PARTITIONS`) |
| `make list-topics` | List all topics |
| `make produce` | Run producer perf test |
| `make consume` | Run consumer perf test |
| `make console-producer` | Start interactive console producer |
| `make console-consumer` | Start interactive console consumer |
| `make demo` | Run the producer/consumer perf demo and tear it down on exit |
| `make share-demo` | Run the share-group demo and tear it down on exit |
| `make lakehouse-demo` | Run the Kafka-to-DuckDB end-to-end assertion |
| `make duckdb` | Open DuckDB against the local Polaris catalog |

**Note**: Diskless topics enforce RF=1 (replication factor of 1) because data is stored in remote storage, not replicated across brokers.

## Ports

Every port binds to `127.0.0.1`.

| Service | Port | Description |
|---------|------|-------------|
| kafka-1 | 29092 | Kafka broker 1 |
| kafka-2 | 39092 | Kafka broker 2 |
| kafka-3 | 49092 | Kafka broker 3 |
| oxia | 6648 | Oxia metadata service |
| minio | 19000 | MinIO S3 API |
| minio | 19001 | MinIO console |
| polaris | 18181 | Iceberg REST and management APIs |
| polaris | 18182 | Health and metrics APIs |

## MinIO console

Access the MinIO console at http://localhost:19001

- **Username**: minioadmin
- **Password**: minioadmin

The `kafka-ursa` bucket holds the Ursa WAL (`ursa/wal`) and the managed compacted objects (`ursa/compacted`). The `lakehouse` bucket holds the Iceberg warehouse.

## Configuration

### Oxia settings

Oxia is configured with 8 shards (`--shards=8`) to prevent WAL offset conflicts under high concurrent write load. This is a workaround for [oxia#796](https://github.com/oxia-db/oxia/issues/796).

### Broker environment variables

Diskless storage is configured through the `x-kafka-environment` anchor in `docker-compose.yml`:

| Variable | Description |
|----------|-------------|
| `KAFKA_URSA_STORAGE_ENABLE` | Enable diskless storage (`true`) |
| `KAFKA_URSA_STORAGE_BACKEND_TYPE` | Storage backend (`S3`) |
| `KAFKA_URSA_CATALOG_OXIA_SERVICE_URL` | Oxia connection URL for the Ursa log catalog |
| `KAFKA_URSA_OXIA_SERVICE_URL` | Oxia connection URL for Ursa storage metadata and producer-state snapshots |
| `KAFKA_URSA_STORAGE_PATH` | Object prefix for the WAL (`ursa/wal`); a local path only for the `LOCAL` backend |
| `KAFKA_URSA_STORAGE_S3_ENDPOINT` | S3 endpoint URL |
| `KAFKA_URSA_STORAGE_S3_BUCKET` | S3 bucket name |
| `KAFKA_URSA_STORAGE_S3_ACCESS_KEY` | S3 access key |
| `KAFKA_URSA_STORAGE_S3_SECRET_KEY` | S3 secret key |
| `KAFKA_URSA_STORAGE_S3_REGION` | S3 region |
| `KAFKA_URSA_STORAGE_COMPACTION_BUCKET` | Bucket for managed compacted objects |
| `KAFKA_URSA_STORAGE_COMPACTION_PREFIX` | Object prefix for managed compacted objects (`ursa/compacted`) |
| `KAFKA_URSA_STORAGE_WRITE_BUFFER_FLUSH_INTERVAL_MS` | WAL write-buffer flush interval, `URSA_WRITE_BUFFER_FLUSH_INTERVAL_MS` (default 250) |
| `KAFKA_URSA_STORAGE_WRITE_BUFFER_FLUSH_SIZE` | WAL write-buffer flush size, `URSA_WRITE_BUFFER_FLUSH_SIZE` (default 256 MiB) |
| `KAFKA_URSA_STORAGE_WRITE_BUFFER_SIZE` | WAL write-buffer segment size (16 MiB here, 4 MiB by default) |

**Note**: Environment variables use `_` for `.` in property names (e.g., `ursa.storage.enable` → `KAFKA_URSA_STORAGE_ENABLE`).

KRaft metadata lives under `/var/lib/kafka/data/kraft-combined-logs` on each broker's data volume, so recreating a container keeps the cluster's topic metadata.

### Compose variables

| Variable | Default | Description |
|----------|---------|-------------|
| `IMAGE` | `lakestream/kafka:latest` | Kafka broker and CLI image |
| `COMPACTOR_IMAGE` | `lakestream/compactor:latest` | Ursa compactor image |
| `OXIA_IMAGE`, `MINIO_IMAGE`, `MINIO_MC_IMAGE`, `POLARIS_IMAGE`, `POLARIS_SETUP_IMAGE`, `DUCKDB_IMAGE` | pinned versions | Third-party images |
| `PARTITIONS` | `12` | Partition count for the `demo` profile's `create-topic` |
| `DEMO_TOPIC` | `ursa-lakehouse-e2e` | Topic used by the `lakehouse-demo` profile |
| `NUM_RECORDS` | `100` | Record count used by the `lakehouse-demo` profile |
| `COMPACTION_BUCKET` | `kafka-ursa` | Bucket `wait-for-parquet` watches |
| `COMPACTION_PREFIX` | `ursa` | Prefix `wait-for-parquet` watches (spans `ursa/wal` and `ursa/compacted`) |
| `URSA_WRITE_BUFFER_FLUSH_INTERVAL_MS` | `250` | Broker WAL write-buffer flush interval |
| `URSA_WRITE_BUFFER_FLUSH_SIZE` | `268435456` | Broker WAL write-buffer flush size in bytes (256 MiB) |
| `URSA_MATERIALIZATION_ENABLED` | `false` | Whether the compactor also writes an external Iceberg table. Needs the `lakehouse` profile up. |

To use a custom Kafka image:

```bash
IMAGE=myrepo/kafka:v1 docker compose up -d
```

## Testing failover

Diskless storage enables automatic failover when a broker goes down:

```bash
# Stop broker 1
docker compose stop kafka-1

# Verify other brokers still serve the topic
./bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:39092 \
  --topic test-diskless \
  --from-beginning

# Restart broker 1
docker compose start kafka-1
```

## Cleanup

```bash
# Stop all services, every profile included
make down

# Stop everything and remove the volumes as well
make destroy
```

A bare `docker compose down` only removes containers for services in the
enabled profiles, so it leaves the one-shot demo and `lakehouse` containers
behind. Both targets name every profile.

## Troubleshooting

### Kafka fails to start

Check that Oxia and MinIO are healthy:

```bash
docker compose ps
docker compose logs oxia
docker compose logs minio
```

### There is no compactor container

It is not part of the default stack. Enable the `lakehouse` profile and make sure the locally built image exists:

```bash
URSA_STORAGE_DIR=/path/to/ursa-storage ./build-images.sh
docker compose --profile lakehouse up -d
make compaction-logs
```

### Connection refused errors

Ensure the services have fully started:

```bash
docker compose up -d --wait
```

### Permission errors

Ensure Docker version >= 20.10.4:

```bash
docker --version
```
