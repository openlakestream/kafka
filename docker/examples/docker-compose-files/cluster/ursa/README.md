Diskless Storage Docker Setup
==============================

This directory contains Docker Compose configuration for running Kafka with **Diskless/Ursa Storage** - a tiered storage solution that stores data in remote S3-compatible storage instead of local disks.

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
       └─────────────┘          └───────────────┘
```

- **Oxia**: Distributed metadata store for producer state persistence
- **MinIO**: S3-compatible object storage for log segments
- **Kafka Brokers**: 3-node cluster with diskless storage enabled

## Prerequisites

- Docker >= 20.10.4
- Docker Compose v2+
- Python >= 3.7 (for building image)
- Java >= 17 (for building Kafka)

## Quick Start

### 1. Build the Docker Image

```bash
cd docker/examples/docker-compose-files/cluster/ursa
./build-image.sh
```

This will:
1. Build Kafka with diskless storage support
2. Create Docker image `kafka-diskless:latest`

Build a linux/amd64 (x86_64) image (useful on Apple Silicon):

```bash
./build-image.sh --amd64
```

### Compaction (LocalStack + Schema Registry)

For Parquet compaction (via an external compactor container) and schema-registry integration, use:

- Cluster compose: `docker-compose-localstack-compaction.yml`
- Demo overlay (create topic + produce/consume Avro + wait for parquet): `docker-compose-localstack-compaction.demo.yml`

Build the compactor image from the `ursa-storage` repo (UE-based image recommended):

```bash
# From the current repo root:
docker build -t ursa-compact:ue-s3-e2e \
  -f docker/examples/docker-compose-files/cluster/ursa/ursa-compact-ue.Dockerfile \
  </path/to/ursa-storage>
```

Run the full end-to-end demo:

```bash
docker compose -f docker-compose-localstack-compaction.yml up -d
docker compose -f docker-compose-localstack-compaction.yml \
  -f docker-compose-localstack-compaction.demo.yml \
  run --rm avro-consumer
docker compose -f docker-compose-localstack-compaction.yml down -v --remove-orphans
```

Or run the helper script:

```bash
bash ./run-localstack-compaction-demo.sh
```

Notes:
- The demo overlay uses `confluentinc/cp-schema-registry` only as a CLI image (for `kafka-avro-console-producer/consumer`); override with `AVRO_CLI_IMAGE=...` if needed.

### 2. Run the Demo (Recommended)

The easiest way to get started is to run the full demo, which starts the cluster, creates a topic, and runs producer/consumer performance tests:

```bash
make demo
```

This will:
1. Start Oxia, MinIO, and 3 Kafka brokers
2. Create a diskless topic `test-diskless` with 64 partitions
3. Run 2 producer instances (10k msg/s each)
4. Run 1 consumer instance
5. Clean up when done (including on Ctrl+C)

### 3. Manual Setup

If you prefer to start the cluster manually:

```bash
# Start cluster only
make up

# Create a diskless topic
make create-topic

# Or create a custom topic
make create-topic TOPIC=my-topic PARTITIONS=6
```

### 4. Verify Services

```bash
# Check all services are running
make ps

# View Kafka logs
make logs

# List topics
make list-topics
```

### 5. Produce and Consume

**Interactive console producer/consumer:**

```bash
# Start console producer (type messages and press Enter)
make console-producer

# Start console consumer (in another terminal)
make console-consumer
```

**Performance testing:**

```bash
# Run producer perf test (100k messages)
make produce

# Run consumer perf test
make consume

# Test with a custom topic
make produce TOPIC=my-topic
make consume TOPIC=my-topic
```

### 6. Advanced Performance Testing

For more control over performance tests:

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

## Available Make Commands

| Command | Description |
|---------|-------------|
| `make demo` | Start cluster with full demo (topic + producer/consumer) |
| `make up` | Start cluster only |
| `make down` | Stop all services |
| `make destroy` | Stop all services and remove volumes |
| `make logs` | Follow kafka-1 logs |
| `make ps` | Show running services |
| `make create-topic` | Create a diskless topic |
| `make list-topics` | List all topics |
| `make produce` | Run producer perf test |
| `make consume` | Run consumer perf test |
| `make console-producer` | Start interactive console producer |
| `make console-consumer` | Start interactive console consumer |

**Note**: Diskless topics enforce RF=1 (replication factor of 1) because data is stored in remote storage, not replicated across brokers.

## Ports

| Service | Port | Description |
|---------|------|-------------|
| kafka-1 | 29092 | Kafka broker 1 |
| kafka-2 | 39092 | Kafka broker 2 |
| kafka-3 | 49092 | Kafka broker 3 |
| oxia | 6648 | Oxia metadata service |
| minio | 19000 | MinIO S3 API |
| minio | 19001 | MinIO Console |

## MinIO Console

Access the MinIO console at http://localhost:19001

- **Username**: minioadmin
- **Password**: minioadmin

You can browse the `kafka-ursa` bucket to see stored log segments.

## Configuration

### Oxia Settings

Oxia is configured with 8 shards (`--shards=8`) to prevent WAL offset conflicts under high concurrent write load. This is a workaround for [oxia#796](https://github.com/streamnative/oxia/issues/796).

### Environment Variables

Diskless storage is configured via environment variables in `docker-compose.yml`:

| Variable | Description |
|----------|-------------|
| `KAFKA_URSA_STORAGE_ENABLE` | Enable diskless storage (`true`) |
| `KAFKA_URSA_STORAGE_BACKEND_TYPE` | Storage backend (`S3`) |
| `KAFKA_URSA_STORAGE_OXIA_SERVICE_URL` | Oxia connection URL |
| `KAFKA_URSA_STORAGE_S3_ENDPOINT` | S3 endpoint URL |
| `KAFKA_URSA_STORAGE_S3_BUCKET` | S3 bucket name |
| `KAFKA_URSA_STORAGE_S3_ACCESS_KEY` | S3 access key |
| `KAFKA_URSA_STORAGE_S3_SECRET_KEY` | S3 secret key |
| `KAFKA_URSA_STORAGE_S3_REGION` | S3 region |

**Note**: Environment variables use `_` for `.` in property names (e.g., `ursa.storage.enable` → `KAFKA_URSA_STORAGE_ENABLE`).

### Custom Image

To use a custom image:

```bash
IMAGE=myrepo/kafka-diskless:v1 docker compose up -d
```

## Testing Failover

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
# Stop all services
docker compose down

# Remove all data volumes
docker compose down -v
```

## Troubleshooting

### Kafka fails to start

Check if Oxia and MinIO are healthy:

```bash
docker compose ps
docker compose logs oxia
docker compose logs minio
```

### Connection refused errors

Ensure the services have fully started:

```bash
# Wait for health checks
docker compose up -d --wait
```

### Permission errors

Ensure Docker version >= 20.10.4:

```bash
docker --version
```
