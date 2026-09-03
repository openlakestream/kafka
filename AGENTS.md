# AGENTS.md

This file provides guidance to AI coding agents (Codex CLI, OpenCode, Claude Code) when working with code in this repository.

## Agent Compatibility

- Canonical instruction file: AGENTS.md
- Backward compatibility alias: CLAUDE.md (symlink to AGENTS.md)
- When updating instructions, edit AGENTS.md only
- Applies to Codex, OpenCode, and Claude-style repository agents
- If nested AGENTS.md files exist in subdirectories, prefer the nearest one in scope

## Project Overview

Fork of Apache Kafka with **Diskless Storage** via **Ursa**. When enabled, Kafka brokers become stateless — message durability is offloaded to Ursa's distributed storage layer and producer state to **Oxia** (a distributed KV store). The primary goal is to maintain compatibility with upstream Kafka while adding the Ursa storage bypass layer.

Design document: `docs/LIP-diskless-storage-with-ursa-integration.md`

## Build Commands

```bash
# Full build (jar only, skip tests)
./gradlew jar

# Compile check (fast feedback loop)
./gradlew :clients:compileJava :core:compileScala :storage:compileJava :storage:storage-diskless-api:compileJava :storage:storage-diskless-ursa:compileJava :server:compileJava

# Run all tests
./gradlew test

# Run a specific test class
./gradlew :core:test --tests "kafka.network.SocketServerTest"
./gradlew :server:test --tests "org.apache.kafka.server.ursa.integration.UrsaStorageE2ETest"
./gradlew :storage:storage-diskless-api:test --tests "org.apache.kafka.storage.diskless.DisklessStorageEngineLoaderTest"
./gradlew :storage:storage-diskless-ursa:test --tests "org.apache.kafka.storage.diskless.handlers.UrsaStorageStateTest"

# Run a specific test method
./gradlew :core:test --tests "kafka.api.ProducerFailureHandlingTest.testCannotSendToInternalTopic"

# Run only Ursa integration tests (isolated from main suite)
./gradlew test -Pkafka.ci.isolated.tests=only

# Run main tests excluding Ursa integration tests
./gradlew test -Pkafka.ci.isolated.tests=exclude

# Checkstyle / SpotBugs / Spotless (run automatically with test)
./gradlew :storage:checkstyleMain
./gradlew :storage:storage-diskless-api:checkstyleMain
./gradlew :storage:storage-diskless-ursa:checkstyleMain
./gradlew :core:checkstyleMain
./gradlew spotlessCheck        # import order check
./gradlew spotlessApply        # auto-fix import order
```

## Code Quality

- **Checkstyle**: Each module has its own import-control XML (`checkstyle/import-control-*.xml`). The `storage` module uses `import-control-storage.xml` which defines allowed package dependencies per subpackage.
- **SpotBugs**: Exclusions in `gradle/spotbugs-exclude.xml`.
- **Spotless**: Enforces import order: `kafka`, `org.apache.kafka`, `com`, `net`, `org`, `java`, `javax`, then static imports.
- **Compiler**: `-Werror` is enabled for main sources. Java release target is 17 (except clients/streams which target 11).
- **Imports**: Always use `import` statements instead of fully-qualified class names in code (e.g., write `import java.util.HashMap;` and use `HashMap`, not `java.util.HashMap` inline).

## Architecture: Diskless Storage Integration

### How Ursa Integrates with Kafka

The diskless layer is a **bypass** in `ReplicaManager` that routes storage operations to Ursa instead of local logs, on a per-topic basis (`ursa.storage.enable=true` topic config). Brokers open partitions with create-if-absent, creating the Lakestream stream on first partition open. The active controller reconciles properties, partition growth, deletion, and orphan cleanup through one `DisklessTopicLifecycle` SPI call that fans out to both the catalog and producer-state cleanup.

```
KafkaApis → ReplicaManager → DisklessStorageReplicaManagerSupport
                                  ├── diskless topics → PartitionWriter / PartitionReader → Ursa
                                  └── classic topics  → local Log (unchanged)

Controller → DisklessTopicLifecycleReconciler → DisklessTopicLifecycle → StreamCatalog + ProducerState
```

### Key Modules

| Module | Language | Ursa Role |
|--------|----------|-----------|
| `storage/` | Java | Upstream Kafka storage code plus shared storage internals |
| `storage/diskless-api/` | Java | Generic diskless SPI/common classes used by broker code |
| `storage/diskless-ursa/` | Java | Ursa implementation: Lakestream writer/reader, Oxia store, producer state |
| `core/` | Scala | Broker: `ReplicaManager`, `KafkaApis`, `SocketServer`, `BrokerServer` |
| `server/` | Java | Server configs (`SocketServerConfigs`), Ursa E2E integration tests |
| `clients/` | Java | Network layer plus generic plugin classloader utilities |

### Key Source Files

**Diskless API/common** (`storage/diskless-api/src/main/java/org/apache/kafka/storage/diskless/`):
- `DisklessStorageReplicaManagerSupport.java` — Entry point; partitions requests between diskless and classic paths
- `DisklessStorageEngine.java` — Generic engine SPI implemented by Ursa
- `DisklessStorageEngineLoader.java` — Loads the implementation through the isolated Ursa classpath
- `DisklessTopicLifecycle.java` — Controller-side topic lifecycle SPI (ensureTopic, deleteTopic, listManagedTopics, sweepOrphans)
- `DisklessTopicLifecycleLoader.java` — Isolated loader for `DisklessTopicLifecycle`
- `DisklessFutures.java` — Shared futures helper for unwrapping exceptions
- `DisklessTopics.java` — Shared helper for topic configuration checks
- `DisklessClassLoaderRegistry.java` — Shares plugin classloader instances by classpath and parent until all leases close
- `handlers/UrsaStorageConfig.java` — Configuration holder shared by broker-side code and the Ursa implementation

**Ursa implementation** (`storage/diskless-ursa/src/main/java/org/apache/kafka/storage/diskless/`):
- `handlers/UrsaStorageEngineImpl.java` — Diskless storage engine implementation backed by Ursa
- `handlers/PartitionReader.java` — Read path for fetch and list offsets, with a cached cursor and an offset window cached for 100 ms (`OFFSET_RANGE_REFRESH_MS`); local appends widen it at once, this broker's own retention trim or a close drops it (another owner's trim, like its appends, lands within the interval), and `OFFSET_OUT_OF_RANGE` and `ListOffsets(EARLIEST/EARLIEST_LOCAL)` are only ever answered from a window read after the request arrived — `LATEST`/`MAX_TIMESTAMP` take the cached window and may lag by up to the interval
- `handlers/PartitionWriter.java` — Write path: validation, append, producer state, append notifications
- `handlers/PartitionRetention.java` — Coalesced retention worker
- `handlers/LakestreamStorageHolder.java` — Catalog and Oxia client ownership; opens partitions with create-if-absent
- `handlers/UrsaDisklessTopicLifecycle.java` — StreamCatalog-backed topic lifecycle implementation
- `idempotent/ProducerStateManager.java` — Producer state tracking backed by Oxia snapshots

**Plugin classloading**:
- `clients/src/main/java/org/apache/kafka/common/utils/KafkaPluginClassLoader.java` — Child-first plugin-private loading with parent-first Kafka/logging/Scala shared APIs
- `server-common/src/main/java/org/apache/kafka/server/util/KafkaPluginClassPaths.java` — Resolves configured classpaths or `$KAFKA_HOME/<runtime-dir>/*` defaults

**Broker integration** (`core/src/main/scala/kafka/server/`):
- `ReplicaManager.scala` — Calls `DisklessStorageReplicaManagerSupport` for diskless topics
- `KafkaApis.scala` — Request handling, async produce/fetch for diskless
- `BrokerServer.scala` — Initializes diskless storage support
- `ReplicaFetcherThread.scala` — Skips fetching for diskless partitions
- `metadata/DisklessTopicLifecycleReconciler.java` — Active-controller reconciler: ensures/deletes diskless topics from metadata deltas and sweeps orphans on a configurable interval

**Network / Pipelining** (`core/src/main/scala/kafka/network/`):
- `SocketServer.scala` — Request pipelining support (`socket.server.enable.request.pipelining`)

### Design Principles

1. **Topic-level granularity**: Diskless mode per-topic; mixed deployments supported
2. **Async-first**: All Ursa operations return `CompletableFuture`; never block request handler threads
3. **RF=1**: Ursa handles durability; Kafka-level ISR replication bypassed for diskless topics
4. **Transparent clients**: No protocol changes; existing producers/consumers work unmodified
5. **Avoid code duplication**: Extract common logic into helper methods
6. **Follow existing Kafka code conventions and patterns when possible**

### Limitations

- No transactional producer support for diskless topics
- No K/V log compaction (only external Ursa compaction to Parquet)
- Internal topics (`__consumer_offsets`, `__transaction_state`) always use local storage

## Critical Patterns

### Ursa Dependencies
Keep Ursa implementation dependencies out of Kafka's main classpath. Ursa, Oxia, cloud SDKs, and lakehouse dependencies belong in the isolated `storage:storage-diskless-ursa` runtime, not in `storage` or `core`.

The diskless storage data/read path compiles only against `lakestream-api`. Its production sources must not import `io.lakestream.ursa.*`, select a compacted-reader implementation, or depend on Ursa catalog/Oxia metadata layouts. `ursa-storage-kafka-runtime` is the single `runtimeOnly` bundle that discovers the catalog provider and internally assembles Ursa storage plus the Kafka lakehouse reader. The separate Oxia API dependency is outside this data/read boundary: it supports Kafka-owned producer-state snapshots and their deletion fence.

Release tarballs package those isolated runtime jars under `./ursa-storage/`. Kafka, Scala, SLF4J, Log4j, and other platform jars are provided by `./libs/` and should not be duplicated into `./ursa-storage/` unless the dependency is intentionally private to the Ursa runtime. The `KafkaPluginClassLoader` loads plugin-private classes child-first while keeping Kafka/logging/Scala API packages parent-first.

`ursa.storage.class.path` is the shared config used by broker and controller diskless-storage loaders and their isolated integration tests. In production, the default is `$KAFKA_HOME/ursa-storage/*`.

### Isolated CI Tests
Ursa integration tests (`org/apache/kafka/server/ursa/integration/**`, `org/apache/kafka/storage/diskless/**`) are isolated from the main test suite. Use `-Pkafka.ci.isolated.tests=only` to run them, or `=exclude` to skip them.

### Verify License After Dependency Changes
When adding, removing, or upgrading dependencies in `build.gradle`, the binary distribution's `LICENSE-binary` file must be kept in sync. `LICENSE-binary` lists every third-party jar bundled under `./libs` and `./ursa-storage` in the release tarball, grouped by license type (Apache 2.0, MIT, BSD, etc.). Run the verification script to check for mismatches:
```bash
# Full check: builds releaseTarGz, extracts tarball, compares bundled jar directories against LICENSE-binary
python committer-tools/verify_license.py

# Skip the build if you already have a tarball from a previous build
python committer-tools/verify_license.py --skip-build
```
The script reports jars missing from `LICENSE-binary` (need to add) and stale entries in `LICENSE-binary` no longer bundled under `./libs` or `./ursa-storage` (need to remove). If adding a dependency with a non-Apache license, also add the license text to the `licenses/` directory and reference it in the appropriate section of `LICENSE-binary`.

## Docker Demo

A single `docker-compose.yml` holds everything. `docker compose up` starts the core cluster; the compaction/Iceberg services sit behind the `lakehouse` profile and the workloads behind `demo`, `share-demo`, `lakehouse-demo` and `tools`.

```bash
cd docker/examples/docker-compose-files/cluster/ursa
./build-image.sh          # Build lakestream/kafka:latest
URSA_STORAGE_DIR=/path/to/ursa-storage ./build-images.sh  # + lakestream/compactor:latest
make up                   # Core cluster: Oxia + MinIO + 3 brokers
make create-topic         # Create diskless topic
make demo                 # Profile `demo`: perf producers + consumer, torn down on exit
make lakehouse-demo       # Profiles `lakehouse` + `lakehouse-demo`: Kafka -> Iceberg -> DuckDB
make destroy              # Tear down every profile and remove volumes
```

Architecture: 3 Kafka brokers + Oxia (metadata) + MinIO (S3 storage), plus Polaris (Iceberg catalog) and the Ursa compactor under the `lakehouse` profile. Only that profile needs the locally built compactor image. Ports (all bound to 127.0.0.1): kafka-1:29092, kafka-2:39092, kafka-3:49092, oxia:6648, minio:19000/19001, polaris:18181/18182.

## Upstream Compatibility

This is a fork — changes should minimize divergence from upstream Apache Kafka. When modifying core Kafka code:
- Prefer additive changes (new methods, new config options) over modifying existing behavior
- Keep diskless logic behind `isDisklessTopic()` checks
- Ensure all existing Kafka tests continue to pass with diskless disabled
