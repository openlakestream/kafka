# AGENTS.md

This file provides guidance to AI coding agents (Codex CLI, OpenCode, Claude Code) when working with code in this repository.

## Agent Compatibility

- Canonical instruction file: AGENTS.md
- Backward compatibility alias: CLAUDE.md (symlink to AGENTS.md)
- When updating instructions, edit AGENTS.md only
- Applies to Codex, OpenCode, and Claude-style repository agents
- If nested AGENTS.md files exist in subdirectories, prefer the nearest one in scope

## Project Overview

Fork of Apache Kafka (4.3.0-SNAPSHOT) with **Diskless Storage** via **StreamNative Ursa**. When enabled, Kafka brokers become stateless — message durability is offloaded to Ursa's distributed storage layer and producer state to **Oxia** (a distributed KV store). The primary goal is to maintain compatibility with upstream Kafka while adding the Ursa storage bypass layer.

Design document: `docs/SNIP-diskless-storage-with-ursa-integration.md`

## Build Commands

```bash
# Full build (jar only, skip tests)
./gradlew jar

# Compile check (fast feedback loop)
./gradlew :clients:compileJava :core:compileScala :storage:compileJava :server:compileJava

# Run all tests
./gradlew test

# Run a specific test class
./gradlew :core:test --tests "kafka.network.SocketServerTest"
./gradlew :server:test --tests "org.apache.kafka.server.ursa.integration.UrsaStorageE2ETest"

# Run a specific test method
./gradlew :core:test --tests "kafka.api.ProducerFailureHandlingTest.testCannotSendToInternalTopic"

# Run only Ursa integration tests (isolated from main suite)
./gradlew test -Pkafka.ci.isolated.tests=only

# Run main tests excluding Ursa integration tests
./gradlew test -Pkafka.ci.isolated.tests=exclude

# Checkstyle / SpotBugs / Spotless (run automatically with test)
./gradlew :storage:checkstyleMain
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

The diskless layer is a **bypass** in `ReplicaManager` that routes storage operations to Ursa instead of local logs, on a per-topic basis (`ursa.storage.enable=true` topic config).

```
KafkaApis → ReplicaManager → DisklessStorageReplicaManagerSupport
                                  ├── diskless topics → UrsaManagedLedgerWriter / UrsaManagedLedgerReader → Ursa
                                  └── classic topics  → local Log (unchanged)
```

### Key Modules

| Module | Language | Ursa Role |
|--------|----------|-----------|
| `storage/` | Java | Diskless storage core: Writer, Reader, ProducerStateStore, config |
| `core/` | Scala | Broker: `ReplicaManager`, `KafkaApis`, `SocketServer`, `BrokerServer` |
| `server/` | Java | Server configs (`SocketServerConfigs`), Ursa E2E integration tests |
| `clients/` | Java | Network layer: `KafkaChannel`, `NetworkReceive`, `Selector` |

### Key Source Files

**Diskless storage** (`storage/src/main/java/org/apache/kafka/storage/diskless/`):
- `DisklessStorageReplicaManagerSupport.java` — Entry point; partitions requests between diskless and classic paths
- `handlers/UrsaManagedLedgerWriter.java` — Write path (async append via ManagedLedger)
- `handlers/UrsaManagedLedgerReader.java` — Read path (Fetch + ListOffsets)
- `handlers/UrsaStorageState.java` — Stream IDs, offset tracking, shared state
- `handlers/UrsaStorageConfig.java` — Configuration holder
- `idempotent/UrsaProducerStateStore.java` — Producer state persistence to Oxia

**Broker integration** (`core/src/main/scala/kafka/server/`):
- `ReplicaManager.scala` — Calls `DisklessStorageReplicaManagerSupport` for diskless topics
- `KafkaApis.scala` — Request handling, async produce/fetch for diskless
- `BrokerServer.scala` — Initializes diskless storage support
- `ReplicaFetcherThread.scala` — Skips fetching for diskless partitions

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
The `storage` module depends on `io.streamnative:ursa-storage-core`, `ursa-storage-ml`, `managed-ledger`, and `io.github.oxia-db:oxia-client-api`. These are excluded from most test classpaths via `excludeUrsaStorageDeps` (defined in root `build.gradle`) because they slow down ClassGraph scanning.

### Isolated CI Tests
Ursa integration tests (`org/apache/kafka/server/ursa/integration/**`, `org/apache/kafka/storage/diskless/**`) are isolated from the main test suite. Use `-Pkafka.ci.isolated.tests=only` to run them, or `=exclude` to skip them.

### Verify License After Dependency Changes
When adding, removing, or upgrading dependencies in `build.gradle`, the binary distribution's `LICENSE-binary` file must be kept in sync. `LICENSE-binary` lists every third-party jar bundled under `./libs` in the release tarball, grouped by license type (Apache 2.0, MIT, BSD, etc.). Run the verification script to check for mismatches:
```bash
# Full check: builds releaseTarGz, extracts tarball, compares libs/ against LICENSE-binary
python committer-tools/verify_license.py

# Skip the build if you already have a tarball from a previous build
python committer-tools/verify_license.py --skip-build
```
The script reports jars missing from `LICENSE-binary` (need to add) and stale entries in `LICENSE-binary` no longer in `./libs` (need to remove). If adding a dependency with a non-Apache license, also add the license text to the `licenses/` directory and reference it in the appropriate section of `LICENSE-binary`.

## Docker Demo

```bash
cd docker/examples/docker-compose-files/cluster/ursa
./build-image.sh          # Build kafka-diskless:latest
make demo                 # Full demo: 3-broker cluster + Oxia + MinIO + perf test
make up                   # Start cluster only
make create-topic         # Create diskless topic
```

Architecture: 3 Kafka brokers + Oxia (metadata) + MinIO (S3 storage). Ports: kafka-1:29092, kafka-2:39092, kafka-3:49092, oxia:6648, minio:19000/19001.

## Upstream Compatibility

This is a fork — changes should minimize divergence from upstream Apache Kafka. When modifying core Kafka code:
- Prefer additive changes (new methods, new config options) over modifying existing behavior
- Keep diskless logic behind `isDisklessTopic()` checks
- Ensure all existing Kafka tests continue to pass with diskless disabled
