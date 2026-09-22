# Ursa for Apache Kafka: instructions for coding agents

This file is for AI coding agents such as Claude Code, Codex, Copilot and Cursor. People should start
with [CONTRIBUTING.md](CONTRIBUTING.md).

If a subdirectory has its own `AGENTS.md`, it applies to that subdirectory. Read it before you change
code there. `CLAUDE.md` in this repository is a symlink to `AGENTS.md`. **Always edit `AGENTS.md`,
never `CLAUDE.md`.** A write that replaces the file would turn the symlink into a copy.

## Rules that come first

These rules implement the project's [AI policy](AI_POLICY.md) and override anything else in this
file.

- **Never add a `Signed-off-by:` line, even if asked.** Only the human can certify the Developer
  Certificate of Origin. When a commit is ready, tell them to review it and run
  `git commit --amend -s --no-edit`, or `git rebase --signoff origin/4.3-ursa` for several commits.
- **End each commit message with exactly one AI trailer: `Assisted-by: <tool>`.** This repository
  configures Claude Code to use `Assisted-by: Claude Code` (see `.claude/settings.json`). If your tool
  adds its own `Co-authored-by:` trailer, that counts; don't add both.
- **Don't act outside the local checkout without explicit approval for that specific action.** That
  covers pushing, opening or editing a pull request or issue, and commenting on GitHub. Being asked
  to start a task isn't permission to publish it. When a maintainer asks you through an `@claude`
  mention on GitHub, that request approves your reply, and the commits it asks for, in that run.
- **Leave force-pushes to the human.** If a push needs `--force`, for example after the commits
  were re-signed, hand it over.
- **Draft pull request descriptions from
  [the template](.github/pull_request_template.md)**, including the *AI assistance* section. Give the
  draft to the human to edit and post.
- **Report what you verified and what you didn't.** Never say a test or check passed unless you ran
  it and saw it pass.

## Project

Ursa for Apache Kafka (UFK) is a Kafka distribution built on the Lakestream API and specification.
Diskless topics keep their records on object storage through **Ursa**; every other topic stays Kafka.
It is built on Apache Kafka 4.3.1. On a diskless topic, brokers hold no partition data: message
durability is offloaded to Ursa's storage layer on object storage, and producer state to **Oxia**, a
distributed key-value store. Diskless storage is opt-in per topic. The primary goal is to stay
compatible with upstream Kafka while adding the Ursa storage bypass.

Design document:
[LIP-162](https://github.com/openlakestream/lips/blob/main/proposals/LIP-162-Diskless-Storage-with-Ursa-Integration.md).
Design proposals (LIPs) for every Lakestream component live in
[openlakestream/lips](https://github.com/openlakestream/lips).

## Architecture: diskless storage integration

### How Ursa integrates with Kafka

Upstream Kafka has no pluggable replica manager, so UFK carries the diskless path in its own `ReplicaManager`. The diskless layer is a **bypass** in `ReplicaManager` that routes storage operations to Ursa instead of local logs, on a per-topic basis (`ursa.storage.enable=true` topic config). Brokers open partitions with create-if-absent, creating the Lakestream stream on first partition open. The active controller reconciles properties, partition growth, deletion, and orphan cleanup through one `DisklessTopicLifecycle` SPI call that fans out to both the catalog and producer-state cleanup.

```text
KafkaApis → ReplicaManager → DisklessStorageReplicaManagerSupport
                                  ├── diskless topics → PartitionWriter / PartitionReader → Ursa
                                  └── classic topics  → local Log (unchanged)

Controller → DisklessTopicLifecycleReconciler → DisklessTopicLifecycle → StreamCatalog + ProducerState
```

Materializers, which turn a stream into a table in another system, run in the Ursa compactor, not in
the brokers. Their SPI and reference implementations live in
[openlakestream/ursa](https://github.com/openlakestream/ursa). What UFK writes for them is in
[docs/developer/materializers.md](docs/developer/materializers.md).

### Key modules

| Module | Language | Ursa Role |
|--------|----------|-----------|
| `storage/` | Java | Upstream Kafka storage code plus shared storage internals |
| `storage/diskless-api/` | Java | Generic diskless SPI/common classes used by broker code |
| `storage/diskless-ursa/` | Java | Ursa implementation: Lakestream writer/reader, Oxia store, producer state |
| `core/` | Scala, Java | Broker: `ReplicaManager`, `KafkaApis`, `SocketServer`, `BrokerServer`; controller-side `DisklessTopicLifecycleReconciler` |
| `server/` | Java | Server configs (`SocketServerConfigs`), Ursa E2E integration tests |
| `clients/` | Java | Network layer plus generic plugin classloader utilities |

### Key source files

**Diskless API/common** (`storage/diskless-api/src/main/java/org/apache/kafka/storage/diskless/`):
- `DisklessStorageReplicaManagerSupport.java`: Entry point; partitions requests between diskless and classic paths
- `DisklessStorageEngine.java`: Generic engine SPI implemented by Ursa
- `DisklessStorageEngineLoader.java`: Loads the implementation through the isolated Ursa classpath
- `DisklessTopicLifecycle.java`: Controller-side topic lifecycle SPI (ensureTopic, deleteTopic, listManagedTopics, sweepOrphans)
- `DisklessTopicLifecycleLoader.java`: Isolated loader for `DisklessTopicLifecycle`
- `DisklessFutures.java`: Shared futures helper for unwrapping exceptions
- `DisklessTopics.java`: Shared helper for topic configuration checks
- `DisklessClassLoaderRegistry.java`: Shares plugin classloader instances by classpath and parent until all leases close
- `handlers/UrsaStorageConfig.java`: Configuration holder shared by broker-side code and the Ursa implementation

**Ursa implementation** (`storage/diskless-ursa/src/main/java/org/apache/kafka/storage/diskless/`):
- `handlers/UrsaStorageEngineImpl.java`: Diskless storage engine implementation backed by Ursa
- `handlers/PartitionReader.java`: Read path for fetch and list offsets, with a cached cursor and an offset window cached for 100 ms (`OFFSET_RANGE_REFRESH_MS`). Local appends widen it at once, and this broker's own retention trim or a close drops it (another owner's trim, like its appends, lands within the interval). `OFFSET_OUT_OF_RANGE` and `ListOffsets(EARLIEST/EARLIEST_LOCAL)` are only ever answered from a window read after the request arrived. `LATEST`/`MAX_TIMESTAMP` take the cached window and may lag by up to the interval.
- `handlers/PartitionWriter.java`: Write path: validation, append, producer state, append notifications
- `handlers/PartitionRetention.java`: Coalesced retention worker
- `handlers/LakestreamStorageHolder.java`: Catalog and Oxia client ownership; opens partitions with create-if-absent
- `handlers/KafkaStreamIdentity.java`: Maps a topic to its Lakestream stream identity and the stream properties Kafka writes
- `handlers/UrsaDisklessTopicLifecycle.java`: StreamCatalog-backed topic lifecycle implementation
- `idempotent/ProducerStateManager.java`: Producer state tracking backed by Oxia snapshots

**Plugin classloading**:
- `clients/src/main/java/org/apache/kafka/common/utils/KafkaPluginClassLoader.java`: Child-first plugin-private loading with parent-first Kafka/logging/Scala shared APIs
- `server-common/src/main/java/org/apache/kafka/server/util/KafkaPluginClassPaths.java`: Resolves configured classpaths or `$KAFKA_HOME/<runtime-dir>/*` defaults

**Broker integration** (`core/src/main/scala/kafka/server/`):
- `ReplicaManager.scala`: Calls `DisklessStorageReplicaManagerSupport` for diskless topics
- `KafkaApis.scala`: Request handling, async produce/fetch for diskless
- `BrokerServer.scala`: Initializes diskless storage support
- `ReplicaFetcherThread.scala`: Skips fetching for diskless partitions

**Controller integration** (`core/src/main/java/kafka/server/metadata/`):
- `DisklessTopicLifecycleReconciler.java`: Active-controller reconciler: ensures/deletes diskless topics from metadata deltas and sweeps orphans on a configurable interval

**Network / Pipelining** (`core/src/main/scala/kafka/network/`):
- `SocketServer.scala`: Request pipelining support (`socket.server.enable.request.pipelining`)

### Design principles

1. **Topic-level granularity**: Diskless mode per-topic; mixed deployments supported
2. **Async-first**: All Ursa operations return `CompletableFuture`; never block request handler threads
3. **RF=1**: Ursa handles durability; Kafka-level ISR replication bypassed for diskless topics
4. **Transparent clients**: No protocol changes; existing producers and consumers work without code changes (diskless topics reject transactional producers)
5. **Avoid code duplication**: Extract common logic into helper methods
6. **Follow existing Kafka code conventions and patterns when possible**

### Limitations

- No transactional producer support for diskless topics
- No K/V log compaction (only external Ursa compaction to Parquet)
- Internal topics (`__consumer_offsets`, `__transaction_state`) always use local storage

## Commands

You need JDK 17 or later. Docker must be running for the isolated Ursa integration tests, which start
Oxia and LocalStack with Testcontainers. Check the daemon with `docker info` before you run them.

```bash
# Full build (jar only, skip tests)
./gradlew jar

# Compile check (fast feedback loop)
./gradlew :clients:compileJava :core:compileScala :storage:compileJava :storage:storage-diskless-api:compileJava :storage:storage-diskless-ursa:compileJava :server:compileJava

# Run all tests, including the isolated Ursa integration tests (needs Docker, takes hours)
./gradlew test

# Run a specific test class
./gradlew :core:test --tests "kafka.network.SocketServerTest"
./gradlew :server:test --tests "org.apache.kafka.server.ursa.integration.UrsaStorageE2ETest"
./gradlew :storage:storage-diskless-api:test --tests "org.apache.kafka.storage.diskless.DisklessStorageEngineLoaderTest"
./gradlew :storage:storage-diskless-ursa:test --tests "org.apache.kafka.storage.diskless.handlers.UrsaStorageStateTest"

# Run a specific test method
./gradlew :storage:storage-diskless-ursa:test --tests "org.apache.kafka.storage.diskless.handlers.UrsaStorageStateTest.testCleanupPartitionClosesLogAndClearsProducerState"

# Run only Ursa integration tests (isolated from main suite)
./gradlew test -Pkafka.ci.isolated.tests=only

# Run main tests excluding Ursa integration tests
./gradlew test -Pkafka.ci.isolated.tests=exclude

# Checkstyle and SpotBugs (a module's test task runs both for that module)
./gradlew :storage:checkstyleMain
./gradlew :storage:storage-diskless-api:checkstyleMain
./gradlew :storage:storage-diskless-ursa:checkstyleMain
./gradlew :core:checkstyleMain

# Import order (the test task doesn't run this)
./gradlew spotlessCheck        # import order check
./gradlew spotlessApply        # auto-fix import order

# License headers
./gradlew rat
```

`./gradlew rat` checks nothing in a git worktree. `build.gradle` enables it only when `.git` is a
directory, and in a worktree `.git` is a file. It also skips files git doesn't track yet, so stage new
files first. If you work in a worktree, say that `rat` didn't run, and leave it to the human to run
from a regular clone.

## Code quality

- **Checkstyle**: Each module has its own import-control XML (`checkstyle/import-control-*.xml`). The `storage` module uses `import-control-storage.xml` which defines allowed package dependencies per subpackage.
- **SpotBugs**: Exclusions in `gradle/spotbugs-exclude.xml`.
- **Spotless**: Enforces import order: `kafka`, `org.apache.kafka`, `com`, `net`, `org`, `java`, `javax`, then static imports.
- **Compiler**: `-Werror` is enabled for main sources. Java release target is 17, except the modules in `modulesNeedingJava11` in `build.gradle` (clients, streams and a few others), which target 11.
- **Imports**: Always use `import` statements instead of fully-qualified class names in code (e.g., write `import java.util.HashMap;` and use `HashMap`, not `java.util.HashMap` inline).
- **License headers**: Every new source file starts with the Apache license header; Markdown files carry it in an HTML comment, as the LIPs do. `./gradlew rat` checks it. Files that don't carry a header are listed in the `rat` block of `build.gradle`.

## Critical patterns

### Ursa dependencies
Keep Ursa implementation dependencies out of Kafka's main classpath. Ursa, Oxia, cloud SDKs, and lakehouse dependencies belong in the isolated `storage:storage-diskless-ursa` runtime, not in `storage` or `core`.

The diskless storage data/read path compiles only against `lakestream-api`. Its production sources must not import `io.lakestream.ursa.*`, select a compacted-reader implementation, or depend on Ursa catalog/Oxia metadata layouts. `ursa-storage-kafka-runtime` is the single `runtimeOnly` bundle that discovers the catalog provider and internally assembles Ursa storage plus the Kafka lakehouse reader. The separate Oxia API dependency is outside this data/read boundary: it supports Kafka-owned producer-state snapshots and their deletion fence.

Release tarballs package those isolated runtime jars under `./ursa-storage/`. Kafka, Scala, SLF4J, Log4j, and other platform jars are provided by `./libs/` and should not be duplicated into `./ursa-storage/` unless the dependency is intentionally private to the Ursa runtime. The `KafkaPluginClassLoader` loads plugin-private classes child-first while keeping Kafka/logging/Scala API packages parent-first.

`ursa.storage.class.path` is the shared config used by broker and controller diskless-storage loaders and their isolated integration tests. In production, the default is `$KAFKA_HOME/ursa-storage/*`.

### Isolated CI tests
Ursa integration tests (`org/apache/kafka/server/ursa/integration/**`, `org/apache/kafka/storage/diskless/**`, `org/apache/kafka/controller/DisklessTopicDefaultsTest*`) are isolated from the main test suite. Use `-Pkafka.ci.isolated.tests=only` to run them, or `=exclude` to skip them. Without the property, `./gradlew test` runs both groups.

### Verify license after dependency changes
When adding, removing, or upgrading dependencies in `build.gradle`, the binary distribution's `LICENSE-binary` file must be kept in sync. `LICENSE-binary` lists every third-party jar bundled under `./libs` and `./ursa-storage` in the release tarball, grouped by license type (Apache 2.0, MIT, BSD, etc.). Run the verification script to check for mismatches:
```bash
# Full check: builds releaseTarGz, extracts tarball, compares bundled jar directories against LICENSE-binary
python committer-tools/verify_license.py

# Skip the build if you already have a tarball from a previous build
python committer-tools/verify_license.py --skip-build
```
The script reports jars missing from `LICENSE-binary` (need to add) and stale entries in `LICENSE-binary` no longer bundled under `./libs` or `./ursa-storage` (need to remove). If adding a dependency with a non-Apache license, also add the license text to the `licenses/` directory and reference it in the appropriate section of `LICENSE-binary`.

## Boundaries

**Always**

- Keep diskless logic behind `isDisklessTopic()` checks, so classic topics behave as they do in Apache
  Kafka.
- Before you say a change is done, run the test classes that cover it
  (`./gradlew :<module>:test --tests <class>`), plus `spotlessCheck`. Diskless integration tests need
  Docker. Before you draft a pull request, run `./gradlew rat` too, subject to the worktree note under
  [Commands](#commands).
- Honor buffer ownership in the diskless code: release what you own, on every path.
- Follow existing Kafka patterns in the code you're changing.

**Ask first**

- Adding or upgrading a dependency. It also needs `LICENSE-binary`, `NOTICE-binary` when the
  dependency ships a NOTICE, and `verify_license.py`.
- Anything that needs a LIP (see the
  [LIP process](https://github.com/openlakestream/lips/blob/main/CONTRIBUTING.md)): what Kafka clients
  observe on a diskless topic, `ursa.*` configuration keys, the contract between Kafka and Lakestream
  (stream identity and properties, the WAL payload), and how the controller manages diskless topics.
- Changing classic-topic behavior, or upstream Kafka code outside the diskless paths.
- Changing `checkstyle/import-control-*.xml`.
- Changing anything under `.github/workflows`.
- Changing the contributor and agent policy: `AGENTS.md`, `AI_POLICY.md`, `CONTRIBUTING.md`,
  `.claude/` and `.github/CODEOWNERS`.
- Deleting, disabling or loosening a test.

**Never**

- Edit `LICENSE` or `NOTICE`, or change the text of license headers.
- Put Ursa, Oxia, cloud SDK or lakehouse dependencies on Kafka's main classpath, or import
  `io.lakestream.ursa.*` in production sources.
- Change the Kafka protocol message schemas in `clients/src/main/resources/common/message/`. UFK makes
  no protocol changes.
- Add SpotBugs exclusions, checkstyle suppressions or `@SuppressWarnings` just to get a green build.
- Force-push, rewrite published history, or commit secrets.
- Claim performance, cost or compatibility results in code or docs unless a test or benchmark in this
  repository backs them.

## Docker demo

A single `docker-compose.yml` holds everything. `docker compose up` starts the core cluster, including the Ursa compactor; the Iceberg catalog, schema registry and REST proxy sit behind the `lakehouse` profile and the workloads behind `demo`, `share-demo`, `lakehouse-demo` and `tools`.

```bash
cd docker/examples/docker-compose-files/cluster/ursa
./build-image.sh          # Build lakestream/kafka:latest (brokers, CLI and Ursa compactor)
make up                   # Core cluster: Oxia + MinIO + 3 brokers + compactor
make create-topic         # Create diskless topic
make demo                 # Profile `demo`: perf producers + consumer, torn down on exit
make lakehouse-demo       # Profiles `lakehouse` + `lakehouse-demo`: Kafka -> Iceberg -> DuckDB
make destroy              # Tear down every profile and remove volumes
```

Architecture: 3 Kafka brokers + Oxia (metadata) + MinIO (S3 storage) + the Ursa compactor (same image as the brokers), with Polaris (Iceberg catalog), a schema registry and a REST proxy under the `lakehouse` profile. Ports (all bound to 127.0.0.1): kafka-1:29092, kafka-2:39092, kafka-3:49092, oxia:6648, minio:19000/19001, polaris:18181/18182, schema-registry:18081, kafka-rest:18082.

## Strimzi image

`lakestream/kafka-strimzi:<strimzi>-kafka-<kafka>` is the same distribution in the Strimzi image layout, for clusters managed by the Strimzi cluster operator. `docker/strimzi/Dockerfile` starts from `quay.io/strimzi/kafka:<strimzi>-kafka-<upstream kafka>` and swaps in UFK's tarball, merging `libs/` by artifact name so Strimzi's agents and third-party libs stay while no artifact appears twice. The Strimzi and base Kafka versions default in `docker/strimzi/build-image.sh` (`STRIMZI_VERSION`, `STRIMZI_KAFKA_VERSION`) and must share the Kafka minor with the tarball. `docker/strimzi/verify-image.sh` checks the layout; CI runs it, and `release_docker_image.yml` publishes the image next to `lakestream/kafka`. Deployment notes and a sample `Kafka` resource live in `docker/strimzi/README.md` and `docker/strimzi/examples/`.

```bash
docker/strimzi/build-image.sh --tarball core/build/distributions/kafka_2.13-<version>.tgz
docker/strimzi/verify-image.sh lakestream/kafka-strimzi:1.2.0-kafka-<version>
```

## Upstream compatibility

UFK stays close to upstream Apache Kafka, so changes should minimize divergence from it. When modifying core Kafka code:
- Prefer additive changes (new methods, new config options) over modifying existing behavior
- Keep diskless logic behind `isDisklessTopic()` checks
- Ensure all existing Kafka tests continue to pass with diskless disabled

## Where to read next

- [Contributing](CONTRIBUTING.md) and the [AI policy](AI_POLICY.md)
- The [LIP process](https://github.com/openlakestream/lips/blob/main/CONTRIBUTING.md), and the
  diskless storage design in
  [LIP-162](https://github.com/openlakestream/lips/blob/main/proposals/LIP-162-Diskless-Storage-with-Ursa-Integration.md)
- [Materializers and UFK](docs/developer/materializers.md)
- [Docker Compose stack](docker/examples/docker-compose-files/cluster/ursa/README.md)
- Ursa's [instructions for coding agents](https://github.com/openlakestream/ursa/blob/main/AGENTS.md)
  and [Write a materializer](https://github.com/openlakestream/ursa/blob/main/docs/developer/materializer-guide.md)
- The [upstream Apache Kafka README](https://github.com/apache/kafka/blob/trunk/README.md) for the
  general build, IDE and test setup
