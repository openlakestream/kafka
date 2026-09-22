Strimzi Image for Kafka with Diskless/Ursa Storage
===================================================

`lakestream/kafka-strimzi` is the image to use when the cluster is managed by the
[Strimzi](https://strimzi.io) cluster operator. It is the official Strimzi Kafka
image for the same Kafka minor with the bundled Apache Kafka distribution replaced
by this fork's release tarball, so it keeps everything the operator relies on:

- `kafka_run.sh`, `kafka_readiness.sh`, `kafka_liveness.sh` and the other Strimzi entrypoints
- `kafka-agent` and `tracing-agent` in `/opt/kafka/libs`
- Strimzi's third-party libs (OpenTelemetry, gRPC), Cruise Control, kafka_exporter and the JMX exporter
- UID 1001 in group 0, `KAFKA_HOME=/opt/kafka`

and adds what this fork ships:

- the fork's `libs/`, `bin/` and `config/`
- the isolated Ursa runtime under `/opt/kafka/ursa-storage/`, which the default
  `ursa.storage.class.path` (`$KAFKA_HOME/ursa-storage/*`) picks up unchanged

Jars are merged by artifact name. Any jar in Strimzi's `libs/` whose name without
version also exists in the tarball is dropped and the tarball's copy wins, so no
artifact appears twice on the classpath. The Ursa compactor is not part of this
image; see [Compactor](#compactor) below.

Tags follow Strimzi's convention: `lakestream/kafka-strimzi:<strimzi>-kafka-<kafka>`,
for example `lakestream/kafka-strimzi:1.2.0-kafka-4.3.1.1`. Every `vX.Y.Z.W` release
tag publishes this image next to `lakestream/kafka:X.Y.Z.W`, and both are also
pushed as `latest` for final releases (versions without a `-rc1`-style suffix)
(`.github/workflows/release_docker_image.yml`).

Building
--------

```bash
# Build the release tarball, then the image (defaults: Strimzi 1.2.0 on Kafka 4.3.1)
docker/strimzi/build-image.sh

# Reuse an existing tarball
docker/strimzi/build-image.sh --tarball core/build/distributions/kafka_2.13-4.3.1.1.tgz

# Another Strimzi release; the base Kafka version must share the minor with the tarball
STRIMZI_VERSION=1.3.0 STRIMZI_KAFKA_VERSION=4.3.2 docker/strimzi/build-image.sh

# Multi-arch build pushed to the registry
docker/strimzi/build-image.sh --push --platform linux/amd64,linux/arm64

# Check the layout of a built image (also run in CI)
docker/strimzi/verify-image.sh lakestream/kafka-strimzi:1.2.0-kafka-4.3.1.1
```

Deploying
---------

`examples/kafka-diskless.yaml` is a complete starting point: controller and broker
node pools, a `Kafka` resource with the Ursa broker settings, and a diskless
`KafkaTopic`. The essential parts:

```yaml
apiVersion: kafka.strimzi.io/v1
kind: Kafka
spec:
  kafka:
    version: 4.3.1                                   # a version Strimzi ships
    metadataVersion: 4.3-IV0
    image: lakestream/kafka-strimzi:1.2.0-kafka-4.3.1.1   # ours
    config:
      ursa.storage.enable: true
      ursa.catalog.oxia.service.url: oxia://oxia.oxia.svc:6648/default
      ursa.oxia.service.url: oxia://oxia.oxia.svc:6648/default
      ursa.storage.backend.type: S3
      ursa.storage.s3.bucket: kafka-ursa
      # ... see the example for the full list
```

- `spec.kafka.version` must be a Kafka version the installed Strimzi release knows,
  because the operator validates it against its compiled-in list. Strimzi 1.2.0 knows
  4.2.0, 4.2.1, 4.3.0 and 4.3.1; use the one matching the image's base
  (`STRIMZI_KAFKA_VERSION`). `spec.kafka.image` then overrides which image runs.
- Broker settings go in `spec.kafka.config`. Use the `env` config provider and
  `template.kafkaContainer.env` to pull credentials from a Secret, as in the example.
- Topics become diskless through the topic config `ursa.storage.enable=true`, whether
  created via a `KafkaTopic` resource or the Kafka admin API. Use `replicas: 1`;
  Ursa provides durability.
- Brokers still need volumes for the KRaft metadata log and for classic topics such
  as `__consumer_offsets`.

Compactor
---------

Compaction is what makes diskless storage reclaimable: retention on a diskless
topic only hides records, and WAL objects are deleted behind a watermark that
compaction alone advances. The compactor is not a broker, so Strimzi does not run
it. Deploy `examples/ursa-compactor.yaml`: a plain Deployment of the
`lakestream/kafka` image running `/opt/kafka/bin/ursa-compactor.sh` against the
same Oxia service and bucket as the brokers. It never connects to Kafka.

- Keep the Oxia URLs, WAL bucket/prefix and compaction bucket/prefix identical to
  the brokers' `ursa.*` settings.
- Instances elect a leader through Oxia; the leader publishes tasks and runs the
  WAL cleaner, all instances run workers. One replica is enough to start, more
  add throughput.
- Keep `materializationEnabled=true`. Without `lakehouseType` and catalog settings
  the compactor registers no table catalog and compaction is storage-only; add
  `lakehouseType=ICEBERG` plus the `iceberg.catalog.*` settings to also write
  external Iceberg tables (the compose stack's `lakehouse/compactor-entrypoint.sh`
  lists them). `materializationEnabled=false` selects a legacy path that fails on
  Ursa 1.0.0 with `Unsupported lakehouse type: NONE` and never reclaims WAL objects.

Known limitation: broker scale-down
-----------------------------------

Strimzi validates `default.replication.factor`, `offsets.topic.replication.factor`
and `transaction.state.log.replication.factor` against the broker count first;
lower them from the example's `3` before shrinking the pool below three brokers,
or the `Kafka` resource goes `NotReady` with an `InvalidResourceException` and the
check below never runs.

Before removing a broker, Strimzi refuses if the broker still holds partition
replicas. Diskless partitions report their current owner as their single replica,
so a broker that owns any diskless partition looks "in use": the operator logs
`Cannot scale down brokers [n] because [n] have assigned partition-replicas`,
sets a `ScaleDownPreventionCheck` warning on the `Kafka` resource and reverts the
node pool to its previous replica count. Upstream Strimzi has no diskless
awareness yet.

To scale down anyway, set the annotation
`strimzi.io/skip-broker-scaledown-check: "true"` on the `Kafka` resource and
lower the pool's replicas again. Diskless partitions owned by the removed broker
are re-homed to the remaining brokers on their own and their data stays readable
and writable, because the data lives in Ursa, not on the broker. The annotation
disables the check for classic topics too, so before scaling down make sure no
classic partition (including `__consumer_offsets`) has its last replica on the
brokers being removed.

Verified against Strimzi 1.2.0 on a local cluster: deploy from the examples,
produce and consume through `<cluster>-kafka-bootstrap`, a manual rolling update
of the broker pool, a blocked scale-down, and a scale-down with the annotation
followed by consuming every record from a partition the removed broker had owned.
