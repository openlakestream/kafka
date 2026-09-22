# Contributing to Ursa for Apache Kafka

Thanks for your interest in Ursa for Apache Kafka (UFK). UFK is a Kafka distribution built on the
Lakestream API and specification. Diskless topics keep their records on object storage through
[Ursa](https://github.com/openlakestream/ursa); every other topic stays Kafka. It is built on
[Apache Kafka](https://kafka.apache.org) 4.3.1.

[Lakestream](https://openlakestream.org) is an open API and specification for stream storage on
object storage, with a stream materialization framework that defines how a stream becomes a
lakehouse table. Ursa implements it as a storage engine; Ursa for Apache Kafka is a Kafka
distribution built on it. Both are open source under Apache 2.0.

Bug reports, fixes, documentation, tests and feedback on how diskless topics behave are all welcome.
Diskless topics have [limitations](README.md#limitations-of-diskless-topics) that are worth reading
before you start.

> **Using an AI assistant?** Read the [AI policy](AI_POLICY.md) first.
> **Are you a coding agent?** Start with [AGENTS.md](AGENTS.md).

## Here or upstream?

UFK tracks Apache Kafka and keeps its own changes small. Before you start, check which project your
change belongs to:

- **Here:** diskless topics, the Ursa integration, and anything behind `isDisklessTopic()`. The
  Docker Compose demo and these docs belong here too.
- **Upstream, in Apache Kafka:** problems that also happen on the Apache Kafka release UFK is built
  on. Follow Apache Kafka's [contributing guide](https://kafka.apache.org/contributing.html). UFK
  picks up upstream fixes when it syncs with Apache Kafka.

Trying the same steps on a classic topic (`ursa.storage.enable=false`) is a quick first check, but it
isn't the whole answer. UFK changes some code that classic topics share, such as `ReplicaManager` and
`KafkaApis`, so confirm on Apache Kafka before you report a problem upstream. If you're not sure,
open an issue and we'll work it out with you.

## Ways to contribute

- **Report a bug or request a feature.** Open an
  [issue](https://github.com/openlakestream/kafka/issues/new/choose).
- **Ask a question or share an idea.** Start a
  [discussion](https://github.com/openlakestream/kafka/discussions).
- **Fix something.** Issues labeled
  [`good first issue`](https://github.com/openlakestream/kafka/labels/good%20first%20issue) and
  [`help wanted`](https://github.com/openlakestream/kafka/labels/help%20wanted) are good places to
  start. Comment on the issue to say you're working on it, so nobody duplicates your work.
- **Improve the docs.** If something confused you, it will confuse the next person too.
- **Run the demo and tell us what broke.** The
  [Docker Compose stack](docker/examples/docker-compose-files/cluster/ursa/README.md) runs brokers,
  Ursa and a lakehouse on your laptop.
- **Write a materializer.** A materializer turns a stream into a table in another system.
  Materializers are written against Ursa, not UFK, so start with Ursa's
  [Write a materializer](https://github.com/openlakestream/ursa/blob/main/docs/developer/materializer-guide.md)
  guide. [Materializers and UFK](docs/developer/materializers.md) covers what UFK hands a
  materializer.

## Where to talk

| For | Use |
|---|---|
| Bugs and concrete feature requests | [Issues](https://github.com/openlakestream/kafka/issues) |
| Questions | [Discussions: Q&A](https://github.com/openlakestream/kafka/discussions/categories/q-a) |
| Design ideas, and proposals before they become a LIP | [Discussions: Ideas](https://github.com/openlakestream/kafka/discussions/categories/ideas) |
| Lakestream Improvement Proposals (LIPs) | [openlakestream/lips](https://github.com/openlakestream/lips) |
| Something you built with UFK | [Discussions: Show and tell](https://github.com/openlakestream/kafka/discussions/categories/show-and-tell) |
| Bugs in Apache Kafka itself | Apache Kafka, as described in its [contributing guide](https://kafka.apache.org/contributing.html) |
| Security vulnerabilities | Report privately, as described in [SECURITY.md](SECURITY.md) |
| Conduct concerns | See [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) |

The project doesn't run a Slack workspace or a mailing list, so decisions happen where everyone can
read them.

## Before you write code

- **Small, self-contained changes** can go straight to a pull request. Examples: a bug fix with a
  test, a documentation correction, a typo.
- **For anything larger**, open an issue or a discussion first. That includes a new feature, a
  refactor across modules, or a change in behavior. Agreeing on the approach first saves you from
  writing code that has to be redone.
- **Some changes need a LIP** (Lakestream Improvement Proposal):
  - changes to what a Kafka client can observe on a diskless topic
  - new or changed `ursa.*` configuration keys
  - changes to the contract between Kafka and Lakestream: how a topic maps to a stream, the stream
    properties Kafka writes, and what Kafka writes to the write-ahead log (WAL)
  - changes to how the controller creates, grows, deletes or cleans up diskless topics

  LIPs for every Lakestream component live in [openlakestream/lips](https://github.com/openlakestream/lips).
  Start with a thread in
  [Discussions: Ideas](https://github.com/openlakestream/kafka/discussions/categories/ideas), then
  follow the [LIP process](https://github.com/openlakestream/lips/blob/main/CONTRIBUTING.md).

## Build and test

You need JDK 17 or later, and Docker. Gradle comes with the repository, as the `./gradlew` wrapper. The first
build downloads the Ursa runtime (`org.openlakestream:ursa-storage`) from Maven Central.

```bash
git clone https://github.com/openlakestream/kafka.git
cd kafka
./gradlew jar    # build every module, skipping tests
```

On Windows, `CLAUDE.md` in this repository is a symbolic link. Clone with
`git clone -c core.symlinks=true https://github.com/openlakestream/kafka.git`. Creating symlinks also
requires Developer Mode or administrator rights.

For quick feedback, compile the modules that carry diskless code, and run single tests:

```bash
./gradlew :clients:compileJava :core:compileScala :storage:compileJava \
  :storage:storage-diskless-api:compileJava :storage:storage-diskless-ursa:compileJava :server:compileJava

# One test class, or one test method
./gradlew :storage:storage-diskless-ursa:test --tests "org.apache.kafka.storage.diskless.handlers.UrsaStorageStateTest"
./gradlew :storage:storage-diskless-ursa:test --tests "org.apache.kafka.storage.diskless.handlers.UrsaStorageStateTest.testCleanupPartitionClosesLogAndClearsProducerState"
```

The diskless integration tests are kept apart from the rest of the suite. They are the tests under
`org/apache/kafka/server/ursa/integration/` and `org/apache/kafka/storage/diskless/`, plus
`DisklessTopicDefaultsTest`. Many of them start Oxia and an S3 emulator in containers through
Testcontainers, so Docker has to be running.

```bash
./gradlew test -Pkafka.ci.isolated.tests=only      # only the diskless integration tests
./gradlew test -Pkafka.ci.isolated.tests=exclude   # everything else
```

A plain `./gradlew test` runs both groups, so it needs Docker as well. The whole suite takes hours.
Run the tests that cover your change. CI runs the diskless integration tests on every pull request,
and the whole suite after merge.

Running a module's tests also runs checkstyle and SpotBugs on that module. Before you open a pull
request, also run:

```bash
./gradlew spotlessCheck    # import order; fix with: ./gradlew spotlessApply
./gradlew rat              # license headers
```

`./gradlew rat` checks only files that git tracks, so `git add` new files first. It checks nothing at
all in a git worktree, so run it from a regular clone.

If you add, remove or upgrade a dependency, keep `LICENSE-binary` in sync and run
`python committer-tools/verify_license.py`.
[AGENTS.md](AGENTS.md#verify-license-after-dependency-changes) explains how.

### What CI runs

| Job | When | What it runs |
|---|---|---|
| Validate | Every pull request | `./gradlew check releaseTarGz -x test`: compilation, checkstyle, SpotBugs, spotless, license headers and the release tarball. Then the `LICENSE-binary` check and, for branches in this repository, a Docker Compose smoke test. |
| Isolated tests | Every pull request that isn't a draft | The diskless integration tests, on JDK 17 |
| Full test suite | Pushes to `4.3-ursa`, weekends, and pull requests that a maintainer labels `run-junit-tests` | The whole Apache Kafka suite, on JDK 17 and 25. It takes several hours. |

For more:

- The [upstream README](https://github.com/apache/kafka/blob/trunk/README.md) covers the general
  Apache Kafka build, IDE setup and test options. They all still apply.
- The [Docker Compose stack](docker/examples/docker-compose-files/cluster/ursa/README.md) runs a local
  cluster, including the lakehouse path.

## How the code is organized

Most of the tree is Apache Kafka. The code UFK adds lives here:

```text
kafka/
├── storage/diskless-api/                                # Diskless SPI and the classes the broker uses
├── storage/diskless-ursa/                               # The Ursa implementation, loaded in isolation
├── core/src/main/scala/kafka/server/                    # Broker hooks: ReplicaManager, KafkaApis, BrokerServer
├── core/src/main/java/kafka/server/metadata/            # DisklessTopicLifecycleReconciler, on the controller
├── server/src/test/java/org/apache/kafka/server/ursa/   # End-to-end tests against Oxia and S3
└── docker/examples/docker-compose-files/cluster/ursa/   # Docker Compose demo
```

A few rules keep UFK close to upstream:

- Diskless behavior stays behind `isDisklessTopic()` checks. Classic topics behave as they do in
  Apache Kafka, and the upstream test suite passes with diskless storage disabled.
- Prefer additive changes to Kafka code, such as new methods and new configuration options, over
  changes to existing behavior.
- Ursa, Oxia, cloud SDK and lakehouse dependencies stay in the isolated `storage:storage-diskless-ursa`
  runtime. They never go on Kafka's main classpath.
- The diskless data and read path compiles only against `lakestream-api`. It doesn't import
  `io.lakestream.ursa.*`.

To go deeper:

- [AGENTS.md](AGENTS.md) describes the architecture and the key source files. It is written for coding
  agents, but it works just as well for people.
- [LIP-162](https://github.com/openlakestream/lips/blob/main/proposals/LIP-162-Diskless-Storage-with-Ursa-Integration.md)
  is the design of diskless storage.

## Code style

Checkstyle and spotless enforce most of these rules. The rest come up in review.

- Every new source file starts with the Apache license header. `./gradlew rat` checks it.
- Follow the conventions of the Kafka code around your change.
- Imports are ordered `kafka`, `org.apache.kafka`, `com`, `net`, `org`, `java`, `javax`, then static
  imports. `./gradlew spotlessApply` fixes the order.
- Use `import` statements, not fully qualified class names in code.
- Each module's `checkstyle/import-control-*.xml` file lists the packages it may depend on. If your
  change needs a new dependency between packages, explain why in the pull request.
- Main sources compile with `-Werror`. Most modules target Java 17. `clients`, `streams` and the other
  modules listed in `modulesNeedingJava11` in `build.gradle` target Java 11.
- In the diskless code, keep buffer ownership explicit. Say whether a method takes, retains or copies
  a reference-counted buffer, and release what you own.
- Fix real SpotBugs findings instead of adding exclusions.

## Tests

- Add a test for the behavior you change. Test class names end in `Test`.
- Put diskless integration tests in `server/src/test/java/org/apache/kafka/server/ursa/integration/`,
  or in the `org.apache.kafka.storage.diskless` packages of the `storage` modules. CI runs the
  isolated tests only in `server`, `storage`, `metadata` and the two diskless modules, and leaves
  them out of every other module's run, so a matching test anywhere else never runs in CI.
- Tests that need Oxia or object storage start them with Testcontainers.
- Classic topics must keep passing the upstream tests with diskless storage disabled.

## Commits

### Sign your commits (DCO)

Every commit needs a Developer Certificate of Origin sign-off:

```bash
git commit -s -m "Fence stream lifecycle operations"
```

The `-s` flag adds a line such as `Signed-off-by: Your Name <you@example.com>`. The line certifies
that you wrote the change, or otherwise have the right to submit it under the project's license. The
full text is at [developercertificate.org](https://developercertificate.org/).

A DCO check runs on every pull request. If you forgot to sign off, fix the last commit with
`git commit --amend -s --no-edit`, or a series with `git rebase --signoff origin/4.3-ursa`, and then
force-push your branch.

We don't use a CLA. The DCO sign-off is all we ask.

### Write useful messages

Start with a short summary in the imperative mood, such as "Fence stream lifecycle operations". Then
explain why the change is needed, if that isn't obvious. Apache Kafka's `KAFKA-1234:`
prefixes refer to its Jira, so don't use them here.

Pull requests are squash-merged. The squash commit keeps your commit messages, with their trailers.
Its subject is the pull request title, or the commit's own subject when the pull request has a single
commit, so write both the way you'd write a commit summary.

### Say when AI helped

If an AI tool helped meaningfully, add an `Assisted-by:` trailer. `Co-authored-by:` is also
accepted. The [AI policy](AI_POLICY.md) explains what counts.

## Pull requests

1. Fork the repository on GitHub, and add your fork as a remote:
   `git remote add fork https://github.com/<your-username>/kafka.git`. This repository isn't a GitHub
   fork of Apache Kafka, so if you already have a repository named `kafka`, give this fork another
   name and use it in the URL. Create a branch for your change, and push it to `fork`.
2. Open the pull request against `4.3-ursa`, the default branch. Its name follows the Apache Kafka
   release that UFK is built on, so it changes when UFK moves to a newer release.
3. Keep each pull request to one logical change. Smaller pull requests get reviewed sooner.
4. Fill in the pull request template: what changed and why, compatibility, how you tested it, and AI
   assistance.
5. Update the documentation in the same pull request when you change behavior or a contract.
6. Make sure CI passes.
7. A code owner for the files you touched reviews and approves the change. Code owners are listed in
   [CODEOWNERS](.github/CODEOWNERS). These docs call them maintainers.

We aim to respond promptly. If your pull request has been quiet for a while, @-mention one of the code
owners for the files you changed.

## License

UFK is licensed under the [Apache License 2.0](LICENSE), like Apache Kafka, and so is your
contribution. The binary distribution bundles third-party jars, which
[LICENSE-binary](LICENSE-binary) lists with their licenses. CI checks that list against the jars in
the release tarball.
