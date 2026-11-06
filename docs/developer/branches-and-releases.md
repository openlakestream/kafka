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

# Branches and releases

UFK has two kinds of branches. Development branches take pull requests. Release branches hold what
ships: an Apache Kafka release with the UFK commits on top.

| Branch | Example | What it is | What goes in | Release tags |
|---|---|---|---|---|
| Development | `4.3-ursa` | UFK on an Apache Kafka release line | Pull requests, and merges of upstream release tags | None |
| Release | `4.3.2-ursa` | One Apache Kafka release, with the UFK commits on top | Cherry-picks from the development branch, by maintainers | `v4.3.2.1`, `v4.3.2.2`, ... |

The newest development branch is the default branch on GitHub. Neither kind of branch is
force-pushed, so nobody has to rebase their work after an upstream update.

A release reads as "Apache Kafka 4.3.2 plus these commits": `git log --oneline 4.3.2..v4.3.2.1` lists
every UFK change in it and nothing else. On the development branch,
`git log --oneline --no-merges 4.3.2..4.3-ursa` gives the same list.

Pushes to both kinds of branches run the full CI suite (`.github/workflows/ci.yml`).

## Merge an upstream release

When Apache Kafka releases 4.3.2, merge its tag into the development branch:

```bash
git fetch --no-tags https://github.com/apache/kafka.git tag 4.3.2
git switch 4.3-ursa
git pull --ff-only
git merge --signoff 4.3.2
git push origin 4.3-ursa
```

Merge the release tag, not the head of the upstream `4.3` branch. Then every upstream commit on the
development branch belongs to an upstream release, and `4.3.2..4.3-ursa` holds only UFK commits and
merges.

Push the merge yourself rather than through a pull request. Pull requests are squash-merged, which
would flatten 4.3.2 into a single commit and cut the branch off from the upstream history.

## Cut a release branch

After the merge, create the release branch:

```bash
git switch -c 4.3.2-ursa 4.3.2
git rev-list --reverse --no-merges 4.3.2..4.3-ursa | git cherry-pick -x --allow-empty --stdin
git diff --exit-code 4.3-ursa 4.3.2-ursa    # must print nothing
git push origin 4.3.2-ursa
```

This replays every UFK commit onto 4.3.2 in order, and leaves out the merge commits. `-x` records the
development-branch commit in each message, which is how a release finds what is still to pick.
`--allow-empty` keeps the commits that are empty on the development branch, too.

The diff check proves that the release branch has the same code as the development branch. If a
cherry-pick stops on a conflict, resolve it the way the merge did, then run
`git cherry-pick --continue`. With `git config rerere.enabled true`, Git records your resolutions and
reuses them later.

## Release

Tag releases on a release branch, never on a development branch. First bring the release branch up
to date, and check it against the development branch:

```bash
git switch 4.3.2-ursa
git log -1 --format=%b | grep 'cherry picked from' | tail -n 1         # the last 4.3-ursa commit picked
git log --oneline --reverse --no-merges 4.3.2..4.3-ursa ^<that commit>  # still to pick
git cherry-pick -x <commit>...
git diff --exit-code 4.3-ursa 4.3.2-ursa
git push origin 4.3.2-ursa
```

When CI passes, tag the release:

```bash
git tag v4.3.2.1
git push origin v4.3.2.1
```

The tag runs `.github/workflows/release_docker_image.yml`, which builds the release and publishes the
images. The workflow moves `latest` only for the highest final release, so a fix released from an
older release branch leaves `latest` alone.

Once the development branch merges a newer upstream release, the older release branch is frozen. It
takes only the fixes that a release from it needs, and the diff check no longer applies.

## Start a release line

When UFK moves to Apache Kafka 4.4, replay the UFK commits onto 4.4.0:

```bash
git fetch --no-tags https://github.com/apache/kafka.git tag 4.4.0
git switch -c 4.4-ursa 4.3-ursa
git rebase --onto 4.4.0 4.3.N    # 4.3.N: the last upstream release merged into 4.3-ursa
git push origin 4.4-ursa
```

The rebase drops merge commits, and skips commits that 4.4.0 already has. Then:

1. Cut `4.4.0-ursa` from `4.4-ursa`, as in [Cut a release branch](#cut-a-release-branch).
2. Make `4.4-ursa` the default branch on GitHub.
3. Replace `4.3-ursa` in CONTRIBUTING.md, AGENTS.md, AI_POLICY.md and the CI badge in README.md.
