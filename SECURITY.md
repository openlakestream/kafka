# Security policy

## Supported versions

We fix security issues in the latest release of Ursa for Apache Kafka (UFK). Fixes land on the default
branch and ship in the next release. Older releases don't receive patches, so upgrade to the latest
release to pick up a fix.

## Reporting a vulnerability

**Please don't report security problems in a public issue, pull request or discussion.**

Report them privately, in either of these ways:

1. **GitHub private vulnerability reporting.** Use the *Report a vulnerability* button on the
   [Security tab](https://github.com/openlakestream/kafka/security) of this repository. We prefer this
   channel: it's private, it keeps the conversation in one thread, and it stays attached to the
   repository.
2. **Email `security@openlakestream.org`**, with the repository name in the subject line.

As much as you have of the following helps us act quickly:

- the affected version, image tag or commit
- what an attacker could do, and under which configuration
- steps to reproduce the problem
- anything you already know about the impact

A rough report sent early is better than a polished one sent late.

### Vulnerabilities in Apache Kafka

Most of this repository is Apache Kafka. If a problem also affects an Apache Kafka release, report it
to the Apache Kafka security team, as described at <https://kafka.apache.org/project-security>. UFK
picks up the fix when it syncs with Apache Kafka. If you're not sure which project a problem belongs
to, report it to us, and we'll work it out with you.

## What happens next

We'll acknowledge your report, investigate it, and keep you updated as we go. We coordinate
disclosure with you. By default we aim to publish within 90 days of the report, and sooner once a fix
is available.

When the fix is released, we publish a security advisory in this repository and credit you in it,
unless you ask us not to.

We don't run a bug bounty program.

## Scope

This policy covers the code in this repository, except for problems that also affect Apache Kafka,
which go to the Apache Kafka security team as described above. Vulnerabilities in StreamNative's
commercial products or hosted services can also be reported to `security@streamnative.io`, and we'll
route them to the right team.
