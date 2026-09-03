<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements. See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# LIP-002: Ursa Zone-Aware Owner Selection

- *Author(s)*: Kai Wang
- *Proposal time*: 2026-03
- *Scope*: Diskless topics only
- *Released*: NO
- *Status*: Implemented
- *Repository*: https://github.com/lakestream-io/kafka
- *Discussion Link*: N/A

## TL;DR

This design adds zone-aware owner selection for diskless topics in this Kafka fork.
Clients can attach `zone_id=<zone>` inside `client.id`, and the broker prefers alive brokers whose `broker.rack` matches that zone. If no matching broker exists, the system falls back to the full alive-broker set. Kafka clients still see a normal leader, but that leader is a zone-aware pseudo leader computed from the request context.

## Background Knowledge

### Existing Diskless Path

The Kafka fork already supports diskless topics and routes `Produce`, `Fetch`, `ListOffsets`, and related request paths into Ursa through `DisklessStorageReplicaManagerSupport`.

The existing diskless path already provides:

1. Dedicated owner-selection logic for diskless topics.
2. Pseudo leaders in `Metadata` and `DescribeTopicPartitions`.
3. `NOT_LEADER_OR_FOLLOWER` on non-owner brokers.
4. `currentLeader` in error responses so clients can refresh metadata.

### Current Limitation

Before this change, diskless owner selection was still based on a globally stable hash over the alive-broker set. That means:

1. The server did not parse zone information from `client.id`.
2. Owner selection did not prefer brokers in the client's zone.
3. Missing zone information did not have a well-defined `no_zone` fallback.
4. Metadata and request handling were not zone-aware end to end.

### Desired Zone-Aware Behavior

The target behavior is straightforward:

1. Clients can include `zone_id=...` in `client.id`.
2. The broker extracts that zone from the request context.
3. Owner selection prefers alive brokers in the same zone.
4. If no in-zone broker is available, the system falls back to all alive brokers.

This document defines that behavior for the Kafka fork.

### Kafka Broker Topology Metadata

Kafka already exposes broker topology through `broker.rack`, and the client-facing `Node` metadata includes `rack`.

This makes `broker.rack` the natural source of zone topology for diskless owner selection. Reusing it avoids adding new broker-registration fields or a second Ursa-specific zone configuration.

## Motivation

### Current Limitations

Without zone-aware owner selection:

1. Diskless metadata returns the same pseudo leader for every client, regardless of location.
2. A client in `zone-a` may be routed to a broker in `zone-b` even when an in-zone broker is alive.
3. Request handling and client topology become less predictable in multi-zone deployments.
4. Failover semantics are not aligned with the intended zone-aware routing model.

### Why Zone-Aware Owner Selection

Zone-aware selection improves the diskless path in several ways:

1. It keeps routing closer to the client's zone when possible.
2. It reduces cross-zone traffic in steady-state operation.
3. It preserves graceful fallback when the requested zone is absent or unhealthy.
4. It keeps the protocol compatible with existing Kafka clients by continuing to expose a leader.

## Goals

### In Scope

1. Parse `zone_id=<zone>` from `client.id`.
2. Use `broker.rack` as the only broker-zone source.
3. Apply the same zone-aware owner-selection rule to `Metadata`, `DescribeTopicPartitions`, `Produce`, `Fetch`, and `ListOffsets`.
4. Define explicit fallback behavior when the client provides no zone or no alive broker matches the requested zone.
5. Keep classic-topic behavior unchanged.

### Out of Scope

1. Modifying the Kafka wire protocol.
2. Exposing protocol-level `NO_LEADER` partitions.
3. Introducing a separate control plane or zone registry.
4. Changing leader selection for classic topics.
5. Implementing external control-plane semantics such as shadow namespaces or load managers.

## High Level Design

The design keeps Kafka's leader-shaped client behavior but computes that leader from zone-aware diskless ownership rules.

For diskless topics:

1. The client sends `zone_id=<zone>` inside `client.id`.
2. The broker extracts the client zone.
3. The broker computes the owner from the alive-broker set, preferring brokers whose `Node.rack()` matches the client zone.
4. Metadata returns that broker as the pseudo leader.
5. Real request handling uses the same rule, so the broker that appears as leader is also the broker that accepts the request.

```
Client (client.id includes zone_id=...)
        |
        v
KafkaApis / ReplicaManager
        |
        v
Diskless zone-aware selector
        |
        +--> in-zone alive brokers, if any
        |
        +--> otherwise all alive brokers
        |
        v
Pseudo leader / owner
        |
        +--> Metadata / DescribeTopicPartitions
        +--> Produce / Fetch / ListOffsets
```

### Key Design Decisions

1. Keep a pseudo leader instead of exposing a protocol-level leaderless model.
2. Use `broker.rack` as the only broker-zone source.
3. Parse `zone_id=` from `client.id` rather than introducing a new client-side protocol field.
4. Fall back to all alive brokers when the client zone is missing or unmatched.
5. Use one selector implementation for both metadata shaping and request admission.

## Detailed Design

### Broker Zone Source

The broker zone comes only from `broker.rack`.

That means:

1. If `broker.rack` is configured, it is used as the broker zone.
2. If `broker.rack` is not configured, the broker is treated as having no zone.

This preserves existing Kafka semantics:

1. Broker metadata already propagates `rack`.
2. `Node.rack()` already exposes that value to clients and internal selection logic.
3. No extra Ursa-specific zone configuration is needed.

### Client Zone Extraction

The client zone is parsed from `client.id` using the following syntax:

`zone_id=<zone>`

It can appear anywhere in `client.id` and ends at the next comma. Valid examples include:

1. `zone_id=az1`
2. `producer-a,zone_id=az1`
3. `role=consumer, zone_id=az1`
4. `zone_id=az1,tenant=t1`

If no valid match is found, the client zone is treated as `null`.

### Owner Selection Algorithm

For a diskless partition:

1. Get the alive brokers for the current listener.
2. Extract the client zone from `client.id`.
3. If the client zone is present, filter the alive brokers to those where `Node.rack == clientZone`.
4. If the filtered subset is not empty, use that subset.
5. Otherwise, fall back to the full alive-broker set.
6. Compute the owner using a stable hash:

`murmur2(topicId + "-" + partitionIndex)`

7. Map the hash onto the candidate broker list sorted by broker ID.

This gives the following properties:

1. Stable results for the same topic ID, partition, broker set, and client zone.
2. Different client zones may resolve to different leaders.
3. Zone-local routing when an in-zone broker exists.
4. Automatic fallback when a zone is absent or unhealthy.

### Metadata and DescribeTopicPartitions

For diskless topics:

1. `leaderId` is computed using the zone-aware selector.
2. `replicaNodes` contains only the pseudo leader.
3. `isrNodes` contains only the pseudo leader.
4. `offlineReplicas` remains empty.
5. `leaderEpoch` keeps the current diskless convention.

For classic topics:

1. No behavior changes.

### Produce, Fetch, and ListOffsets

For diskless topics:

1. The broker parses `client.id`.
2. The broker computes ownership using the same selector used by metadata.
3. The owner broker handles the request.
4. A non-owner broker returns `NOT_LEADER_OR_FOLLOWER`.
5. If supported by the request version, the response includes `currentLeader` computed by the same rule.

This applies to:

1. `Produce`
2. `Fetch`
3. `ListOffsets`

### Failure and Fallback Semantics

When `client.id` does not contain `zone_id=`:

1. The request is not rejected.
2. The request does not fail because the zone is missing.
3. The owner is selected from all alive brokers.

When `client.id` contains an unknown zone:

1. The request is not rejected.
2. The request does not fail because the zone is unmatched.
3. The selector falls back to all alive brokers.

When all brokers in a zone become unavailable:

1. Metadata recalculates the pseudo leader from the global alive-broker set.
2. `currentLeader` returned from non-owner brokers points to the fallback owner.
3. Existing client retry and metadata refresh behavior handles failover.

### Consistency Requirements

The following invariants must hold:

1. The pseudo leader returned in `Metadata` must match the broker that actually accepts `Produce`, `Fetch`, and `ListOffsets`.
2. `currentLeader` in an error response must match the leader returned by metadata for the same `client.id`.
3. Zone-aware behavior must apply only to diskless topics.
4. The system must not return leader A in metadata while only leader B accepts writes.

### Testing

Test coverage includes:

1. `client.id` parsing:
   - plain `zone_id=...`
   - mixed key-value strings
   - empty values
   - no match
   - false-positive-like strings

2. Broker-zone handling:
   - `broker.rack` configured
   - `broker.rack` not configured

3. Selector behavior:
   - matching in-zone broker subset
   - fallback when zone is missing
   - fallback when zone is unknown
   - stable results for identical inputs

4. Metadata behavior:
   - diskless topics returning different leaders for different zones
   - classic topics remaining unchanged

5. Request-path behavior:
   - owner broker success for `Produce`, `Fetch`, and `ListOffsets`
   - `NOT_LEADER_OR_FOLLOWER` on non-owner brokers
   - correct `currentLeader`

6. Multi-zone integration behavior:
   - cross-zone bootstrap with zone-correct routing
   - multiple producers and consumers using different zones
   - expected partition-log state only on the relevant zone owners

7. Failover behavior:
   - automatic fallback when in-zone brokers are unavailable
   - consistent metadata and error-response routing

## Security Considerations

### Authentication and Trust

The zone is derived from `client.id`, which is client-supplied metadata. It should be treated as a routing hint, not as a security boundary.

This means:

1. It should not be used for authorization.
2. It should not be used to infer tenant identity.
3. It is safe only as a best-effort routing signal.

### Operational Safety

Because the selector falls back to all alive brokers when a zone is missing or unmatched, an invalid `client.id` zone does not make the partition unavailable. The failure mode is degraded locality, not request unavailability.

## Backward and Forward Compatibility

### Client Compatibility

This change is transparent to existing clients.

Clients that want zone-aware routing only need to include:

`zone_id=<zone>`

inside `client.id`.

Clients that do not set it continue to work and fall back to the global alive-broker set.

### Broker Metadata Compatibility

Broker metadata continues to expose `broker.rack` through the standard `rack` field.

This preserves Kafka's existing topology model and avoids adding new metadata fields or registration changes.

### Revert

The behavior can be reverted by:

1. Removing the zone-aware selector logic.
2. Returning to global alive-broker hashing for diskless topics.
3. Keeping `client.id` parsing unused.

### Upgrade

The feature is additive:

1. Existing clients continue to work without changes.
2. Existing clusters that already set `broker.rack` can immediately participate in zone-aware routing.
3. Clusters without `broker.rack` continue to work with global fallback semantics.

## Alternatives

### Alternative 1: Add a Dedicated Ursa Zone Configuration

One alternative is to introduce a separate broker property such as `ursa.storage.zone`.

Why this was not chosen:

1. It duplicates existing topology metadata already available in Kafka.
2. It introduces precedence rules between `broker.rack` and the new field.
3. It increases configuration and operational complexity.

### Alternative 2: Expose a Protocol-Level Leaderless Model

Another alternative is to make diskless topics appear leaderless at the Kafka protocol layer.

Why this was not chosen:

1. Existing Kafka clients fundamentally depend on leader semantics.
2. It would require deeper protocol and compatibility work.
3. It is much riskier than preserving the pseudo leader model.

### Alternative 3: Ignore Client Zone Entirely

Another alternative is to keep the current global hash for all clients.

Why this was not chosen:

1. It leaves cross-zone routing unoptimized.
2. It provides worse locality in multi-zone deployments.

## General Notes

### Why the Pseudo Leader Model Remains

Kafka clients still need a leader-shaped view of the world for routing, retry, and metadata refresh behavior. The pseudo leader keeps compatibility while allowing diskless ownership to remain dynamic and zone-aware.

### Why `broker.rack` Is the Only Topology Source

Using `broker.rack` keeps the design boring and predictable:

1. It matches Kafka's existing topology semantics.
2. It reuses existing broker metadata propagation.
3. It avoids configuration drift between two different zone definitions.
