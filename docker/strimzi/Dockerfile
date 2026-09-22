# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Strimzi-compatible image for Kafka with Diskless/Ursa Storage.
#
# It starts from the official Strimzi Kafka image of the same Kafka minor and
# swaps the bundled Apache Kafka distribution for this fork's release tarball.
# Everything Strimzi layers on top of Kafka stays in place: kafka_run.sh and
# the other operator entrypoints, kafka-agent and tracing-agent, Strimzi's
# third-party libs, Cruise Control and the exporters. The Strimzi cluster
# operator can therefore run it in place of quay.io/strimzi/kafka through
# `spec.kafka.image`, with `spec.kafka.version` set to STRIMZI_KAFKA_VERSION.
#
# Jars are merged by artifact name: every jar in Strimzi's /opt/kafka/libs whose
# name-without-version also appears in the tarball's libs/ is dropped and the
# tarball's copy wins, so the classpath never carries two versions of one
# artifact. Strimzi-only jars (the agents, OpenTelemetry, gRPC, ...) are kept.
# The isolated ./ursa-storage/ runtime is copied as-is; the default
# `ursa.storage.class.path` resolves it through KAFKA_HOME=/opt/kafka.

ARG STRIMZI_VERSION=1.2.0
ARG STRIMZI_KAFKA_VERSION=4.3.1
ARG STRIMZI_BASE_IMAGE=quay.io/strimzi/kafka:${STRIMZI_VERSION}-kafka-${STRIMZI_KAFKA_VERSION}

FROM ${STRIMZI_BASE_IMAGE} AS merge

USER root

COPY kafka.tgz /tmp/kafka.tgz

RUN set -eux; \
    mkdir /tmp/dist; \
    tar xzf /tmp/kafka.tgz -C /tmp/dist --strip-components 1; \
    rm /tmp/kafka.tgz; \
    for jar in /tmp/dist/libs/*.jar; do \
        key="$(basename "${jar}" | sed -E 's/-[0-9][0-9A-Za-z.+_-]*\.jar$//')"; \
        rm -f "${KAFKA_HOME}/libs/${key}"-[0-9]*.jar; \
    done; \
    rm -rf "${KAFKA_HOME}/bin" "${KAFKA_HOME}/config" "${KAFKA_HOME}/licenses" \
           "${KAFKA_HOME}/site-docs" "${KAFKA_HOME}/LICENSE" "${KAFKA_HOME}/NOTICE" \
           "${KAFKA_HOME}/ursa-storage"; \
    cp -a /tmp/dist/. "${KAFKA_HOME}/"; \
    rm -rf /tmp/dist; \
    chown -R 1001:0 "${KAFKA_HOME}"

FROM ${STRIMZI_BASE_IMAGE}

ARG STRIMZI_VERSION
ARG KAFKA_VERSION
ARG build_date

LABEL org.label-schema.name="lakestream/kafka-strimzi" \
      org.label-schema.description="Apache Kafka ${KAFKA_VERSION} with Diskless/Ursa Storage, Strimzi ${STRIMZI_VERSION} image layout" \
      org.label-schema.build-date="${build_date}" \
      org.label-schema.vcs-url="https://github.com/lakestream-io/kafka" \
      org.opencontainers.image.source="https://github.com/lakestream-io/kafka"

ENV KAFKA_VERSION=${KAFKA_VERSION}

USER root

# Replace the whole distribution rather than layering on top of it, so jars the
# merge stage removed do not survive in the base layers' view of /opt/kafka.
RUN rm -rf "${KAFKA_HOME}"
COPY --from=merge --chown=1001:0 /opt/kafka /opt/kafka

WORKDIR /opt/kafka

USER 1001
