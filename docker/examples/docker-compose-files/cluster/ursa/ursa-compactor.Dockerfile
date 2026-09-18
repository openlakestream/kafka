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
#
# Standalone Ursa compactor image. Resolves org.openlakestream:ursa-storage-compact
# and its runtime dependencies from Maven Central; no ursa-storage checkout needed.
#
#   docker build --build-arg URSA_STORAGE_VERSION=1.0.0 -f ursa-compactor.Dockerfile .
FROM maven:3.9-eclipse-temurin-17 AS deps
ARG URSA_STORAGE_VERSION=1.0.0
WORKDIR /build
RUN printf '%s\n' \
      '<project xmlns="http://maven.apache.org/POM/4.0.0">' \
      '  <modelVersion>4.0.0</modelVersion>' \
      '  <groupId>local</groupId><artifactId>compactor-deps</artifactId><version>0</version>' \
      '  <dependencies><dependency>' \
      '    <groupId>org.openlakestream</groupId>' \
      '    <artifactId>ursa-storage-compact</artifactId>' \
      "    <version>${URSA_STORAGE_VERSION}</version>" \
      '  </dependency></dependencies>' \
      '</project>' > pom.xml \
    && mvn -B -ntp dependency:copy-dependencies -DincludeScope=runtime -DoutputDirectory=/opt/ursa/lib

FROM eclipse-temurin:17-jre

USER root
WORKDIR /opt/ursa

COPY --from=deps /opt/ursa/lib/ /opt/ursa/lib/

RUN useradd --uid 10000 --gid 0 --home-dir /opt/ursa --no-create-home \
      --shell /usr/sbin/nologin ursa \
    && chown -R 10000:0 /opt/ursa

ENV HOME=/opt/ursa
USER 10000:0

ENTRYPOINT ["java", "-cp", "/opt/ursa/lib/*", "io.lakestream.ursa.compact.CompactionMain"]
