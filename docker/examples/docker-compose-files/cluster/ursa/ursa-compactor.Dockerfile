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
# Local development image for the standalone Ursa compactor Maven package.
# Build context must be the ursa-storage repository root after running:
#
#   mvn -B -ntp -pl ursa-storage-compact -am -DskipTests clean package
#
# The package contract is a versioned compact jar plus its runtime dependency
# directory at ursa-storage-compact/target/lib/.
FROM eclipse-temurin:17-jre

USER root
WORKDIR /opt/ursa

COPY ursa-storage-compact/target/ursa-storage-compact-*.jar /opt/ursa/ursa-storage-compact.jar
COPY ursa-storage-compact/target/lib/ /opt/ursa/lib/

RUN useradd --uid 10000 --gid 0 --home-dir /opt/ursa --no-create-home \
      --shell /usr/sbin/nologin ursa \
    && chown -R 10000:0 /opt/ursa

ENV HOME=/opt/ursa
USER 10000:0

ENTRYPOINT ["java", "-cp", "/opt/ursa/ursa-storage-compact.jar:/opt/ursa/lib/*", "io.lakestream.ursa.compact.CompactionMain"]
