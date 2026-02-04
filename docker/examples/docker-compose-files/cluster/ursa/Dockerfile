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

FROM eclipse-temurin:21-jre-alpine

EXPOSE 9092

USER root

ARG build_date

LABEL org.label-schema.name="kafka-diskless" \
      org.label-schema.description="Apache Kafka with Diskless/Ursa Storage" \
      org.label-schema.build-date="${build_date}" \
      org.label-schema.vcs-url="https://github.com/streamnative/ursa-for-kafka"

COPY kafka.tgz kafka.tgz

RUN mkdir -p /opt/kafka && \
    set -eux && \
    apk update && \
    apk upgrade && \
    apk add --no-cache bash gcompat && \
    tar xfz kafka.tgz -C /opt/kafka --strip-components 1 && \
    rm kafka.tgz && \
    mkdir -p /var/lib/kafka/data /etc/kafka/secrets && \
    mkdir -p /etc/kafka/docker /usr/logs /mnt/shared/config && \
    adduser -h /home/appuser -D --shell /bin/bash appuser && \
    chown appuser:appuser -R /usr/logs /opt/kafka /mnt/shared/config && \
    chown appuser:root -R /var/lib/kafka /etc/kafka/secrets /etc/kafka && \
    chmod -R ug+w /etc/kafka /var/lib/kafka /etc/kafka/secrets && \
    cp /opt/kafka/config/log4j2.yaml /etc/kafka/docker/log4j2.yaml && \
    cp /opt/kafka/config/tools-log4j2.yaml /etc/kafka/docker/tools-log4j2.yaml

COPY --chown=appuser:appuser resources/common-scripts /etc/kafka/docker
COPY --chown=appuser:appuser jvm/launch /etc/kafka/docker/launch
COPY server.properties /etc/kafka/docker/server.properties

USER appuser

VOLUME ["/etc/kafka/secrets", "/var/lib/kafka/data", "/mnt/shared/config"]

CMD ["/etc/kafka/docker/run"]
