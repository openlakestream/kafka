#
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
# Local dev/test-only compactor image for Oslo E2E tests.
#
# Build context should be the ursa-storage repo root (so the `COPY .../target/*.jar` paths resolve).
#
# Example:
#   docker build -t ursa-compact:ue-s3-e2e -f docker/ursa-compact-ue.Dockerfile PATH-TO-ursa-storage-repo
#
FROM snstage/pulsar-cloud-ue:4.1.0-SNAPSHOT-bom

USER root

# Cleanup the existing package in the image
RUN rm -rf /pulsar/lib/ursa-storage-*.jar && \
    rm -rf /pulsar/bin/compact && \
    rm -rf /pulsar/conf/ursa_storage.conf

# Copy the pulsar storage package into the image
COPY ursa-storage-core/target/*.jar /pulsar/lib
COPY ursa-storage-ml/target/*.jar /pulsar/lib
COPY ursa-storage-lakehouse/target/*.jar /pulsar/lib
COPY ursa-storage-compact/target/*.jar /pulsar/lib
COPY ursa-storage-common/target/*.jar /pulsar/lib
COPY ursa-storage-pulsar/target/ursa-storage-pulsar-*.jar /pulsar/offloaders/ursa-stream-tieredstorage.nar
COPY ursa-storage-pulsar-ml/target/*.jar /pulsar/lib
COPY bin/compact /pulsar/bin
RUN chmod ug+rw /pulsar/bin/compact
COPY conf/ursa_storage.conf /pulsar/conf
RUN chmod ug+rw /pulsar/conf/ursa_storage.conf

USER 10000
