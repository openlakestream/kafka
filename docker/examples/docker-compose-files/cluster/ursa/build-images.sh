#!/usr/bin/env bash
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
# Build both local project images for the compose stack:
#   - the Kafka broker image (skip with SKIP_KAFKA_BUILD=true)
#   - the standalone Ursa compactor image (from Maven Central)
#
# Environment:
#   URSA_STORAGE_VERSION  ursa-storage Maven version for the compactor (default: 1.0.0)
#   IMAGE              Kafka image name (default: lakestream/kafka:latest)
#   COMPACTOR_IMAGE    Compactor image name (default: lakestream/compactor:latest)
#   GRADLE_ARGS        Extra Gradle arguments, e.g. --offline

set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
kafka_image="${IMAGE:-lakestream/kafka:latest}"
compactor_image="${COMPACTOR_IMAGE:-lakestream/compactor:latest}"

if [[ "${SKIP_KAFKA_BUILD:-false}" != "true" ]]; then
  "$here/build-image.sh" "$kafka_image"
fi

docker build \
  -t "$compactor_image" \
  --build-arg "URSA_STORAGE_VERSION=${URSA_STORAGE_VERSION:-1.0.0}" \
  -f "$here/ursa-compactor.Dockerfile" \
  "$here"

echo "Built $kafka_image and $compactor_image."
