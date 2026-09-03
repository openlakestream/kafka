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
#   - the standalone Ursa compactor image (skip with SKIP_URSA_BUILD=true)
#
# Environment:
#   URSA_STORAGE_DIR   Local ursa-storage checkout (required, or pass as $1)
#   IMAGE              Kafka image name (default: lakestream/kafka:latest)
#   COMPACTOR_IMAGE    Compactor image name (default: lakestream/compactor:latest)
#   GRADLE_ARGS        Extra Gradle arguments, e.g. --offline
#   MAVEN_ARGS         Extra Maven arguments, e.g. -o

set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ursa_storage_dir="${URSA_STORAGE_DIR:-${1:-}}"
kafka_image="${IMAGE:-lakestream/kafka:latest}"
compactor_image="${COMPACTOR_IMAGE:-lakestream/compactor:latest}"

if [[ -z "$ursa_storage_dir" || ! -f "$ursa_storage_dir/pom.xml" ]]; then
  cat >&2 <<'EOF'
Set URSA_STORAGE_DIR to a local ursa-storage checkout (or pass it as the
first argument), for example:

  URSA_STORAGE_DIR=/path/to/ursa-storage ./build-images.sh
EOF
  exit 2
fi

if [[ "${SKIP_KAFKA_BUILD:-false}" != "true" ]]; then
  "$here/build-image.sh" "$kafka_image"
fi

if [[ "${SKIP_URSA_BUILD:-false}" != "true" ]]; then
  maven_extra_args=()
  if [[ -n "${MAVEN_ARGS:-}" ]]; then
    # Word splitting is intended: MAVEN_ARGS may carry several flags.
    # shellcheck disable=SC2206
    maven_extra_args=(${MAVEN_ARGS})
  fi
  (
    cd "$ursa_storage_dir"
    mvn -B -ntp "${maven_extra_args[@]+"${maven_extra_args[@]}"}" \
      -Dmaven.gitcommitid.skip=true \
      -pl ursa-storage-compact -am \
      -DskipTests clean package
  )
fi

docker build \
  -t "$compactor_image" \
  -f "$here/ursa-compactor.Dockerfile" \
  "$ursa_storage_dir"

echo "Built $kafka_image and $compactor_image."
