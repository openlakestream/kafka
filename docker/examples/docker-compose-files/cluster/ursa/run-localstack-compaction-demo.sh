#!/usr/bin/env bash
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

set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

base="$here/docker-compose-localstack-compaction.yml"
demo="$here/docker-compose-localstack-compaction.demo.yml"

cleanup() {
  docker compose -f "$base" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker compose -f "$base" up -d

echo "Running schema (Avro) produce/consume demo..."
docker compose -f "$base" -f "$demo" run --rm avro-consumer

echo "Demo finished successfully."
