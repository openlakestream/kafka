#!/bin/bash
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
# Build Docker image for Kafka with Diskless/Ursa Storage
#
# Usage:
#   ./build-image.sh [--platform <platform>] [IMAGE_NAME:TAG]
#
# Examples:
#   ./build-image.sh                          # Builds lakestream/kafka:latest
#   ./build-image.sh myrepo/kafka-ursa:v1     # Builds with custom name
#   ./build-image.sh --amd64                  # Builds linux/amd64 image (x86_64)
#   ./build-image.sh --platform linux/amd64   # Same as --amd64
#
# Environment:
#   GRADLE_ARGS   Extra arguments for the release build, e.g. GRADLE_ARGS=--offline
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../../../../.." && pwd)"
DOCKER_DIR="${PROJECT_ROOT}/docker"

usage() {
    cat <<'EOF'
Usage:
  ./build-image.sh [--platform <platform>] [IMAGE_NAME:TAG]

Options:
  --platform <platform>  Target platform (e.g. linux/amd64, linux/arm64)
  --amd64                Alias for --platform linux/amd64 (x86_64)
  -h, --help              Show this help

Environment:
  GRADLE_ARGS             Extra arguments for the release build (default: empty),
                          for example GRADLE_ARGS=--offline

Examples:
  ./build-image.sh
  ./build-image.sh myrepo/kafka:latest
  ./build-image.sh --amd64
  ./build-image.sh --platform linux/amd64 myrepo/kafka:amd64
EOF
}

PLATFORM=""
IMAGE_NAME="${IMAGE:-lakestream/kafka:latest}"
IMAGE_NAME_SET="false"

while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)
            usage
            exit 0
            ;;
        --platform)
            if [[ $# -lt 2 ]]; then
                echo "ERROR: --platform requires a value" >&2
                usage >&2
                exit 1
            fi
            PLATFORM="$2"
            shift 2
            ;;
        --amd64|--x86_64)
            PLATFORM="linux/amd64"
            shift
            ;;
        --*)
            echo "ERROR: Unknown option: $1" >&2
            usage >&2
            exit 1
            ;;
        *)
            if [[ "${IMAGE_NAME_SET}" == "true" ]]; then
                echo "ERROR: Unexpected argument: $1" >&2
                usage >&2
                exit 1
            fi
            IMAGE_NAME="$1"
            IMAGE_NAME_SET="true"
            shift
            ;;
    esac
done

echo "=============================================="
echo "Building Kafka Diskless Storage Docker Image"
echo "=============================================="
echo "Project root: ${PROJECT_ROOT}"
echo "Image name: ${IMAGE_NAME}"
if [[ -n "${PLATFORM}" ]]; then
    echo "Platform: ${PLATFORM}"
fi
echo ""

cd "${PROJECT_ROOT}"

echo "[1/3] Building Kafka release tarball..."
GRADLE_BUILD_ARGS=(./gradlew releaseTarGz --no-daemon -q)
if [[ -n "${GRADLE_ARGS:-}" ]]; then
    # Word splitting is intended: GRADLE_ARGS may carry several flags.
    # shellcheck disable=SC2206
    GRADLE_BUILD_ARGS+=(${GRADLE_ARGS})
fi
# The tarball's doc generators each run in a forked JVM whose classpath carries
# slf4j-api without a binding, so every one of them prints the same three-line
# "StaticLoggerBinder" warning on stderr. Gradle's -q silences its own output but
# not a child JVM's, so filter out just those lines. stdout goes straight through
# on fd 3; the rest of stderr, including real build failures, is left intact.
exec 3>&1
set +e
"${GRADLE_BUILD_ARGS[@]}" 2>&1 1>&3 | grep -v '^SLF4J: ' >&2
gradle_status=${PIPESTATUS[0]}
set -e
exec 3>&-
if [[ ${gradle_status} -ne 0 ]]; then
    exit "${gradle_status}"
fi

TARBALL=$(find "${PROJECT_ROOT}/core/build/distributions" -name "kafka_2.13-*.tgz" | head -1)
if [ -z "$TARBALL" ]; then
    echo "ERROR: Could not find kafka tarball in core/build/distributions/"
    exit 1
fi
echo "Found tarball: ${TARBALL}"

echo ""
echo "[2/3] Preparing build context..."
BUILD_CONTEXT=$(mktemp -d)
trap "rm -rf ${BUILD_CONTEXT}" EXIT

cp "${TARBALL}" "${BUILD_CONTEXT}/kafka.tgz"
cp "${SCRIPT_DIR}/Dockerfile" "${BUILD_CONTEXT}/Dockerfile"
cp -r "${DOCKER_DIR}/resources" "${BUILD_CONTEXT}/resources"
cp -r "${DOCKER_DIR}/jvm" "${BUILD_CONTEXT}/jvm"
cp "${DOCKER_DIR}/server.properties" "${BUILD_CONTEXT}/server.properties"

echo ""
echo "[3/3] Building Docker image..."
DOCKER_BUILD_ARGS=(
    docker build
    -f "${BUILD_CONTEXT}/Dockerfile"
    -t "${IMAGE_NAME}"
    --build-arg "build_date=$(date +%Y-%m-%d)"
)
if [[ -n "${PLATFORM}" ]]; then
    DOCKER_BUILD_ARGS+=(--platform "${PLATFORM}")
fi
DOCKER_BUILD_ARGS+=("${BUILD_CONTEXT}")
"${DOCKER_BUILD_ARGS[@]}"

echo ""
echo "=============================================="
echo "SUCCESS: Docker image built"
echo "Image: ${IMAGE_NAME}"
echo ""
echo "To run the diskless cluster:"
echo "  cd ${SCRIPT_DIR}"
echo "  IMAGE=${IMAGE_NAME} docker compose up -d"
echo "=============================================="
