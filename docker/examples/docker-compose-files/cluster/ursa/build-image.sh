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
#   ./build-image.sh [--tarball <kafka_2.13-*.tgz>] [--platform <platform>] [--push] [--tag <name:tag>]... [IMAGE_NAME:TAG]
#
# Examples:
#   ./build-image.sh                          # Builds lakestream/kafka:latest
#   ./build-image.sh --tarball core/build/distributions/kafka_2.13-4.3.1.1.tgz
#                                             # Reuse an existing release tarball
#   ./build-image.sh myrepo/kafka-ursa:v1     # Builds with custom name
#   ./build-image.sh --amd64                  # Builds linux/amd64 image (x86_64)
#   ./build-image.sh --platform linux/amd64   # Same as --amd64
#   ./build-image.sh --push --platform linux/amd64,linux/arm64 lakestream/kafka:4.3.1.1
#                                             # Multi-arch build via buildx, pushed to the registry
#   ./build-image.sh --tag lakestream/kafka:latest lakestream/kafka:4.3.1.1
#                                             # Same image under two tags
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
  ./build-image.sh [--tarball <kafka_2.13-*.tgz>] [--platform <platform>] [--push] [--tag <name:tag>]... [IMAGE_NAME:TAG]

Options:
  --tarball <path>       Use this release tarball instead of running ./gradlew releaseTarGz
  --platform <platform>  Target platform (e.g. linux/amd64, linux/arm64)
  --amd64                Alias for --platform linux/amd64 (x86_64)
  --push                 Build with buildx and push (multi-platform ok, image is not loaded locally)
  --tag <name:tag>       Also tag the image with this name (repeatable), e.g. --tag lakestream/kafka:latest
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

TARBALL=""
PLATFORM=""
PUSH="false"
EXTRA_TAGS=()
IMAGE_NAME="${IMAGE:-lakestream/kafka:latest}"
IMAGE_NAME_SET="false"

while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)
            usage
            exit 0
            ;;
        --tarball)
            if [[ $# -lt 2 ]]; then
                echo "ERROR: --tarball requires a value" >&2
                usage >&2
                exit 1
            fi
            # Absolute, since the script cd's to the project root before using it.
            if [[ "$2" == /* ]]; then TARBALL="$2"; else TARBALL="${PWD}/$2"; fi
            shift 2
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
        --push)
            PUSH="true"
            shift
            ;;
        --tag)
            if [[ $# -lt 2 ]]; then
                echo "ERROR: --tag requires a value" >&2
                usage >&2
                exit 1
            fi
            EXTRA_TAGS+=("$2")
            shift 2
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
if [[ ${#EXTRA_TAGS[@]} -gt 0 ]]; then
    echo "Extra tags: ${EXTRA_TAGS[*]}"
fi
if [[ -n "${PLATFORM}" ]]; then
    echo "Platform: ${PLATFORM}"
fi
echo ""

cd "${PROJECT_ROOT}"

if [[ -z "${TARBALL}" ]]; then
    echo "[1/3] Building Kafka release tarball..."
    TARBALL=$("${DOCKER_DIR}/release-tarball.sh")
else
    echo "[1/3] Using provided Kafka release tarball..."
    if [[ ! -f "${TARBALL}" ]]; then
        echo "ERROR: Tarball not found: ${TARBALL}" >&2
        exit 1
    fi
fi
echo "Tarball: ${TARBALL}"

echo ""
echo "[2/3] Preparing build context..."
BUILD_CONTEXT=$(mktemp -d)
trap "rm -rf ${BUILD_CONTEXT}" EXIT

cp "${TARBALL}" "${BUILD_CONTEXT}/kafka.tgz"
cp "${SCRIPT_DIR}/Dockerfile" "${BUILD_CONTEXT}/Dockerfile"
cp -r "${DOCKER_DIR}/resources" "${BUILD_CONTEXT}/resources"
cp -r "${DOCKER_DIR}/jvm" "${BUILD_CONTEXT}/jvm"
cp "${DOCKER_DIR}/server.properties" "${BUILD_CONTEXT}/server.properties"
cp "${SCRIPT_DIR}/ursa-compactor.sh" "${BUILD_CONTEXT}/ursa-compactor.sh"

# The bundled compactor must match the ursa-storage runtime shipped in the tarball.
URSA_STORAGE_VERSION=$(tar tzf "${TARBALL}" | sed -n 's#.*/ursa-storage/ursa-storage-core-\(.*\)\.jar$#\1#p' | head -1)
if [ -z "${URSA_STORAGE_VERSION}" ]; then
    echo "ERROR: Could not detect ursa-storage version from ${TARBALL}"
    exit 1
fi
echo "Ursa storage version: ${URSA_STORAGE_VERSION}"

echo ""
echo "[3/3] Building Docker image..."
DOCKER_BUILD_ARGS=(docker build)
if [[ "${PUSH}" == "true" ]]; then
    DOCKER_BUILD_ARGS=(docker buildx build --push)
fi
DOCKER_BUILD_ARGS+=(
    -f "${BUILD_CONTEXT}/Dockerfile"
    -t "${IMAGE_NAME}"
    --build-arg "build_date=$(date +%Y-%m-%d)"
    --build-arg "URSA_STORAGE_VERSION=${URSA_STORAGE_VERSION}"
)
for tag in ${EXTRA_TAGS[@]+"${EXTRA_TAGS[@]}"}; do
    DOCKER_BUILD_ARGS+=(-t "${tag}")
done
if [[ -n "${PLATFORM}" ]]; then
    DOCKER_BUILD_ARGS+=(--platform "${PLATFORM}")
fi
DOCKER_BUILD_ARGS+=("${BUILD_CONTEXT}")
"${DOCKER_BUILD_ARGS[@]}"

echo ""
echo "=============================================="
echo "SUCCESS: Docker image built"
echo "Image: ${IMAGE_NAME}"
for tag in ${EXTRA_TAGS[@]+"${EXTRA_TAGS[@]}"}; do
    echo "Also tagged: ${tag}"
done
echo ""
echo "To run the diskless cluster:"
echo "  cd ${SCRIPT_DIR}"
echo "  IMAGE=${IMAGE_NAME} docker compose up -d"
echo "=============================================="
