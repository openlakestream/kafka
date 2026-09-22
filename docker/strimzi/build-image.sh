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
# Build the Strimzi-compatible image for Kafka with Diskless/Ursa Storage.
#
# Usage:
#   ./build-image.sh [--tarball <kafka_2.13-*.tgz>] [--platform <platform>] [--push] [--tag <name:tag>]... [IMAGE_NAME:TAG]
#
# Examples:
#   ./build-image.sh                                   # Builds the tarball, then lakestream/kafka-strimzi:1.2.0-kafka-<version>
#   ./build-image.sh --tarball core/build/distributions/kafka_2.13-4.3.1.1.tgz
#   ./build-image.sh --push --platform linux/amd64,linux/arm64
#   ./build-image.sh --tag lakestream/kafka-strimzi:latest   # Same image under a second tag
#   STRIMZI_VERSION=1.3.0 STRIMZI_KAFKA_VERSION=4.3.2 ./build-image.sh
#
# Environment:
#   STRIMZI_VERSION        Strimzi release whose Kafka image is the base (default: 1.2.0)
#   STRIMZI_KAFKA_VERSION  Apache Kafka version of that base image. It must be a
#                          version this Strimzi release ships and share the
#                          Kafka minor with the tarball (default: 4.3.1)
#   GRADLE_ARGS            Extra arguments for the release build, e.g. GRADLE_ARGS=--offline
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

STRIMZI_VERSION="${STRIMZI_VERSION:-1.2.0}"
STRIMZI_KAFKA_VERSION="${STRIMZI_KAFKA_VERSION:-4.3.1}"

usage() {
    cat <<'EOF'
Usage:
  ./build-image.sh [--tarball <kafka_2.13-*.tgz>] [--platform <platform>] [--push] [--tag <name:tag>]... [IMAGE_NAME:TAG]

Options:
  --tarball <path>       Use this release tarball instead of running ./gradlew releaseTarGz
  --platform <platform>  Target platform (e.g. linux/amd64, linux/arm64)
  --push                 Build with buildx and push (multi-platform ok, image is not loaded locally)
  --tag <name:tag>       Also tag the image with this name (repeatable), e.g. --tag lakestream/kafka-strimzi:latest
  -h, --help             Show this help

Environment:
  STRIMZI_VERSION        Strimzi release whose Kafka image is the base (default: 1.2.0)
  STRIMZI_KAFKA_VERSION  Apache Kafka version of that base image (default: 4.3.1)
  GRADLE_ARGS            Extra arguments for the release build (default: empty)

The image name defaults to lakestream/kafka-strimzi:<STRIMZI_VERSION>-kafka-<tarball version>.
EOF
}

TARBALL=""
PLATFORM=""
PUSH="false"
EXTRA_TAGS=()
IMAGE_NAME=""

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
            TARBALL="$2"
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
            if [[ -n "${IMAGE_NAME}" ]]; then
                echo "ERROR: Unexpected argument: $1" >&2
                usage >&2
                exit 1
            fi
            IMAGE_NAME="$1"
            shift
            ;;
    esac
done

echo "=============================================="
echo "Building Kafka Diskless Storage Strimzi Image"
echo "=============================================="
echo "Strimzi version: ${STRIMZI_VERSION}"
echo "Strimzi base Kafka version: ${STRIMZI_KAFKA_VERSION}"
if [[ -n "${PLATFORM}" ]]; then
    echo "Platform: ${PLATFORM}"
fi
echo ""

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

TARBALL_NAME="$(basename "${TARBALL}")"
KAFKA_VERSION="${TARBALL_NAME#kafka_2.13-}"
KAFKA_VERSION="${KAFKA_VERSION%.tgz}"
if [[ "${KAFKA_VERSION}" == "${TARBALL_NAME}" || -z "${KAFKA_VERSION}" ]]; then
    echo "ERROR: Cannot derive the Kafka version from ${TARBALL_NAME}; expected kafka_2.13-<version>.tgz" >&2
    exit 1
fi
echo "Kafka version: ${KAFKA_VERSION}"

# Strimzi's scripts, kafka-agent and third-party libs are built for one Kafka
# minor, and the operator only knows the versions its release ships. Refuse a
# base image from a different minor instead of producing an image the operator
# cannot run.
kafka_minor="$(echo "${KAFKA_VERSION}" | cut -d. -f1,2)"
strimzi_kafka_minor="$(echo "${STRIMZI_KAFKA_VERSION}" | cut -d. -f1,2)"
if [[ "${kafka_minor}" != "${strimzi_kafka_minor}" ]]; then
    echo "ERROR: Tarball is Kafka ${KAFKA_VERSION} but the Strimzi base image is Kafka ${STRIMZI_KAFKA_VERSION}." >&2
    echo "       Set STRIMZI_VERSION/STRIMZI_KAFKA_VERSION to a Strimzi release that ships Kafka ${kafka_minor}.x." >&2
    exit 1
fi

if [[ -z "${IMAGE_NAME}" ]]; then
    IMAGE_NAME="lakestream/kafka-strimzi:${STRIMZI_VERSION}-kafka-${KAFKA_VERSION}"
fi
echo "Image name: ${IMAGE_NAME}"
if [[ ${#EXTRA_TAGS[@]} -gt 0 ]]; then
    echo "Extra tags: ${EXTRA_TAGS[*]}"
fi

echo ""
echo "[2/3] Preparing build context..."
BUILD_CONTEXT=$(mktemp -d)
trap "rm -rf ${BUILD_CONTEXT}" EXIT

cp "${TARBALL}" "${BUILD_CONTEXT}/kafka.tgz"
cp "${SCRIPT_DIR}/Dockerfile" "${BUILD_CONTEXT}/Dockerfile"

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
    --build-arg "STRIMZI_VERSION=${STRIMZI_VERSION}"
    --build-arg "STRIMZI_KAFKA_VERSION=${STRIMZI_KAFKA_VERSION}"
    --build-arg "KAFKA_VERSION=${KAFKA_VERSION}"
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
if [[ "${PUSH}" != "true" ]]; then
    echo "To verify the image layout:"
    echo "  ${SCRIPT_DIR}/verify-image.sh ${IMAGE_NAME}"
    echo ""
fi
echo "Deploy with Strimzi ${STRIMZI_VERSION} by setting on the Kafka resource:"
echo "  spec.kafka.version: ${STRIMZI_KAFKA_VERSION}"
echo "  spec.kafka.image:   ${IMAGE_NAME}"
echo "See ${SCRIPT_DIR}/README.md and examples/kafka-diskless.yaml."
echo "=============================================="
