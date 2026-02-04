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
# Get MinIO metrics from the diskless cluster
#
# Usage: ./get-minio-metrics.sh [metric-type]
#   metric-type: cluster (default), bucket, resource, node
#
# Examples:
#   ./get-minio-metrics.sh           # Get cluster metrics
#   ./get-minio-metrics.sh bucket    # Get bucket metrics
#   ./get-minio-metrics.sh resource  # Get resource metrics
#

set -e

MINIO_HOST="${MINIO_HOST:-localhost}"
MINIO_PORT="${MINIO_PORT:-19000}"
MINIO_USER="${MINIO_USER:-minioadmin}"
MINIO_PASS="${MINIO_PASS:-minioadmin}"

METRIC_TYPE="${1:-cluster}"

# Validate metric type
case "$METRIC_TYPE" in
    cluster|bucket|resource|node)
        ;;
    *)
        echo "Error: Invalid metric type '$METRIC_TYPE'"
        echo "Valid types: cluster, bucket, resource, node"
        exit 1
        ;;
esac

# Check if mc (MinIO client) is available in the container
if docker exec minio mc --version &>/dev/null; then
    # Use mc admin prometheus generate to get the token
    echo "=== Fetching MinIO $METRIC_TYPE Metrics ===" >&2
    echo "" >&2
    
    # Create alias if not exists
    docker exec minio mc alias set local http://localhost:9000 "$MINIO_USER" "$MINIO_PASS" &>/dev/null || true
    
    # Generate prometheus config and extract bearer token
    BEARER_TOKEN=$(docker exec minio mc admin prometheus generate local 2>/dev/null | grep -oP 'bearer_token:\s*\K\S+' || echo "")
    
    if [ -n "$BEARER_TOKEN" ]; then
        # Fetch metrics with bearer token
        curl -s -H "Authorization: Bearer $BEARER_TOKEN" \
            "http://${MINIO_HOST}:${MINIO_PORT}/minio/v2/metrics/$METRIC_TYPE"
    else
        # Fallback: try without auth (may work if public metrics are enabled)
        echo "Warning: Could not get bearer token, trying without auth..." >&2
        curl -s "http://${MINIO_HOST}:${MINIO_PORT}/minio/v2/metrics/$METRIC_TYPE"
    fi
else
    # mc not available in minio container, use direct API with basic auth
    echo "=== Fetching MinIO $METRIC_TYPE Metrics (direct) ===" >&2
    echo "" >&2
    
    # Try to exec mc from a temporary container
    BEARER_TOKEN=$(docker run --rm --network container:minio minio/mc:latest \
        sh -c "mc alias set local http://localhost:9000 $MINIO_USER $MINIO_PASS >/dev/null 2>&1 && \
               mc admin prometheus generate local 2>/dev/null" 2>/dev/null | \
        grep -oP 'bearer_token:\s*\K\S+' || echo "")
    
    if [ -n "$BEARER_TOKEN" ]; then
        curl -s -H "Authorization: Bearer $BEARER_TOKEN" \
            "http://${MINIO_HOST}:${MINIO_PORT}/minio/v2/metrics/$METRIC_TYPE"
    else
        echo "Error: Could not generate bearer token for metrics access" >&2
        echo "" >&2
        echo "Try using the MinIO Client (mc) directly:" >&2
        echo "  docker run --rm --network host minio/mc:latest admin prometheus generate local" >&2
        exit 1
    fi
fi
