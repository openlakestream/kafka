#!/bin/sh
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

set -eu

token_response="$(curl --fail-with-body -sS -X POST "$POLARIS_OAUTH2_URI" \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d 'grant_type=client_credentials' \
  -d "client_id=$POLARIS_CLIENT_ID" \
  -d "client_secret=$POLARIS_CLIENT_SECRET" \
  -d 'scope=PRINCIPAL_ROLE:ALL')"

token="$(printf '%s' "$token_response" \
  | sed -n 's/.*"access_token"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')"
if [ -z "$token" ]; then
  echo "Could not parse a Polaris access token: $token_response" >&2
  exit 1
fi

catalog_payload="$(cat <<EOF
{
  "catalog": {
    "name": "$POLARIS_CATALOG_NAME",
    "type": "INTERNAL",
    "readOnly": false,
    "properties": {
      "default-base-location": "$POLARIS_WAREHOUSE"
    },
    "storageConfigInfo": {
      "storageType": "S3",
      "allowedLocations": ["$POLARIS_WAREHOUSE"],
      "endpoint": "$POLARIS_S3_ENDPOINT",
      "endpointInternal": "$POLARIS_S3_ENDPOINT",
      "stsUnavailable": true,
      "pathStyleAccess": true,
      "region": "$AWS_REGION"
    }
  }
}
EOF
)"

response_file=/tmp/polaris-create-catalog.json
status="$(curl -sS -o "$response_file" -w '%{http_code}' \
  -X POST "$POLARIS_MANAGEMENT_URI/v1/catalogs" \
  -H "Authorization: Bearer $token" \
  -H "Polaris-Realm: $POLARIS_REALM" \
  -H 'Accept: application/json' \
  -H 'Content-Type: application/json' \
  -d "$catalog_payload")"

case "$status" in
  200|201)
    echo "Created Polaris catalog '$POLARIS_CATALOG_NAME'."
    ;;
  409)
    echo "Polaris catalog '$POLARIS_CATALOG_NAME' already exists."
    ;;
  *)
    echo "Polaris catalog creation failed with HTTP $status:" >&2
    cat "$response_file" >&2
    exit 1
    ;;
esac
