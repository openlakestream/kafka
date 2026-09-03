-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements. See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License. You may obtain a copy of the License at
--
--    http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

INSTALL httpfs;
LOAD httpfs;
INSTALL iceberg;
LOAD iceberg;

CREATE OR REPLACE SECRET minio_secret (
    TYPE s3,
    PROVIDER config,
    KEY_ID 'minioadmin',
    SECRET 'minioadmin',
    REGION 'us-east-1',
    ENDPOINT 'minio:9000',
    URL_STYLE 'path',
    USE_SSL false
);

CREATE OR REPLACE SECRET polaris_secret (
    TYPE iceberg,
    CLIENT_ID 'root',
    CLIENT_SECRET 's3cr3t',
    OAUTH2_SERVER_URI 'http://polaris:8181/api/catalog/v1/oauth/tokens',
    OAUTH2_SCOPE 'PRINCIPAL_ROLE:ALL'
);

ATTACH 'ursa' AS lakehouse (
    TYPE iceberg,
    ENDPOINT 'http://polaris:8181/api/catalog',
    SECRET polaris_secret,
    ACCESS_DELEGATION_MODE 'none',
    SUPPORT_NESTED_NAMESPACES true
);
