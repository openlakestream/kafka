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

DOCKER := docker
COMPOSE := $(DOCKER) compose
# `down` only touches services in the enabled profiles, so teardown has to name
# every profile or the lakehouse and one-shot demo containers survive it. Keep
# this list in sync with ALL_PROFILES in committer-tools/docker_cluster_smoke.py
# and compose_all() in run-lakehouse-demo.sh.
ALL_PROFILES := --profile lakehouse --profile demo --profile share-demo --profile lakehouse-demo --profile tools
# Polaris and its setup job are not part of the default stack. The compactor is:
# without it the WAL delete watermark never advances and Kafka retention frees no
# storage. The `lakehouse` profile adds the Iceberg catalog, and
# URSA_MATERIALIZATION_ENABLED makes the compactor write the external table too.
LAKEHOUSE_PROFILE := --profile lakehouse
LAKEHOUSE_ENV := URSA_MATERIALIZATION_ENABLED=true

TOPIC ?= test-diskless
PARTITIONS ?= 12

.PHONY: help
help:
	@echo "Diskless Storage Demo Commands:"
	@echo ""
	@echo "  make build-images     - Build the Kafka and Ursa compactor images"
	@echo "                          (URSA_STORAGE_DIR=/path/to/ursa-storage)"
	@echo "  make up               - Start the core cluster (Oxia, MinIO, 3 brokers,"
	@echo "                          compactor); needs both locally built images"
	@echo "  make down             - Stop all services"
	@echo "  make destroy          - Stop all services and remove volumes"
	@echo "  make logs             - Follow kafka-1 logs"
	@echo "  make ps               - Show running services"
	@echo "  make create-topic     - Create a diskless topic (TOPIC=name, PARTITIONS=n)"
	@echo "  make produce          - Run producer perf test"
	@echo "  make consume          - Run consumer perf test"
	@echo "  make list-topics      - List all topics"
	@echo "  make console-producer - Interactive console producer"
	@echo "  make console-consumer - Interactive console consumer"
	@echo ""
	@echo "  make demo             - Producer/consumer perf demo, torn down on exit"
	@echo "  make share-demo       - Share-group demo, torn down on exit"
	@echo "  make lakehouse-demo   - Kafka -> Iceberg -> DuckDB end-to-end assertion"
	@echo "                          (adds Polaris and the compactor)"
	@echo "  make duckdb           - Open DuckDB against the local Polaris catalog"
	@echo "  make compaction-logs  - Follow compactor logs"
	@echo ""

.PHONY: build-images
build-images:
	URSA_STORAGE_DIR="$(URSA_STORAGE_DIR)" ./build-images.sh

.PHONY: up
up:
	$(COMPOSE) up -d
	@echo "Waiting for cluster to be ready..."
	@sleep 10
	@$(COMPOSE) exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --list 2>/dev/null || echo "Cluster still starting..."
	@echo ""
	@echo "Cluster started. Use 'make create-topic' to create a diskless topic."
	@echo "For the Iceberg catalog, add:"
	@echo "  URSA_MATERIALIZATION_ENABLED=true docker compose --profile lakehouse up -d"

.PHONY: down
down:
	$(COMPOSE) $(ALL_PROFILES) down

.PHONY: destroy
destroy:
	$(COMPOSE) $(ALL_PROFILES) down -v --remove-orphans

.PHONY: logs
logs:
	$(COMPOSE) logs -f kafka-1

.PHONY: compaction-logs
compaction-logs:
	$(COMPOSE) logs -f compactor

.PHONY: ps
ps:
	$(COMPOSE) ps

.PHONY: create-topic
create-topic:
	$(COMPOSE) exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server kafka-1:19092 \
		--create \
		--if-not-exists \
		--config ursa.storage.enable=true \
		--topic $(TOPIC) \
		--partitions $(PARTITIONS) \
		--replication-factor 1

.PHONY: list-topics
list-topics:
	$(COMPOSE) exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server kafka-1:19092 \
		--list

.PHONY: produce
produce:
	$(COMPOSE) exec kafka-1 /opt/kafka/bin/kafka-producer-perf-test.sh \
		--producer-props \
			bootstrap.servers=kafka-1:19092 \
			batch.size=1000000 \
			linger.ms=100 \
			max.request.size=64000000 \
			acks=1 \
		--topic $(TOPIC) \
		--record-size 1000 \
		--num-records 100000 \
		--throughput -1

.PHONY: consume
consume:
	$(COMPOSE) exec kafka-1 /opt/kafka/bin/kafka-consumer-perf-test.sh \
		--bootstrap-server kafka-1:19092 \
		--topic $(TOPIC) \
		--messages 100000 \
		--reporting-interval 5000 \
		--show-detailed-stats \
		--timeout 60000 \
		--fetch-size 512000

.PHONY: console-producer
console-producer:
	$(COMPOSE) exec kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
		--bootstrap-server kafka-1:19092 \
		--topic $(TOPIC)

.PHONY: console-consumer
console-consumer:
	$(COMPOSE) exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
		--bootstrap-server kafka-1:19092 \
		--topic $(TOPIC) \
		--from-beginning

.PHONY: demo
demo:
	@set +e; \
	cleanup() { status=$$?; trap - EXIT INT TERM; $(MAKE) destroy; exit $$status; }; \
	trap cleanup EXIT INT TERM; \
	$(COMPOSE) --profile demo up

.PHONY: share-demo
share-demo:
	@set +e; \
	cleanup() { status=$$?; trap - EXIT INT TERM; $(MAKE) destroy; exit $$status; }; \
	trap cleanup EXIT INT TERM; \
	$(COMPOSE) --profile share-demo up

.PHONY: lakehouse-demo
lakehouse-demo:
	./run-lakehouse-demo.sh

.PHONY: duckdb
duckdb:
	$(LAKEHOUSE_ENV) $(COMPOSE) $(LAKEHOUSE_PROFILE) --profile tools run --rm duckdb
