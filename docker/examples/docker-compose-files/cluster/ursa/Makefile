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

.PHONY: help
help:
	@echo "Diskless Storage Demo Commands:"
	@echo ""
	@echo "  make demo         - Start cluster with demo (topic creation + producer/consumer perf tests)"
	@echo "  make up           - Start cluster only (no demo workloads)"
	@echo "  make down         - Stop all services"
	@echo "  make destroy      - Stop all services and remove volumes"
	@echo "  make logs         - Follow kafka-1 logs"
	@echo "  make ps           - Show running services"
	@echo "  make create-topic - Create a diskless topic (TOPIC=name, default: test-diskless)"
	@echo "  make produce      - Run producer perf test"
	@echo "  make consume      - Run consumer perf test"
	@echo ""

.PHONY: demo
demo:
	@set +e; \
	cleanup() { status=$$?; trap - EXIT INT TERM; $(MAKE) destroy; exit $$status; }; \
	trap cleanup EXIT INT TERM; \
	$(DOCKER) compose -f docker-compose.yml -f docker-compose.demo.yml up

.PHONY: demo-compaction
demo-compaction:
	$(DOCKER) compose -f docker-compose-localstack-compaction.yml up -d
	$(DOCKER) compose -f docker-compose-localstack-compaction.yml -f docker-compose-localstack-compaction.demo.yml run --rm avro-consumer
	$(DOCKER) compose -f docker-compose-localstack-compaction.yml down -v --remove-orphans

.PHONY: up
up:
	$(DOCKER) compose -f docker-compose.yml up -d
	@echo "Waiting for cluster to be ready..."
	@sleep 10
	@$(DOCKER) compose exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --list 2>/dev/null || echo "Cluster still starting..."
	@echo ""
	@echo "Cluster started. Use 'make create-topic' to create a diskless topic."

.PHONY: down
down:
	$(DOCKER) compose down

.PHONY: destroy
destroy:
	$(DOCKER) compose down -v --remove-orphans

.PHONY: up-compaction
up-compaction:
	$(DOCKER) compose -f docker-compose-localstack-compaction.yml up -d

.PHONY: down-compaction
down-compaction:
	$(DOCKER) compose -f docker-compose-localstack-compaction.yml down

.PHONY: destroy-compaction
destroy-compaction:
	$(DOCKER) compose -f docker-compose-localstack-compaction.yml down -v --remove-orphans

.PHONY: logs
logs:
	$(DOCKER) compose logs -f kafka-1

.PHONY: ps
ps:
	$(DOCKER) compose ps

TOPIC ?= test-diskless
PARTITIONS ?= 12

.PHONY: create-topic
create-topic:
	$(DOCKER) compose exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server kafka-1:19092 \
		--create \
		--if-not-exists \
		--config ursa.storage.enable=true \
		--topic $(TOPIC) \
		--partitions $(PARTITIONS) \
		--replication-factor 1

.PHONY: list-topics
list-topics:
	$(DOCKER) compose exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server kafka-1:19092 \
		--list

.PHONY: produce
produce:
	$(DOCKER) compose exec kafka-1 /opt/kafka/bin/kafka-producer-perf-test.sh \
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
	$(DOCKER) compose exec kafka-1 /opt/kafka/bin/kafka-consumer-perf-test.sh \
		--bootstrap-server kafka-1:19092 \
		--topic $(TOPIC) \
		--messages 100000 \
		--reporting-interval 5000 \
		--show-detailed-stats \
		--timeout 60000 \
		--fetch-size 512000

.PHONY: console-producer
console-producer:
	$(DOCKER) compose exec kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
		--bootstrap-server kafka-1:19092 \
		--topic $(TOPIC)

.PHONY: console-consumer
console-consumer:
	$(DOCKER) compose exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
		--bootstrap-server kafka-1:19092 \
		--topic $(TOPIC) \
		--from-beginning
