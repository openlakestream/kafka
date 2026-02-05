#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
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

import argparse
import datetime
import os
import re
import shutil
import subprocess
import sys
import tempfile


def run_gradlew(project_dir: str) -> None:
    print("Running './gradlew clean releaseTarGz'")
    subprocess.run(["./gradlew", "clean", "releaseTarGz"], check=True, cwd=project_dir)


def get_tarball_path(project_dir: str) -> str:
    distributions_dir = os.path.join(project_dir, "core", "build", "distributions")
    if not os.path.isdir(distributions_dir):
        print("Error: Distributions directory not found:", distributions_dir)
        sys.exit(1)

    pattern = re.compile(r"^kafka_2\.13-(?!.*docs).+\.tgz$", re.IGNORECASE)
    candidates = [
        os.path.join(distributions_dir, f)
        for f in os.listdir(distributions_dir)
        if pattern.match(f)
    ]
    if not candidates:
        print("Error: No tarball matching 'kafka_2.13-*.tgz' found in:", distributions_dir)
        sys.exit(1)

    return max(candidates, key=os.path.getmtime)


def build_kafka_diskless_image(project_dir: str, tarball: str, image_tag: str) -> None:
    dockerfile = os.path.join(
        project_dir,
        "docker",
        "examples",
        "docker-compose-files",
        "cluster",
        "ursa",
        "Dockerfile",
    )
    resources_dir = os.path.join(project_dir, "docker", "resources")
    jvm_dir = os.path.join(project_dir, "docker", "jvm")
    server_properties = os.path.join(project_dir, "docker", "server.properties")

    for path in [dockerfile, resources_dir, jvm_dir, server_properties]:
        if not os.path.exists(path):
            print("Error: Missing required docker build input:", path)
            sys.exit(1)

    docker = shutil.which("docker")
    if not docker:
        print("Error: docker CLI not found on PATH.")
        sys.exit(1)

    with tempfile.TemporaryDirectory() as build_ctx:
        shutil.copyfile(tarball, os.path.join(build_ctx, "kafka.tgz"))
        shutil.copyfile(dockerfile, os.path.join(build_ctx, "Dockerfile"))
        shutil.copytree(resources_dir, os.path.join(build_ctx, "resources"))
        shutil.copytree(jvm_dir, os.path.join(build_ctx, "jvm"))
        shutil.copyfile(server_properties, os.path.join(build_ctx, "server.properties"))

        build_date = datetime.date.today().isoformat()
        print("Building docker image:", image_tag)
        subprocess.run(
            [
                docker,
                "build",
                "-f",
                os.path.join(build_ctx, "Dockerfile"),
                "-t",
                image_tag,
                "--build-arg",
                f"build_date={build_date}",
                build_ctx,
            ],
            check=True,
        )


def docker_compose(compose_dir: str, args: list[str], env: dict[str, str]) -> None:
    docker = shutil.which("docker")
    if not docker:
        print("Error: docker CLI not found on PATH.")
        sys.exit(1)
    subprocess.run(
        [docker, "compose", *args],
        cwd=compose_dir,
        env=env,
        check=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Smoke-test the localstack compaction docker compose by starting Kafka brokers and creating a diskless topic."
    )
    parser.add_argument("--skip-build", action="store_true", help="skip building the tarball")
    parser.add_argument(
        "--image-tag",
        default="kafka-diskless:ci-smoke",
        help="docker image tag to build/use for the brokers and CLI containers",
    )
    parser.add_argument(
        "--compose-dir",
        default=os.path.join(
            "docker", "examples", "docker-compose-files", "cluster", "ursa"
        ),
        help="directory containing docker-compose-localstack-compaction*.yml",
    )
    args = parser.parse_args()

    project_dir = os.getcwd()
    print("Using project directory:", project_dir)

    compose_dir = os.path.abspath(args.compose_dir)
    base_compose = os.path.join(compose_dir, "docker-compose-localstack-compaction.yml")
    demo_compose = os.path.join(compose_dir, "docker-compose-localstack-compaction.demo.yml")
    if not os.path.isfile(base_compose):
        print("Error: compose file not found:", base_compose)
        sys.exit(1)
    if not os.path.isfile(demo_compose):
        print("Error: compose file not found:", demo_compose)
        sys.exit(1)

    if args.skip_build:
        print("Skip running './gradlew clean releaseTarGz'")
    else:
        run_gradlew(project_dir)

    tarball = get_tarball_path(project_dir)
    print("Tarball:", tarball)

    build_kafka_diskless_image(project_dir, tarball, args.image_tag)

    env = os.environ.copy()
    env["IMAGE"] = args.image_tag
    env.setdefault("URSA_STORAGE_PATH", "ursa")

    # Start only the services required for brokers (skip compactor to avoid extra images).
    up_services = [
        "oxia",
        "localstack",
        "localstack-init",
        "schema-registry",
        "kafka-1",
        "kafka-2",
        "kafka-3",
    ]

    try:
        print("\nStarting cluster services:", " ".join(up_services))
        docker_compose(
            compose_dir,
            ["-f", base_compose, "up", "-d", *up_services],
            env,
        )

        print("\nWaiting for Kafka cluster to be ready...")
        docker_compose(
            compose_dir,
            ["-f", base_compose, "-f", demo_compose, "run", "--rm", "kafka-ready"],
            env,
        )

        print("\nCreating a diskless topic...")
        docker_compose(
            compose_dir,
            ["-f", base_compose, "-f", demo_compose, "run", "--rm", "create-topic"],
            env,
        )

        print("\nOK: docker compose smoke test completed.")
    finally:
        print("\nCleaning up docker compose resources...")
        try:
            docker_compose(
                compose_dir,
                ["-f", base_compose, "down", "-v", "--remove-orphans"],
                env,
            )
        except Exception as e:
            print("Warning: cleanup failed:", e)


if __name__ == "__main__":
    main()

