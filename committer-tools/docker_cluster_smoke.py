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


def build_kafka_image(project_dir: str, tarball: str, image_tag: str) -> None:
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


# `down` only removes containers for services in the enabled profiles, so the
# cleanup has to name every profile the compose file defines. Keep this list in
# sync with ALL_PROFILES in the compose directory's Makefile and with
# compose_all() in run-lakehouse-demo.sh.
ALL_PROFILES = [
    "--profile", "lakehouse",
    "--profile", "demo",
    "--profile", "share-demo",
    "--profile", "lakehouse-demo",
    "--profile", "tools",
]


def docker_compose(
    compose_dir: str, args: list[str], env: dict[str, str], check: bool = True
) -> None:
    docker = shutil.which("docker")
    if not docker:
        print("Error: docker CLI not found on PATH.")
        sys.exit(1)
    subprocess.run(
        [docker, "compose", *args],
        cwd=compose_dir,
        env=env,
        check=check,
    )


def print_compose_diagnostics(
    compose_dir: str, compose_file: str, env: dict[str, str], services: list[str]
) -> None:
    print("\nDocker compose startup failed. Collecting diagnostics...")

    # `ps`/`logs` only see services in the enabled profiles, so the demo profile
    # has to be on for a failing kafka-ready or create-topic to show up.
    diagnostic_commands = [
        (
            "docker compose ps -a",
            ["-f", compose_file, "--profile", "demo", "ps", "-a"],
        ),
        (
            "docker compose logs",
            [
                "-f", compose_file, "--profile", "demo",
                "logs", "--no-color", "--tail", "200",
                *services, "kafka-ready", "create-topic",
            ],
        ),
    ]

    for title, args in diagnostic_commands:
        print(f"\n[{title}]")
        docker_compose(compose_dir, args, env, check=False)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Smoke-test the Ursa docker compose stack by starting Kafka brokers and creating a diskless topic."
    )
    parser.add_argument("--skip-build", action="store_true", help="skip building the tarball")
    parser.add_argument(
        "--image-tag",
        default="lakestream/kafka:ci-smoke",
        help="docker image tag to build/use for the brokers and CLI containers",
    )
    parser.add_argument(
        "--compose-dir",
        default=os.path.join(
            "docker", "examples", "docker-compose-files", "cluster", "ursa"
        ),
        help="directory containing docker-compose.yml",
    )
    args = parser.parse_args()

    project_dir = os.getcwd()
    print("Using project directory:", project_dir)

    compose_dir = os.path.abspath(args.compose_dir)
    compose_file = os.path.join(compose_dir, "docker-compose.yml")
    if not os.path.isfile(compose_file):
        print("Error: compose file not found:", compose_file)
        sys.exit(1)

    if args.skip_build:
        print("Skip running './gradlew clean releaseTarGz'")
    else:
        run_gradlew(project_dir)

    tarball = get_tarball_path(project_dir)
    print("Tarball:", tarball)

    build_kafka_image(project_dir, tarball, args.image_tag)

    env = os.environ.copy()
    env["IMAGE"] = args.image_tag

    # Start only the services the brokers need. Polaris and the compactor stay
    # down so CI never needs the locally built compactor image.
    up_services = [
        "oxia",
        "minio",
        "minio-init",
        "kafka-1",
        "kafka-2",
        "kafka-3",
    ]

    try:
        print("\nStarting cluster services:", " ".join(up_services))
        docker_compose(
            compose_dir,
            ["-f", compose_file, "up", "-d", *up_services],
            env,
        )

        print("\nWaiting for Kafka cluster to be ready...")
        docker_compose(
            compose_dir,
            ["-f", compose_file, "--profile", "demo", "run", "--rm", "kafka-ready"],
            env,
        )

        print("\nCreating a diskless topic...")
        docker_compose(
            compose_dir,
            ["-f", compose_file, "--profile", "demo", "run", "--rm", "create-topic"],
            env,
        )

        print("\nOK: docker compose smoke test completed.")
    except subprocess.CalledProcessError:
        print_compose_diagnostics(compose_dir, compose_file, env, up_services)
        raise
    finally:
        print("\nCleaning up docker compose resources...")
        try:
            docker_compose(
                compose_dir,
                ["-f", compose_file, *ALL_PROFILES, "down", "-v", "--remove-orphans"],
                env,
            )
        except Exception as e:
            print("Warning: cleanup failed:", e)


if __name__ == "__main__":
    main()
