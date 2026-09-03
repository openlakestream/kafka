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

import os
import re
import sys
import tarfile
import tempfile
import subprocess
import argparse

# Constant: Regex to extract dependency tokens from the LICENSE file.
#
# In Kafka binary distributions, some jar basenames include additional suffixes
# beyond the semantic version (e.g. platform classifiers like "-linux-x86_64",
# or multi-part suffixes like "-empty-to-avoid-conflict-with-guava").
#
# We extract the full token after the leading dash up to the next comma/whitespace.
# Most jar basenames include a semantic version with dots (e.g. "-1.2.3"), but a
# small number may use a numeric version without dots (e.g. "javax.inject-1",
# "json-20250517"). We accept both forms while still requiring a trailing
# version-like suffix to avoid matching non-dependency bullet points.
LICENSE_DEP_PATTERN = re.compile(
    r"^\s*-\s*([A-Za-z0-9_.+-]+-(?:[0-9]+\.[^\s,]+|[0-9]+))", re.MULTILINE
)
SECTION_TITLES = {
    "./libs": "Additional bundled dependencies present in ./libs.",
    "./ursa-storage": "Additional bundled dependencies present in ./ursa-storage.",
}
FIRST_PARTY_JAR_PATTERN = re.compile(
    r"^(?:kafka(?:_|-)|connect-|trogdor-)", re.IGNORECASE
)

def run_gradlew(project_dir):
    print("Running './gradlew clean releaseTarGz'")
    subprocess.run(["./gradlew", "clean", "releaseTarGz"], check=True, cwd=project_dir)

def get_tarball_path(project_dir):
    distributions_dir = os.path.join(project_dir, "core", "build", "distributions")
    if not os.path.isdir(distributions_dir):
        print("Error: Distributions directory not found:", distributions_dir)
        sys.exit(1)
    
    pattern = re.compile(r'^kafka_2\.13-(?!.*docs).+\.tgz$', re.IGNORECASE)
    candidates = [
        os.path.join(distributions_dir, f)
        for f in os.listdir(distributions_dir)
        if pattern.match(f)
    ]
    if not candidates:
        print("Error: No tarball matching 'kafka_2.13-*.tgz' found in:", distributions_dir)
        sys.exit(1)
    
    tarball_path = max(candidates, key=os.path.getmtime)
    return tarball_path

def extract_tarball(tarball, extract_dir):
    with tarfile.open(tarball, "r:gz") as tar:
        # Use a filter to avoid future deprecation warnings.
        tar.extractall(path=extract_dir, filter=lambda tarinfo, dest: tarinfo)
    print("Tarball extracted to:", extract_dir)

def get_libs_set(libs_dir):
    return {
        fname[:-4]
        for fname in os.listdir(libs_dir)
        if fname.endswith(".jar") and not FIRST_PARTY_JAR_PATTERN.search(fname)
    }

def get_license_deps(license_text):
    return set(LICENSE_DEP_PATTERN.findall(license_text))

def get_license_section_deps(license_text, section_title):
    pattern = re.compile(
        r"^-{10,}\n"
        + re.escape(section_title)
        + r"\n(?P<body>.*?)(?=\n(?:-{10,}|={10,})\n)",
        re.MULTILINE | re.DOTALL,
    )
    match = pattern.search(license_text)
    return None if match is None else get_license_deps(match.group("body"))

def main():
    # Argument parser
    parser = argparse.ArgumentParser(description="Whether to skip executing ReleaseTarGz.")
    parser.add_argument("--skip-build", action="store_true", help="skip the build")
    args = parser.parse_args()

    # Assume the current working directory is the project root.
    project_dir = os.getcwd()
    print("Using project directory:", project_dir)

    if args.skip_build:
        print("Skip running './gradlew clean releaseTarGz'")
    else:
        # Build the tarball.
        run_gradlew(project_dir)
    tarball = get_tarball_path(project_dir)
    print("Tarball created at:", tarball)
    
    # Extract the tarball into a temporary directory.
    with tempfile.TemporaryDirectory() as tmp_dir:
        extract_tarball(tarball, tmp_dir)
        extracted_dirs = os.listdir(tmp_dir)
        if not extracted_dirs:
            print("Error: No directory found after extraction.")
            sys.exit(1)
        extracted = os.path.join(tmp_dir, extracted_dirs[0])
        print("Tarball extracted to:", extracted)
        
        # Locate the LICENSE file and bundled jar directories.
        license_path = os.path.join(extracted, "LICENSE")
        libs_dir = os.path.join(extracted, "libs")
        ursa_storage_dir = os.path.join(extracted, "ursa-storage")
        if not os.path.exists(license_path) or not os.path.exists(libs_dir):
            print("Error: LICENSE file or libs directory not found in the extracted project.")
            sys.exit(1)
        bundled_jar_dirs = [("./libs", libs_dir)]
        if os.path.isdir(ursa_storage_dir):
            bundled_jar_dirs.append(("./ursa-storage", ursa_storage_dir))
        
        with open(license_path, "r", encoding="utf-8") as f:
            license_text = f.read()

        failed = False
        verified_labels = set()
        for label, bundled_jar_dir in bundled_jar_dirs:
            verified_labels.add(label)
            bundled_deps = get_libs_set(bundled_jar_dir)
            license_deps = get_license_section_deps(license_text, SECTION_TITLES[label])

            print(f"\nDependencies from {label} (extracted from jar names):")
            for dep in sorted(bundled_deps):
                print(" -", dep)

            if license_deps is None:
                print(f"\nMissing LICENSE section: {SECTION_TITLES[label]}")
                failed = True
                continue

            print(f"\nDependencies extracted from LICENSE section for {label}:")
            for dep in sorted(license_deps):
                print(" -", dep)

            missing_in_license = bundled_deps - license_deps
            extra_in_license = license_deps - bundled_deps

            if missing_in_license:
                print(f"\nThe following bundled jars from {label} are missing in the LICENSE file. "
                      "These should be added to the LICENSE-binary file:")
                for dep in sorted(missing_in_license):
                    print(" -", dep)
            else:
                print(f"\nAll bundled jars from {label} are present in the LICENSE file.")

            if extra_in_license:
                print(f"\nThe following entries are in the LICENSE section for {label} but not present in {label}. "
                      "These should be removed from the LICENSE-binary file:")
                for dep in sorted(extra_in_license):
                    print(" -", dep)
            else:
                print(f"\nNo extra dependencies in the LICENSE section for {label}.")

            failed = failed or bool(missing_in_license) or bool(extra_in_license)

        for label, section_title in SECTION_TITLES.items():
            if label not in verified_labels:
                license_deps = get_license_section_deps(license_text, section_title)
                if license_deps:
                    print(f"\nThe LICENSE section for {label} exists, but {label} was not bundled. "
                          "These entries should be removed from the LICENSE-binary file:")
                    for dep in sorted(license_deps):
                        print(" -", dep)
                    failed = True

        if failed:
            sys.exit(1)

if __name__ == "__main__":
    main()
