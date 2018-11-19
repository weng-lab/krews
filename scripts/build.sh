#!/bin/bash
# Builds without running tests
set -e

# cd to project root directory
cd "$(dirname "$(dirname "$0")")"

./gradlew clean assemble