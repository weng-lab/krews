#!/bin/bash
# Runs end to end tests. Skips unit and integration tests.
set -e

# cd to project root directory
cd "$(dirname "$(dirname "$0")")"

./gradlew test -Dkotlintest.tags.include=E2E