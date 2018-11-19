#!/bin/bash
# Runs unit and integration tests. Skips end to end tests.
set -e

# cd to project root directory
cd "$(dirname "$(dirname "$0")")"

./gradlew test -Dkotlintest.tags.exclude=E2E