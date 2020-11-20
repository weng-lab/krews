#!/bin/bash
# Runs unit and integration tests. Skips end to end tests.
set -e

# cd to project root directory
cd "$(dirname "$(dirname "$0")")"
cd src/test/kotlin/krews/util/test-image && docker build -t krewstest:latest . && cd ../../../../../..

./gradlew test -Dkotlintest.tags.exclude=E2E
