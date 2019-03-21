#!/bin/bash
# Publishes krews to JCenter
set -e

# cd to project root directory
cd "$(dirname "$(dirname "$0")")"

./gradlew bintrayUpload