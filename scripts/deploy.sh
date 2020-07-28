#!/bin/bash
# Builds without running tests
set -e

# cd to project root directory
cd "$(dirname "$(dirname "$0")")"

git clone git@github.com:weng-lab/devops.git /tmp/devops
DEPLOY_KEY=$(cat /tmp/devops/krews-deploy-key.txt)
rm -rf /tmp/devops

./gradlew bintrayUpload -PbintrayUser=jsonbrooks -PbintrayKey="$DEPLOY_KEY"