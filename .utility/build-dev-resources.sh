#!/bin/bash
# Don't set -e in case the S3 pull fails
set -v
# Pull dev-resources jar from S3
pushd dev-resources
# Fallback to building the jar if the S3 pull failed
echo -e "Building dev-resources..."
mvn clean install
popd