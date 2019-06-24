#!/bin/bash
set -v
pushd dev-resources
# Build the dev-resources jar
echo -e "Building dev-resources..."
mvn clean install
popd