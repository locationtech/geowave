#!/bin/bash

MVN="$(cd "$(dirname "$0")/.." && pwd)/mvnw"
set -v
pushd dev-resources
# Build the dev-resources jar
echo -e "Building dev-resources..."
"$MVN" clean install
popd