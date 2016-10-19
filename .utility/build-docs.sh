#!/bin/bash
set -ev

if [ "$TRAVIS_REPO_SLUG" == "ngageoint/geowave" ] && [ "$BUILD_DOCS" == "true" ] && [ "$TRAVIS_BRANCH" == "master" ]; then
  echo -e "Building docs...\n"
  mvn -P docs -pl docs install
  
  echo -e "Building javadocs...\n"
  mvn -q javadoc:aggregate
fi