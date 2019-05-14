#!/bin/bash
set -ev

if [ "$TRAVIS_REPO_SLUG" == "locationtech/geowave" ] && [ "$BUILD_AND_PUBLISH" == "true" ] && [ "$TRAVIS_BRANCH" == "master" ]; then
  echo -e "Building docs...\n"
  mvn -P html -pl docs install -DskipTests -Dspotbug.skip
  
  echo -e "Installing local artifacts...\n"
  mvn -q install -DskipTests -Dspotbugs.skip
  
  echo -e "Building javadocs...\n"
  mvn -q javadoc:aggregate -DskipTests -Dspotbugs.skip
fi
