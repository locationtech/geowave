#!/bin/bash
set -ev

if [ "$TRAVIS_REPO_SLUG" == "locationtech/geowave" ] && [ "$BUILD_AND_PUBLISH" == "true" ] && [ "$TRAVIS_BRANCH" == "master" ] && ["$TRAVIS_PULL_REQUEST" == "false"]; then
  echo -e "Building docs...\n"
  mvn -P html -pl docs install -DskipTests -Dspotbugs.skip
  
  echo -e "Installing local artifacts...\n"
  mvn -q install -DskipTests -Dspotbugs.skip
  
  echo -e "Building javadocs...\n"
  mvn -q javadoc:aggregate -DskipTests -Dspotbugs.skip
  
  echo -e "Building python docs...\n"
  source .utility/build-python-docs.sh
fi
