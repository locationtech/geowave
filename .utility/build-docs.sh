#!/bin/bash
set -ev

if [ "$TRAVIS_REPO_SLUG" == "ngageoint/geowave" ] && [ "$BUILD_DOCS" == "true" ] && [ "$TRAVIS_BRANCH" == "master" ]; then
  echo -e "Running mvn clean install...\n"
  mvn -q clean install -Dfindbugs.skip -Daccumulo.version=$ACCUMULO_VERSION -Daccumulo.api=$ACCUMULO_API -Dhbase.version=$HBASE_VERSION -Dhadoop.version=$HADOOP_VERSION -Dgeotools.version=$GEOTOOLS_VERSION -Dgeoserver.version=$GEOSERVER_VERSION -DskipITs=true -DskipTests=true -Dformatter.skip=true -P $PLATFORM_VERSION
  
  echo -e "Building docs...\n"
  mvn -P docs -pl docs install
  
  echo -e "Building javadocs...\n"
  mvn -q javadoc:aggregate
fi