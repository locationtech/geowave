#!/bin/bash
set -ev

if [ "$TRAVIS_REPO_SLUG" == "ngageoint/geowave" ]; then
  if [ "$IT_ONLY" == "true" ]; then
    echo -e "Skipping unit tests w/ verify...\n"
    mvn -q clean verify -Dtest=SkipUnitTests -DfailIfNoTests=false -Daccumulo.version=$ACCUMULO_VERSION -Daccumulo.api=$ACCUMULO_API -Dhbase.version=$HBASE_VERSION -Dhadoop.version=$HADOOP_VERSION -Dgeotools.version=$GEOTOOLS_VERSION -Dgeoserver.version=$GEOSERVER_VERSION -P $PLATFORM_VERSION
  else
    echo -e "Running unit tests only w/ verify...\n"
    mvn -q clean verify -DskipITs=true -Daccumulo.version=$ACCUMULO_VERSION -Daccumulo.api=$ACCUMULO_API -Dhbase.version=$HBASE_VERSION -Dhadoop.version=$HADOOP_VERSION -Dgeotools.version=$GEOTOOLS_VERSION -Dgeoserver.version=$GEOSERVER_VERSION -P $PLATFORM_VERSION
  fi
fi