#!/bin/bash
set -ev

if [ "$IT_ONLY" == "true" ]; then
  echo -e "Skipping unit tests w/ verify...\n"
<<<<<<< HEAD
  mvn -q verify -Dtest=SkipUnitTests -DfailIfNoTests=false -Daccumulo.version=$ACCUMULO_VERSION -Daccumulo.api=$ACCUMULO_API -Dhbase.version=$HBASE_VERSION -Dhadoop.version=$HADOOP_VERSION -Dgeotools.version=$GEOTOOLS_VERSION -Dgeoserver.version=$GEOSERVER_VERSION -P $PLATFORM_VERSION $ADDITIONAL_ARGS
else
  echo -e "Running unit tests only w/ verify...\n"
  mvn -q verify -DskipITs=true -Daccumulo.version=$ACCUMULO_VERSION -Daccumulo.api=$ACCUMULO_API -Dhbase.version=$HBASE_VERSION -Dhadoop.version=$HADOOP_VERSION -Dgeotools.version=$GEOTOOLS_VERSION -Dgeoserver.version=$GEOSERVER_VERSION -P $PLATFORM_VERSION $ADDITIONAL_ARGS
=======
  mvn -q verify -Dtest=SkipUnitTests -DfailIfNoTests=false -P $MAVEN_PROFILES
else
  echo -e "Running unit tests only w/ verify...\n"
  mvn -q verify -P $MAVEN_PROFILES
>>>>>>> 286d34e401dad6d1be1d2547dca3fbc195a5ccb0
fi
