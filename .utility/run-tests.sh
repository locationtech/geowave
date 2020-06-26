#!/bin/bash
set -ev
if [ "$PYTHON_BUILD" == "true" ]; then
  echo -e "Running Python tests...\n"
  source .utility/run-python-tests.sh
elif [ "$BUILD_AND_PUBLISH" == "false" ]; then
  if [ "$IT_ONLY" == "true" ]; then
    echo -e "Skipping unit tests w/ verify...\n"
    wget https://archive.apache.org/dist/hadoop/common/hadoop-2.8.4/hadoop-2.8.4.tar.gz
    tar -xzf ./hadoop-2.8.4.tar.gz hadoop-2.8.4/lib/native/
    export LD_LIBRARY_PATH=$(pwd)/hadoop-2.8.4/lib/native/
    mvn -q verify -am -pl test -Dtest=SkipUnitTests -Dfindbugs.skip -Dspotbugs.skip -DfailIfNoTests=false -P $MAVEN_PROFILES
  else
    echo -e "Running unit tests only w/ verify...\n"
    mvn -q verify -Dformatter.action=validate -P $MAVEN_PROFILES
  fi
fi
