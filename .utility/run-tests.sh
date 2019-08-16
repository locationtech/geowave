#!/bin/bash
set -ev
if [ "$PYTHON_BUILD" == "true" ]; then
  echo -e "Running Python tests...\n"
  source .utility/run-python-tests.sh
elif [ "$BUILD_AND_PUBLISH" == "false" ]; then 
  if [ "$IT_ONLY" == "true" ]; then
    echo -e "Skipping unit tests w/ verify...\n"
    mvn -q verify -am -pl test -Dtest=SkipUnitTests -Dfindbugs.skip -Dspotbugs.skip -DfailIfNoTests=false -P $MAVEN_PROFILES
  else
    echo -e "Running unit tests only w/ verify...\n"
    mvn -q verify -Dformatter.action=validate -P $MAVEN_PROFILES
  fi
fi
