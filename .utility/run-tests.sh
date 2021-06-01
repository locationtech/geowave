#!/bin/bash
set -ev
chmod +x .utility/*.sh

.utility/build-dev-resources.sh
if [ "$PYTHON_BUILD" == "true" ]; then
  echo -e "Running Python tests...\n"
  source .utility/run-python-tests.sh
else
  if [ "$IT_ONLY" == "true" ]; then
    echo -e "Skipping unit tests w/ verify...\n"
    wget -q https://archive.apache.org/dist/hadoop/common/hadoop-3.1.2/hadoop-3.1.2.tar.gz
    tar -xzf ./hadoop-3.1.2.tar.gz hadoop-3.1.2/lib/native/
    export LD_LIBRARY_PATH=$(pwd)/hadoop-3.1.2/lib/native/
    mvn -q -B verify -am -pl test -Dtest=SkipUnitTests -Dfindbugs.skip -Dspotbugs.skip -DfailIfNoTests=false -P $MAVEN_PROFILES
  else
    echo -e "Running unit tests only w/ verify...\n"
    mvn -q -B verify -Dformatter.action=validate -P $MAVEN_PROFILES
  fi
fi
