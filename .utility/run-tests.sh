#!/bin/bash
set -ev
chmod +x .utility/*.sh

MVN="$(cd "$(dirname "$0")/.." && pwd)/mvnw"

.utility/build-dev-resources.sh
if [ "$PYTHON_BUILD" == "true" ]; then
  echo -e "Running Python tests...\n"
  source .utility/run-python-tests.sh
else
  if [ "$IT_ONLY" == "true" ]; then
    echo -e "Skipping unit tests w/ verify...\n"
    # The integration tests need Hadoop's native libraries. Take the version
    # from the build rather than hardcoding it, so this cannot drift from the
    # hadoop the tests actually run against.
    HADOOP_VERSION=$("$MVN" -q -N help:evaluate -Dexpression=hadoop.version -DforceStdout)
    echo -e "Fetching Hadoop $HADOOP_VERSION native libraries...\n"
    wget -q "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz"
    tar -xzf "./hadoop-${HADOOP_VERSION}.tar.gz" "hadoop-${HADOOP_VERSION}/lib/native/"
    export LD_LIBRARY_PATH=$(pwd)/hadoop-${HADOOP_VERSION}/lib/native/
    # -Dtest=SkipUnitTests is a sentinel that deliberately matches nothing, so that
    # only the integration tests run. Surefire 2.x ignored a pattern that matched
    # no tests; 3.x treats it as an error unless told otherwise.
    "$MVN" -q -B verify -am -pl test -Dtest=SkipUnitTests -Dspotbugs.skip \
      -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false -P $MAVEN_PROFILES
  else
    echo -e "Running unit tests only w/ verify...\n"
    "$MVN" -q -B verify -Dformatter.action=validate -P $MAVEN_PROFILES
  fi
fi
