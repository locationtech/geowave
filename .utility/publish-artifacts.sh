#!/bin/bash

MVN="$(cd "$(dirname "$0")/.." && pwd)/mvnw"
set -ev

echo -e "Building javadocs...\n"
"$MVN" javadoc:javadoc -B -DskipTests -Dspotbugs.skip

echo $GPG_SECRET_KEYS | base64 --decode | gpg --import --no-tty --batch --yes
echo $GPG_OWNERTRUST | base64 --decode | gpg --import-ownertrust --no-tty --batch --yes

# dev-resources is consumed as a plugin dependency, so it has to already exist
# in a repository before the reactor starts -- it cannot be a reactor module.
# Publish it only when this version is not out there yet. Ask Maven rather than
# a Nexus REST endpoint; OSSRH was retired on 2025-06-30.
if ! "$MVN" -q -B dependency:get \
      -Dartifact=org.locationtech.geowave:geowave-dev-resources:${DEV_RESOURCES_VERSION}:pom \
      -Dtransitive=false > /dev/null 2>&1;
  then
    pushd dev-resources
    echo -e "Deploying dev-resources..."
    "$MVN" deploy --settings ../.utility/.maven.xml -DskipTests -Dspotbugs.skip -B -U -Prelease
    popd
fi
echo -e "Deploying geowave artifacts..."
"$MVN" deploy --settings .utility/.maven.xml -DskipTests -Dspotbugs.skip -B -U -Prelease

# Get the version from the build.properties file
filePath=deploy/target/classes/build.properties
GEOWAVE_VERSION=$(grep project.version $filePath|  awk -F= '{print $2}')

# Don't publish snapshots to PyPi
if [[ ! "$GEOWAVE_VERSION" =~ "SNAPSHOT" ]] ; then
  if [[ -z "${PYPI_CREDENTIALS}" ]]; then
    echo -e "No PyPi credentials, skipping PyPi distribution..."
  else
    echo -e "Deploying pygw to PyPi..."
    pushd python/src/main/python
    python3 -m venv publish-venv
    source ./publish-venv/bin/activate
  
    pip install --upgrade pip wheel setuptools twine
    python3 setup.py bdist_wheel --python-tag=py3 sdist
    twine upload --skip-existing -u __token__ -p $PYPI_CREDENTIALS dist/*
    deactivate
    popd
  fi
fi 
