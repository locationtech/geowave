#!/bin/bash
set -ev

echo -e "Building javadocs...\n"
mvn javadoc:javadoc -B -DskipTests -Dspotbugs.skip

echo $GPG_SECRET_KEYS | base64 --decode | gpg --import --no-tty --batch --yes
echo $GPG_OWNERTRUST | base64 --decode | gpg --import-ownertrust --no-tty --batch --yes

# Build the dev-resources jar
if ! curl --head --silent --fail  https://oss.sonatype.org/service/local/repositories/releases/content/org/locationtech/geowave/geowave-dev-resources/${DEV_RESOURCES_VERSION}/geowave-dev-resources-${DEV_RESOURCES_VERSION}.pom 2> /dev/null;
  then
    pushd dev-resources
    echo -e "Deploying dev-resources..."
    mvn deploy --settings ../.utility/.maven.xml -DskipTests -Dspotbugs.skip -B -U -Prelease
    popd
fi
echo -e "Deploying geowave artifacts..."
mvn deploy --settings .utility/.maven.xml -DskipTests -Dspotbugs.skip -B -U -Prelease

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
