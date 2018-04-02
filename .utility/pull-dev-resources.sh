#!/bin/bash
# Don't set -e in case the S3 pull fails
set -v
version=${DEV_RESOURCES_VERSION:=1.0}
echo -e "Pull or build dev-resources-${version}.jar..."

if [ "$TRAVIS_REPO_SLUG" == "locationtech/geowave" ]; then
	# Pull dev-resources jar from S3
	pushd dev-resources
	if [ ! -d target ]; then
		mkdir target
	else
		echo -e "target folder exists"
	fi
	pushd target
	if [ ! -f geowave-dev-resources-${version}.jar ]; then
		echo -e "Pulling dev-resources jar from S3...\n"
		wget https://s3.amazonaws.com/geowave.deploy.emr.4/geowave-clone/geowave-dev-resources-${version}.jar
	else
		echo -e "dev-resources jar exists"
	fi
	popd
	if [ -f target/geowave-dev-resources-${version}.jar ]; then
		# Install downloaded jar to local repo
		mvn install:install-file -Dfile=target/geowave-dev-resources-${version}.jar -DgroupId=mil.nga.giat -DartifactId=geowave-dev-resources -Dversion=${version} -Dpackaging=jar
	else
		# Fallback to building the jar if the S3 pull failed
		echo -e "Pull from S3 failed. Building..."
		mvn clean install
	fi
fi