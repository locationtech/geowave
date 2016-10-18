#!/bin/bash
set -ev

if [ "$TRAVIS_REPO_SLUG" == "ngageoint/geowave" ]; then
	echo -e "Verifying dev-resources jar...\n"
	
	# Pull dev-resources jar from S3
	pushd dev-resources
	if [ ! -d target ]; then
		mkdir target
	else
		echo -e "target folder exists"
	fi
	pushd target
	if [ ! -f geowave-dev-resources-1.1.jar ]; then
		echo -e "Pulling dev-resources jar from S3...\n"
		wget https://s3.amazonaws.com/geowave.deploy.emr.4/geowave-clone/geowave-dev-resources-1.1.jar
	else
		echo -e "dev-resources jar exists"
	fi
	# Fallback to building the jar if the S3 pull failed
	if [ ! -f geowave-dev-resources-1.1.jar ]; then
		echo -e "Pull from S3 failed. Building..."
		popd
		mvn clean install
	fi
fi