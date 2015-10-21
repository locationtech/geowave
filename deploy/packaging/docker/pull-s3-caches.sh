#!/bin/bash

# If we've not specifically disabled and there is no current Maven repo
# pull a cache from S3 so the first run won't take forever
if [ -z $NO_MAVEN_INIT ] && [ ! -d ~/.m2 ]; then
	echo "Downloading Maven Cache ..."
	MVN_CACHE_BASE=https://s3.amazonaws.com/geowave-maven/cache-bundle
	CACHE_FILE=mvn-repo-cache-20151021.tar
	pushd ~
	curl -O $MVN_CACHE_BASE/$CACHE_FILE
	tar xf $CACHE_FILE
	rm -f $CACHE_FILE
	popd
	type getenforce >/dev/null 2>&1 &&  getenforce >/dev/null 2>&1 && chcon -Rt svirt_sandbox_file_t ~/.m2;
	echo "Finished Downloading Maven Cache ..."
fi
