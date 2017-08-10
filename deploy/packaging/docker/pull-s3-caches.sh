#-------------------------------------------------------------------------------
# Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
# 
# See the NOTICE file distributed with this work for additional
# information regarding copyright ownership.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License,
# Version 2.0 which accompanies this distribution and is available at
# http://www.apache.org/licenses/LICENSE-2.0.txt
#-------------------------------------------------------------------------------
#!/bin/bash

# If we've not specifically disabled and there is no current Maven repo
# pull a cache from S3 so the first run won't take forever
if [ -z $NO_MAVEN_INIT ] && [ ! -d $1/.m2 ]; then
	echo "Downloading Maven Cache ..."
	MVN_CACHE_BASE=https://s3.amazonaws.com/geowave-deploy/cache-bundle
	CACHE_FILE=mvn-repo-cache-20170810.tar.gz
	pushd $1
	curl -O $MVN_CACHE_BASE/$CACHE_FILE
	tar xf $1/$CACHE_FILE
	rm -f $1/$CACHE_FILE
	popd
	#if run in docker, do the following:
	#type getenforce >/dev/null 2>&1 &&  getenforce >/dev/null 2>&1 && chcon -Rt svirt_sandbox_file_t $1/.m2;
	echo "Finished Downloading Maven Cache ..."
fi
