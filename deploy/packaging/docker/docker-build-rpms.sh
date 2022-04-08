#-------------------------------------------------------------------------------
# Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
# 
# See the NOTICE file distributed with this work for additional
# information regarding copyright ownership.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License,
# Version 2.0 which accompanies this distribution and is available at
# http://www.apache.org/licenses/LICENSE-2.0.txt
#-------------------------------------------------------------------------------
#!/bin/bash
#
# This script will build and package all of the configurations listed in the BUILD_ARGS_MATRIX array.
#
# Source all our reusable functionality, argument is the location of this script.
trap 'chmod -R 777 $WORKSPACE && exit' ERR
echo "INSTALL4J_HOME=${INSTALL4J_HOME}"
echo "GEOWAVE_BUCKET=${GEOWAVE_BUCKET}"
echo "GEOWAVE_RPM_BUCKET=${GEOWAVE_RPM_BUCKET}"
echo '###### Build Variables'
declare -A ARGS
while [ $# -gt 0 ]; do
    # Trim the first two chars off of the arg name ex: --foo
    case "$1" in
        *) NAME="${1:2}"; shift; ARGS[$NAME]="$1" ;;
    esac
    shift
done
BUILD_ARGS_MATRIX=${ARGS[buildargsmatrix]}
DOCKER_ARGS=${ARGS[dockerargs]}
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
TIME_TAG=$(date +"%Y%m%d%H%M")
SKIP_EXTRA="-Dspotbugs.skip -Dformatter.skip -DskipTests"
cd "$SCRIPT_DIR/../../.."
WORKSPACE="$(pwd)"
DOCKER_ROOT=$WORKSPACE/docker-root
LOCAL_REPO_DIR="${LOCAL_REPO_DIR:-/jenkins/gw-repo/snapshots}"
LOCK_DIR=/var/lock/subsys

# If you'd like to build a different set of artifacts rename build-args-matrix.sh.example
if [ -z $BUILD_ARGS_MATRIX  ]; then
	if [ -f $SCRIPT_DIR/build-args-matrix.sh ]; then
		source $SCRIPT_DIR/build-args-matrix.sh
	else
		# Default build arguments
    	BUILD_ARGS_MATRIX=(
	"-Dvendor.version=apache"
	"-P cloudera -Dvendor.version=cdh5"
    	)
	fi
fi

# make the docker_root directory if it has not been created already
if [[ ! -d $DOCKER_ROOT ]]; then
  echo "WARNING: The docker-root directory did not exist. Creating now." 
  mkdir $DOCKER_ROOT
fi

if [ -z ${INSTALL4J_HOME} ]; then
    echo "Setting INSTALL4J_HOME=/opt/install4j7"
    INSTALL4J_HOME=/opt/install4j7
fi

if [ -z ${GEOWAVE_RPM_BUCKET} ]; then
    echo "Setting GEOWAVE_RPM_BUCKET=geowave-rpms"
    GEOWAVE_RPM_BUCKET=geowave-rpms
fi

if [ -z ${GEOWAVE_BUCKET} ]; then
    echo "Setting GEOWAVE_BUCKET=geowave"
    GEOWAVE_BUCKET=geowave
fi

if [ -f ~/.install4j ]; then
   cp ~/.install4j $DOCKER_ROOT/
fi

if [ -d ~/.install4j7 ]; then
   cp -R ~/.install4j7 $DOCKER_ROOT/
fi

$WORKSPACE/deploy/packaging/rpm/centos/7/rpm.sh --command clean

docker run $DOCKER_ARGS --rm \
  -e WORKSPACE=/usr/src/geowave \
  -e MAVEN_OPTS="-Xmx1500m" \
  -e GEOWAVE_BUCKET="$GEOWAVE_BUCKET" \
  -v $DOCKER_ROOT:/root \
  -v $WORKSPACE:/usr/src/geowave \
  -v $INSTALL4J_HOME:/opt/install4j7 \
  locationtech/geowave-centos7-java8-build \
  /bin/bash -c \
  "cd \$WORKSPACE && deploy/packaging/docker/init.sh && deploy/packaging/docker/build-src/build-geowave-common.sh $SKIP_EXTRA"
	
docker run $DOCKER_ARGS --rm \
  -e WORKSPACE=/usr/src/geowave \
  -e BUILD_SUFFIX="common" \
  -e TIME_TAG="$TIME_TAG" \
  -e GEOWAVE_BUCKET="$GEOWAVE_BUCKET" \
  -v $DOCKER_ROOT:/root \
  -v $WORKSPACE:/usr/src/geowave \
  locationtech/geowave-centos7-rpm-build \
  /bin/bash -c \
  "cd \$WORKSPACE && deploy/packaging/docker/build-rpm/build-rpm.sh"

docker run $DOCKER_ARGS --rm \
  -e WORKSPACE=/usr/src/geowave \
  -e LOCAL_REPO_DIR=/usr/src/repo \
  -e LOCK_DIR=/usr/src/lock \
  -e TIME_TAG="$TIME_TAG" \
  -e GEOWAVE_BUCKET="$GEOWAVE_BUCKET" \
  -e GEOWAVE_RPM_BUCKET="$GEOWAVE_RPM_BUCKET" \
  -v $DOCKER_ROOT:/root \
  -v $WORKSPACE:/usr/src/geowave \
  -v $LOCAL_REPO_DIR:/usr/src/repo \
  -v $LOCK_DIR:/usr/src/lock \
  locationtech/geowave-centos7-publish \
  /bin/bash -c \
  "cd \$WORKSPACE && deploy/packaging/docker/publish/publish-common-rpm.sh --buildroot deploy/packaging/rpm/centos/7 --arch noarch --repo geowave"

for build_args in "${BUILD_ARGS_MATRIX[@]}"
do
    export BUILD_ARGS="$build_args"
    
    $WORKSPACE/deploy/packaging/rpm/centos/7/rpm.sh --command clean

    docker run --rm $DOCKER_ARGS \
      -e WORKSPACE=/usr/src/geowave \
      -e BUILD_ARGS="$build_args" \
      -e MAVEN_OPTS="-Xmx1500m" \
      -e GEOWAVE_BUCKET="$GEOWAVE_BUCKET" \
      -v $DOCKER_ROOT:/root \
      -v $WORKSPACE:/usr/src/geowave \
      locationtech/geowave-centos7-java8-build \
      /bin/bash -c \
      "cd \$WORKSPACE && deploy/packaging/docker/init.sh && deploy/packaging/docker/build-src/build-geowave-vendor.sh $SKIP_EXTRA"

    docker run --rm $DOCKER_ARGS \
      -e WORKSPACE=/usr/src/geowave \
      -e BUILD_ARGS="$build_args" \
      -e BUILD_SUFFIX="vendor" \
      -e TIME_TAG="$TIME_TAG" \
      -e GEOWAVE_BUCKET="$GEOWAVE_BUCKET" \
      -v $DOCKER_ROOT:/root \
      -v $WORKSPACE:/usr/src/geowave \
      -v $LOCAL_REPO_DIR:/usr/src/repo \
      locationtech/geowave-centos7-rpm-build \
      /bin/bash -c \
      "cd \$WORKSPACE && deploy/packaging/docker/build-rpm/build-rpm.sh"
    
    docker run $DOCKER_ARGS --rm \
      -e WORKSPACE=/usr/src/geowave \
      -e BUILD_ARGS="$build_args" \
      -e TIME_TAG="$TIME_TAG" \
      -e GEOWAVE_BUCKET="$GEOWAVE_BUCKET" \
      -v $WORKSPACE:/usr/src/geowave \
      locationtech/geowave-centos7-rpm-build \
      /bin/bash -c \
      "cd \$WORKSPACE && deploy/packaging/docker/build-rpm/build-services-rpm.sh --buildroot deploy/packaging/rpm/centos/7 --arch noarch"

    docker run --rm $DOCKER_ARGS \
      -e WORKSPACE=/usr/src/geowave \
      -e BUILD_ARGS="$build_args" \
      -e LOCAL_REPO_DIR=/usr/src/repo \
      -e LOCK_DIR=/usr/src/lock \
      -e TIME_TAG="$TIME_TAG" \
      -e GEOWAVE_BUCKET="$GEOWAVE_BUCKET" \
      -v $DOCKER_ROOT:/root \
      -v $WORKSPACE:/usr/src/geowave \
      -v $LOCAL_REPO_DIR:/usr/src/repo \
      -v $LOCK_DIR:/usr/src/lock \
      locationtech/geowave-centos7-publish \
      /bin/bash -c \
      "cd \$WORKSPACE && deploy/packaging/docker/publish/publish-vendor-rpm.sh --buildroot deploy/packaging/rpm/centos/7 --arch noarch --repo geowave"	
done
