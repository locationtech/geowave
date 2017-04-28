#!/bin/bash
#
# This script will build and package all of the configurations listed in the BUILD_ARGS_MATRIX array.
#
# Source all our reusable functionality, argument is the location of this script.

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
SKIP_EXTRA="-Dfindbugs.skip -Dformatter.skip -DskipTests"
cd "$SCRIPT_DIR/../../.."
WORKSPACE="$(pwd)"
DOCKER_ROOT=$WORKSPACE/docker-root
GEOSERVER_VERSION=geoserver-2.10.0-bin.zip
GEOSERVER_ARTIFACT=$WORKSPACE/deploy/packaging/rpm/centos/6/SOURCES/geoserver.zip
LOCAL_REPO_DIR=/var/www/html/repos/snapshots
LOCK_DIR=/var/lock/subsys

if [ -z $GEOSERVER_DOWNLOAD_BASE ]; then
	GEOSERVER_DOWNLOAD_BASE=https://s3.amazonaws.com/geowave-deploy/third-party-downloads/geoserver
fi

if [ -z $GEOSERVER_VERSION ]; then
	GEOSERVER_VERSION=geoserver-2.10.0-bin.zip
fi

if [ ! -f "$GEOSERVER_ARTIFACT" ]; then
	curl $GEOSERVER_DOWNLOAD_BASE/$GEOSERVER_VERSION > $GEOSERVER_ARTIFACT
fi

# If you'd like to build a different set of artifacts rename build-args-matrix.sh.example
if [ -z $BUILD_ARGS_MATRIX  ]; then
	if [ -f $SCRIPT_DIR/build-args-matrix.sh ]; then
		source $SCRIPT_DIR/build-args-matrix.sh
	else
		# Default build arguments
    	BUILD_ARGS_MATRIX=(
	"-Daccumulo.version=1.7.2 -Daccumulo.api=1.7 -Dhadoop.version=2.7.3 -Dgeotools.version=16.0 -Dgeoserver.version=2.10.0 -Dhbase.version=1.3.0 -Dvendor.version=apache"
	"-Daccumulo.version=1.7.2-cdh5.5.0 -Daccumulo.api=1.7 -Dhadoop.version=2.6.0-cdh5.9.0 -Dgeotools.version=16.0 -Dgeoserver.version=2.10.0 -Dhbase.version=1.2.0-cdh5.9.0 -P cloudera -Dvendor.version=cdh5"
	"-Daccumulo.version=1.7.0.2.4.2.0-258 -Daccumulo.api=1.7 -Dhadoop.version=2.7.1.2.4.2.0-258 -Dgeotools.version=16.0 -Dgeoserver.version=2.10.0 -Dhbase.version=1.1.2.2.4.2.0-258 -P hortonworks -Dvendor.version=hdp2"
    	)
	fi
fi
mkdir $DOCKER_ROOT

$WORKSPACE/deploy/packaging/docker/pull-s3-caches.sh $DOCKER_ROOT
$WORKSPACE/deploy/packaging/rpm/centos/6/rpm.sh --command clean
	
docker run $DOCKER_ARGS --rm \
	-e WORKSPACE=/usr/src/geowave \
	-e MAVEN_OPTS="-Xmx1500m" \
	-v $DOCKER_ROOT:/root \
	-v $WORKSPACE:/usr/src/geowave \
	ngageoint/geowave-centos6-java8-build \
	/bin/bash -c \
	"cd \$WORKSPACE && deploy/packaging/docker/build-src/build-geowave-common.sh $SKIP_EXTRA"
	
docker run $DOCKER_ARGS --rm \
    -e WORKSPACE=/usr/src/geowave \
	-e GEOSERVER_VERSION="$GEOSERVER_VERSION" \
	-e BUILD_SUFFIX="common" \
	-e TIME_TAG="$TIME_TAG" \
    -v $DOCKER_ROOT:/root \
    -v $WORKSPACE:/usr/src/geowave \
    ngageoint/geowave-centos6-rpm-build \
    /bin/bash -c \
    "cd \$WORKSPACE && deploy/packaging/docker/build-rpm/build-rpm.sh"

docker run $DOCKER_ARGS --rm \
    -e WORKSPACE=/usr/src/geowave \
    -e LOCAL_REPO_DIR=/usr/src/repo \
    -e LOCK_DIR=/usr/src/lock \
	-e TIME_TAG="$TIME_TAG" \
    -v $DOCKER_ROOT:/root \
    -v $WORKSPACE:/usr/src/geowave \
    -v $LOCAL_REPO_DIR:/usr/src/repo \
    -v $LOCK_DIR:/usr/src/lock \
    ngageoint/geowave-centos6-publish \
    /bin/bash -c \
    "cd \$WORKSPACE && deploy/packaging/docker/publish/publish-common-rpm.sh --buildroot deploy/packaging/rpm/centos/6 --arch noarch --repo geowave"

for build_args in "${BUILD_ARGS_MATRIX[@]}"
do
	export BUILD_ARGS="$build_args"
	
	$WORKSPACE/deploy/packaging/rpm/centos/6/rpm.sh --command clean
	docker run --rm $DOCKER_ARGS \
		-e WORKSPACE=/usr/src/geowave \
		-e BUILD_ARGS="$build_args" \
		-e MAVEN_OPTS="-Xmx1500m" \
		-v $DOCKER_ROOT:/root \
		-v $WORKSPACE:/usr/src/geowave \
		ngageoint/geowave-centos6-java8-build \
		/bin/bash -c \
		"cd \$WORKSPACE && deploy/packaging/docker/build-src/build-geowave-vendor.sh $SKIP_EXTRA"

	docker run --rm $DOCKER_ARGS \
    	-e WORKSPACE=/usr/src/geowave \
    	-e BUILD_ARGS="$build_args" \
		-e GEOSERVER_VERSION="$GEOSERVER_VERSION" \
		-e BUILD_SUFFIX="vendor" \
		-e TIME_TAG="$TIME_TAG" \
    	-v $DOCKER_ROOT:/root \
    	-v $WORKSPACE:/usr/src/geowave \
    	-v $LOCAL_REPO_DIR:/usr/src/repo \
    	ngageoint/geowave-centos6-rpm-build \
    	/bin/bash -c \
    	"cd \$WORKSPACE && deploy/packaging/docker/build-rpm/build-rpm.sh"
    
    docker run --rm $DOCKER_ARGS \
    	-e WORKSPACE=/usr/src/geowave \
    	-e BUILD_ARGS="$build_args" \
    	-e LOCAL_REPO_DIR=/usr/src/repo \
    	-e LOCK_DIR=/usr/src/lock \
		-e TIME_TAG="$TIME_TAG" \
    	-v $DOCKER_ROOT:/root \
    	-v $WORKSPACE:/usr/src/geowave \
    	-v $LOCAL_REPO_DIR:/usr/src/repo \
    	-v $LOCK_DIR:/usr/src/lock \
    	ngageoint/geowave-centos6-publish \
    	/bin/bash -c \
    	"cd \$WORKSPACE && deploy/packaging/docker/publish/publish-vendor-rpm.sh --buildroot deploy/packaging/rpm/centos/6 --arch noarch --repo geowave"	
done
