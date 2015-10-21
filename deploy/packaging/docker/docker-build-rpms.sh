#!/bin/bash
#
# This script will build and package all of the configurations listed in teh BUILD_ARGS_MATRIX array.
#

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR/../../.."
WORKSPACE="$(pwd)"

# selinux config if needed
type getenforce >/dev/null 2>&1 && getenforce >/dev/null 2>&1 && chcon -Rt svirt_sandbox_file_t $WORKSPACE;

# If you'd like to build a different set of artifacts rename build-args-matrix.sh.example
if [ -f $SCRIPT_DIR/build-args-matrix.sh ]; then
	source $SCRIPT_DIR/build-args-matrix.sh
else
	# Default build arguments
    BUILD_ARGS_MATRIX=(
      "-Daccumulo.version=1.6.0-cdh5.1.4 -Dhadoop.version=2.6.0-cdh5.4.0 -Dgeotools.version=13.0 -Dgeoserver.version=2.7.3 -Dvendor.version=cdh5 -P cloudera"
      "-Daccumulo.version=1.6.2 -Dhadoop.version=2.6.0 -Dgeotools.version=13.2 -Dgeoserver.version=2.7.3 -Dvendor.version=apache -Dvendor.version=apache -P \"\""
      "-Daccumulo.version=1.6.1.2.2.4.0-2633 -Dhadoop.version=2.6.0.2.2.4.0-2633 -Dgeotools.version=13.2 -Dgeoserver.version=2.7.3 -Dvendor.version=hdp2 -P hortonworks"
    )
fi

export MVN_PACKAGE_FAT_JARS_CMD="/usr/src/geowave/deploy/packaging/rpm/admin-scripts/jenkins-build-geowave.sh $SKIP_TESTS"

$WORKSPACE/deploy/packaging/docker/pull-s3-caches.sh
$WORKSPACE/deploy/packaging/rpm/centos/6/rpm.sh --command clean

for build_args in "${BUILD_ARGS_MATRIX[@]}"
do
	export BUILD_ARGS="$build_args"
	export MVN_BUILD_AND_TEST_CMD="mvn install $SKIP_TESTS $BUILD_ARGS"

	sudo docker run --rm \
		-e WORKSPACE=/usr/src/geowave \
		-e BUILD_ARGS="$build_args" \
		-e MAVEN_OPTS="-Xmx1500m" \
		-v $HOME:/root -v $HOME/geowave:/usr/src/geowave \
		ngageoint/geowave-centos6-java8-build \
		/bin/bash -c \
		"cd \$WORKSPACE && $MVN_BUILD_AND_TEST_CMD && $MVN_PACKAGE_FAT_JARS_CMD"

	sudo docker run --rm \
    	-e WORKSPACE=/usr/src/geowave \
    	-e BUILD_ARGS="$build_args" \
    	-v $HOME/geowave:/usr/src/geowave \
    	ngageoint/geowave-centos6-rpm-build \
    	/bin/bash -c \
    	"cd \$WORKSPACE && deploy/packaging/docker/build-rpm.sh"
done
sudo chown -R $(whoami) $WORKSPACE/deploy/packaging
