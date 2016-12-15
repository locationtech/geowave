#!/bin/bash
#
# GeoWave Vendor-specific Build Script
#

# This script runs with a volume mount to $WORKSPACE, this ensures that any signal failure will leave all of the files $WORKSPACE editable by the host  
trap 'chmod -R 777 $WORKSPACE' EXIT

GEOWAVE_VERSION=$(mvn -q -o -Dexec.executable="echo" -Dexec.args='${project.version}' --non-recursive -f $WORKSPACE/pom.xml exec:exec | sed -e 's/"//g' -e 's/-SNAPSHOT//g')
# Set a default version
VENDOR_VERSION=apache

if [[ ! -z "$BUILD_ARGS" ]]; then
	VENDOR_VERSION=$(echo "$BUILD_ARGS" | grep -oi "vendor.version=\w*" | sed "s/vendor.version=//g")
fi
echo "---------------------------------------------------------------"
echo "  Building GeoWave Vendor-specific with the following settings"
echo "---------------------------------------------------------------"
echo "GEOWAVE_VERSION=${GEOWAVE_VERSION}"
echo "VENDOR_VERSION=${VENDOR_VERSION}"
echo "BUILD_ARGS=${BUILD_ARGS} ${@}"
echo "---------------------------------------------------------------"

# Throughout the build, capture jace artifacts to support testing
mkdir -p $WORKSPACE/deploy/target/geowave-c++/bin

# Build each of the "fat jar" artifacts and rename to remove any version strings in the file name

mvn -o -q package -am -pl deploy -P geotools-container-singlejar -Dgeotools.finalName=geowave-geoserver-${GEOWAVE_VERSION}-${VENDOR_VERSION} $BUILD_ARGS "$@"

mvn -o -q package -am -pl deploy -P accumulo-container-singlejar -Daccumulo.finalName=geowave-accumulo-${GEOWAVE_VERSION}-${VENDOR_VERSION} $BUILD_ARGS "$@"

mvn -o -q package -am -pl deploy -P hbase-container-singlejar -Dhbase.finalName=geowave-hbase-${GEOWAVE_VERSION}-${VENDOR_VERSION} $BUILD_ARGS "$@"

mvn -o -q package -am -pl deploy -P geowave-tools-singlejar -Dtools.finalName=geowave-tools-${GEOWAVE_VERSION}-${VENDOR_VERSION} $BUILD_ARGS "$@"

# Copy the tools fat jar
cp $WORKSPACE/deploy/target/geowave-tools-${GEOWAVE_VERSION}-${VENDOR_VERSION}.jar $WORKSPACE/deploy/target/geowave-c++/bin/geowave-tools-${GEOWAVE_VERSION}-${VENDOR_VERSION}.jar

# Run the Jace hack
cd $WORKSPACE
chmod +x $WORKSPACE/deploy/packaging/docker/build-src/install-jace.sh
$WORKSPACE/deploy/packaging/docker/build-src/install-jace.sh $BUILD_ARGS "$@"

cd $WORKSPACE
# Build the jace bindings
if [[ ! -f $WORKSPACE/deploy/target/geowave-c++-${GEOWAVE_VERSION}-${VENDOR_VERSION}.tar.gz ]]; then
	rm -rf $WORKSPACE/deploy/target/dependency
	mvn -o -q package -am -pl deploy -P generate-geowave-jace -Djace.finalName=geowave-runtime-${GEOWAVE_VERSION}-${VENDOR_VERSION} $BUILD_ARGS "$@"
    mv $WORKSPACE/deploy/target/geowave-runtime-${GEOWAVE_VERSION}-${VENDOR_VERSION}.jar $WORKSPACE/deploy/target/geowave-c++/bin/geowave-runtime-${GEOWAVE_VERSION}-${VENDOR_VERSION}.jar
    cp $WORKSPACE/deploy/jace/CMakeLists.txt $WORKSPACE/deploy/target/geowave-c++
    cp -R $WORKSPACE/deploy/target/dependency/jace/source $WORKSPACE/deploy/target/geowave-c++
    cp -R $WORKSPACE/deploy/target/dependency/jace/include $WORKSPACE/deploy/target/geowave-c++
	tar -czf $WORKSPACE/deploy/target/geowave-c++-${GEOWAVE_VERSION}-${VENDOR_VERSION}.tar.gz -C $WORKSPACE/deploy/target/ geowave-c++
fi
