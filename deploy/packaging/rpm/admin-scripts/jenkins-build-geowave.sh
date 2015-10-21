#!/bin/bash
#
# GeoWave Jenkins Build Script
#

echo "---------------------------------------------------------------"
echo "         Building GeoWave with the following settings"
echo "---------------------------------------------------------------"
echo "BUILD_ARGS=${BUILD_ARGS} ${@}"
echo "---------------------------------------------------------------"

cd $WORKSPACE/deploy

# Create an archive of all the ingest format plugins
mkdir -p target/plugins >/dev/null 2>&1
pushd target/plugins
find $WORKSPACE/extensions/formats -name "*.jar" -not -path "*/service/target/*" -exec cp {} . \;
tar cvzf ../plugins.tar.gz *.jar
popd

# Build each of the "fat jar" artifacts and rename to remove any version strings in the file name

mvn package -P geotools-container-singlejar $BUILD_ARGS "$@"
mv $WORKSPACE/deploy/target/*-geoserver-singlejar.jar $WORKSPACE/deploy/target/geowave-geoserver.jar

mvn package -P accumulo-container-singlejar $BUILD_ARGS "$@"
mv $WORKSPACE/deploy/target/*-accumulo-singlejar.jar $WORKSPACE/deploy/target/geowave-accumulo.jar

mvn package -P geowave-tools-singlejar $BUILD_ARGS "$@"
mv $WORKSPACE/deploy/target/*-tools.jar $WORKSPACE/deploy/target/geowave-tools.jar

pushd $WORKSPACE/analytics/mapreduce
mvn package -P analytics-singlejar $BUILD_ARGS "$@"
mv $WORKSPACE/analytics/mapreduce/target/munged/geowave-analytic-mapreduce-*-analytics-singlejar.jar $WORKSPACE/deploy/target/geowave-analytic-mapreduce.jar
popd

# Build the jace artifacts (release, debug, source) and include geotools-vector ingest tool to support testing
mkdir -p $WORKSPACE/deploy/target/jace

# Build the test ingest jar
cd $WORKSPACE/extensions/formats/geotools-vector
mvn package -P geowave-tools-singlejar $BUILD_ARGS "$@"
mv $WORKSPACE/extensions/formats/geotools-vector/target/*-tools.jar $WORKSPACE/deploy/target/jace/geowave-ingest.jar

# Run the Jace hack
cd $WORKSPACE
chmod +x $WORKSPACE/.utility/maven-jace-hack.sh
$WORKSPACE/.utility/maven-jace-hack.sh

# Build the debug bindings
cd $WORKSPACE/deploy
if [ ! -f $WORKSPACE/deploy/target/jace-linux-amd64-debug.tar.gz ]; then
    mvn package -P generate-jace-proxies,linux-amd64-gcc-debug $BUILD_ARGS "$@"
    mv $WORKSPACE/deploy/target/*-jace.jar $WORKSPACE/deploy/target/jace/geowave-jace.jar
    tar -czf $WORKSPACE/deploy/target/jace-linux-amd64-debug.tar.gz \
        -C $WORKSPACE/deploy/target/jace geowave-ingest.jar \
        -C $WORKSPACE/deploy/target/jace geowave-jace.jar \
        -C $WORKSPACE/deploy/target/dependency/jace libjace.so \
        -C $WORKSPACE/deploy/target/dependency/jace include
fi

# Build the release bindings
if [ ! -f $WORKSPACE/deploy/target/jace-linux-amd64-release.tar.gz ] || [ ! -f $WORKSPACE/deploy/target/jace-source.tar.gz ]; then
    mvn package -P generate-jace-proxies,linux-amd64-gcc-release $BUILD_ARGS "$@"
    tar -czf $WORKSPACE/deploy/target/jace-linux-amd64-release.tar.gz \
        -C $WORKSPACE/deploy/target/jace geowave-ingest.jar \
        -C $WORKSPACE/deploy/target/jace geowave-jace.jar \
        -C $WORKSPACE/deploy/target/dependency/jace libjace.so \
        -C $WORKSPACE/deploy/target/dependency/jace include

    tar -czf $WORKSPACE/deploy/target/jace-source.tar.gz \
        -C $WORKSPACE/deploy/target/jace geowave-ingest.jar \
        -C $WORKSPACE/deploy/target/jace geowave-jace.jar \
        -C $WORKSPACE/deploy/target/dependency/jace CMakeLists.txt \
        -C $WORKSPACE/deploy/target/dependency/jace source \
        -C $WORKSPACE/deploy/target/dependency/jace include
fi

# Build and archive HTML/PDF docs
cd $WORKSPACE/
if [ ! -f $WORKSPACE/target/site.tar.gz ]; then
    mvn javadoc:aggregate
    mvn -P docs -pl docs install
    tar -czf $WORKSPACE/target/site.tar.gz -C $WORKSPACE/target site
fi

# Build and archive the man pages
if [ ! -f $WORKSPACE/docs/target/manpages.tar.gz ]; then
    mkdir -p $WORKSPACE/docs/target/{asciidoc,manpages}
    cp -fR $WORKSPACE/docs/content/manpages/* $WORKSPACE/docs/target/asciidoc
    find $WORKSPACE/docs/target/asciidoc/ -name "*.txt" -exec sed -i "s|//:||" {} \;
    find $WORKSPACE/docs/target/asciidoc/ -name "*.txt" -exec a2x -d manpage -f manpage {} -D $WORKSPACE/docs/target/manpages \;
    tar -czf $WORKSPACE/docs/target/manpages.tar.gz -C $WORKSPACE/docs/target/manpages/ .
fi

## Copy over the puppet scripts
if [ ! -f $WORKSPACE/deploy/target/puppet-scripts.tar.gz ]; then
    tar -czf $WORKSPACE/deploy/target/puppet-scripts.tar.gz -C $WORKSPACE/deploy/packaging/puppet geowave
fi
