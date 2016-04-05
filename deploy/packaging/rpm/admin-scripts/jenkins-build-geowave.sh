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

# Throughout the build, capture jace artifacts to support testing
mkdir -p $WORKSPACE/deploy/target/geowave-jace/bin
cp $WORKSPACE/extensions/formats/geotools-vector/target/*.jar $WORKSPACE/deploy/target/geowave-jace/bin/geowave-format-vector.jar
cp $WORKSPACE/examples/target/*.jar $WORKSPACE/deploy/target/geowave-jace/bin/geowave-example.jar

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

# Copy the tools fat jar
cp $WORKSPACE/deploy/target/geowave-tools.jar $WORKSPACE/deploy/target/geowave-jace/bin/geowave-tools.jar

# Run the Jace hack
cd $WORKSPACE
chmod +x $WORKSPACE/.utility/maven-jace-hack.sh
$WORKSPACE/.utility/maven-jace-hack.sh

cd $WORKSPACE/deploy
# Build the jace bindings
if [ ! -f $WORKSPACE/deploy/target/jace-source.tar.gz ]; then
    mvn package -P generate-geowave-jace $BUILD_ARGS "$@"
    mv $WORKSPACE/deploy/target/geowave-deploy*-jace.jar $WORKSPACE/deploy/target/geowave-jace/bin/geowave-runtime.jar
    cp $WORKSPACE/deploy/jace/CMakeLists.txt $WORKSPACE/deploy/target/geowave-jace
    cp -R $WORKSPACE/deploy/target/dependency/jace/source $WORKSPACE/deploy/target/geowave-jace
    cp -R $WORKSPACE/deploy/target/dependency/jace/include $WORKSPACE/deploy/target/geowave-jace
    tar -czf $WORKSPACE/deploy/target/geowave-jace.tar.gz -C $WORKSPACE/deploy/target/ geowave-jace
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
