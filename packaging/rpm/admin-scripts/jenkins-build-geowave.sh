#!/bin/bash
#
# GeoWave Jenkins Build Script
#
# In the Execute Shell block before calling this script set the versions
# export BUILD_ARGS=" -Daccumulo.version=1.6.0 -Dhadoop.version=2.3.0-cdh5.0.3 -Dhadoop.version.mr=2.3.0-cdh5.0.3 -Dgeotools.version=11.2 -Dgeoserver.version=2.5.2"

# Build the various artifacts
cd geowave-deploy
mvn package -P geotools-container-singlejar $BUILD_ARGS
mv $WORKSPACE/geowave-deploy/target/*-geoserver-singlejar.jar $WORKSPACE/geowave-deploy/target/geowave-geoserver.jar

mvn package -P accumulo-container-singlejar $BUILD_ARGS
mv $WORKSPACE/geowave-deploy/target/*-accumulo-singlejar.jar $WORKSPACE/geowave-deploy/target/geowave-accumulo.jar

cd $WORKSPACE/geowave-types
mvn package -Pingest-singlejar $BUILD_ARGS
mv $WORKSPACE/geowave-types/target/*-ingest-tool.jar $WORKSPACE/geowave-types/target/geowave-ingest-tool.jar

# Text version of man pages for inclusion into HTML/PDF docs
for file in `ls $WORKSPACE/docs/content/manpages/*.adoc`; do
  a2x -f text $file -D $WORKSPACE/docs/content/manpages/
done
# Build the docs
mvn clean install javadoc:aggregate -DskipITs=true -DskipTests=true
mvn -P docs -pl docs install
pushd $WORKSPACE/target/site
cp -r $WORKSPACE/docs/content/manpages/*.adoc $WORKSPACE/target/site/manpages
tar czf ../site.tar.gz *
popd
# Copy over the puppet scripts
tar -cvzf $WORKSPACE/geowave-deploy/target/puppet-scripts.tar.gz -C $WORKSPACE/packaging/puppet geowave
