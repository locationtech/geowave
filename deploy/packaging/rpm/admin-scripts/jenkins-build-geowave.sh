#!/bin/bash
#
# GeoWave Jenkins Build Script
#
# In the Execute Shell block before calling this script set the versions

# Build the various artifacts
cd $WORKSPACE/geowave-deploy
mvn package -P geotools-container-singlejar $BUILD_ARGS
mv $WORKSPACE/geowave-deploy/target/*-geoserver-singlejar.jar $WORKSPACE/geowave-deploy/target/geowave-geoserver.jar

mvn package -P accumulo-container-singlejar $BUILD_ARGS
mv $WORKSPACE/geowave-deploy/target/*-accumulo-singlejar.jar $WORKSPACE/geowave-deploy/target/geowave-accumulo.jar

cd $WORKSPACE/geowave-types
mvn package -P ingest-singlejar $BUILD_ARGS
mv $WORKSPACE/geowave-types/target/*-ingest-tool.jar $WORKSPACE/geowave-types/target/geowave-ingest-tool.jar

# Build and archive HTML/PDF docs
cd $WORKSPACE/
mvn install javadoc:aggregate -DskipITs=true -DskipTests=true
mvn -P docs -pl docs install
tar -czf $WORKSPACE/target/site.tar.gz -C $WORKSPACE/target site

# Build and archive the man pages
mkdir -p $WORKSPACE/docs/target/{asciidoc,manpages}
cp -fR $WORKSPACE/docs/content/manpages/* $WORKSPACE/docs/target/asciidoc
find $WORKSPACE/docs/target/asciidoc/ -name "*.txt" -exec sed -i "s|//:||" {} \;
find $WORKSPACE/docs/target/asciidoc/ -name "*.txt" -exec a2x -d manpage -f manpage {} -D $WORKSPACE/docs/target/manpages \;
tar -czf $WORKSPACE/docs/target/manpages.tar.gz -C $WORKSPACE/docs/target/manpages/ .

# Copy over the puppet scripts
tar -czf $WORKSPACE/geowave-deploy/target/puppet-scripts.tar.gz -C $WORKSPACE/packaging/puppet geowave
