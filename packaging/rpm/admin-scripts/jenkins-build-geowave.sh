#!/bin/bash
#
# GeoWave Jenkins Build Script
#
# In the Execute Shell block before calling this script set the versions
# export BUILD_ARGS=" -Daccumulo.version=1.6.0 -Dhadoop.version=2.3.0-cdh5.0.3 -Dhadoop.version.mr=2.3.0-cdh5.0.3 -Dgeotools.version=11.2 -Dgeoserver.version=2.5.2"

# Build the various artifacts
cd $WORKSPACE/geowave-deploy
mvn package -P geotools-container-singlejar $BUILD_ARGS
mv $WORKSPACE/geowave-deploy/target/*-geoserver-singlejar.jar $WORKSPACE/geowave-deploy/target/geowave-geoserver.jar

mvn package -P accumulo-container-singlejar $BUILD_ARGS
mv $WORKSPACE/geowave-deploy/target/*-accumulo-singlejar.jar $WORKSPACE/geowave-deploy/target/geowave-accumulo.jar

cd $WORKSPACE/geowave-types
mvn package -Pingest-singlejar $BUILD_ARGS
mv $WORKSPACE/geowave-types/target/*-ingest-tool.jar $WORKSPACE/geowave-types/target/geowave-ingest-tool.jar

# Build and archive HTML/PDF docs
cd $WORKSPACE/
mvn install javadoc:aggregate -DskipITs=true -DskipTests=true
mvn -P docs -pl docs install
tar -czf $WORKSPACE/target/site.tar.gz -C $WORKSPACE/target site

# Copy over the puppet scripts
tar -czf $WORKSPACE/geowave-deploy/target/puppet-scripts.tar.gz -C $WORKSPACE/packaging/puppet geowave
