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
# GeoWave Common Build Script
#

# This script runs with a volume mount to $WORKSPACE, this ensures that any signal failure will leave all of the files $WORKSPACE editable by the host  
trap 'chmod -R 777 $WORKSPACE' EXIT
trap 'chmod -R 777 $WORKSPACE && exit' ERR

# Get the version
GEOWAVE_VERSION=$(cat $WORKSPACE/deploy/target/version.txt)
BUILD_TYPE=$(cat $WORKSPACE/deploy/target/build-type.txt)
GEOWAVE_RPM_VERSION=$(cat $WORKSPACE/deploy/target/rpm_version.txt)

echo "---------------------------------------------------------------"
echo "         Building GeoWave Common"
echo "---------------------------------------------------------------"
echo "GEOWAVE_VERSION=${GEOWAVE_VERSION}"
echo "INSTALL4J_HOME=${INSTALL4J_HOME}"
echo "BUILD_ARGS=${BUILD_ARGS} ${@}"
echo "---------------------------------------------------------------"
# Build and archive HTML/PDF docs
if [[ ! -f $WORKSPACE/target/site-${GEOWAVE_VERSION}.tar.gz ]]; then
    mvn -q javadoc:aggregate $BUILD_ARGS "$@"
    mvn -q -P pdf,epub,html -pl docs install $BUILD_ARGS "$@"
    tar -czf $WORKSPACE/target/site-${GEOWAVE_VERSION}.tar.gz -C $WORKSPACE/target site
fi

# Build and archive the man pages
if [[ ! -f $WORKSPACE/docs/target/manpages-${GEOWAVE_VERSION}.tar.gz ]]; then
    mkdir -p $WORKSPACE/docs/target/{asciidoc,manpages}
    cp -fR $WORKSPACE/docs/content/commands/manpages/* $WORKSPACE/docs/target/asciidoc
    find $WORKSPACE/docs/target/asciidoc/ -name "*.txt" -exec sed -i "s|//:||" {} \;
    find $WORKSPACE/docs/target/asciidoc/ -name "*.txt" -exec sed -i "s|^====|==|" {} \;
    find $WORKSPACE/docs/target/asciidoc/ -name "*.txt" -exec asciidoctor -d manpage -b manpage {} -D $WORKSPACE/docs/target/manpages \;
    tar -czf $WORKSPACE/docs/target/manpages-${GEOWAVE_VERSION}.tar.gz -C $WORKSPACE/docs/target/manpages/ .
fi
## Copy over the puppet scripts
if [[ ! -f $WORKSPACE/deploy/target/puppet-scripts-${GEOWAVE_VERSION}.tar.gz ]]; then
    tar -czf $WORKSPACE/deploy/target/puppet-scripts-${GEOWAVE_VERSION}.tar.gz -C $WORKSPACE/deploy/packaging/puppet geowave
fi

## Build the pyspark module
if [[ ! -f $WORKSPACE/analytics/pyspark/target/geowave_pyspark-${GEOWAVE_VERSION}.tar.gz ]]; then
    mvn package -am -pl analytics/pyspark -P python -Dpython.executable=python3.6 $BUILD_ARGS "$@"
    if [[ ! -f $WORKSPACE/analytics/pyspark/target/geowave_pyspark-${GEOWAVE_VERSION}.tar.gz ]]; then
      mv $WORKSPACE/analytics/pyspark/target/geowave_pyspark-*.tar.gz $WORKSPACE/analytics/pyspark/target/geowave_pyspark-${GEOWAVE_VERSION}.tar.gz
    fi
fi
if [ -d /opt/install4j7 ]; then
    # Build standalone installer
    echo '###### Building standalone installer'
    mvn -pl '!test' package -P build-installer-plugin $BUILD_ARGS "$@"
    mvn package -pl deploy -P build-installer-main -Dinstall4j.home=/opt/install4j7 $BUILD_ARGS "$@"
fi