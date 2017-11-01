#-------------------------------------------------------------------------------
# Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
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
# This script will create the geowave services rpms
#

# This script runs with a volume mount to $WORKSPACE, this ensures that any signal failure will leave all of the files $WORKSPACE editable by the host  
trap 'chmod -R 777 $WORKSPACE/deploy/packaging/rpm' EXIT
trap 'chmod -R 777 $WORKSPACE/deploy/packaging/rpm && exit' ERR

GEOWAVE_VERSION=$(cat $WORKSPACE/deploy/target/version.txt)
GEOSERVER_VERSION="2.11.2"
TOMCAT_VERSION="8.5.20"

#Make a temp directory and work out of there
mkdir temp
cd temp

#grab the different war files
wget http://sourceforge.net/projects/geoserver/files/GeoServer/2.12.0/geoserver-2.12.0-war.zip
wget http://archive.apache.org/dist/tomcat/tomcat-8/v8.5.20/bin/apache-tomcat-8.5.20.tar.gz
cp $WORKSPACE/services/rest-webapp/target/geowave-service-rest-webapp-$GEOWAVE_VERSION.war .

#get the war files ready
unzip geoserver-2.12.0-war.zip
rm -rf GPL.txt LICENSE.txt geoserver-2.12.0-war.zip target/

# Ensure mounted volume permissions are OK for access
chown -R root:root $WORKSPACE/deploy/packaging/rpm

#Grab the tomcat tarball
tar xzf apache-tomcat-8.5.20.tar.gz && mv apache-tomcat-8.5.20 tomcat8
#WHICH ONE OF THESE WILL BE UPLOADED 
fpm -s dir -t rpm -n 'tomcat8' -v $TOMCAT_VERSION -a 'noarch' \
    -p tomcat8.rpm --rpm-os linux --license "Apache Version 2.0" \
    -d java-1.8.0-openjdk.x86_64 \
    -vendor apache --description "Apache Tomcat is an open source software implementation of the Java Servlet and JavaServer Pages technologies." \
    -url "http://tomcat.apache.org/" --directories /opt/apache-tomcat/tomcat8 \
    --post-install fpm_scripts/tomcat8_post_install.sh \
    --pre-uninstall fpm_scripts/tomcat8_pre_uninstall.sh \
    --post-uninstall fpm_scripts/tomcat8_post_uninstall.sh \
    fpm_scripts/tomcat8.service=/etc/systemd/system/tomcat.service \
    tomcat8/=/opt/apache-tomcat/tomcat8/

fpm -s dir -t rpm -n 'geoserver' -v $GEOSERVER_VERSION -a 'noarch' \
    -p geoserver.rpm --rpm-os linux --license "GNU General Public License Version 2.0" \
    -d tomcat8 \
    --vendor geoserver --description "GeoServer is an open source server for sharing geospatial data." \
    --url "https://geoserver.org/" --prefix /opt/apache-tomcat/tomcat8/webapps geoserver.war

fpm -s dir -t rpm -n "geowave-services-$GEOWAVE_VERSION" -v $GEOWAVE_VERSION -a 'noarch' \
    -p geowave-services-$GEOWAVE_VERSION-$TIME_TAG.rpm --rpm-os linux --license "Apache Version 2.0" \
    -d tomcat8 \
    --vendor geowave --description "Geowave rest services rpm. This deploys the Geowave services WAR file to the Tomcat server." \
    --url "https://locationtech.github.io/geowave" --prefix /opt/apache-tomcat/tomcat8/webapps geowave.war

#clean up - not needed though because this is a container
rm -rf tomcat8
rm -rf apache-tomcat-8.5.20.tar.gz
