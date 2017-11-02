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
trap 'chmod -R 777 $WORKSPACE' EXIT
trap 'chmod -R 777 $WORKSPACE && exit' ERR

declare -A ARGS
while [ $# -gt 0 ]; do
  # Trim the first two chars off of the arg name ex: --foo
  case "$1" in
    *) NAME="${1:2}"; shift; ARGS[$NAME]="$1" ;;
  esac
  shift
done

GEOWAVE_VERSION=$(cat $WORKSPACE/deploy/target/version.txt)
BUILD_TYPE=$(cat $WORKSPACE/deploy/target/build-type.txt)
GEOWAVE_DIR="/usr/local/geowave"
GEOSERVER_VERSION="2.11.2"

#Make a tmp directory and work out of there
mkdir services_tmp
cd services_tmp

#grab the geoserver war file and tomcat tarball
#Check if the files already exists before grabbing them
if [ ! -f geoserver-2.12.0-war.zip ]; then
  echo "Downloading geoserver-2.12.0-war"
  wget -q http://sourceforge.net/projects/geoserver/files/GeoServer/2.12.0/geoserver-2.12.0-war.zip
fi

if [ ! -f apache-tomcat-8.5.20.tar.gz ]; then
  wget http://archive.apache.org/dist/tomcat/tomcat-8/v8.5.20/bin/apache-tomcat-8.5.20.tar.gz
  tar xzf apache-tomcat-8.5.20.tar.gz && mv apache-tomcat-8.5.20 tomcat8
fi

#Check if the RPM directory exists. If not create it
DIRECTORY="$WORKSPACE/$ARGS[buildroot]/RPM/$ARGS[arch]"
if [ ! -d $DIRECTORY ]; then
  mkdir -p $WORKSPACE/$ARGS[buildroot]/RPM/$ARGS[arch]
fi

# Ensure mounted volume permissions are OK for access
chown -R root:root $WORKSPACE/deploy/packaging/rpm

if [ $ARGS[build] = "tomcat" ]; then
  #TODO MAKE DEFAULT PORT CONFIGURABLE server.xml "connector port=8080"
  fpm -s dir -t rpm -n "geowave-${GEOWAVE_VERSION}-tomcat8" -v $GEOWAVE_VERSION -a $ARGS[arch] \
      -p geowave-${GEOWAVE_VERSION}-tomcat8.$TIME_TAG.noarch.rpm --rpm-os linux --license "Apache Version 2.0" \
      -d java-1.8.0-openjdk.x86_64 \
      -d geowave-${GEOWAVE_VERSION}-core \ 
      -vendor apache --description "Apache Tomcat is an open source software implementation of the Java Servlet and JavaServer Pages technologies." \
      -url "http://tomcat.apache.org/" --directories ${GEOWAVE_DIR}/tomcat8 \
      --post-install fpm_scripts/gw_tomcat8_post_install.sh \
      --pre-uninstall fpm_scripts/gw_tomcat8_pre_uninstall.sh \
      --post-uninstall fpm_scripts/gw_tomcat8_post_uninstall.sh \
      fpm_scripts/gw_tomcat8.service=/etc/systemd/system/gw_tomcat8.service \
      tomcat8/=${GEOWAVE_DIR}/tomcat8/
  cp geowave-${GEOWAVE_VERSION}-tomcat8.$TIME_TAG.noarch.rpm $WORKSPACE/$ARGS[buildroot])/RPM/$ARGS[arch])/*.rpm
fi

if [ $ARGS[build] = "services" ]; then
  #grab the rest services war file
  cp $WORKSPACE/services/rest-webapp/target/*${GEOWAVE_VERSION}-${VENDOR_VERSION}.war .

  #get geoserver the war files ready
  #unpack it in tmp dir
  unzip geoserver-2.12.0-war.zip geoserver.war
  mkdir tmp && cd tmp
  jar -xf ../geoserver.war
  rm -rf data/layergropus/*
  rm -rf data/workspaces/*
  mkdir data/workspaces/geowave
  cp $WORKSPACE/$ARGS[buildroot]/SOURCES/geowave-geoserver-${GEOWAVE_VERSION}-${VENDOR_VERSION}.jar WEB-INF/lib/
  cp $WORKSPACE/$ARGS[buildroot]/SOURCES/default.xml data/workspaces/
  cp $WORKSPACE/$ARGS[buildroot]/SOURCES/namespace.xml data/workspaces/geowave/
  cp $WORKSPACE/$ARGS[buildroot]/SOURCES/workspace.xml data/workspaces/geowave/

  #package the war file
  jar -cf geoserver.war *
  mv geoserver.war ../
  rm -rf tmp

  fpm -s dir -t rpm -n "geowave-${GEOWAVE_VERSION}-${VENDOR_VERSION}-geoserver" -v $GEOSERVER_VERSION -a $ARGS[arch]  \
      -p geowave-${GEOWAVE_VERSION}-${VENDOR_VERSION}-geoserver.$TIME_TAG.noarch.rpm --rpm-os linux --license "GNU General Public License Version 2.0" \
      -d geowave-${GEOWAVE_VERSION}-tomcat8 \
      --vendor geoserver --description "GeoServer is an open source server for sharing geospatial data." \
      --url "https://geoserver.org/" --prefix ${GEOWAVE_DIR}/tomcat8/webapps geoserver.war

  fpm -s dir -t rpm -n "geowave-${GEOWAVE_VERSION}-${VENDOR_VERSION}-restservices" -v $GEOWAVE_VERSION -a $ARGS[arch] \
      -p geowave-${GEOWAVE_VERSION}-${VENDOR_VERSION}-restservices.$TIME_TAG.noarch.rpm --rpm-os linux --license "Apache Version 2.0" \
      -d geowave-${GEOWAVE_VERSION}-tomcat8 \
      --vendor geowave --description "Geowave rest services rpm. This deploys the Geowave services WAR file to the Tomcat server." \
      --url "https://locationtech.github.io/geowave" --prefix ${GEOWAVE_DIR}/tomcat8/webapps geowave.war

  #Move the rpms to the repo to indexed later
  cp geowave-${GEOWAVE_VERSION}-${VENDOR_VERSION}-restservices.$TIME_TAG.noarch.rpm $WORKSPACE/$ARGS[buildroot])/RPM/$ARGS[arch])/*.rpm
  cp geowave-${GEOWAVE_VERSION}-${VENDOR_VERSION}-geoserver.$TIME_TAG.noarch.rpm $WORKSPACE/$ARGS[buildroot])/RPM/$ARGS[arch])/*.rpm
fi

#clean up
cd $WORKSPACE
rm -rf services_tmp
