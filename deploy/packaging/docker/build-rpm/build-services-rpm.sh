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
set -e

# Set a default version
VENDOR_VERSION=apache

if [ ! -z "$BUILD_ARGS" ]; then
	VENDOR_VERSION=$(echo "$BUILD_ARGS" | grep -oi "vendor.version=\w*" | sed "s/vendor.version=//g")
fi

declare -A ARGS
while [ $# -gt 0 ]; do
  # Trim the first two chars off of the arg name ex: --foo
  case "$1" in
    *) NAME="${1:2}"; shift; ARGS[$NAME]="$1" ;;
  esac
  shift
done

GEOWAVE_VERSION=$(cat $WORKSPACE/deploy/target/version.txt)
FPM_SCRIPTS="${WORKSPACE}/deploy/packaging/docker/build-rpm/fpm_scripts"
GEOWAVE_DIR="/usr/local/geowave"
GEOSERVER_VERSION="2.12.0"

#Make a tmp directory and work out of there
if [ ! -d 'services_tmp' ]; then
  mkdir services_tmp
fi
cd services_tmp

#grab the geoserver war file and tomcat tarball
#Check if the files already exists before grabbing them
if [ ! -f geoserver-2.12.0-war.zip ]; then
  echo "Downloading geoserver-2.12.0-war"
  wget -q http://sourceforge.net/projects/geoserver/files/GeoServer/2.12.0/geoserver-2.12.0-war.zip
fi

if [ ! -f apache-tomcat-8.5.20.tar.gz ]; then
  echo "Downloading tomcat-8.5.20"
  wget -q http://archive.apache.org/dist/tomcat/tomcat-8/v8.5.20/bin/apache-tomcat-8.5.20.tar.gz
  tar xzf apache-tomcat-8.5.20.tar.gz && mv apache-tomcat-8.5.20 tomcat8
fi

#Check if the RPM directory exists. If not create it
DIRECTORY="$WORKSPACE/${ARGS[buildroot]}/RPM/${ARGS[arch]}"
if [ ! -d $DIRECTORY ]; then
  mkdir -p $WORKSPACE/${ARGS[buildroot]}/RPM/${ARGS[arch]}
fi

# Ensure mounted volume permissions are OK for access
chmod -R 777 $WORKSPACE/deploy

if [ ${ARGS[build]} = "tomcat" ]; then
  set -x
  echo "Creating tomcat rpm"
  fpm -s dir -t rpm -n "geowave-${GEOWAVE_VERSION}-gwtomcat8" -v $GEOWAVE_VERSION -a ${ARGS[arch]} \
      -p geowave-${GEOWAVE_VERSION}-gwtomcat8.$TIME_TAG.noarch.rpm --rpm-os linux --license "Apache Version 2.0" \
      -d java-1.8.0-openjdk.x86_64 \
      -d geowave-${GEOWAVE_VERSION}-core \
      --vendor "apache" \
      --description "Apache Tomcat is an open source software implementation of the Java Servlet and JavaServer Pages technologies." \
      --url "http://tomcat.apache.org/" \
      --directories ${GEOWAVE_DIR}/tomcat8 \
      --post-install ${FPM_SCRIPTS}/gwtomcat8_post_install.sh \
      --pre-uninstall ${FPM_SCRIPTS}/gwtomcat8_pre_uninstall.sh \
      --post-uninstall ${FPM_SCRIPTS}/gwtomcat8_post_uninstall.sh \
      ${FPM_SCRIPTS}/gwtomcat8.service=/etc/systemd/system/gwtomcat8.service \
      tomcat8/=${GEOWAVE_DIR}/tomcat8/
  echo "created tomcat rpm"
  cp geowave-${GEOWAVE_VERSION}-gwtomcat8.$TIME_TAG.noarch.rpm $WORKSPACE/${ARGS[buildroot]}/RPMS/${ARGS[arch]}/geowave-${GEOWAVE_VERSION}-gwtomcat8.${TIME_TAG}.noarch.rpm
fi

if [ ${ARGS[build]} = "services" ]; then
  set -x
  #grab the rest services war file
  cp $WORKSPACE/services/rest-webapp/target/*${GEOWAVE_VERSION}-${VENDOR_VERSION}.war restservices.war

  #get geoserver the war files ready
  #unpack it in tmp dir
  unzip -o geoserver-2.12.0-war.zip geoserver.war
  mkdir tmp && cd tmp
  jar -xf ../geoserver.war
  rm -rf data/layergropus/*
  rm -rf data/workspaces/*
  mkdir data/workspaces/geowave
  cp $WORKSPACE/${ARGS[buildroot]}/SOURCES/geowave-geoserver-${GEOWAVE_VERSION}-${VENDOR_VERSION}.jar WEB-INF/lib/
  cp $WORKSPACE/${ARGS[buildroot]}/SOURCES/default.xml data/workspaces/
  cp $WORKSPACE/${ARGS[buildroot]}/SOURCES/namespace.xml data/workspaces/geowave/
  cp $WORKSPACE/${ARGS[buildroot]}/SOURCES/workspace.xml data/workspaces/geowave/

  #package the war file
  jar -cf geoserver.war *
  mv geoserver.war ../
  cd ..
  rm -rf tmp
  echo "Creating Geoserver and services rpm"
  fpm -s dir -t rpm -n "geowave-${GEOWAVE_VERSION}-${VENDOR_VERSION}-gwgeoserver" -v $GEOSERVER_VERSION -a ${ARGS[arch]}  \
      -p geowave-${GEOWAVE_VERSION}-${VENDOR_VERSION}-gwgeoserver.$TIME_TAG.noarch.rpm --rpm-os linux --license "GNU General Public License Version 2.0" \
      -d geowave-${GEOWAVE_VERSION}-gwtomcat8 \
      --vendor geoserver --description "GeoServer is an open source server for sharing geospatial data." \
      --url "https://geoserver.org/" --prefix ${GEOWAVE_DIR}/tomcat8/webapps geoserver.war

  fpm -s dir -t rpm -n "geowave-${GEOWAVE_VERSION}-${VENDOR_VERSION}-restservices" -v $GEOWAVE_VERSION -a ${ARGS[arch]} \
      -p geowave-${GEOWAVE_VERSION}-${VENDOR_VERSION}-restservices.$TIME_TAG.noarch.rpm --rpm-os linux --license "Apache Version 2.0" \
      -d geowave-${GEOWAVE_VERSION}-gwtomcat8 \
      --vendor geowave --description "Geowave rest services rpm. This deploys the Geowave services WAR file to the Tomcat server." \
      --url "https://locationtech.github.io/geowave" --prefix ${GEOWAVE_DIR}/tomcat8/webapps restservices.war

  #Move the rpms to the repo to indexed later
  cp geowave-${GEOWAVE_VERSION}-${VENDOR_VERSION}-gwgeoserver.$TIME_TAG.noarch.rpm $WORKSPACE/${ARGS[buildroot]}/RPMS/${ARGS[arch]}/geowave-${GEOWAVE_VERSION}-${VENDOR_VERSION}-gwgeoserver.$TIME_TAG.noarch.rpm
  cp geowave-${GEOWAVE_VERSION}-${VENDOR_VERSION}-restservices.$TIME_TAG.noarch.rpm $WORKSPACE/${ARGS[buildroot]}/RPMS/${ARGS[arch]}/geowave-${GEOWAVE_VERSION}-${VENDOR_VERSION}-restservices.$TIME_TAG.noarch.rpm
  rm -rf geoserver.war
fi

#Go back to where we started from
cd $WORKSPACE
