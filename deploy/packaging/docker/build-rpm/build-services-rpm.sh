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
GEOWAVE_DIR="/usr/local/geowave-${GEOWAVE_VERSION}-${VENDOR_VERSION}"
GEOSERVER_VERSION=$(cat $WORKSPACE/deploy/target/geoserver_version.txt)

echo "---------------------------------------------------------------"
echo "      Building Services RPMS with the following settings"
echo "---------------------------------------------------------------"
echo "GEOWAVE_VERSION=${GEOWAVE_VERSION}"
echo "GEOSERVER_VERSION=${GEOSERVER_VERSION}"
echo "TIME_TAG=${TIME_TAG}"
echo "BUILD_ARGS=${BUILD_ARGS}"
echo "VENDOR_VERSION=${VENDOR_VERSION}"
echo "---------------------------------------------------------------"

set -x
#Make a tmp directory and work out of there
if [ ! -d 'services_tmp' ]; then
  mkdir services_tmp
fi
cd services_tmp

#grab the geoserver war file and tomcat tarball
#Check if the files already exists before grabbing them
if [ ! -f geoserver-$GEOSERVER_VERSION-war.zip ]; then
  echo "Downloading geoserver-$GEOSERVER_VERSION-war"
  if [[ $(curl -I --write-out %{http_code} --silent --output /dev/null  https://s3.amazonaws.com/geowave/third-party-downloads/geoserver/geoserver-$GEOSERVER_VERSION-war.zip) == 200 ]]; then
    echo "Downloading from Geoserver Bucket"
    wget -q https://s3.amazonaws.com/geowave/third-party-downloads/geoserver/geoserver-$GEOSERVER_VERSION-war.zip
  else
    echo "Downloading from Geoserver.org"
    wget -q https://build.geoserver.org/geoserver/release/$GEOSERVER_VERSION/geoserver-$GEOSERVER_VERSION-war.zip
    aws s3 cp geoserver-$GEOSERVER_VERSION-war.zip s3://geowave/third-party-downloads/geoserver/geoserver-$GEOSERVER_VERSION-war.zip
  fi
fi

if [ ! -f apache-tomcat-8.5.20.tar.gz ]; then
  echo "Downloading tomcat-8.5.20"
  wget -q https://s3.amazonaws.com/geowave/third-party-downloads/tomcat/apache-tomcat-8.5.20.tar.gz
  tar xzf apache-tomcat-8.5.20.tar.gz && mv apache-tomcat-8.5.20 tomcat8


  #Prep the tomcat8 directory for packaging
  rm -rf tomcat8/webapps/*

  #put in root page redirect
  mkdir tomcat8/webapps/ROOT
  echo "<% response.sendRedirect(\"/geoserver\"); %>" > tomcat8/webapps/ROOT/index.jsp

fi

#Check if the RPM directory exists. If not create it
DIRECTORY="$WORKSPACE/${ARGS[buildroot]}/RPM/${ARGS[arch]}"
if [ ! -d $DIRECTORY ]; then
  mkdir -p $WORKSPACE/${ARGS[buildroot]}/RPM/${ARGS[arch]}
fi

# Ensure mounted volume permissions are OK for access
chmod -R 777 $WORKSPACE/deploy

echo "Creating tomcat rpm"
#Create the gwtomcat_tools.sh script
cp ${FPM_SCRIPTS}/gwtomcat_tools.sh.template ${FPM_SCRIPTS}/gwtomcat_tools.sh
sed -i -e s/GEOWAVE_VERSION=\"temp\"/GEOWAVE_VERSION=\"${GEOWAVE_VERSION}\"/g ${FPM_SCRIPTS}/gwtomcat_tools.sh
sed -i -e s/VENDOR_VERSION=\"temp\"/VENDOR_VERSION=\"${VENDOR_VERSION}\"/g ${FPM_SCRIPTS}/gwtomcat_tools.sh

fpm -s dir -t rpm -n "geowave-${GEOWAVE_VERSION}-${VENDOR_VERSION}-gwtomcat" -v ${GEOWAVE_VERSION} -a ${ARGS[arch]} \
    -p geowave-${GEOWAVE_VERSION}-${VENDOR_VERSION}-gwtomcat.$TIME_TAG.noarch.rpm --rpm-os linux --license "Apache Version 2.0" \
    -d java-1.8.0-openjdk \
    -d geowave-${GEOWAVE_VERSION}-core \
    -d geowave-${GEOWAVE_VERSION}-${VENDOR_VERSION}-tools \
    --iteration $TIME_TAG \
    --vendor "geowave" \
    --description "Apache Tomcat is an open source software implementation of the Java Servlet and JavaServer Pages technologies." \
    --url "http://tomcat.apache.org/" \
    --directories ${GEOWAVE_DIR}/tomcat8 \
    --post-install ${FPM_SCRIPTS}/gwtomcat_post_install.sh \
    --pre-uninstall ${FPM_SCRIPTS}/gwtomcat_pre_uninstall.sh \
    --post-uninstall ${FPM_SCRIPTS}/gwtomcat_post_uninstall.sh \
    ${FPM_SCRIPTS}/gwtomcat_tools.sh=${GEOWAVE_DIR}/tomcat8/bin/gwtomcat_tools.sh \
    ${FPM_SCRIPTS}/gwtomcat=/etc/init.d/gwtomcat \
    ${FPM_SCRIPTS}/gwtomcat_logrotate=/etc/logrotate.d/gwtomcat \
    tomcat8/=${GEOWAVE_DIR}/tomcat8/

#clean up the tmp scripts and move the rpm to the right place to be indexed
echo "created tomcat rpm"
rm -f ${FPM_SCRIPTS}/gwtomcat_tools.sh
cp geowave-${GEOWAVE_VERSION}-${VENDOR_VERSION}-gwtomcat.$TIME_TAG.noarch.rpm $WORKSPACE/${ARGS[buildroot]}/RPMS/${ARGS[arch]}/geowave-${GEOWAVE_VERSION}-${VENDOR_VERSION}-gwtomcat.${TIME_TAG}.noarch.rpm

#grab the rest services war file
echo "Copy REST Services file"
cp $WORKSPACE/services/rest/target/*${GEOWAVE_VERSION}-${VENDOR_VERSION}.war restservices.war

# Copy accumulo 1.7 restservices war file 
cp $WORKSPACE/services/rest/target/geowave-restservices-${GEOWAVE_VERSION}-${VENDOR_VERSION}-accumulo1.7.war $WORKSPACE/${ARGS[buildroot]}/SOURCES/geowave-restservices-${GEOWAVE_VERSION}-${VENDOR_VERSION}-accumulo1.7.war

#get geoserver the war files ready
#unpack it in tmp dir
unzip -o geoserver-$GEOSERVER_VERSION-war.zip geoserver.war
mkdir tmp && cd tmp
jar -xf ../geoserver.war
rm -rf data/layergroups/*
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
fpm -s dir -t rpm -n "geowave-${GEOWAVE_VERSION}-${VENDOR_VERSION}-gwgeoserver" -v ${GEOWAVE_VERSION} -a ${ARGS[arch]}  \
    -p geowave-${GEOWAVE_VERSION}-${VENDOR_VERSION}-gwgeoserver.$TIME_TAG.noarch.rpm --rpm-os linux --license "GNU General Public License Version 2.0" \
    -d geowave-${GEOWAVE_VERSION}-${VENDOR_VERSION}-gwtomcat \
    --iteration $TIME_TAG \
    --vendor geowave --description "GeoServer is an open source server for sharing geospatial data." \
    --url "https://geoserver.org/" \
    ${FPM_SCRIPTS}/gwgeoserver_logrotate=/etc/logrotate.d/gwgeoserver \
    geoserver.war=${GEOWAVE_DIR}/tomcat8/webapps/geoserver.war

fpm -s dir -t rpm -n "geowave-${GEOWAVE_VERSION}-${VENDOR_VERSION}-restservices" -v ${GEOWAVE_VERSION} -a ${ARGS[arch]} \
    -p geowave-${GEOWAVE_VERSION}-${VENDOR_VERSION}-restservices.$TIME_TAG.noarch.rpm --rpm-os linux --license "Apache Version 2.0" \
    -d geowave-${GEOWAVE_VERSION}-${VENDOR_VERSION}-gwtomcat \
    --iteration $TIME_TAG \
    --vendor geowave --description "Geowave rest services rpm. This deploys the Geowave services WAR file to the Tomcat server." \
    --url "https://locationtech.github.io/geowave" \
    restservices.war=${GEOWAVE_DIR}/tomcat8/webapps/restservices.war

#Move the rpms to the repo to indexed later
cp geowave-${GEOWAVE_VERSION}-${VENDOR_VERSION}-gwgeoserver.$TIME_TAG.noarch.rpm $WORKSPACE/${ARGS[buildroot]}/RPMS/${ARGS[arch]}/geowave-${GEOWAVE_VERSION}-${VENDOR_VERSION}-gwgeoserver.$TIME_TAG.noarch.rpm
cp geowave-${GEOWAVE_VERSION}-${VENDOR_VERSION}-restservices.$TIME_TAG.noarch.rpm $WORKSPACE/${ARGS[buildroot]}/RPMS/${ARGS[arch]}/geowave-${GEOWAVE_VERSION}-${VENDOR_VERSION}-restservices.$TIME_TAG.noarch.rpm

# Move the restservices war to the repo
cp restservices.war $WORKSPACE/${ARGS[buildroot]}/SOURCES/geowave-restservices-${GEOWAVE_VERSION}-${VENDOR_VERSION}.war

#Clean up tmp files
rm -rf geoserver.war
rm -rf restservices.war

#Go back to where we started from
cd $WORKSPACE
