#!/usr/bin/env bash
#
# Bootstrap a GeoWave cluster node
#

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# 
# Config Settings you might want to update

# Accumulo
USER=accumulo
# NOTE: This password, the Accumulo instance secret and the geoserver password are left at
# The default settings. The default EMR Security group setting only allows ssh/22 open to
# external access so access to internal consoles and web UIs has to be done over SSH.
# At some point in the future when this is revisited remember that nodes can be added to an
# EMR at any point after creation so the password set during the initial spin-up would have
# to be persisted somewhere and provided to the newly created nodes at some later date.
USERPW=secret # TODO: Can't change until trace.password in accumulo-site.xml is updated
ACCUMULO_VERSION=1.6.4
ACCUMULO_TSERVER_OPTS=3GB
INSTALL_DIR=/opt

# GeoWave
GEOWAVE_REPO_RPM=geowave-repo-dev-1.0-3.noarch.rpm # TODO: Should have a prod->latest rpm
GEOWAVE_VERSION='0.9.0'
GEOSERVER_PORT='8000'
GEOSERVER_MEMORY="-Xmx512m -XX:MaxPermSize=128m"

# Java JAI and ImageIO URLS
JAI_URL=http://data.opengeo.org/suite/jai/jai-1_1_3-lib-linux-amd64-jdk.bin
IMAGEIO_URL=http://data.opengeo.org/suite/jai/jai_imageio-1_1-lib-linux-amd64-jdk.bin
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Step #1: I've externalized commands into library functions for clarity, download and source
if [ ! -f /tmp/geowave-install-lib.sh ]; then
	aws s3 cp s3://geowave/emr/4/geowave-install-lib.sh /tmp/geowave-install-lib.sh
fi
source /tmp/geowave-install-lib.sh

# Step #2: The EMR customize hooks run _before_ everything else, so Hadoop is not yet ready
THIS_SCRIPT="$(realpath "${BASH_SOURCE[0]}")"
RUN_FLAG="${THIS_SCRIPT}.run"
# On first boot skip past this script to allow EMR to set up the environment. Set a callback
# which will poll for availability of HDFS and then install Accumulo and then GeoWave
if [ ! -f "$RUN_FLAG" ]; then
	touch "$RUN_FLAG"
	TIMEOUT= is_master && TIMEOUT=3 || TIMEOUT=4
	echo "bash -x $(realpath "${BASH_SOURCE[0]}") > /tmp/geowave-install.log" | at now + $TIMEOUT min
	exit 0 # Bail and let EMR finish initializing
fi

# Step #3: Get Accumulo running
os_tweaks && configure_zookeeper
create_accumulo_user && install_accumulo && configure_accumulo

# Step #4: Install imagery libs then GeoWave components on master node
install_image_libs
if is_master ; then
	install_geowave	
fi

# Step #5: Optionally initialize all volumes
if $INIT_VOLUMES ; then
    initialize_volumes
fi
