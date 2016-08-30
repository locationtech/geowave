#!/bin/bash
export STAGING_DIR=/mnt
export INSTANCE=accumulo

#PARIS 11/13/2015-11/14/2015
#export TIME_REGEX=2015111[34]
#export EAST=2.63791
#export WEST=2.08679
#export NORTH=49.04694
#export SOUTH=48.658291

#Europe 02/2016
export TIME_REGEX=201602
export EAST=40
export WEST=-31.25
export NORTH=81
export SOUTH=27.6363

export HDFS_PORT=8020
export RESOURCE_MAN_PORT=8032
export NUM_PARTITIONS=32
export GEOSERVER_HOME=/usr/local/geowave/geoserver                             
export GEOSERVER_DATA_DIR=$GEOSERVER_HOME/data_dir
export GEOWAVE_TOOL_JAVA_OPT=-Xmx4g
export GEOWAVE_TOOLS_HOME=/usr/local/geowave/tools
export GEOWAVE_VERSION='0.9.3-SNAPSHOT'
export ACCUMULO_HOME=/opt/accumulo
export PATH=$PATH:${ACCUMULO_HOME}/bin
