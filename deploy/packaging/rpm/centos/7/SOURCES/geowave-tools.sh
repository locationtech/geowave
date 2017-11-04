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

# Which java to use
if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi

# Setting up Hadoop env
if [ -z "$HADOOP_HOME" ]; then
  VENDOR_VERSION=$( cat $GEOWAVE_TOOLS_HOME/geowave-tools-build.properties | grep -oi "vendor.version=\w*" | sed "s/vendor.version=//g")
  if [[ $VENDOR_VERSION == apache ]]; then
    export HADOOP_HOME=/usr/lib/hadoop
  elif [[ $VENDOR_VERSION == hdp* ]]; then
    export HADOOP_HOME=/usr/hdp/current/hadoop-client
    export HDP_VERSION=$(hdp-select| grep  hadoop-hdfs-namenode| sed "s/hadoop-hdfs-namenode - //g")
    export GEOWAVE_TOOL_JAVA_OPT="$GEOWAVE_TOOL_JAVA_OPT -Dhdp.version=${HDP_VERSION}"
  elif [[ $VENDOR_VERSION == cdh* ]]; then
    export HADOOP_HOME=/usr/lib/hadoop
  else
    echo "Unknown Hadoop Distribution. Set env variable HADOOP_HOME."
  fi
fi


# set up HADOOP specific env only if HADOOP is installed
if [ -n "${HADOOP_HOME}" ] && [ -d "${HADOOP_HOME}" ]; then
     . $HADOOP_HOME/libexec/hadoop-config.sh
     HADOOP_CLASSPATH=""
     for i in $(echo $CLASSPATH | sed "s/:/ /g")
     do
       if [[ "$i" != *slf4j-log4j*.jar ]]; then
         HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:$i
       fi
     done
fi

CLASSPATH=${HADOOP_CLASSPATH}

# Setting up Spark env
if [ -z "$SPARK_HOME" ]; then
  VENDOR_VERSION=$( cat $GEOWAVE_TOOLS_HOME/geowave-tools-build.properties | grep -oi "vendor.version=\w*" | sed "s/vendor.version=//g")
  if [[ $VENDOR_VERSION == apache ]]; then
    export SPARK_HOME=/usr/lib/spark
  elif [[ $VENDOR_VERSION == hdp* ]]; then
    export SPARK_HOME=/usr/hdp/current/spark2-client
  elif [[ $VENDOR_VERSION == cdh* ]]; then
    export SPARK_HOME=/usr/lib/spark
  else
    echo "Unknown Spark Distribution. Set env variable SPARK_HOME."
  fi
fi

# Ensure both our tools jar and anything in the plugins directory is on the classpath
# Add Spark jars to class path only if SPARK_HOME directory exists
if [ -n "${SPARK_HOME}" ] && [ -d "${SPARK_HOME}" ]; then
  . "${SPARK_HOME}"/bin/load-spark-env.sh
  SPARK_CLASSPATH=""
  for i in $(ls ${SPARK_HOME}/jars/* )
  do
     if [[ "$i" != *slf4j-log4j*.jar ]]; then
       SPARK_CLASSPATH=${SPARK_CLASSPATH}:$i
     fi
  done  

  CLASSPATH="$GEOWAVE_TOOLS_HOME/$GEOWAVE_TOOLS_JAR:$GEOWAVE_TOOLS_HOME/plugins/*:${SPARK_HOME}/conf:${SPARK_CLASSPATH}:${CLASSPATH}"

else
  CLASSPATH="$GEOWAVE_TOOLS_HOME/$GEOWAVE_TOOLS_JAR:$GEOWAVE_TOOLS_HOME/plugins/*:${CLASSPATH}"
fi


# Using -cp and the classname instead of -jar because Java 7 and below fail to auto-launch jars with more than 65k files
exec $JAVA $GEOWAVE_TOOL_JAVA_OPT -cp $CLASSPATH mil.nga.giat.geowave.core.cli.GeoWaveMain "$@"
