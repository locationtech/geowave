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
  VENDOR_VERSION=$( cat /usr/local/geowave/tools/geowave-tools-build.properties | grep -oi "vendor.version=\w*" | sed "s/vendor.version=//g")
  if [[ $VENDOR_VERSION == apache ]]; then
    export HADOOP_HOME=/usr/lib/hadoop
    . $HADOOP_HOME/libexec/hadoop-config.sh
  elif [[ $VENDOR_VERSION == hdp* ]]; then
   export HADOOP_HOME=/usr/hdp/current/hadoop-client
   export HDP_VERSION=$(hdp-select| grep  hadoop-hdfs-namenode| sed "s/hadoop-hdfs-namenode - //g")
    . $HADOOP_HOME/libexec/hadoop-config.sh
   export GEOWAVE_TOOL_JAVA_OPT="$GEOWAVE_TOOL_JAVA_OPT -Dhdp.version=${HDP_VERSION}"
  elif [[ $VENDOR_VERSION == cdh* ]]; then
    export HADOOP_HOME=/usr/lib/hadoop
    . $HADOOP_HOME/libexec/hadoop-config.sh
   else
    echo "Unknow Hadoop Distribution. Set env variable HADOOP_HOME."
   fi
else
  echo "hadoop set home=$HADOOP_HOME"
  . $HADOOP_HOME/libexec/hadoop-config.sh
fi

# Setting up Spark env
if [ -z "$SPARK_HOME" ]; then
  VENDOR_VERSION=$( cat $GEOWAVE_TOOLS_HOME/geowave-tools-build.properties | grep -oi "vendor.version=\w*" | sed "s/vendor.version=//g")
  if [[ $VENDOR_VERSION == apache ]]; then
    export SPARK_HOME=/usr/lib/spark
    . "${SPARK_HOME}"/bin/load-spark-env.sh
  elif [[ $VENDOR_VERSION == hdp* ]]; then
    export SPARK_HOME=/usr/hdp/current/spark2-client
    . "${SPARK_HOME}"/bin/load-spark-env.sh
  elif [[ $VENDOR_VERSION == cdh* ]]; then
    export SPARK_HOME=/usr/lib/spark
    . "${SPARK_HOME}"/bin/load-spark-env.sh
  else
    echo "Unknow Hadoop Distribution. Set env variable SPARK_HOME."
  fi
else
  . "${SPARK_HOME}"/bin/load-spark-env.sh
fi

# Ensure both our tools jar and anything in the plugins directory is on the classpath
# Add Spark jars to class path only if SPARK_HOME directory exists
if [ -d "${SPARK_HOME}" ]; then
  CLASSPATH="$GEOWAVE_TOOLS_HOME/$GEOWAVE_TOOLS_JAR:$GEOWAVE_TOOLS_HOME/plugins/*:${SPARK_HOME}/conf:${SPARK_HOME}/jars/*:${CLASSPATH}"
else
  CLASSPATH="$GEOWAVE_TOOLS_HOME/$GEOWAVE_TOOLS_JAR:$GEOWAVE_TOOLS_HOME/plugins/*:${CLASSPATH}"
fi

# Using -cp and the classname instead of -jar because Java 7 and below fail to auto-launch jars with more than 65k files
exec $JAVA $GEOWAVE_TOOL_JAVA_OPT -cp $CLASSPATH mil.nga.giat.geowave.core.cli.GeoWaveMain "$@"
