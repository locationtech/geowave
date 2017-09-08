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

# Ensure both our tools jar and anything in the plugins directory is on the classpath
CLASSPATH="$GEOWAVE_TOOLS_HOME/$GEOWAVE_TOOLS_JAR:$GEOWAVE_TOOLS_HOME/plugins/*"

# Using -cp and the classname instead of -jar because Java 7 and below fail to auto-launch jars with more than 65k files
exec $JAVA $GEOWAVE_TOOL_JAVA_OPT -cp $CLASSPATH mil.nga.giat.geowave.core.cli.GeoWaveMain "$@"
