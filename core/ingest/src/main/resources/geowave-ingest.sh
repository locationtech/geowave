#!/bin/bash

# Which java to use
if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi

# Using -cp and the classname instead of -jar because Java 7 and below fail to auto-launch jars with more than 65k files
exec $JAVA -cp /usr/local/geowave/ingest/geowave-ingest-tool.jar mil.nga.giat.geowave.ingest.IngestMain $@
