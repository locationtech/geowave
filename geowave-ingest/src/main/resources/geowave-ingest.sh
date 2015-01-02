#!/bin/bash

# Which java to use
if [ -z "$JAVA_HOME" ]; then
  JAVA="java"
else
  JAVA="$JAVA_HOME/bin/java"
fi

exec $JAVA -jar /usr/local/geowave/ingest/geowave-ingest-tool.jar $@
