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
# For use by geowave jetty server, set if not already set elsewhere
if [ "x" == "x$JAVA_HOME" ]; then
    export JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:bin/java::")
fi
if [ "x" == "x$GEOSERVER_HOME" ]; then
    export GEOSERVER_HOME=/usr/local/geowave/tomcat8/webapps/geoserver
fi
if [ "x" == "x$GEOSERVER_DATA_DIR" ]; then
    export GEOSERVER_DATA_DIR=/usr/local/geowave/tomcat8/webapps/geoserver/data
fi
