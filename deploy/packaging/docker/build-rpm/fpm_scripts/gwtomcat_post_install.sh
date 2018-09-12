#-------------------------------------------------------------------------------
# Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
# 
# See the NOTICE file distributed with this work for additional
# information regarding copyright ownership.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License,
# Version 2.0 which accompanies this distribution and is available at
# http://www.apache.org/licenses/LICENSE-2.0.txt
#-------------------------------------------------------------------------------
GEOWAVE_DIR="/usr/local/geowave"
#make sure correct permissions are in place
chown -R geowave:geowave ${GEOWAVE_DIR}/tomcat8

#change settings on service script
chmod 755 /etc/init.d/gwtomcat
chown root:root /etc/init.d/gwtomcat

#Removing class path spam when starting and shutting down
sed -e /"Using CLASSPATH:"/d -i ${GEOWAVE_DIR}/tomcat8/bin/catalina.sh
