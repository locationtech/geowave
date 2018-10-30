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
#!/bin/bash

# Touch the logfile
touch /var/log/gwgrpc.log

# Set SystemD File Modes
chmod 644 /etc/geowave/gwgrpc
chmod 644 /etc/systemd/system/gwgrpc.service
chmod 644 /etc/logrotate.d/gwgrpc

# Service Permissions
chown geowave:geowave /var/log/gwgrpc.log
chown -R geowave:geowave /usr/local/geowave*
chown -R geowave:geowave /etc/geowave
