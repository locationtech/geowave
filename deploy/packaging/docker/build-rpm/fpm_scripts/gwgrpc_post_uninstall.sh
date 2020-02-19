#-------------------------------------------------------------------------------
# Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
# 
# See the NOTICE file distributed with this work for additional
# information regarding copyright ownership.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License,
# Version 2.0 which accompanies this distribution and is available at
# http://www.apache.org/licenses/LICENSE-2.0.txt
#-------------------------------------------------------------------------------
#!/bin/bash

# Remove SystemD Files
rm -rf /etc/geowave/gwgrpc
rm -rf /etc/systemd/system/gwgrpc.service
rm -rf /etc/rsyslog.d/gwgrpc.conf
rm -rf /etc/logrotate.d/gwgrpc
