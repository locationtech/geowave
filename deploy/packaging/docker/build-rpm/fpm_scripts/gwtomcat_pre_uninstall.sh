#-------------------------------------------------------------------------------
# Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
# 
# See the NOTICE file distributed with this work for additional
# information regarding copyright ownership.
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Apache License,
# Version 2.0 which accompanies this distribution and is available at
# http://www.apache.org/licenses/LICENSE-2.0.txt
#-------------------------------------------------------------------------------
#Check if the service is running before removing it
PROCESS_NAME=gwtomcat
pidfile=${PIDFILE-/var/run/${PROCESS_NAME}.pid};
PID=$(cat ${pidfile})
if [[ (-n ${PID}) && ($PID -gt 0) ]]; then
  service ${PROCESS_NAME} stop
  sleep 1
fi

