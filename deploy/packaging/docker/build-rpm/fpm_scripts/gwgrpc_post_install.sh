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
