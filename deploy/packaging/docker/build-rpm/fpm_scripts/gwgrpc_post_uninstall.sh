#!/bin/bash

# Remove SystemD Files
rm -rf /etc/geowave/gwgrpc
rm -rf /etc/systemd/system/gwgrpc.service
rm -rf /etc/rsyslog.d/gwgrpc.conf
rm -rf /etc/logrotate.d/gwgrpc
