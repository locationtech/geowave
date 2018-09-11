mkdir -p /usr/local/geowave/grpc/logs

if [ -d "/etc/logrotate.d" ]; then
FILE="/etc/logrotate.d/geowave-grpc"

/bin/cat <<EOM >$FILE
/usr/local/geowave/grpc/logs/*.log {
    compress
    copytruncate
    dateext
    size=+1k
    notifempty
    missingok
    create  644
}
EOM

fi
	
