/usr/local/geowave/geoserver/logs/*.log {
    compress
    copytruncate
    dateext
    size=+1k
    notifempty
    missingok
    create  644
}
