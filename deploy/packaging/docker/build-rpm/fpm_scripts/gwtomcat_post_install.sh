#make sure correct permissions are in place
chown -R geowave:geowave /usr/local/geowave/tomcat8

#change settings on service script
chmod 755 /etc/init.d/gwtomcat
chown root:root /etc/init.d/gwtomcat
