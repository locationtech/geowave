GEOWAVE_DIR="/usr/local/geowave"
#make sure correct permissions are in place
chown -R geowave:geowave ${GEOWAVE_DIR}/tomcat8

#change settings on service script
chmod 755 /etc/init.d/gwtomcat
chown root:root /etc/init.d/gwtomcat

#Removing class path spam when starting and shutting down
sed -e /"Using CLASSPATH:"/d -i ${GEOWAVE_DIR}/tomcat8/bin/catalina.sh
