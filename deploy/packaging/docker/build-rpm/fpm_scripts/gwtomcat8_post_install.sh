#clean up and empty webapps
rm -rf /usr/local/geowave/tomcat8/webapps/*

#put in root page redirect
mkdir /usr/local/geowave/tomcat8/webapps/ROOT
echo "<% response.sendRedirect(\"/geoserver\"); %>" > /usr/local/geowave/tomcat8/webapps/ROOT/index.jsp

#make sure correct permissions are in place
chown -R geowave:geowave /usr/local/geowave/tomcat8

#change settings on service script
chmod 755 /etc/systemd/system/gwtomcat8.services
