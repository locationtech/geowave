#Add the user and group
groupadd geowave
sudo useradd -M -s /bin/nologin -g geowave -d /usr/local/geowave/tomcat8 geowave

#Change tomcat directory settings
chown -R geowave:geowave /usr/local/geowave/tomcat8

#clean up and empty webapps
rm -rf /usr/local/geowave/tomcat8/webapps/*

#put in root page redirect
mkdir /usr/local/geowave/tomcat8/webapps/ROOT
echo "<% response.sendRedirect(\"/geoserver\"); %>" > /usr/local/geowave/tomcat8/webapps/ROOT/index.jsp
chown -R geowave:geowave /usr/local/geowave/tomcat8

#change settings on service script
chmod 755 /etc/systemd/system/gw_tomcat8.services
