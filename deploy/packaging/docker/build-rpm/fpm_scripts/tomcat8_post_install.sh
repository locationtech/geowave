#Add the user and group
groupadd tomcat
sudo useradd -M -s /bin/nologin -g tomcat -d /opt/apache-tomcat/tomcat8 tomcat

#Change tomcat directory settings
chown -R tomcat:tomcat /opt/apache-tomcat/tomcat8

#clean up and empty webapps
rm -rf /opt/apache-tomcat/tomcat8/webapps/*

#put in root page redirect
mkdir /opt/apache-tomcat/tomcat8/webapps/ROOT
echo "<% response.sendRedirect(\"/geoserver\"); %>" > /opt/apache-tomcat/tomcat8/webapps/ROOT/index.jsp
chown -R tomcat:tomcat /opt/apache-tomcat/tomcat8

#change settings on init.d file
chmod 755 /etc/systemd/system/tomcat8.services
