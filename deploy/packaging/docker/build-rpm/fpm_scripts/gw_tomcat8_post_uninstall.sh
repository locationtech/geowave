#Ensure all files in dir are removed
DIRECTORY="/usr/local/geowave/tomcat8"
if [ -d $DIRECTORY ]; then
  rm -rf $DIRECTORY
fi

rm -rf /etc/systemd/system/gw_tomcat8.service
