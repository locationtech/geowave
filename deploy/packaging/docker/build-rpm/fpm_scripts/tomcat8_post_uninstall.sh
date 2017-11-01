#delete user
userdel -r tomcat

#Ensure all files in dir are removed
DIRECTORY="/opt/apache-tomcat/tomcat8"
if [ -d $DIRECTORY ]; then
  rm -rf $DIRECTORY
fi

rm -rf /etc/systemd/system/tomcat8.service
