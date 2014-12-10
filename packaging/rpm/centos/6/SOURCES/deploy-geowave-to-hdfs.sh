#!/bin/bash
#
# Upload geowave-accumulo.jar into HDFS /accumulo/lib directory
#
# Add the following snippet to the accumulo-site.xml_service_safety_valve configuration setting
# 1.) Search for accumulo-site.xml_service_safety_valve in the Accumulo Configuration section
# 2.) Save the XML snippet below as the new value (update namenode hostname and remove comments)
# 3.) Distribute the new configuration and restart the service
#
# <property>
#   <name>general.vfs.classpaths</name>
#   <value>hdfs://NAMENODE_HOSTNAME:8020/accumulo/lib/.*\.jar</value>
# </property>
#
# Add the following snippet to hdfs-site.xml configuration setting
# 1.) Search for the Service-Wide / Advanced section's property for "HDFS Service Advanced Configuration Snippet (Safety Valve) for hdfs-site.xml"
# 2.) Search for any Gateway group properties labeled "HDFS Client Advanced Configuration Snippet (Safety Valve) for hdfs-site.xml"
# 3.) Distribute the new configuration and restart the service
#
# <property>
#   <name>dfs.datanode.synconclose</name>
#   <value>true</value>
# </property>

HDFS_USER=hdfs
ACCUMULO_USER=accumulo
GEOWAVE_ACCUMULO_HOME=/usr/local/geowave
ACCUMULO_LIB_DIR=/accumulo/classpath

REQUIRED_APPS=('hadoop' 'sudo')

# Test for installed apps required to run this script
dependency_tests() {
    for app in "${REQUIRED_APPS[@]}"
    do
        type $app >/dev/null 2>&1 || { echo >&2 "$0 needs the $app command to be installed . Aborting."; exit 1; } 
    done
}

# Sanity check
dependency_tests

# Test to see if Accumulo has been initialized by looking at hdfs contents
sudo -u $HDFS_USER hadoop fs -ls /accumulo
if [ $? -ne 0 ]; then
    echo >&2 "/accumulo directory not found in hdfs. Aborting."; exit 1;
fi

# Check to see if lib directory is already present
sudo -u $HDFS_USER hadoop fs -ls $ACCUMULO_LIB_DIR
if [ $? -ne 0 ]; then # Try creating
    sudo -u $HDFS_USER hadoop fs -mkdir $ACCUMULO_LIB_DIR
    if [ $? -ne 0 ]; then
        echo >&2 "Unable to create $ACCUMULO_LIB_DIR directory in hdfs. Aborting."; exit 1;
    fi
fi

sudo -u $HDFS_USER hadoop fs -put   $GEOWAVE_ACCUMULO_HOME/accumulo/geowave-accumulo.jar $ACCUMULO_LIB_DIR/geowave-accumulo.jar
if [ $? -ne 0 ]; then
    echo >&2 "Unable to upload geowave-accumulo.jar into hdfs. Aborting."; exit 1;
fi

sudo -u $HDFS_USER hadoop fs -chown -R $ACCUMULO_USER $ACCUMULO_LIB_DIR
if [ $? -ne 0 ]; then
    echo >&2 "Unable to change ownership of the $ACCUMULO_LIB_DIR directory in hdfs. Aborting."; exit 1;
fi
