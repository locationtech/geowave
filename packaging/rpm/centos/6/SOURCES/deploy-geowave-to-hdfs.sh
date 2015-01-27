#!/bin/bash
#
# Upload geowave-accumulo.jar into HDFS
#

HDFS_USER=hdfs
ACCUMULO_USER=accumulo
GEOWAVE_ACCUMULO_HOME=/usr/local/geowave/accumulo
ACCUMULO_LIB_DIR=/accumulo/classpath/geowave

REQUIRED_APPS=('hadoop')

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
su $HDFS_USER -c "hadoop fs -ls /accumulo"
if [ $? -ne 0 ]; then
    echo >&2 "/accumulo directory not found in hdfs. Aborting."; exit 1;
fi

# Check to see if lib directory is already present
su $HDFS_USER -c "hadoop fs -ls $ACCUMULO_LIB_DIR"
if [ $? -ne 0 ]; then # Try creating
    su $HDFS_USER -c "hadoop fs -mkdir -p $ACCUMULO_LIB_DIR"
    if [ $? -ne 0 ]; then
        echo >&2 "Unable to create $ACCUMULO_LIB_DIR directory in hdfs. Aborting."; exit 1;
    fi
fi

# Check to see if the library is already present and remove if so (put will not replace)
su $HDFS_USER -c "hadoop fs -ls $ACCUMULO_LIB_DIR/geowave-accumulo.jar"
if [ $? -eq 0 ]; then
    su $HDFS_USER -c "hadoop fs -rm $ACCUMULO_LIB_DIR/geowave-accumulo.jar"
fi

# Upload library to hdfs
su $HDFS_USER -c "hadoop fs -put $GEOWAVE_ACCUMULO_HOME/geowave-accumulo.jar $ACCUMULO_LIB_DIR/geowave-accumulo.jar"
if [ $? -ne 0 ]; then
    echo >&2 "Unable to upload geowave-accumulo.jar into hdfs. Aborting."; exit 1;
fi

# Set ownership to Accumulo user
su $HDFS_USER -c "hadoop fs -chown -R $ACCUMULO_USER $ACCUMULO_LIB_DIR"
if [ $? -ne 0 ]; then
    echo >&2 "Unable to change ownership of the $ACCUMULO_LIB_DIR directory in hdfs. Aborting."; exit 1;
fi
