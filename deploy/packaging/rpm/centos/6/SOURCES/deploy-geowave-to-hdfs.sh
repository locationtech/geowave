#!/bin/bash
#
# Upload geowave-accumulo.jar into HDFS
# Use a naming convention within the HDFS directory structure to differentiate versions
# Attempt to use a variety of common HDFS root usernames an optional user arg will override
#
# deploy-geowave-to-hdfs.sh [--user HDFS_ROOT_USERNAME]
#

# Test for installed apps required to run this script
dependency_tests() {
    REQUIRED_APPS=('hadoop')

    for app in "${REQUIRED_APPS[@]}"
    do
        type $app >/dev/null 2>&1 || { echo >&2 "$0 needs the $app command to be installed . Aborting."; exit 1; }
    done
}

# Sanity check of environment
dependency_tests

# Start detecting the other required settings
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ACCUMULO_USER=accumulo

# Parse any arguments passed to the script
declare -A ARGS
while [ $# -gt 0 ]; do
    case "$1" in
        *) NAME="${1:2}"; shift; ARGS[$NAME]="$1" ;;
    esac
    shift
done

determine_hdfs_user() {
    # Various usernames distros configure to be the one with "root" HDFS permissions
    HADOOP_USERS=('hdfs' 'hadoop' 'cloudera-scm')
    if [ ! -z ${ARGS[user]} ]; then # Use custom user if provided
        HADOOP_USERS=( ${ARGS[user]} )
    fi
    HADOOP_USER=
    for user in "${HADOOP_USERS[@]}"
    do
        getent passwd $user > /dev/null
        if [ $? -eq 0 ] ; then
            HADOOP_USER=$user
            break
        fi
    done

    if [ ! -z $HADOOP_USER ]; then
        echo $HADOOP_USER
    else
        echo >&2 "Cannot determine user account to use for HDFS, tried '${HADOOP_USERS[@]}'. Aborting."
        exit 1
    fi
}

HDFS_USER=$(determine_hdfs_user)

parseVersion() {
    echo $(cat "$SCRIPT_DIR/geowave-accumulo-build.properties" | grep "project.version=" | sed -e 's/"//g' -e 's/-SNAPSHOT//g' -e 's/project.version=//g')
}

# Test to see if Accumulo has been initialized by looking at hdfs contents
determine_accumulo_hdfs_root() {
    ACCUMULO_ROOT_DIRS=('/accumulo' '/apps/accumulo')
    ROOT_DIR=
    for dir in "${ACCUMULO_ROOT_DIRS[@]}"
    do
        su $HDFS_USER -c "hadoop fs -ls $dir" > /dev/null
        if [ $? -eq 0 ] ; then
            ROOT_DIR=$dir
            break
        fi
    done

    if [ ! -z $ROOT_DIR ]; then
        echo $ROOT_DIR
    else
        echo >&2 "Accumulo application directory not found in HDFS, tried '${ACCUMULO_ROOT_DIRS[@]}'. Aborting."
        exit 1
    fi
}

ACCUMULO_HDFS_ROOT=$(determine_accumulo_hdfs_root)

# To support concurrent version and vendor installs we're naming the directory that contains the iterator with
# both the vendor and application version so we can support things like 0.8.7-cdh5, 0.8.7-cdh6, 0.8.8-hdp2 etc.
determine_vendor_version() {
    while [ $# -gt 0 ]; do
        ARG="${1:2}"
        KEY="${ARG%%=*}"
        VALUE="${ARG#*=}"
        case "$KEY" in
            "vendor.version") echo "$VALUE" ;;
            *) # Do nothing
        esac
        shift
    done
}
BUILD_ARGS_KEY="project.build.args="
BUILD_ARGS_VAL=$(cat $SCRIPT_DIR/geowave-accumulo-build.properties | grep "$BUILD_ARGS_KEY" | sed -e "s/$BUILD_ARGS_KEY//")
VENDOR_VERSION=$(determine_vendor_version $BUILD_ARGS_VAL)
if [ ! -z $VENDOR_VERSION ]; then
    VENDOR_VERSION="$(parseVersion)-$VENDOR_VERSION"
else
    VENDOR_VERSION="$(parseVersion)"
fi
ACCUMULO_LIB_DIR="$(determine_accumulo_hdfs_root)/classpath/geowave/$VENDOR_VERSION"
GEOWAVE_ACCUMULO_HOME=/usr/local/geowave-$VENDOR_VERSION/accumulo

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

# Also upload the build metadata file for ease of inspection
su $HDFS_USER -c "hadoop fs -ls $ACCUMULO_LIB_DIR/geowave-accumulo-build.properties"
if [ $? -eq 0 ]; then
    su $HDFS_USER -c "hadoop fs -rm $ACCUMULO_LIB_DIR/geowave-accumulo-build.properties"
fi

su $HDFS_USER -c "hadoop fs -put $GEOWAVE_ACCUMULO_HOME/geowave-accumulo-build.properties $ACCUMULO_LIB_DIR/geowave-accumulo-build.properties"
if [ $? -ne 0 ]; then
    echo >&2 "Unable to upload geowave-accumulo-build.properties into hdfs. Aborting."; exit 1;
fi

# Set ownership to Accumulo user
su $HDFS_USER -c "hadoop fs -chown -R $ACCUMULO_USER:$ACCUMULO_USER $ACCUMULO_LIB_DIR"
if [ $? -ne 0 ]; then
    echo >&2 "Unable to change ownership of the $ACCUMULO_LIB_DIR directory in hdfs. Aborting."; exit 1;
fi
