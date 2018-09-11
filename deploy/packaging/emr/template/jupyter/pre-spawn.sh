#!/usr/bin/env bash

USER=$1
if ["$USER" == ""]; then
    echo "must include username argument"
    exit 1
fi
# Start the Bootstrap Process
echo "bootstrap process running for user $USER ..."

# User Directory: That's the private directory for the user to be created, if none exists
USER_DIRECTORY="/home/${USER}/notebooks/"

# TODO: I don't like this but it fixes an error with pixiedust creating files owned by the first user to import it.
# Really there needs to be some changes to how pixiedust itself looks for user config + db files to support multi-user access
# for jupyterhub that don't exist currently.
sudo chmod -R 777 /usr/local/pixiedust/

if [ -d "$USER_DIRECTORY" ]; then
    echo "home directory for user already exists. skipped creation"
else
    echo "creating a home directory for the user: $USER_DIRECTORY"
    mkdir ${USER_DIRECTORY}

    echo "...copying example notebooks for user ..."
    cp -R /usr/local/notebooks/. ${USER_DIRECTORY}

    chown -R ${USER}:${USER} ${USER_DIRECTORY}
fi

if [ hadoop fs -test -d /user/${USER} ]; then
    echo "hdfs directory for user already exists. skipped creation."
else
    echo "creating hdfs directory for user."
    sudo -u hdfs hdfs dfs -mkdir /user/${USER}
    sudo -u hdfs hdfs dfs -chmod 777 /user/${USER}
fi

exit 0

