
# For use by geowave jetty server, set if not already set elsewhere
if [ "x" == "x$JAVA_HOME" ]; then
    export JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:bin/java::")
fi
if [ "x" == "x$GEOSERVER_HOME" ]; then
    export GEOSERVER_HOME=/usr/local/geowave/geoserver
fi
if [ "x" == "x$GEOSERVER_DATA_DIR" ]; then
    export GEOSERVER_DATA_DIR=/usr/local/geowave/geoserver/data_dir
fi

# Sourcing of this file does not always work with default profile settings so we'll ensure with this
if [ -f /etc/bash_completion.d/geowave-tools-cmd-completion.sh ] && ! shopt -oq posix; then
    . /etc/bash_completion.d/geowave-tools-cmd-completion.sh
fi
