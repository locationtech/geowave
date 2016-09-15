#!/bin/bash

echo "Ingesting GeoWave sample data and running kernel density estimate..."
source geowave-env.sh

# Configure accumulo
cat <<EOF | accumulo shell -u root -p secret -e "createuser geowave"
geowave
geowave
EOF
accumulo shell -u root -p secret -e "createnamespace geowave"
accumulo shell -u root -p secret -e "grant NameSpace.CREATE_TABLE -ns geowave -u geowave"
accumulo shell -u root -p secret -e "config -s general.vfs.context.classpath.geowave=hdfs://${HOSTNAME}:8020/accumulo/classpath/geowave/${GEOWAVE_VERSION}-apache/[^.].*.jar"
accumulo shell -u root -p secret -e "config -ns geowave -s table.classpath.context=geowave"

# Grab whatever gdelt data matches $TIME_REGEX. The example is set to 201602
sudo mkdir $STAGING_DIR/gdelt;cd $STAGING_DIR/gdelt
sudo wget http://data.gdeltproject.org/events/md5sums
for file in `cat md5sums | cut -d' ' -f3 | grep "^${TIME_REGEX}"` ; do sudo wget http://data.gdeltproject.org/events/$file ; done
md5sum -c md5sums 2>&1 | grep "^${TIME_REGEX}"
cd $STAGING_DIR

# Ingest the data. Indexed spatial only in this example. It can also be indexed using spatial-temporal
geowave config addstore -t accumulo gdelt-accumulo --gwNamespace geowave.gdelt --zookeeper $HOSTNAME:2181 --instance $INSTANCE --user geowave --password geowave
geowave config addindex -t spatial gdelt-spatial --partitionStrategy round_robin --numPartitions $NUM_PARTITIONS
geowave ingest localtogw $STAGING_DIR/gdelt gdelt-accumulo gdelt-spatial -f gdelt --gdelt.cql "BBOX(geometry,${WEST},${SOUTH},${EAST},${NORTH})"
geowave config addstore -t accumulo gdelt-accumulo-out --gwNamespace geowave.kde_gdelt --zookeeper $HOSTNAME:2181 --instance $INSTANCE --user geowave --password geowave

# Run a kde to produce a heatmap
hadoop jar ${GEOWAVE_TOOLS_HOME}/geowave-tools.jar analytic kde --featureType gdeltevent --minLevel 5 --maxLevel 26 --minSplits $NUM_PARTITIONS --maxSplits $NUM_PARTITIONS --coverageName gdeltevent_kde --hdfsHostPort ${HOSTNAME}:${HDFS_PORT} --jobSubmissionHostPort ${HOSTNAME}:${RESOURCE_MAN_PORT} --tileSize 1 gdelt-accumulo gdelt-accumulo-out

# Run the geoserver workspace setup script
cd $STAGING_DIR
./setup-geoserver-geowave-workspace.sh

