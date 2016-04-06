#!/bin/bash
echo "Ingesting GeoWave sample data and running kernel density estimate..."
source geowave-env.sh

## configure accumulo
cat <<EOF | accumulo shell -u root -p secret -e "createuser geowave"
geowave
geowave
EOF
accumulo shell -u root -p secret -e "createnamespace geowave"
accumulo shell -u root -p secret -e "grant NameSpace.CREATE_TABLE -ns geowave -u geowave"
accumulo shell -u root -p secret -e "config -s general.vfs.context.classpath.geowave=hdfs://${HOSTNAME}:${HDFS_PORT}/accumulo/classpath/geowave/${GEOWAVE_VERSION}-apache/[^.].*.jar"
accumulo shell -u root -p secret -e "config -ns geowave -s table.classpath.context=geowave"

mkdir $STAGING_DIR/gdelt;cd $STAGING_DIR/gdelt
# just grab whateve data matches the time regex
wget http://data.gdeltproject.org/events/md5sums
for file in `cat md5sums | cut -d' ' -f3 | grep "^${TIME_REGEX}"` ; do wget http://data.gdeltproject.org/events/$file ; done
md5sum -c md5sums 2>&1 | grep "^${TIME_REGEX}"
cd $STAGING_DIR
# ingest it, indexed spatial only, it can be indexed spatial-temporally by changing -dim, pre-split with 24 shard IDs
geowave -localingest -f gdelt -b $STAGING_DIR/gdelt -dim spatial -cql "BBOX(geometry,${WEST},${SOUTH},${EAST},${NORTH})" -gwNamespace geowave.gdelt -datastore accumulo -zookeeper $HOSTNAME:2181 -instance $INSTANCE -user geowave -password geowave -partitionStrategy round_robin -numPartitions $NUM_PARTITIONS

# run a kde to produce a heatmap
hadoop jar ${GEOWAVE_TOOLS_HOME}/geowave-tools.jar -kde -featureType gdeltevent -minLevel 5 -maxLevel 26 -minSplits $NUM_PARTITIONS -maxSplits $NUM_PARTITIONS -coverageName gdeltevent_kde -hdfsHostPort ${HOSTNAME}:${HDFS_PORT} -jobSubmissionHostPort ${HOSTNAME}:${RESOURCE_MAN_PORT} -tileSize 1 -output_datastore accumulo -output_gwNamespace geowave.kde_gdelt -output_connectionParams "zookeeper=${HOSTNAME}:2181;instance=${INSTANCE};user=geowave;password=geowave" -input_datastore accumulo -input_gwNamespace geowave.gdelt -input_connectionParams "zookeeper=${HOSTNAME}:2181;instance=${INSTANCE};user=geowave;password=geowave"
