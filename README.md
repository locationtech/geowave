<p align="center">
	<a href="http://locationtech.github.io/geowave/">
	<img float="center" width="65%" src="https://raw.githubusercontent.com/locationtech/geowave/master/docs/content/geowave-index/images/geowave-logo-transluscent.png" alt="GeoWave"><br/><br/>
	</a>
</p>

## About  

| Continuous Integration | License | Chat |            
|:------------------:|:-------:|:----:| 
| <a href="https://github.com/locationtech/geowave/actions?query=workflow%3ATests+branch%3Amaster"><img alt="GitHub Action Test Status" src="https://github.com/locationtech/geowave/workflows/Tests/badge.svg?branch=master"/></a> | [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) | [![Join the chat at https://gitter.im/locationtech/geowave](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/locationtech/geowave?utm_source=badge&utm_medium=badge&utm_content=badge) |  

GeoWave is an open source set of software that:

* Capabilities
  * Adds multi-dimensional indexing capability to key/value stores (currently [Apache Accumulo](https://accumulo.apache.org), [Apache HBase](https://hbase.apache.org), [Apache Cassandra](http://cassandra.apache.org/), [Amazon DynamoDB](https://aws.amazon.com/dynamodb/), [Cloud Bigtable](https://cloud.google.com/bigtable/), [Redis](https://redis.io/), [RocksDB](https://rocksdb.org/), and [Apache Kudu](https://kudu.apache.org/), as well as direct FileSystem support)
  * Adds support for geographic objects and geospatial operators to these stores
  * Provides Map-Reduce input and output formats for distributed processing and analysis of geospatial data
* Geospatial software plugins
  * [GeoServer](http://geoserver.org/) plugin to allow geospatial data in various key/value stores to be shared and visualized via OGC standard services
  
Basically, GeoWave is working to bridge geospatial software with modern key/value stores and distributed compute systems.

## The Docs
* [GeoWave](https://locationtech.github.io/geowave/latest/index.html) - Latest snapshot documentation homepage
* [GeoWave Overview](https://locationtech.github.io/geowave/latest/overview.html) - Overview of GeoWave's capabilities
* [Installation Guide](https://locationtech.github.io/geowave/latest/installation-guide.html) - Installation instructions for standalone installers and from RPMs
* [Quickstart Guide](https://locationtech.github.io/geowave/latest/quickstart.html) - A quick demo of GeoWave features using the command-line interface
* [User Guide](https://locationtech.github.io/geowave/latest/userguide.html) - A guide for using GeoWave through the command-line interface and GeoServer plugin
* [Developer Guide](https://locationtech.github.io/geowave/latest/devguide.html) - A guide for developing applications that utilize GeoWave
* [Command-Line Interface](https://locationtech.github.io/geowave/latest/commands.html) - Full documentation for the GeoWave CLI
* [Changelog](https://locationtech.github.io/geowave/latest/changelog.html) - Changes and features for each of our [GitHub releases](https://github.com/locationtech/geowave/releases)
* The underlying principles employed in GeoWave are outlined in past academic publications to include largely the background theory in [Advances in Spatial and Temporal Databases 2017](https://link.springer.com/chapter/10.1007/978-3-319-64367-0_6) and a derivative, more applied paper in [FOSS4G Conference Proceedings 2017](http://scholarworks.umass.edu/cgi/viewcontent.cgi?article=1027&context=foss4g).

## The Software
* We have [multi-platform standalone installers](https://locationtech.github.io/geowave/latest/installation-guide.html#standalone-installers) for the GeoWave's command-line tools to help get started
  * This is often the quickest and easiest way to get started using GeoWave on your own machine
* We have a [RPM repository](https://locationtech.github.io/geowave/latest/downloads.html)
  * This contains various packages including puppet modules, best used for distributed environments.
  * See the [Installation Guide](https://locationtech.github.io/geowave/latest/installation-guide.html#installation-from-rpm) for more info.
* Maven artifacts are available on Maven Central
* And you can always [build from source](https://locationtech.github.io/geowave/latest/devguide.html#development-setup)

## Community

* Community support is available on [chat](https://gitter.im/locationtech/geowave) and on [our mailing list](mailto:geowave-dev@eclipse.org).

## Getting Started
### Programmatic Access
You can use Maven to reference pre-built GeoWave artifacts with the following pom.xml snippet (replacing `${keyvalue-datastore}` with your data store of choice and `${geowave.version}` with the GeoWave version you'd like to use):
```
	<dependencies>
		<dependency>
			<groupId>org.locationtech.geowave</groupId>
			<artifactId>geowave-datastore-${keyvalue-datastore}</artifactId>
			<version>${geowave.version}</version>
		</dependency>
		<dependency>
			<groupId>org.locationtech.geowave</groupId>
			<artifactId>geowave-adapter-vector</artifactId>
			<version>${geowave.version}</version>
		</dependency>
		<dependency>
			<groupId>org.locationtech.geowave</groupId>
			<artifactId>geowave-adapter-raster</artifactId>
			<version>${geowave.version}</version>
		</dependency>
	</dependencies>
```

Use the libraries available in the `api` package to leverage GeoWave's capabilities (where `<data store options>` might be `AccumuloRequiredOptions` or `HBaseRequiredOptions` and simple examples of creating the data type and index can be found in `SimpleIngest` within the `examples` directory):
```java
DataStore store = DataStoreFactory.createDataStore(<data store options>);
store.addType(<my data type>, <my index>);
try(Writer writer = store.createWriter()){
  //write data
  writer.writer(<data>);
}
 
//this just queries everything
try(CloseableIterator it = store.query(QueryBuilder.newBuilder().build())){
  while(it.hasNext()){
    //retrieve results matching query criteria and do something
    it.next();
  }
}
```
See the [Developer Guide](https://locationtech.github.io/geowave/latest/devguide.html#programmatic-api-examples) for more detailed programmatic API examples.

### Command-line Access
Alternatively, you can always use the GeoWave command-line to access the same capabilities:
```bash
# Add a new RocksDB data store called myStore in the current directory
geowave store add -t rocksdb myStore

# Add a spatial index called spatialIdx to myStore
geowave index add -t spatial myStore spatialIdx

# Ingest a shapefile with states into myStore in the spatialIdx index
geowave ingest localToGW -f geotools-vector states.shp myStore spatialIdx

# Query all the data in the states type from myStore
geowave vector query "SELECT * FROM myStore.states"
```
See the [CLI documentation](https://locationtech.github.io/geowave/latest/commands.html) for a full list of commands and their options.

## Some GeoWave rendered eye candy

<p align="center">
	<a href="https://raw.githubusercontent.com/locationtech/geowave/master/docs/content/overview/images/geolife-density-13.jpg" target="_blank"><img align="center" src="https://raw.githubusercontent.com/locationtech/geowave/master/docs/content/overview/images/geolife-density-13-thumb.jpg" alt="Geolife data at city scale"></a><br/><br/>
	<a href="https://raw.githubusercontent.com/locationtech/geowave/master/docs/content/overview/images/geolife-density-17.jpg" target="_blank"><img align="center" src="https://raw.githubusercontent.com/locationtech/geowave/master/docs/content/overview/images/geolife-density-17-thumb.jpg" alt="Geolife data at block scale"></a><br/><br/>
	<a href="https://raw.githubusercontent.com/locationtech/geowave/master/docs/content/overview/images/osmgpx.jpg" target="_blank"><img align="center" src="https://raw.githubusercontent.com/locationtech/geowave/master/docs/content/overview/images/osmgpx-thumb.jpg" alt="OSM GPX tracks at country scale"></a><br/>
	
</p>

See [Example Screenshots](https://locationtech.github.io/geowave/latest/overview.html#example-screenshots) in the GeoWave Overview for more information.

## Supported versions of core libraries

We work to maintain a N and N-1 tested and supported version pace for the following core libraries.

| GeoServer | GeoTools | Accumulo | HBase | Hadoop | Java |
|:---------:|:--------:|:--------:|:-----:|:------:|:----:|
| 2.19.x | 25.x | [1.9.x,2.0.x] | 2.4.x | [2.10.x,3.1.x] | Java8 |

* [Apache Maven](http://maven.apache.org/) 3.x or greater is required for building



## Origin

GeoWave was originally developed at the National Geospatial-Intelligence Agency (NGA) in collaboration with [RadiantBlue Technologies](http://www.radiantblue.com/) (now [Maxar Technologies](https://www.maxar.com/)) and [Booz Allen Hamilton](http://www.boozallen.com/). The software use, modification, and distribution rights are stipulated within the [Apache 2.0](http://www.apache.org/licenses/LICENSE-2.0.html) license.  


## Contributing

All pull request contributions to this project will be released under the Apache 2.0 or compatible license. Contributions are welcome and guidelines are provided [here](https://locationtech.github.io/geowave/latest/devguide.html#how-to-contribute).

Did I mention our [documentation!](https://locationtech.github.io/geowave/latest/index.html)
