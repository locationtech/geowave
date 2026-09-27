<p align="center">
	<a href="https://locationtech.github.io/geowave/">
	<img float="center" width="65%" src="https://raw.githubusercontent.com/locationtech/geowave/master/docs/content/geowave-index/images/geowave-logo-transluscent.png" alt="GeoWave"><br/><br/>
	</a>
</p>

## About  

| Tests | IP Check | Maven Central | License |
|:-----:|:--------:|:-------------:|:-------:|
| [![Tests](https://github.com/locationtech/geowave/actions/workflows/test.yml/badge.svg?branch=master)](https://github.com/locationtech/geowave/actions/workflows/test.yml?query=branch%3Amaster) | [![IP Check](https://github.com/locationtech/geowave/actions/workflows/ip-check.yml/badge.svg?branch=master)](https://github.com/locationtech/geowave/actions/workflows/ip-check.yml?query=branch%3Amaster) | [![Maven Central](https://img.shields.io/maven-central/v/org.locationtech.geowave/geowave-core-store)](https://central.sonatype.com/artifact/org.locationtech.geowave/geowave-core-store) | [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE) |

GeoWave is an open source set of software that:

* Capabilities
  * Adds multi-dimensional indexing capability to key/value stores (currently [Apache Accumulo](https://accumulo.apache.org), [Apache HBase](https://hbase.apache.org), [Apache Cassandra](https://cassandra.apache.org/), [Amazon DynamoDB](https://aws.amazon.com/dynamodb/), [Redis](https://redis.io/), and [RocksDB](https://rocksdb.org/), as well as direct FileSystem support)
  * Adds support for geographic objects and geospatial operators to these stores
  * Provides Map-Reduce input and output formats for distributed processing and analysis of geospatial data
* Geospatial software plugins
  * [GeoServer](https://geoserver.org/) plugin to allow geospatial data in various key/value stores to be shared and visualized via OGC standard services
  
Basically, GeoWave is working to bridge geospatial software with modern key/value stores and distributed compute systems.

## The Docs
The published documentation site was last generated in 2022, for the 2.0.x line. Until publishing resumes, the documentation for the current `master` (3.0, Java 21) lives in this repository under [`docs/content`](docs/content).

* [GeoWave](https://locationtech.github.io/geowave/latest/index.html) - Documentation homepage
* [GeoWave Overview](https://locationtech.github.io/geowave/latest/overview.html) - Overview of GeoWave's capabilities
* [Installation Guide](https://locationtech.github.io/geowave/latest/installation-guide.html) - Getting the GeoWave command-line tools
* [Quickstart Guide](https://locationtech.github.io/geowave/latest/quickstart.html) - A quick demo of GeoWave features using the command-line interface
* [User Guide](https://locationtech.github.io/geowave/latest/userguide.html) - A guide for using GeoWave through the command-line interface and GeoServer plugin
* [Developer Guide](https://locationtech.github.io/geowave/latest/devguide.html) - A guide for developing applications that utilize GeoWave
* [Command-Line Interface](https://locationtech.github.io/geowave/latest/commands.html) - Full documentation for the GeoWave CLI
* [Changelog](https://locationtech.github.io/geowave/latest/changelog.html) - Changes and features for each of our [GitHub releases](https://github.com/locationtech/geowave/releases)
* The underlying principles employed in GeoWave are outlined in past academic publications to include largely the background theory in [Advances in Spatial and Temporal Databases 2017](https://link.springer.com/chapter/10.1007/978-3-319-64367-0_6) and a derivative, more applied paper in [FOSS4G Conference Proceedings 2017](https://scholarworks.umass.edu/cgi/viewcontent.cgi?article=1027&context=foss4g).

## The Software
* Released Maven artifacts are on [Maven Central](https://central.sonatype.com/namespace/org.locationtech.geowave). The latest release there is 2.0.1, from the Java 8 line; 3.0 is not published yet, pending the Eclipse Foundation's approval of the `org.locationtech` namespace on the Sonatype Central Portal.
* From 3.0 on, release binaries such as the command-line tools and the GeoServer plugin will be attached to [GitHub Releases](https://github.com/locationtech/geowave/releases).
* Until then, [build from source](https://locationtech.github.io/geowave/latest/devguide.html#development-setup). `master` requires JDK 21; for Java 8, use the [`2.x-jdk8`](https://github.com/locationtech/geowave/tree/2.x-jdk8) branch.

## Community

* Community support is available through [GitHub Issues](https://github.com/locationtech/geowave/issues) and on [our mailing list](mailto:geowave-dev@eclipse.org).

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
  writer.write(<data>);
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
geowave query myStore "SELECT * FROM states"
```

See the [CLI documentation](https://locationtech.github.io/geowave/latest/commands.html) for a full list of commands and their options.

## Some GeoWave rendered eye candy

<p align="center">
	<a href="https://raw.githubusercontent.com/locationtech/geowave/master/docs/content/overview/images/geolife-density-13.jpg" target="_blank"><img align="center" src="https://raw.githubusercontent.com/locationtech/geowave/master/docs/content/overview/images/geolife-density-13-thumb.jpg" alt="Geolife data at city scale"></a><br/><br/>
	<a href="https://raw.githubusercontent.com/locationtech/geowave/master/docs/content/overview/images/geolife-density-17.jpg" target="_blank"><img align="center" src="https://raw.githubusercontent.com/locationtech/geowave/master/docs/content/overview/images/geolife-density-17-thumb.jpg" alt="Geolife data at block scale"></a><br/><br/>
	<a href="https://raw.githubusercontent.com/locationtech/geowave/master/docs/content/overview/images/osmgpx.jpg" target="_blank"><img align="center" src="https://raw.githubusercontent.com/locationtech/geowave/master/docs/content/overview/images/osmgpx-thumb.jpg" alt="OSM GPX tracks at country scale"></a><br/>
	
</p>

See [Example Screenshots](https://locationtech.github.io/geowave/latest/overview.html#example-screenshots) in the GeoWave Overview for more information.

## Core library versions

`master` builds and tests against:

| GeoServer | GeoTools | Accumulo | HBase | Hadoop | Spark | Java |
|:---------:|:--------:|:--------:|:-----:|:------:|:-----:|:----:|
| 3.0.x | 35.x | 2.0.x | 2.4.x | 3.1.x | 4.0.x (Scala 2.13) | 21 |

* Building requires JDK 21 and [Apache Maven](https://maven.apache.org/) 3.9 or later, or the committed `./mvnw` wrapper.
* For Java 8, use the [`2.x-jdk8`](https://github.com/locationtech/geowave/tree/2.x-jdk8) branch.

## Origin

GeoWave was originally developed at the National Geospatial-Intelligence Agency (NGA) in collaboration with RadiantBlue Technologies (now [Maxar Technologies](https://www.maxar.com/)) and [Booz Allen Hamilton](https://www.boozallen.com/). The software use, modification, and distribution rights are stipulated within the [Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0.html) license.  


## Contributing

All pull request contributions to this project will be released under the Apache 2.0 or compatible license. Contributions are welcome; see [CONTRIBUTING.md](CONTRIBUTING.md), which covers the Eclipse Contributor Agreement and third-party dependency checks, and the [contribution guidelines](https://locationtech.github.io/geowave/latest/devguide.html#how-to-contribute) in the Developer Guide. Please follow the [Code of Conduct](CODE_OF_CONDUCT.md), and report security vulnerabilities privately as described in [SECURITY.md](SECURITY.md).

Did I mention our [documentation!](https://locationtech.github.io/geowave/latest/index.html)
