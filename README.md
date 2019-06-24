<p align="center">
	<a href="http://locationtech.github.io/geowave/">
	<img float="center" width="65%" src="https://raw.githubusercontent.com/locationtech/geowave/master/docs/content/geowave-index/images/geowave-logo-transluscent.png" alt="GeoWave"><br/><br/>
	</a>
</p>

## About  

| Continuous Integration | License | Chat |            
|:------------------:|:-------:|:----:| 
| <a href="https://travis-ci.org/locationtech/geowave/branches"><img alt="Travis-CI test status" src="https://travis-ci.org/locationtech/geowave.svg?branch=master"/></a> | [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) | [![Join the chat at https://gitter.im/locationtech/geowave](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/locationtech/geowave?utm_source=badge&utm_medium=badge&utm_content=badge) |  

GeoWave is an open source set of software that:

* Capabilities
  * Adds multi-dimensional indexing capability to key-value stores (currently [Apache Accumulo](https://accumulo.apache.org), [Apache HBase](https://hbase.apache.org), [Apache Cassandra](http://cassandra.apache.org/), [AmazonDynamoDB](https://aws.amazon.com/dynamodb/), [Cloud BigTable](https://cloud.google.com/bigtable/), [Redis](https://redis.io/), [RocksDB](https://rocksdb.org/)), and [Apache Kudu](https://kudu.apache.org/)
  * Adds support for geographic objects and geospatial operators to these stores
  * Provides Map-Reduce input and output formats for distributed processing and analysis of geospatial data
* Geospatial software plugins
  * [GeoServer](http://geoserver.org/) plugin to allow geospatial data in various key-value stores to be shared and visualized via OGC standard services
  * [PDAL](http://www.pdal.io/) plugin for working with point cloud data
  * [Mapnik](http://mapnik.org/) plugin for generating map tiles and generally making good looking maps. 
  
Basically, GeoWave is working to bridge geospatial software with modern key-value stores and distributed compute systems.

## The Docs
* Check out our [GeoWave io page](http://locationtech.github.io/geowave/) page for detailed documentation.
* A [changelog is available](http://locationtech.github.io/geowave/changelog.html) which details the changes and features for each of our [github releases](https://github.com/locationtech/geowave/releases)
* The underlying principles employed in GeoWave are outlined in recent academic publications to include largely the background theory in [Advances in Spatial and Temporal Databases 2017](https://link.springer.com/chapter/10.1007/978-3-319-64367-0_6) and a derivative, more applied paper in [FOSS4G Conference Proceedings 2017](http://scholarworks.umass.edu/cgi/viewcontent.cgi?article=1027&context=foss4g).

## The Software
* We have [multi-platform standalone installers](http://locationtech.github.io/geowave/devguide.html#standalone-installers) for the GeoWave's commandline tools to help get started
  * This is often the quickest and easiest way to get started using GeoWave on your own machine
* We have a [RPM repository](http://locationtech.github.io/geowave/packages.html)
  * This contains various packages including puppet modules, best used for distributed environments.
  * See [Documentation: Installation from RPM](http://locationtech.github.io/geowave/devguide.html#installation-from-rpm) for more info.
* Maven artifacts are available on Maven Central
* And you can always [build from source](http://locationtech.github.io/geowave/devguide.html#development-setup)

## Community

* Community support is available on [chat](https://gitter.im/locationtech/geowave) and on [our mailing list](mailto:geowave-dev@locationtech.org).

## Getting Started
### Programmatic Access
You can use maven to reference pre-built GeoWave artifacts with the following pom.xml snippet (replacing `${keyvalue-datastore}` with your datastore of choice and `${geowave.version}` with the geowave version you'd like to use):
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
  writer.writer(<data);
}
 
//this just queries everything
try(CloseableIterator it = store.query(QueryBuilder.newBuilder().build())){
  while(it.hasNext()){
    //retrieve results matching query criteria and do something
    it.next();
  }
}
```
### Commandline Access
Alternatively, you can always use the GeoWave commandline to access the same capabilities. Install the `geowave-$VERSION-apache-tools` RPM as instructed [here](http://locationtech.github.io/geowave/packages.html).  Then `geowave config addstore ...` and `geowave config addindex ...` are used to create named configurations for connecting to a key-value store (addstore) and describing how you want the data indexed (addindex).  You can use `--help` at any time such as `geowave config addstore --help` or furthermore get additional help after specifying the type with `-t` such as `geowave config addstore -t accumulo --help` to understand accumulo specific parameters. Once you have the indexing and store specified you can use `geowave ingest localtogw <file or directory> <store name> <index name(s)>` to ingest data into the key-value store. For the most basic walkthrough with minimal setup, run through the [quickstart guide](http://locationtech.github.io/geowave/quickstart.html) locally using RocksDB.

## Some GeoWave rendered eye candy

<p align="center">
	<a href="https://raw.githubusercontent.com/locationtech/geowave/master/docs/content/userguide/images/geolife-density-13.jpg" target="_blank"><img align="center" src="https://raw.githubusercontent.com/locationtech/geowave/master/docs/content/userguide/images/geolife-density-13-thumb.jpg" alt="Geolife data at city scale"></a><br/><br/>
	<a href="https://raw.githubusercontent.com/locationtech/geowave/master/docs/content/userguide/images/geolife-density-17.jpg" target="_blank"><img align="center" src="https://raw.githubusercontent.com/locationtech/geowave/master/docs/content/userguide/images/geolife-density-17-thumb.jpg" alt="Geolife data at block scale"></a><br/><br/>
	<a href="https://raw.githubusercontent.com/locationtech/geowave/master/docs/content/userguide/images/osmgpx.jpg" target="_blank"><img align="center" src="https://raw.githubusercontent.com/locationtech/geowave/master/docs/content/userguide/images/osmgpx-thumb.jpg" alt="OSM GPX tracks at country scale"></a><br/>
	
</p>

See [Screenshots](http://locationtech.github.io/geowave/userguide.html#example-screenshots) in the documentation for more information.

## Supported versions of core libraries

We work to maintain a N and N-1 tested and supported version pace for the following core libraries.

| Geoserver | Geotools | Accumulo | HBase | Hadoop | PDAL | Mapnik | Java |
|:---------:|:--------:|:--------:|:-----:|:------:|:----:|:------:|:----:|
| 2.14.x | 20.x | [1.7.x,1.9.x] | [1.1.x,1.4.x] | 2.x | 0.9.9 |  3.x | Java8 |

* [Apache Maven](http://maven.apache.org/) 3.x or greater is required for building
* [Java Advanced Imaging](http://download.java.net/media/jai/builds/release/1_1_3/INSTALL.html) and [Java Image I/O](http://download.java.net/media/jai-imageio/builds/release/1.1/INSTALL-jai_imageio.html) should both be installed on Geoserver for GeoWave versions 0.9.2.1 and below (licensing prohibits us redistributing)
   * At the time of writing, Oracle is migrating Java projects around and these links are subject to change.  Read the INSTALL files to determine the download file name for different operating systems and architectures.  They are stored in the same directory as the INSTALL file.  Here are some common download locations.
   * Java Advanced Imaging
      * Linux ([32-bit](http://download.java.net/media/jai/builds/release/1_1_3/jai-1_1_3-lib-linux-i586.tar.gz) and [64-bit](http://download.java.net/media/jai/builds/release/1_1_3/jai-1_1_3-lib-linux-amd64.tar.gz))
      * Windows ([32-bit](http://download.java.net/media/jai/builds/release/1_1_3/jai-1_1_3-lib-windows-i586.exe))
   * Java Image I/O
      * Linux ([32-bit](http://download.java.net/media/jai-imageio/builds/release/1.1/jai_imageio-1_1-lib-linux-i586.tar.gz) and [64-bit](http://download.java.net/media/jai-imageio/builds/release/1.1/jai_imageio-1_1-lib-linux-amd64.tar.gz))
      * Windows ([32-bit](http://download.java.net/media/jai-imageio/builds/release/1.1/jai_imageio-1_1-lib-windows-i586.exe))
* See our [.travis.yml](https://github.com/locationtech/geowave/blob/master/.travis.yml) file for the currently tested build matrix. 



## Origin

GeoWave was developed at the National Geospatial-Intelligence Agency (NGA) in collaboration with [RadiantBlue Technologies](http://www.radiantblue.com/) (Now DigitalGlobe) and [Booz Allen Hamilton](http://www.boozallen.com/).  The government has ["unlimited rights"](https://github.com/locationtech/geowave/blob/master/NOTICE) and is releasing this software to increase the impact of government investments by providing developers with the opportunity to take things in new directions. The software use, modification, and distribution rights are stipulated within the [Apache 2.0](http://www.apache.org/licenses/LICENSE-2.0.html) license.  


## Contributing

All pull request contributions to this project will be released under the Apache 2.0 or compatible license.
Software source code previously released under an open source license and then modified by NGA staff is considered a "joint work" (see 17 USC § 101); it is partially copyrighted, partially public domain, and as a whole is protected by the copyrights of the non-government authors and must be released according to the terms of the original open source license.

Did I mention our [documentation!](http://locationtech.github.io/geowave/)
