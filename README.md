<p align="center">
	<img float="center" src="https://raw.githubusercontent.com/ngageoint/geowave/master/docs/content/userguide/images/geowave-full-logo-300px.png" alt="GeoWave"><br/><br/>
</p>

## About  

| Continuous Integration | Code Coverage | Static Analysis | Chat |            
|:------------------:|:-------------:|:---------------:|:----:|
| <a href="https://travis-ci.org/ngageoint/geowave/branches"><img alt="Travis-CI test status" src="https://travis-ci.org/ngageoint/geowave.svg?branch=master"/></a> | <a href='https://coveralls.io/r/ngageoint/?branch=master'><img src='https://coveralls.io/repos/ngageoint/geowave/badge.svg?branch=master' alt='Coverage Status' /></a> | <a href="https://scan.coverity.com/projects/3371"><img alt="Coverity Scan Build Status" src="https://scan.coverity.com/projects/3371/badge.svg"/></a> | [![Join the chat at https://gitter.im/ngageoint/geowave](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/ngageoint/geowave?utm_source=badge&utm_medium=badge&utm_content=badge) |  

GeoWave is an open source set of software that:

* Capabilities
  * Adds multi-dimensional indexing capability to [Apache Accumulo](https://accumulo.apache.org) and [Apache HBase](https://hbase.apache.org)
  * Adds support for geographic objects and geospatial operators to [Apache Accumulo](https://accumulo.apache.org) and [Apache HBase](https://hbase.apache.org)
  * Provides Map-Reduce input and output formats for distributed processing and analysis of geospatial data
* Geospatial software plugins
  * [GeoServer](http://geoserver.org/) plugin to allow geospatial data in Accumulo to be shared and visualized via OGC standard services
  * [PDAL](http://www.pdal.io/) plugin for working with point cloud data
  * [Mapnik](http://mapnik.org/) plugin for generating map tiles and generally making good looking maps. 
  
Basically, GeoWave is working to bridge geospatial software with distributed compute systems.

## The Docs
* Check out our [GeoWave io page](http://ngageoint.github.io/geowave/) page for detailed documentation.
* A [changelog is available](http://ngageoint.github.io/geowave/changelog.html) which details the changes and features for each of our [github releases](https://github.com/ngageoint/geowave/releases)

## The Software
* We have a [RPM repository](http://ngageoint.github.io/geowave/packages.html)
  * See [Documentation: Installation from RPM](http://ngageoint.github.io/geowave/userguide.html#installation-from-rpm) for more info.
  * Deb packages if enough people request them
* We have [Maven artifact repositories](http://ngageoint.github.io/geowave/userguide.html#maven-repositories-2) (indexes not enabled, but it works in a maven repo fragment)
  * Releases: http://geowave-maven.s3-website-us-east-1.amazonaws.com/release
  * Snapshots: http://geowave-maven.s3-website-us-east-1.amazonaws.com/snapshot (nightly)
* We have a [vagrant dev environment](https://github.com/ngageoint/geowave-vagrant)
* We have a development all in one RPM package: "geowave-cdh5-single-host"
* And you can always [build from source](http://ngageoint.github.io/geowave/userguide.html#installation-from-source)
  
 
## Community

* GeoWave is currently in the process of moving to a permanent home under [LocationTech and the Eclipse Foundation](https://locationtech.org/proposals/geowave).
* Community support is available on [chat](https://gitter.im/ngageoint/geowave) and on [our mailing list](mailto:geowave-dev@locationtech.org).

## Some GeoWave rendered eye candy

<p align="center">
	<a href="https://raw.githubusercontent.com/ngageoint/geowave/master/docs/content/userguide/images/geolife-density-13.jpg" target="_blank"><img align="center" src="https://raw.githubusercontent.com/ngageoint/geowave/master/docs/content/userguide/images/geolife-density-13-thumb.jpg" alt="Geolife data at city scale"></a><br/><br/>
	<a href="https://raw.githubusercontent.com/ngageoint/geowave/master/docs/content/userguide/images/geolife-density-17.jpg" target="_blank"><img align="center" src="https://raw.githubusercontent.com/ngageoint/geowave/master/docs/content/userguide/images/geolife-density-17-thumb.jpg" alt="Geolife data at block scale"></a><br/><br/>
	<a href="https://raw.githubusercontent.com/ngageoint/geowave/master/docs/content/userguide/images/osmgpx.jpg" target="_blank"><img align="center" src="https://raw.githubusercontent.com/ngageoint/geowave/master/docs/content/userguide/images/osmgpx-thumb.jpg" alt="OSM GPX tracks at country scale"></a><br/>
	
</p>

See [Screenshots](http://ngageoint.github.io/geowave/userguide.html#example-screenshots) in the documentation for more information.

## Supported versions of core libraries

We work to maintain a N and N-1 tested and supported version pace for the following core libraries.

| Geoserver | Geotools | Accumulo | HBase | Hadoop | PDAL | Mapnik | Java |
|:---------:|:--------:|:--------:|:-----:|:------:|:----:|:------:|:----:|
| 2.10.x | 16.x | 1.6.x,1.7.x | 1.1.x,1.2.x,1.3.x | 2.x | 0.9.9 |  master (pull request pending) | Java8 |

* [Apache Maven](http://maven.apache.org/) 3.x or greater is required for building
* [Java Advanced Imaging](http://download.java.net/media/jai/builds/release/1_1_3/INSTALL.html) and [Java Image I/O](http://download.java.net/media/jai-imageio/builds/release/1.1/INSTALL-jai_imageio.html) should both be installed on Geoserver for GeoWave versions 0.9.2.1 and below (licensing prohibits us redistributing)
   * At the time of writing, Oracle is migrating Java projects around and these links are subject to change.  Read the INSTALL files to determine the download file name for different operating systems and architectures.  They are stored in the same directory as the INSTALL file.  Here are some common download locations.
   * Java Advanced Imaging
      * Linux ([32-bit](http://download.java.net/media/jai/builds/release/1_1_3/jai-1_1_3-lib-linux-i586.tar.gz) and [64-bit](http://download.java.net/media/jai/builds/release/1_1_3/jai-1_1_3-lib-linux-amd64.tar.gz))
      * Windows ([32-bit](http://download.java.net/media/jai/builds/release/1_1_3/jai-1_1_3-lib-windows-i586.exe))
   * Java Image I/O
      * Linux ([32-bit](http://download.java.net/media/jai-imageio/builds/release/1.1/jai_imageio-1_1-lib-linux-i586.tar.gz) and [64-bit](http://download.java.net/media/jai-imageio/builds/release/1.1/jai_imageio-1_1-lib-linux-amd64.tar.gz))
      * Windows ([32-bit](http://download.java.net/media/jai-imageio/builds/release/1.1/jai_imageio-1_1-lib-windows-i586.exe))
* See our [.travis.yml](https://github.com/ngageoint/geowave/blob/master/.travis.yml) file for the currently tested build matrix. 



## Origin

GeoWave was developed at the National Geospatial-Intelligence Agency (NGA) in collaboration with [RadiantBlue Technologies](http://www.radiantblue.com/) (Now DigitalGlobe) and [Booz Allen Hamilton](http://www.boozallen.com/).  The government has ["unlimited rights"](https://github.com/ngageoint/geowave/blob/master/NOTICE) and is releasing this software to increase the impact of government investments by providing developers with the opportunity to take things in new directions. The software use, modification, and distribution rights are stipulated within the [Apache 2.0](http://www.apache.org/licenses/LICENSE-2.0.html) license.  


## Contributing

All pull request contributions to this project will be released under the Apache 2.0 or compatible license.
Software source code previously released under an open source license and then modified by NGA staff is considered a "joint work" (see 17 USC § 101); it is partially copyrighted, partially public domain, and as a whole is protected by the copyrights of the non-government authors and must be released according to the terms of the original open source license.

## Everything else
Check out our talk at the [Accumulo Summit](http://accumulosummit.com/program/talks/geowave-geospatial-and-geotemporal-data-storage-and-retrieval-in-accumulo/).

Did I mention our [documentation!](http://ngageoint.github.io/geowave/)
