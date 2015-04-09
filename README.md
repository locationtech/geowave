# GeoWave 0.8.5
## About  

[![Join the chat at https://gitter.im/ngageoint/geowave](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/ngageoint/geowave?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

<a href="https://travis-ci.org/ngageoint/geowave">
	<img alt="Travis-CI test status" 
	     src="https://travis-ci.org/ngageoint/geowave.svg?branch=master"/>
</a>
<br/>
<a href="https://scan.coverity.com/projects/3371">
  <img alt="Coverity Scan Build Status"
       src="https://scan.coverity.com/projects/3371/badge.svg"/>
</a>

<a href='https://coveralls.io/r/ngageoint/?branch=master'>
  <img src='https://coveralls.io/repos/ngageoint/geowave/badge.png?branch=master'
       alt='Coverage Status' />
</a>



GeoWave is an open source set of software that:

* Adds multi-dimensional indexing capability to [Apache Accumulo](https://accumulo.apache.org) 
* Adds support for geographic objects and geospatial operators to [Apache Accumulo](https://accumulo.apache.org) 
* Contains a [GeoServer](http://geoserver.org/) plugin to allow geospatial data in Accumulo to be shared and visualized via OGC standard services
* Provides Map-Reduce input and output formats for distributed processing and analysis of geospatial data

Basically, GeoWave attempts to do for Accumulo as PostGIS does for PostgreSQL.

See [GeoWave io page](http://ngageoint.github.io/geowave/) for more detailed documentation, quickstart, examples, etc.

[GeoWave](https://wiki.eclipse.org/Google_Summer_of_Code_2015_Ideas) is on the project list for the [Google Summer of Code](https://www.google-melange.com/gsoc/homepage/google/gsoc2015).  

## Screenshots

<p align="center">
	<a href="https://raw.githubusercontent.com/ngageoint/geowave/master/docs/content/images/geolife-density-13.jpg" target="_blank"><img align="center" src="https://raw.githubusercontent.com/ngageoint/geowave/master/docs/content/images/geolife-density-13-thumb.jpg" alt="Geolife data at city scale"></a><br/><br/>
	<a href="https://raw.githubusercontent.com/ngageoint/geowave/master/docs/content/images/geolife-density-17.jpg" target="_blank"><img align="center" src="https://raw.githubusercontent.com/ngageoint/geowave/master/docs/content/images/geolife-density-17-thumb.jpg" alt="Geolife data at block scale"></a><br/><br/>
	<a href="https://raw.githubusercontent.com/ngageoint/geowave/master/docs/content/images/osmgpx.jpg" target="_blank"><img align="center" src="https://raw.githubusercontent.com/ngageoint/geowave/master/docs/content/images/osmgpx-thumb.jpg" alt="OSM GPX tracks at country scale"></a><br/>
	
</p>

See [Screenshots](http://ngageoint.github.io/geowave/documentation.html#screenshots-2) for more information.

## Origin

GeoWave was developed at the National Geospatial-Intelligence Agency (NGA) in collaboration with [RadiantBlue Technologies](http://www.radiantblue.com/) and [Booz Allen Hamilton](http://www.boozallen.com/).  The government has ["unlimited rights"](https://github.com/ngageoint/geowave/blob/master/NOTICE) and is releasing this software to increase the impact of government investments by providing developers with the opportunity to take things in new directions. The software use, modification, and distribution rights are stipulated within the [Apache 2.0](http://www.apache.org/licenses/LICENSE-2.0.html) license.  


## Pull Requests

All pull request contributions to this project will be released under the Apache 2.0 license.

Software source code previously released under an open source license and then modified by NGA staff is considered a "joint work" (see *17 USC  101*); it is partially copyrighted, partially public domain, and as a whole is protected by the copyrights of the non-government authors and must be released according to the terms of the original open source license.

##In the News###
[Federal IT Innovation Depends On Being Open](http://www.informationweek.com/government/open-government/federal-it-innovation-depends-on-being-open/a/d-id/1297521), Information Week, 7/24/2014 mentions GeoWave.  

## *Ultra* Quickstart

### Dependencies: Software and Versions
This *ultra* quickstart assumes you have [Apache Maven](http://maven.apache.org/), [Git](http://git-scm.com/), and [Java](http://www.oracle.com/technetwork/java/javase/downloads/index.html) (>= 1.7) installed on your system and on the path.  

To view the data (via standard [OGC](http://www.opengeospatial.org/) services) a [GeoServer](http://geoserver.org/) instance >= 2.5 is required due to [GEOT-4587](http://jira.codehaus.org/browse/GEOT-4587).  
[GeoServer](http://geoserver.org/) 2.5.2 with [Geotools](http://www.geotools.org/) 11.2 the most tested version.

[Apache Accumulo](https://accumulo.apache.org) version 1.5 or greater is required.  1.5.0, 1.5.1, and 1.6.0 have all been tested. 

[Apache Hadoop](http://hadoop.apache.org/) versions 1.x and 2.x *should* all work.  The software has specifically been run on [Cloudera](http://cloudera.com/content/cloudera/en/home.html) CDH4 and [Hortonworks Data Platform](http://hortonworks.com/hdp/) 2.1.   

MapReduce v2 + YARN is the currently preferred MR implementation / scheduler.

[Java Advanced Imaging](http://download.java.net/media/jai/builds/release/1_1_3/) and [Java Image I/O](http://download.java.net/media/jai-imageio/builds/release/1.1/) are also both required to be installed on the GeoServer instance(s) *as well* as on the Accumulo nodes.  The Accumulo support is only required for certain functions (distributed rendering) - so this may be skipped in some cases.

### Building: Maven!

Since GeoWave isn't currently in maven central we will build and install a local copy

	$ git clone git@github.com:ngageoint/geowave.git
	$ cd geowave && mvn install 


### Deploy: GeoServer

First we need to build the geoserver plugin - from the geowave root directory:
    
    $ cd geowave-deploy
    $ mvn package -P geotools-container-singlejar

let's assume you have geoserver deployed in a tomcat container in /opt/tomcat

    $ cp target/geowave-deploy-0.8.5-geoserver-singlejar.jar /opt/tomcat/webapps/geoserver/WEB-INF/lib/

and re-start tomcat

### Deploy: Accumulo

This should be very familiar by now; from the geowave root directory:

    $ cd geowave-deploy
    $ mvn package -P accumulo-container-singlejar

This distributable needs to be in the Accumulo classpath on every tablet server.

See: [Installation Page](http://ngageoint.github.io/geowave/documentation.html#installation-from-rpm) for more information about deployment.

### GeoWave System Integration Test

The geowave-test module will run end-to-end integration testing on either a configured Accumulo instance or a temporary MiniAccumuloCluster.  It will ingest both point and line features spatially and temporally from shapefiles and test that spatial and spatial-temporal queries match expected results.

A specific Accumulo instance can be configured either directly within this pom.xml or as Java options -DzookeeperUrl=&lt;zookeeperUrl&gt; -Dinstance=&lt;instance&gt; -Dusername=&lt;username&gt; -Dpassword=&lt;password&gt;

If any of these configuration parameters are left unspecified the default integration test will use a MiniAccumuloCluster created within a temporary directory.  For this to work on Windows, make sure Cygwin is installed and a "CYGPATH" environment variable must reference the &lt;CYGWIN_HOME&gt;/bin/cygpath.exe file.

### Supported Versions

See the <a href="https://github.com/ngageoint/geowave/blob/master/.travis.yml" target="_blank">travis test matrix</a> for currently tested configurations, but basically:

Accumulo: N and N-1  (1.6.x and 1.5.x currently); 
Hadoop: Apache 2.6, CDH 4.7 -> 5.3, Hortonworks 2.6
GeoTools/GeoServer: 11.4, 12.2/2.5.4, 2.6.2

We have dropped support for hadoop < 2.x;  it's should still work if you want to backport, but no guarantees. 

### Ingest Data

*Coming Soon! With useful details!*

(basically run geowave-ingest::mil.nga.giat.geowave.ingest.IngestMain with the geowave-types module included in the classpath - geowave-ingest uses SPI to discover supported formats and geowave-types provides a set of basic formats) 


### View in GeoServer

*Coming Soon! With screenshots as well!*

(basically add a GeoWave data store through the add datastore menu and configure the layer like normal)
