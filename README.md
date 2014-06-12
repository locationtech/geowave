# GeoWave 0.7.0

## About  

GeoWave is an open source set of software that:
	
* Adds multi-dimensional indexing capability to [Apache Accumulo](http://projects.apache.org/projects/accumulo.html) 
* Adds support for geographic objects and geospatial operators to [Apache Accumulo](http://projects.apache.org/projects/accumulo.html) 
* Contains a [GeoServer](http://geoserver.org/) plugin to allow geospatial data in Accumulo to be shared and visualized via OGC standard services
* Provides Map-Reduce input and output formats for distributed processing and analysis of geospatial data

Basically, GeoWave attempts to do for Accumulo as PostGIS does for PostgreSQL.  

## Origin

GeoWave was developed at the National Geospatial-Intelligence Agency (NGA) in collaboration with [RadiantBlue Technologies](http://www.radiantblue.com/) and [Booz Allen Hamilton](http://www.boozallen.com/).  The government has "unlimited rights" and is releasing this software to increase the impact of government investments by providing developers with the opportunity to take things in new directions. The software use, modification, and distribution rights are stipulated within the [Apache 2.0](http://www.apache.org/licenses/LICENSE-2.0.html) license.  


## Pull Requests

All pull request contributions to this project will be released under the Apache 2.0 license.  

Software source code previously released under an open source license and then modified by NGA staff is considered a "joint work" (see *17 USC  101*); it is partially copyrighted, partially public domain, and as a whole is protected by the copyrights of the non-government authors and must be released according to the terms of the original open source license.

## *Ultra* Quickstart

### Dependencies: Software and Versions
This *ultra* quickstart assumes you have [Apache Maven](http://maven.apache.org/), [Git](http://git-scm.com/), and [Java](http://www.oracle.com/technetwork/java/javase/downloads/index.html) (>= 1.7) installed on your system and on the path.  

To view the data (via standard [OGC](http://www.opengeospatial.org/) services) a [GeoServer](http://geoserver.org/) instance >= 2.5 is required due to [GEOT-4587](http://jira.codehaus.org/browse/GEOT-4587).  
[GeoServer](http://geoserver.org/) 2.5 with [Geotools](http://www.geotools.org/) 11.0 the most tested version.

[Apache Accumulo](http://projects.apache.org/projects/accumulo.html) version 1.5 or greater is required.  1.5.0, 1.5.1, and 1.6.0 have all been tested. 

[Apache Hadoop](http://hadoop.apache.org/) versions 1.x and 2.x *should* all work.  The software has specifically been run on [Cloudera](http://cloudera.com/content/cloudera/en/home.html) CDH4 and [Hortonworks Data Platform](http://hortonworks.com/hdp/) 2.1.   

MapReduce 1 with the new API (org.apache.hadoop.mapreduce.*) is used.  Testing is underway against YARN / MR2 and seems to be positive, but well, it's still underway.  

[Java Advanced Imaging](http://download.java.net/media/jai/builds/release/1_1_3/) and [Java Image I/O](http://download.java.net/media/jai-imageio/builds/release/1.1/) are also both required to be installed on the GeoServer instance(s) *as well* as on the Accumulo nodes.  The Accumulo support is only required for certain functions (distributed rendering) - so this may be skipped in some cases.

### Building: Maven!

There is one dependency - [uzaygezen](https://code.google.com/p/uzaygezen/) (a compact hilbert space filling curve implementation in java) - which is not in maven central.   So let's install that locally first:

    $ svn checkout http://uzaygezen.googlecode.com/svn/trunk/ uzaygezen-read-only
	$ cd uzaygezen-read-only && mvn install

Next, since GeoWave itself isn't currently in maven central we will build and install a local copy

	$ git clone git@github.com:ngageoint/geowave.git
	$ cd geowave && mvn install

If everything worked as expected you should see something like

    [INFO] ------------------------------------------------------------------------
    [INFO] Reactor Summary:
    [INFO]
    [INFO] geowave-parent .................................... SUCCESS [1.132s]
    [INFO] geowave-index ..................................... SUCCESS [6.559s]
    [INFO] geowave-store ..................................... SUCCESS [2.046s]
    [INFO] geowave-accumulo .................................. SUCCESS [4.402s]
    [INFO] geowave-gt ........................................ SUCCESS [5.056s]
    [INFO] geowave-ingest .................................... SUCCESS [2.847s]
    [INFO] geowave-analytics ................................. SUCCESS [4.749s]
    [INFO] ------------------------------------------------------------------------
    [INFO] BUILD SUCCESS
    [INFO] ------------------------------------------------------------------------
    [INFO] Total time: 27.112s
    [INFO] Finished at: Mon Jun 09 21:22:16 EDT 2014
    [INFO] Final Memory: 80M/382M
    [INFO] ------------------------------------------------------------------------


### Deploy: GeoServer

First we need to build the geoserver plugin - from the geowave root directory:
    
    $ cd geowave-gt
    $ mvn package -Pgeotools-container-singlejar

let's assume you have geoserver deployed in a tomcat container in /opt/tomcat

    $ cp target/geowave-gt-0.7.0-geoserver-singlejar.jar /opt/tomcat/webapps/geoserver/WEB-INF/lib/

and re-start tomcat

### Deploy: Accumulo

This should be very familiar by now; from the geowave root directory:

    $ cd geowave-gt
    $ mvn package -Paccumulo-container-singlejar

This distributable needs to be in the Accumulo classpath on every tablet server.  Accumulo (in 1.5) leverages the Apache VFSClassloader to provide an easy way to do this in one shot - load dependencies out of HDFS.  Unfortunately we have a current bug (**GEOWAVE-28**) where the hdfs URI breaks some SPI injection fixes we have in place.  So until this is addressed you need to distribute this jar (*geowave-gt-0.7.0-accumulo-singlejar.jar*) to each tabled server, on the accumulo classpath ( $ACCUMULO_HOME/lib/ext/  is the default choice ).  

### GeoWave System Integration Test

This module will run end-to-end integration testing on either a configured Accumulo instance or a temporary MiniAccumuloCluster.  It will ingest both point and line features spatially and temporally from shapefiles and test that spatial and spatial-temporal queries match expected results.

A specific Accumulo instance can be configured either directly within this pom.xml or as Java options -DzookeeperUrl=&lt;zookeeperUrl&gt; -Dinstance=&lt;instance&gt; -Dusername=&lt;username&gt; -Dpassword=&lt;password&gt;

If any of these configuration parameters are left unspecified the default integration test will use a MiniAccumuloCluster created within a temporary directory.  For this to work on Windows, make sure Cygwin is installed and a "CYGPATH" environment variable must reference the &lt;CYGWIN_HOME&gt;/bin/cygpath.exe file.  

GeoWave was developed at the National Geospatial-Intelligence Agency (NGA) in collaboration with [RadiantBlue Technologies](http://www.radiantblue.com/) and [Booz Allen Hamilton](http://www.boozallen.com/).  The government has "unlimited rights" and is releasing this software to increase the impact of government investments by providing developers with the opportunity to take things in new directions. The software use, modification, and distribution rights are stipulated within the [Apache 2.0](http://www.apache.org/licenses/LICENSE-2.0.html) license.  


## Pull Requests

All pull request contributions to this project will be released under the Apache 2.0 license.  

Software source code previously released under an open source license and then modified by NGA staff is considered a "joint work" (see *17 USC  101*); it is partially copyrighted, partially public domain, and as a whole is protected by the copyrights of the non-government authors and must be released according to the terms of the original open source license.

## *Ultra* Quickstart

### Dependencies: Software and Versions
This *ultra* quickstart assumes you have [Apache Maven](http://maven.apache.org/), [Git](http://git-scm.com/), and [Java](http://www.oracle.com/technetwork/java/javase/downloads/index.html) (>= 1.7) installed on your system and on the path.  

To view the data (via standard [OGC](http://www.opengeospatial.org/) services) a [GeoServer](http://geoserver.org/) instance >= 2.5 is required due to [GEOT-4587](http://jira.codehaus.org/browse/GEOT-4587).  
[GeoServer](http://geoserver.org/) 2.5 with [Geotools](http://www.geotools.org/) 11.0 the most tested version.

[Apache Accumulo](http://projects.apache.org/projects/accumulo.html) version 1.5 or greater is required.  1.5.0, 1.5.1, and 1.6.0 have all been tested. 

[Apache Hadoop](http://hadoop.apache.org/) versions 1.x and 2.x *should* all work.  The software has specifically been run on [Cloudera](http://cloudera.com/content/cloudera/en/home.html) CDH4 and [Hortonworks Data Platform](http://hortonworks.com/hdp/) 2.1.   

MapReduce 1 with the new API (org.apache.hadoop.mapreduce.*) is used.  Testing is underway against YARN / MR2 and seems to be positive, but well, it's still underway.  

[Java Advanced Imaging](http://download.java.net/media/jai/builds/release/1_1_3/) and [Java Image I/O](http://download.java.net/media/jai-imageio/builds/release/1.1/) are also both required to be installed on the GeoServer instance(s) *as well* as on the Accumulo nodes.  The Accumulo support is only required for certain functions (distributed rendering) - so this may be skipped in some cases.

### Building: Maven!

There is one dependency - [uzaygezen](https://code.google.com/p/uzaygezen/) (a compact hilbert space filling curve implementation in java) - which is not in maven central.   So let's install that locally first:

    $ svn checkout http://uzaygezen.googlecode.com/svn/trunk/ uzaygezen-read-only
	$ cd uzaygezen-read-only && mvn install

Next, since GeoWave itself isn't currently in maven central we will build and install a local copy

	$ git clone git@github.com:ngageoint/geowave.git
	$ cd geowave && mvn install

If everything worked as expected you should see something like

    [INFO] ------------------------------------------------------------------------
    [INFO] Reactor Summary:
    [INFO]
    [INFO] geowave-parent .................................... SUCCESS [1.132s]
    [INFO] geowave-index ..................................... SUCCESS [6.559s]
    [INFO] geowave-store ..................................... SUCCESS [2.046s]
    [INFO] geowave-accumulo .................................. SUCCESS [4.402s]
    [INFO] geowave-gt ........................................ SUCCESS [5.056s]
    [INFO] geowave-ingest .................................... SUCCESS [2.847s]
    [INFO] geowave-analytics ................................. SUCCESS [4.749s]
    [INFO] ------------------------------------------------------------------------
    [INFO] BUILD SUCCESS
    [INFO] ------------------------------------------------------------------------
    [INFO] Total time: 27.112s
    [INFO] Finished at: Mon Jun 09 21:22:16 EDT 2014
    [INFO] Final Memory: 80M/382M
    [INFO] ------------------------------------------------------------------------


### Deploy: GeoServer

First we need to build the geoserver plugin - from the geowave root directory:
    
    $ cd geowave-gt
    $ mvn package -Pgeotools-container-singlejar

let's assume you have geoserver deployed in a tomcat container in /opt/tomcat

    $ cp target/geowave-gt-0.7.0-geoserver-singlejar.jar /opt/tomcat/webapps/geoserver/WEB-INF/lib/

and re-start tomcat

### Deploy: Accumulo

This should be very familiar by now; from the geowave root directory:

    $ cd geowave-gt
    $ mvn package -Paccumulo-container-singlejar

This distributable needs to be in the Accumulo classpath on every tablet server.  Accumulo (in 1.5) leverages the Apache VFSClassloader to provide an easy way to do this in one shot - load dependencies out of HDFS.  Unfortunately we have a current bug (**GEOWAVE-28**) where the hdfs URI breaks some SPI injection fixes we have in place.  So until this is addressed you need to distribute this jar (*geowave-gt-0.7.0-accumulo-singlejar.jar*) to each tabled server, on the accumulo classpath ( $ACCUMULO_HOME/lib/ext/  is the default choice ).  

### GeoWave System Integration Test

This module will run end-to-end integration testing on either a configured Accumulo instance or a temporary MiniAccumuloCluster.  It will ingest both point and line features spatially and temporally from shapefiles and test that spatial and spatial-temporal queries match expected results.

A specific Accumulo instance can be configured either directly within this pom.xml or as Java options -DzookeeperUrl=&lt;zookeeperUrl&gt; -Dinstance=&lt;instance&gt; -Dusername=&lt;username&gt; -Dpassword=&lt;password&gt;

If any of these configuration parameters are left unspecified the default integration test will use a MiniAccumuloCluster created within a temporary directory.  For this to work on Windows, make sure Cygwin is installed and a "CYGPATH" environment variable must reference the &lt;CYGWIN_HOME&gt;/bin/cygpath.exe file.  

GeoWave was developed at the National Geospatial-Intelligence Agency (NGA) in collaboration with [RadiantBlue Technologies](http://www.radiantblue.com/) and [Booz Allen Hamilton](http://www.boozallen.com/).  The government has "unlimited rights" and is releasing this software to increase the impact of government investments by providing developers with the opportunity to take things in new directions. The software use, modification, and distribution rights are stipulated within the [Apache 2.0](http://www.apache.org/licenses/LICENSE-2.0.html) license.  


## Pull Requests

All pull request contributions to this project will be released under the Apache 2.0 license.  

Software source code previously released under an open source license and then modified by NGA staff is considered a "joint work" (see *17 USC  101*); it is partially copyrighted, partially public domain, and as a whole is protected by the copyrights of the non-government authors and must be released according to the terms of the original open source license.

## *Ultra* Quickstart

### Dependencies: Software and Versions
This *ultra* quickstart assumes you have [Apache Maven](http://maven.apache.org/), [Git](http://git-scm.com/), and [Java](http://www.oracle.com/technetwork/java/javase/downloads/index.html) (>= 1.7) installed on your system and on the path.  

To view the data (via standard [OGC](http://www.opengeospatial.org/) services) a [GeoServer](http://geoserver.org/) instance >= 2.5 is required due to [GEOT-4587](http://jira.codehaus.org/browse/GEOT-4587).  
[GeoServer](http://geoserver.org/) 2.5 with [Geotools](http://www.geotools.org/) 11.0 the most tested version.

[Apache Accumulo](http://projects.apache.org/projects/accumulo.html) version 1.5 or greater is required.  1.5.0, 1.5.1, and 1.6.0 have all been tested. 

[Apache Hadoop](http://hadoop.apache.org/) versions 1.x and 2.x *should* all work.  The software has specifically been run on [Cloudera](http://cloudera.com/content/cloudera/en/home.html) CDH4 and [Hortonworks Data Platform](http://hortonworks.com/hdp/) 2.1.   

MapReduce 1 with the new API (org.apache.hadoop.mapreduce.*) is used.  Testing is underway against YARN / MR2 and seems to be positive, but well, it's still underway.  

[Java Advanced Imaging](http://download.java.net/media/jai/builds/release/1_1_3/) and [Java Image I/O](http://download.java.net/media/jai-imageio/builds/release/1.1/) are also both required to be installed on the GeoServer instance(s) *as well* as on the Accumulo nodes.  The Accumulo support is only required for certain functions (distributed rendering) - so this may be skipped in some cases.

### Building: Maven!

There is one dependency - [uzaygezen](https://code.google.com/p/uzaygezen/) (a compact hilbert space filling curve implementation in java) - which is not in maven central.   So let's install that locally first:

    $ svn checkout http://uzaygezen.googlecode.com/svn/trunk/ uzaygezen-read-only
	$ cd uzaygezen-read-only && mvn install

Next, since GeoWave itself isn't currently in maven central we will build and install a local copy

	$ git clone git@github.com:ngageoint/geowave.git
	$ cd geowave && mvn install

If everything worked as expected you should see something like

    [INFO] ------------------------------------------------------------------------
    [INFO] Reactor Summary:
    [INFO]
    [INFO] geowave-parent .................................... SUCCESS [1.132s]
    [INFO] geowave-index ..................................... SUCCESS [6.559s]
    [INFO] geowave-store ..................................... SUCCESS [2.046s]
    [INFO] geowave-accumulo .................................. SUCCESS [4.402s]
    [INFO] geowave-gt ........................................ SUCCESS [5.056s]
    [INFO] geowave-ingest .................................... SUCCESS [2.847s]
    [INFO] geowave-analytics ................................. SUCCESS [4.749s]
    [INFO] geowave-test ...................................... SUCCESS [2.158s]
    [INFO] ------------------------------------------------------------------------
    [INFO] BUILD SUCCESS
    [INFO] ------------------------------------------------------------------------
    [INFO] Total time: 29.270s
    [INFO] Finished at: Mon Jun 09 21:22:16 EDT 2014
    [INFO] Final Memory: 80M/382M
    [INFO] ------------------------------------------------------------------------


### Deploy: GeoServer

First we need to build the geoserver plugin - from the geowave root directory:
    
    $ cd geowave-gt
    $ mvn package -Pgeotools-container-singlejar

let's assume you have geoserver deployed in a tomcat container in /opt/tomcat

    $ cp target/geowave-gt-0.7.0-geoserver-singlejar.jar /opt/tomcat/webapps/geoserver/WEB-INF/lib/

and re-start tomcat

### Deploy: Accumulo

This should be very familiar by now; from the geowave root directory:

    $ cd geowave-gt
    $ mvn package -Paccumulo-container-singlejar

This distributable needs to be in the Accumulo classpath on every tablet server.  Accumulo (in 1.5) leverages the Apache VFSClassloader to provide an easy way to do this in one shot - load dependencies out of HDFS.  Unfortunately we have a current bug (**GEOWAVE-28**) where the hdfs URI breaks some SPI injection fixes we have in place.  So until this is addressed you need to distribute this jar (*geowave-gt-0.7.0-accumulo-singlejar.jar*) to each tabled server, on the accumulo classpath ( $ACCUMULO_HOME/lib/ext/  is the default choice ).  

### GeoWave System Integration Test

The geowave-test module will run end-to-end integration testing on either a configured Accumulo instance or a temporary MiniAccumuloCluster.  It will ingest both point and line features spatially and temporally from shapefiles and test that spatial and spatial-temporal queries match expected results.

A specific Accumulo instance can be configured either directly within this pom.xml or as Java options -DzookeeperUrl=&lt;zookeeperUrl&gt; -Dinstance=&lt;instance&gt; -Dusername=&lt;username&gt; -Dpassword=&lt;password&gt;

If any of these configuration parameters are left unspecified the default integration test will use a MiniAccumuloCluster created within a temporary directory.  For this to work on Windows, make sure Cygwin is installed and a "CYGPATH" environment variable must reference the &lt;CYGWIN_HOME&gt;/bin/cygpath.exe file.  

### Ingest Data

*Coming Soon! With useful details!*

(basically see  geowave-ingest::mil.nga.giat.geowave.ingest.VectorFileIngest for an example that can ingest any geotools supported format) 


### View in GeoServer

*Coming Soon! With screenshots as well!*

(basically add a GeoWave data store through the add datastore menu and configure the layer like normal)