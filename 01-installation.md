---
layout: page
title: Installation
header: Installation
group: navigation
permalink: "installation.html"
---
{% include JB/setup %}


# Deploy: GeoServer

## Versions
GeoWave is currently built against Geoserver 2.5.2 and Geotools 11.2.  The Geotools version will typically be linked to the geoserver version.  If wish to deploy against a different version simply change the value in the pom and rebuild (see: building) - but no guaruantees.  

## Install
First we need to build the geoserver plugin - from the geowave root directory:
    
    $ cd geowave-gt
    $ mvn package -Pgeotools-container-singlejar

let's assume you have geoserver deployed in a tomcat container in /opt/tomcat

    $ cp target/geowave-gt-0.7.0-geoserver-singlejar.jar /opt/tomcat/webapps/geoserver/WEB-INF/lib/

and re-start tomcat

# Deploy: Accumulo

## Versions
GeoWave has been tested and works against accumulo 1.5.0, 1.5.1, and 1.6.0.  If you wish to build against a different version simply change the value in the pom.

## Install
This should be very familiar by now; from the geowave root directory:

    $ cd geowave-gt
    $ mvn package -Paccumulo-container-singlejar

This distributable needs to be in the Accumulo classpath on every tablet server.  

There are (with Accumulo 1.5.x or greater) two out of the box ways of doing this:
1. Copy the jar into the $ACCUMULO_HOME/lib/ext/
2. Copy the jar into a HDFS directory that has been registered in the $ACCUMULO_HOME/conf/accumulo-site.xml file under general.vfs.classpaths.   See [ACCUMULO-708](https://issues.apache.org/jira/browse/ACCUMULO-708) for more information.


<div class="note note">
  <h5>Dependency versions</h5>
  <p>
    It was mentioned above, but just to emphasize:<br/>
  	GeoWave is currently (as of v 0.8.1) built against Accumulo 1.5.1, Geoserver 2.5.2, and Geotools 11.2.<br/>
	If this doesn't match the environment you are deploying to be sure to change these values in the parent pom.xml, located in the root of the GeoWave project.
  </p>
</div>
