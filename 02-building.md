---
layout: page
title: Building
header: Building
group: navigation
permalink: "building.html"
---
{% include JB/setup %}


GeoWave will shortly be available in maven central (for tagged releases), but until then - or to get the latest features - building GeoWave from source is the best bet.

## Dependencies

This *ultra* quickstart assumes you have installed and configured: 



- [Git](http://git-scm.com/)
- [Java JDK](http://www.oracle.com/technetwork/java/javase/downloads/index.html) (>= 1.7).  The OracleJDK is the most thoroughly tested, but there are no known issues with OpenJDK.
- [GeoServer](http://geoserver.org/) instance >= 2.5.2 (due to: [GEOT-4587](http://jira.codehaus.org/browse/GEOT-4587))
- [Apache Accumulo](http://projects.apache.org/projects/accumulo.html) version 1.5 or greater is required.  1.5.0, 1.5.1, and 1.6.0 have all been tested.
- [Apache Hadoop](http://hadoop.apache.org/) versions 1.x and 2.x *should* all work.  The software has specifically been run on: 
   - [Cloudera](http://cloudera.com/content/cloudera/en/home.html) CDH4 and CDH5 (MR1)
   - [Hortonworks Data Platform](http://hortonworks.com/hdp/) 2.1.   
   - MapReduce 1 with the new API (org.apache.hadoop.mapreduce.*) is used.  Testing is underway against YARN / MR2 and seems to be positive, but well, it's still underway.
- [Java Advanced Imaging](http://download.java.net/media/jai/builds/release/1_1_3/) and [Java Image I/O](http://download.java.net/media/jai-imageio/builds/release/1.1/) are also both required to be installed on the GeoServer instance(s) *as well* as on the Accumulo nodes.  The Accumulo support is only required for certain functions (distributed rendering) - so this may be skipped in some cases.

## Maven dependencies
Required repositories not in Maven Centrol have been added to the parent POM.   Specifically the cloudera and opengeo repos.  


## Building

Checkout geowave, and run a maven install.

```
$ git clone git@github.com:ngageoint/geowave.git
$ cd geowave && mvn install 
```

<div class="note warning">
  <h5>Integration Tests: Windows</h5>
  <p>
Integration tests are currently not working on Windows out of the box.  If you install cygwin and set the environmental variable CYGPATH to the logation of the cygpath binary provided by cygwin then is <i>should</i> work.
  </p>
</div>


<div class="note note">
  <h5>Eclipse and Avro generated files</h5>
  <p>
	There are Avro generated sources in the geowave-types project and geowave-ingest.  They can be created by running "mvn generate-sources" (and are compiled as part of the standard maven lifecycle normally).  Unfortunately, I can't seem to make eclipse run this portion of the lifecycle on import with the M2E plugin.   On first import simply type "mvn generate-sources" at the command line and then refresh your eclipse project.
  </p>
</div>
