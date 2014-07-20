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
- [GeoServer](http://geoserver.org/) instance >= 2.5 (due to: [GEOT-4587](http://jira.codehaus.org/browse/GEOT-4587))
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

You should see something that looks similar to:

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



