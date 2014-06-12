# GeoWave System Integration Test

## About

This module will run end-to-end integration testing on either a configured Accumulo instance or a temporary MiniAccumuloCluster.  It will ingest both point and line features spatially and temporally from shapefiles and test that spatial and spatial-temporal queries match expected results.

## Setup

A specific Accumulo instance can be configured either directly within this pom.xml or as Java options -DzookeeperUrl=&lt;zookeeperUrl&gt; -Dinstance=&lt;instance&gt; -Dusername=&lt;username&gt; -Dpassword=&lt;password&gt;

If any of these configuration parameters are left unspecified the default integration test will use a MiniAccumuloCluster created within a temporary directory.  For this to work on Windows, make sure Cygwin is installed, &lt;CYGWIN_HOME&gt;/bin is in the "PATH" environment variable, and a "CYGPATH" environment variable must reference the &lt;CYGWIN_HOME&gt;/bin/cygpath.exe file.  

