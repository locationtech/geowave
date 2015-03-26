# Change Log

## [Unreleased](https://github.com/ngageoint/geowave/tree/HEAD)

[Full Changelog](https://github.com/ngageoint/geowave/compare/v0.8.3...HEAD)

**Implemented enhancements:**

- Support subseting of selected attributes within iterators [\#237](https://github.com/ngageoint/geowave/issues/237)

- Add GeoWave to Vagrant config for PDAL. [\#234](https://github.com/ngageoint/geowave/issues/234)

- Maven Checkstyle + format all code [\#227](https://github.com/ngageoint/geowave/issues/227)

- Store statistics for time ranges & connect up to datastore [\#218](https://github.com/ngageoint/geowave/issues/218)

- Determine best Guava version to use [\#205](https://github.com/ngageoint/geowave/issues/205)

- Add a latitude dimension definition that is the same range as the longitude definition [\#200](https://github.com/ngageoint/geowave/issues/200)

- Change default vector index to provide more tiers at lower resolutions [\#199](https://github.com/ngageoint/geowave/issues/199)

- Allow max polygon/line decomposition to be passed as a parameter to index strategy [\#198](https://github.com/ngageoint/geowave/issues/198)

- Add core dependency versions to shaded jar redistributable [\#150](https://github.com/ngageoint/geowave/issues/150)

- Migrate docs to asciidoc [\#147](https://github.com/ngageoint/geowave/issues/147)

- Remote submission of existing mapreduce jobs on yarn [\#128](https://github.com/ngageoint/geowave/issues/128)

- Improvements to JUMP K-Means Cluster integration [\#119](https://github.com/ngageoint/geowave/issues/119)

- Update code, interfaces, and documentation to clarify namespace type: \(accumulo vs. geowave\) [\#65](https://github.com/ngageoint/geowave/issues/65)

- Geospatial benchmark utility [\#28](https://github.com/ngageoint/geowave/issues/28)

- Create pointcloud \(LAS probably\) ingester [\#15](https://github.com/ngageoint/geowave/issues/15)

**Fixed bugs:**

- Exception when exposing data through geoserver with more than two date fields [\#283](https://github.com/ngageoint/geowave/issues/283)

- RPM build jobs failing [\#261](https://github.com/ngageoint/geowave/issues/261)

- Fix race condition in ZooKeeperTransactionsAllocater [\#251](https://github.com/ngageoint/geowave/issues/251)

- Apache Hadoop distros integration [\#241](https://github.com/ngageoint/geowave/issues/241)

- IT Environment fixes [\#231](https://github.com/ngageoint/geowave/issues/231)

- Fix reader/writer support for BigDecimal and BigInteger. [\#79](https://github.com/ngageoint/geowave/issues/79)

**Closed issues:**

- Exclude guava from shaded jar built with geotools-singlejar profile [\#267](https://github.com/ngageoint/geowave/issues/267)

- BigInteger.class errors [\#256](https://github.com/ngageoint/geowave/issues/256)

**Merged pull requests:**

- Geowave 283 [\#287](https://github.com/ngageoint/geowave/pull/287) ([rwgdrummer](https://github.com/rwgdrummer))

- Add: Added in changelog auto generation [\#284](https://github.com/ngageoint/geowave/pull/284) ([chrisbennight](https://github.com/chrisbennight))

- Integration & unit tests for examples [\#281](https://github.com/ngageoint/geowave/pull/281) ([chrisbennight](https://github.com/chrisbennight))

- Coverty triage, phase V [\#277](https://github.com/ngageoint/geowave/pull/277) ([chrisbennight](https://github.com/chrisbennight))

- applied eclipse formatter; attached formatter to process-sources lifecycle [\#276](https://github.com/ngageoint/geowave/pull/276) ([chrisbennight](https://github.com/chrisbennight))

- exclude guava from geotools jar \#267 [\#275](https://github.com/ngageoint/geowave/pull/275) ([rfecher](https://github.com/rfecher))

- Geowave vagrant [\#273](https://github.com/ngageoint/geowave/pull/273) ([jwomeara](https://github.com/jwomeara))

- Sonar plugin version and java doc lint [\#272](https://github.com/ngageoint/geowave/pull/272) ([spohnan](https://github.com/spohnan))

- Geowave 229 \(\#229\) [\#270](https://github.com/ngageoint/geowave/pull/270) ([rfecher](https://github.com/rfecher))

- Fixed spelling [\#266](https://github.com/ngageoint/geowave/pull/266) ([viggyprabhu](https://github.com/viggyprabhu))

- Fixes for coverity id'd issues [\#265](https://github.com/ngageoint/geowave/pull/265) ([chrisbennight](https://github.com/chrisbennight))

- Refactor man page generation [\#262](https://github.com/ngageoint/geowave/pull/262) ([spohnan](https://github.com/spohnan))

- CI config changes to speed up tests [\#260](https://github.com/ngageoint/geowave/pull/260) ([chrisbennight](https://github.com/chrisbennight))

- Fixed BigDecimal readers/writers to properly serialize values, registered missing writers/readers, added integration test [\#259](https://github.com/ngageoint/geowave/pull/259) ([chrisbennight](https://github.com/chrisbennight))

- Minor docu fixes. [\#255](https://github.com/ngageoint/geowave/pull/255) ([bradh](https://github.com/bradh))

- GEOWAVE-251 [\#254](https://github.com/ngageoint/geowave/pull/254) ([rwgdrummer](https://github.com/rwgdrummer))

- GEOWAVE-252: updated README.md to reference new docs URL for images [\#253](https://github.com/ngageoint/geowave/pull/253) ([rfecher](https://github.com/rfecher))

- GEOWAVE-218 [\#249](https://github.com/ngageoint/geowave/pull/249) ([rwgdrummer](https://github.com/rwgdrummer))

- 0.8.4-RELEASE [\#248](https://github.com/ngageoint/geowave/pull/248) ([jwomeara](https://github.com/jwomeara))

- Documentation and man page reorganization [\#246](https://github.com/ngageoint/geowave/pull/246) ([spohnan](https://github.com/spohnan))

- Kmeans cl [\#245](https://github.com/ngageoint/geowave/pull/245) ([rwgdrummer](https://github.com/rwgdrummer))

- Build against Apache Hadoop [\#242](https://github.com/ngageoint/geowave/pull/242) ([dlyle65535](https://github.com/dlyle65535))

## [v0.8.3](https://github.com/ngageoint/geowave/tree/v0.8.3) (2015-02-26)

[Full Changelog](https://github.com/ngageoint/geowave/compare/v0.8.2...v0.8.3)

**Implemented enhancements:**

- Write documentation for PDAL GeoWave driver [\#225](https://github.com/ngageoint/geowave/issues/225)

- Create RPMs for GeoWave S3 Dev and Release Repos [\#214](https://github.com/ngageoint/geowave/issues/214)

- Refactor Integration Test Environments [\#213](https://github.com/ngageoint/geowave/issues/213)

- remove any snapshot dependencies [\#210](https://github.com/ngageoint/geowave/issues/210)

- Update travis config to run against Geoserver 2.6.2 [\#190](https://github.com/ngageoint/geowave/issues/190)

- Relocate geowave-accumulo.jar HDFS location to /accumulo/classpath/geowave [\#186](https://github.com/ngageoint/geowave/issues/186)

- Create man pages for geowave-ingest [\#164](https://github.com/ngageoint/geowave/issues/164)

- Implement OSM ingest plugin [\#158](https://github.com/ngageoint/geowave/issues/158)

- Add data adapter and index caching to improve performance [\#54](https://github.com/ngageoint/geowave/issues/54)

- Create PDAL driver [\#14](https://github.com/ngageoint/geowave/issues/14)

**Fixed bugs:**

- Temporal CQL filter always attempt to use SPATIAL\_TEMPORAL index even if it doesn't exist [\#189](https://github.com/ngageoint/geowave/issues/189)

**Merged pull requests:**

- Geowave 234 [\#247](https://github.com/ngageoint/geowave/pull/247) ([jwomeara](https://github.com/jwomeara))

- Handle no tables added case [\#232](https://github.com/ngageoint/geowave/pull/232) ([chrisbennight](https://github.com/chrisbennight))

- Geowave 228 [\#230](https://github.com/ngageoint/geowave/pull/230) ([chrisbennight](https://github.com/chrisbennight))

- Updated test package versions. [\#226](https://github.com/ngageoint/geowave/pull/226) ([jwomeara](https://github.com/jwomeara))

- Geowave 14 [\#220](https://github.com/ngageoint/geowave/pull/220) ([jwomeara](https://github.com/jwomeara))

- A number of tweaks to our install materials and docs [\#219](https://github.com/ngageoint/geowave/pull/219) ([spohnan](https://github.com/spohnan))

- refactoring test environments into src to be reusable [\#216](https://github.com/ngageoint/geowave/pull/216) ([chrisbennight](https://github.com/chrisbennight))

- Add dev and release repo RPMs to packaging directory [\#215](https://github.com/ngageoint/geowave/pull/215) ([spohnan](https://github.com/spohnan))

- changed snapshot coveralls tweak to release [\#212](https://github.com/ngageoint/geowave/pull/212) ([chrisbennight](https://github.com/chrisbennight))

- incrementing version to 0.8.3 [\#211](https://github.com/ngageoint/geowave/pull/211) ([jwomeara](https://github.com/jwomeara))

- Kmeans cl [\#244](https://github.com/ngageoint/geowave/pull/244) ([rwgdrummer](https://github.com/rwgdrummer))

- Build against Apache Hadoop [\#240](https://github.com/ngageoint/geowave/pull/240) ([dlyle65535](https://github.com/dlyle65535))

- Added a check to ensure that null iterators aren't attached to accumulo. [\#233](https://github.com/ngageoint/geowave/pull/233) ([jwomeara](https://github.com/jwomeara))

## [v0.8.2](https://github.com/ngageoint/geowave/tree/v0.8.2) (2015-01-29)

[Full Changelog](https://github.com/ngageoint/geowave/compare/v0.8.1...v0.8.2)

**Implemented enhancements:**

- Disable speculative execution in ingest framework / document in input/output format docs [\#170](https://github.com/ngageoint/geowave/issues/170)

- Add extension point for programmatically retyping geotools vector data sources [\#169](https://github.com/ngageoint/geowave/issues/169)

- Create a wrapper script for geowave-ingest [\#161](https://github.com/ngageoint/geowave/issues/161)

- Infer time field based on the name of the attribute [\#153](https://github.com/ngageoint/geowave/issues/153)

- Abstract implementation of file based HDFS ingest [\#151](https://github.com/ngageoint/geowave/issues/151)

- Get codecov support working with travis containers [\#148](https://github.com/ngageoint/geowave/issues/148)

- Incorporate setting table classpaths into ingest-tool.jar [\#136](https://github.com/ngageoint/geowave/issues/136)

- Mark JAI as provided only with the geotools container [\#129](https://github.com/ngageoint/geowave/issues/129)

- Add rpm and tarball packaging [\#121](https://github.com/ngageoint/geowave/issues/121)

- YARN support for MR jobs in ingest framework [\#117](https://github.com/ngageoint/geowave/issues/117)

- Implement Maven repo caching in travis-ci config [\#96](https://github.com/ngageoint/geowave/issues/96)

- Implement GeoWaveInputFormat for mapreduce [\#84](https://github.com/ngageoint/geowave/issues/84)

- Consider allowing for visibility within all GEOWAVE\_METADATA persistable objects [\#70](https://github.com/ngageoint/geowave/issues/70)

- REST API for service access to GeoWave datastores [\#42](https://github.com/ngageoint/geowave/issues/42)

**Fixed bugs:**

- Geoserver layer preview broken during packaging [\#182](https://github.com/ngageoint/geowave/issues/182)

- Packaging of puppet RPM fails due to path error in source archive [\#180](https://github.com/ngageoint/geowave/issues/180)

- Integration tests do not properly update zookeeper port for geoserver plugin [\#179](https://github.com/ngageoint/geowave/issues/179)

- deploy-geowave-to-hdfs.sh script issues [\#154](https://github.com/ngageoint/geowave/issues/154)

- Fix heatmap stats render transform [\#141](https://github.com/ngageoint/geowave/issues/141)

- Fix bugs in the persistence model for raster merge metadata [\#137](https://github.com/ngageoint/geowave/issues/137)

- Output streams sometimes closed twice in geowave iterators [\#22](https://github.com/ngageoint/geowave/issues/22)

**Closed issues:**

- Move codecoverage over to coveralls [\#175](https://github.com/ngageoint/geowave/issues/175)

- Readability change for range comparison logic for fine range filter  [\#167](https://github.com/ngageoint/geowave/issues/167)

- Package Puppet scripts as an RPM [\#165](https://github.com/ngageoint/geowave/issues/165)

- Delete no longer considers duplicates [\#159](https://github.com/ngageoint/geowave/issues/159)

- Remove instances of geotools 2.2 filter generation [\#126](https://github.com/ngageoint/geowave/issues/126)

- Allow the Transaction Set to be constructed at install time [\#122](https://github.com/ngageoint/geowave/issues/122)

- Add codecov.io coverage  [\#120](https://github.com/ngageoint/geowave/issues/120)

- release version 0.8.1 and update to 0.8.2-SNAPSHOT [\#112](https://github.com/ngageoint/geowave/issues/112)

**Merged pull requests:**

- release v0.8.2 [\#209](https://github.com/ngageoint/geowave/pull/209) ([rfecher](https://github.com/rfecher))

- fix for integration tests [\#208](https://github.com/ngageoint/geowave/pull/208) ([rfecher](https://github.com/rfecher))

- Geowave 198 [\#207](https://github.com/ngageoint/geowave/pull/207) ([rfecher](https://github.com/rfecher))

- Geowave 199 [\#206](https://github.com/ngageoint/geowave/pull/206) ([rfecher](https://github.com/rfecher))

- adding dimension definition that creates equal sfc ranges in both lat an... [\#203](https://github.com/ngageoint/geowave/pull/203) ([rfecher](https://github.com/rfecher))

- using input and output format for kde and raster format \(issue \#102\); te... [\#202](https://github.com/ngageoint/geowave/pull/202) ([rfecher](https://github.com/rfecher))

- tweaks to make jacoco work / cleanup cobertura error spam [\#197](https://github.com/ngageoint/geowave/pull/197) ([chrisbennight](https://github.com/chrisbennight))

- Add docs for RPM list and Accumulo config [\#196](https://github.com/ngageoint/geowave/pull/196) ([spohnan](https://github.com/spohnan))

- Fix various path issues and add RPM build requirement [\#195](https://github.com/ngageoint/geowave/pull/195) ([spohnan](https://github.com/spohnan))

- GEOWAVE-189 [\#194](https://github.com/ngageoint/geowave/pull/194) ([rwgdrummer](https://github.com/rwgdrummer))

- HDFS upload script should use su instead of sudo -u [\#192](https://github.com/ngageoint/geowave/pull/192) ([spohnan](https://github.com/spohnan))

- Updated geowave-test to support testing for GEOSERVER 2.6.2. [\#191](https://github.com/ngageoint/geowave/pull/191) ([jwomeara](https://github.com/jwomeara))

- Geowave 42 [\#188](https://github.com/ngageoint/geowave/pull/188) ([jwomeara](https://github.com/jwomeara))

- Move the location of the geowave-accumulo.jar in HDFS [\#187](https://github.com/ngageoint/geowave/pull/187) ([spohnan](https://github.com/spohnan))

- Refactor geowave-ingest command docs so we can use for manpages as well [\#184](https://github.com/ngageoint/geowave/pull/184) ([spohnan](https://github.com/spohnan))

- Fixes layer preview bug by removing layergroup config files [\#183](https://github.com/ngageoint/geowave/pull/183) ([spohnan](https://github.com/spohnan))

- Add puppet doc page and fix archive path error that broke build [\#181](https://github.com/ngageoint/geowave/pull/181) ([spohnan](https://github.com/spohnan))

- Fixed a few logic bugs, minor style tweaks, minor cleanup [\#178](https://github.com/ngageoint/geowave/pull/178) ([chrisbennight](https://github.com/chrisbennight))

- Swapped codecoverage over to coveralls; removed codecov.io references [\#177](https://github.com/ngageoint/geowave/pull/177) ([chrisbennight](https://github.com/chrisbennight))

- Geowave 170 [\#176](https://github.com/ngageoint/geowave/pull/176) ([chrisbennight](https://github.com/chrisbennight))

- fix persistence encoding test to not depend on jvm time zone [\#174](https://github.com/ngageoint/geowave/pull/174) ([rfecher](https://github.com/rfecher))

- allowing for isolation of integration tests as well as running as a suite \(issue \#103\) [\#173](https://github.com/ngageoint/geowave/pull/173) ([rfecher](https://github.com/rfecher))

- GEOWAVE-159 [\#172](https://github.com/ngageoint/geowave/pull/172) ([rwgdrummer](https://github.com/rwgdrummer))

- extension point for retyping vector data \(issue \#169\) [\#171](https://github.com/ngageoint/geowave/pull/171) ([rfecher](https://github.com/rfecher))

- tweaks to range comparison fine filtering [\#168](https://github.com/ngageoint/geowave/pull/168) ([chrisbennight](https://github.com/chrisbennight))

- Manage Puppet scripts within GeoWave project and package as an RPM [\#166](https://github.com/ngageoint/geowave/pull/166) ([spohnan](https://github.com/spohnan))

- Add geowave-ingest helper script with bash command completion [\#162](https://github.com/ngageoint/geowave/pull/162) ([spohnan](https://github.com/spohnan))

- Conversion of gh-pages content into AsciiDoc [\#160](https://github.com/ngageoint/geowave/pull/160) ([spohnan](https://github.com/spohnan))

- added more default readers and writers, hdfs file ingest relies on subcl... [\#156](https://github.com/ngageoint/geowave/pull/156) ([rfecher](https://github.com/rfecher))

- Fix su and hdfs -put issues with hdfs upload script [\#155](https://github.com/ngageoint/geowave/pull/155) ([spohnan](https://github.com/spohnan))

- Geowave 151 [\#152](https://github.com/ngageoint/geowave/pull/152) ([rfecher](https://github.com/rfecher))

- Enabled travis containers \(required for caching\) + caching. [\#149](https://github.com/ngageoint/geowave/pull/149) ([chrisbennight](https://github.com/chrisbennight))

- Geowave 141 [\#142](https://github.com/ngageoint/geowave/pull/142) ([rfecher](https://github.com/rfecher))

- see \#117; added configuration for yarn to ingest [\#140](https://github.com/ngageoint/geowave/pull/140) ([rfecher](https://github.com/rfecher))

- minor fix for custom image types [\#139](https://github.com/ngageoint/geowave/pull/139) ([rfecher](https://github.com/rfecher))

- issue \#137: fixed bugs with mergeable metadata in raster persistence mod... [\#138](https://github.com/ngageoint/geowave/pull/138) ([rfecher](https://github.com/rfecher))

- OS packaging for CentOS/RHEL 6 [\#135](https://github.com/ngageoint/geowave/pull/135) ([spohnan](https://github.com/spohnan))

- retract feature instance name: GEOWAVE-132 [\#134](https://github.com/ngageoint/geowave/pull/134) ([rwgdrummer](https://github.com/rwgdrummer))

- raster merging fix [\#133](https://github.com/ngageoint/geowave/pull/133) ([rfecher](https://github.com/rfecher))

- added streaming polygon \(convex hull\) generation [\#131](https://github.com/ngageoint/geowave/pull/131) ([bptran](https://github.com/bptran))

- issue 129: marking jai scope to geotools container; additional fixes to ... [\#130](https://github.com/ngageoint/geowave/pull/130) ([rfecher](https://github.com/rfecher))

- Geowave 126 - Remove instances of geotools 2.2 filter generation [\#127](https://github.com/ngageoint/geowave/pull/127) ([chrisbennight](https://github.com/chrisbennight))

- Update to Apache Accumolo project url [\#125](https://github.com/ngageoint/geowave/pull/125) ([state-hiu](https://github.com/state-hiu))

- added codecov.io support; fixes for dependencies in integration tests [\#124](https://github.com/ngageoint/geowave/pull/124) ([chrisbennight](https://github.com/chrisbennight))

- GEOWAVE-122 [\#123](https://github.com/ngageoint/geowave/pull/123) ([rwgdrummer](https://github.com/rwgdrummer))

- added clustering to analytics [\#118](https://github.com/ngageoint/geowave/pull/118) ([bptran](https://github.com/bptran))

- updated to version 0.8.2-SNAPSHOT [\#114](https://github.com/ngageoint/geowave/pull/114) ([rfecher](https://github.com/rfecher))

- Geowave 102 [\#201](https://github.com/ngageoint/geowave/pull/201) ([rfecher](https://github.com/rfecher))

## [v0.8.1](https://github.com/ngageoint/geowave/tree/v0.8.1) (2014-11-19)

[Full Changelog](https://github.com/ngageoint/geowave/compare/v0.8.0...v0.8.1)

**Implemented enhancements:**

- Parse routes from GPX data [\#89](https://github.com/ngageoint/geowave/issues/89)

- Polygon, line improvement limit row IDs on ingest [\#75](https://github.com/ngageoint/geowave/issues/75)

- Accumulo namespace support [\#66](https://github.com/ngageoint/geowave/issues/66)

- Use pre-computed Bounding Box from Data Statistics metadata [\#60](https://github.com/ngageoint/geowave/issues/60)

- persist indexed bounds \(spatial\) in metadata table [\#46](https://github.com/ngageoint/geowave/issues/46)

- additional support for gridded/raster datasets [\#45](https://github.com/ngageoint/geowave/issues/45)

**Fixed bugs:**

- Expose a namespace parameter per data store for our GeoTools Vector Data Store [\#106](https://github.com/ngageoint/geowave/issues/106)

- testPointRange test failing after pull request \#93  [\#95](https://github.com/ngageoint/geowave/issues/95)

- ZookeeperTransactionAllocatorTest produces inconsistent results [\#94](https://github.com/ngageoint/geowave/issues/94)

- Add example SLDs to geowave-examples [\#91](https://github.com/ngageoint/geowave/issues/91)

- Investigate possible issue with inverse SFC lookup \(getRangeForId\(\) method\) with temporal fields [\#90](https://github.com/ngageoint/geowave/issues/90)

- Implement GeoTools classloader fix for VFSClassloader more generically [\#87](https://github.com/ngageoint/geowave/issues/87)

- update geowave-examples to reflect 1.8.1 changes [\#80](https://github.com/ngageoint/geowave/issues/80)

- fix issue with jai dependency and raster format SPI [\#77](https://github.com/ngageoint/geowave/issues/77)

- Mark jai as provided [\#63](https://github.com/ngageoint/geowave/issues/63)

- Ensure uzaygezen library usage respects end of range as exclusive [\#41](https://github.com/ngageoint/geowave/issues/41)

**Closed issues:**

- Coverity High Priority Defects [\#99](https://github.com/ngageoint/geowave/issues/99)

**Merged pull requests:**

- Geowave 112 [\#113](https://github.com/ngageoint/geowave/pull/113) ([rfecher](https://github.com/rfecher))

- Capture build info within artifacts [\#110](https://github.com/ngageoint/geowave/pull/110) ([spohnan](https://github.com/spohnan))

- Geowave 106 redux [\#109](https://github.com/ngageoint/geowave/pull/109) ([rwgdrummer](https://github.com/rwgdrummer))

- GEOWAVE-106 [\#108](https://github.com/ngageoint/geowave/pull/108) ([rwgdrummer](https://github.com/rwgdrummer))

- GPX GEOWAVE-89 with latest master [\#105](https://github.com/ngageoint/geowave/pull/105) ([rwgdrummer](https://github.com/rwgdrummer))

- geowave input format \(issue \#84\) [\#101](https://github.com/ngageoint/geowave/pull/101) ([rfecher](https://github.com/rfecher))

- GEOWAVE-99. [\#100](https://github.com/ngageoint/geowave/pull/100) ([rwgdrummer](https://github.com/rwgdrummer))

- incorporated last commit from GEOWAVE-75-Opt [\#98](https://github.com/ngageoint/geowave/pull/98) ([rwgdrummer](https://github.com/rwgdrummer))

- GEOWAVE-75 Optimization. The optimization required code to deal with sma... [\#93](https://github.com/ngageoint/geowave/pull/93) ([rfecher](https://github.com/rfecher))

- added example SLDs for decimation and heatmap [\#92](https://github.com/ngageoint/geowave/pull/92) ([rfecher](https://github.com/rfecher))

- Issue \#87: geotools vfs classloader fix without requiring jar naming conventions [\#88](https://github.com/ngageoint/geowave/pull/88) ([rfecher](https://github.com/rfecher))

- GEOWAVE-60  Stats for WFS-T.  Added counts.  Added deletion. [\#86](https://github.com/ngageoint/geowave/pull/86) ([rfecher](https://github.com/rfecher))

- see issues: \#16, \#67, \#73 [\#82](https://github.com/ngageoint/geowave/pull/82) ([rfecher](https://github.com/rfecher))

- updated ingest examples to reflect recent code changes [\#81](https://github.com/ngageoint/geowave/pull/81) ([rfecher](https://github.com/rfecher))

- fixed package name in raster format and jai dependency [\#78](https://github.com/ngageoint/geowave/pull/78) ([rfecher](https://github.com/rfecher))

- Geowave 45 \(support for raster data\) [\#76](https://github.com/ngageoint/geowave/pull/76) ([rfecher](https://github.com/rfecher))

- GeoWave-Analytics Clustering  [\#111](https://github.com/ngageoint/geowave/pull/111) ([bptran](https://github.com/bptran))

- fixed feature namespace issue [\#107](https://github.com/ngageoint/geowave/pull/107) ([mfarwell](https://github.com/mfarwell))

- Capture build info within artifacts [\#104](https://github.com/ngageoint/geowave/pull/104) ([spohnan](https://github.com/spohnan))

## [v0.8.0](https://github.com/ngageoint/geowave/tree/v0.8.0) (2014-09-23)

[Full Changelog](https://github.com/ngageoint/geowave/compare/0.7.0...v0.8.0)

**Implemented enhancements:**

- Create geowave-examples project [\#57](https://github.com/ngageoint/geowave/issues/57)

- Ensure geowave works with Accumulo 1.6 / drop Accumulo 1.4 support if needed [\#56](https://github.com/ngageoint/geowave/issues/56)

- Create Travis build matrix to test multiple configurations [\#55](https://github.com/ngageoint/geowave/issues/55)

- Add locality group caching to address performance issues. [\#49](https://github.com/ngageoint/geowave/issues/49)

- add delete by row ID [\#47](https://github.com/ngageoint/geowave/issues/47)

- Basic Utility functions [\#43](https://github.com/ngageoint/geowave/issues/43)

- Please put up some screenshots [\#40](https://github.com/ngageoint/geowave/issues/40)

- Integrate Continuous Integration \(probably Travis CI\) [\#32](https://github.com/ngageoint/geowave/issues/32)

- Options for AccumuloDataStore [\#29](https://github.com/ngageoint/geowave/issues/29)

- Move uzaygezen dependency over to maven central [\#26](https://github.com/ngageoint/geowave/issues/26)

- Implement get feature by id index [\#17](https://github.com/ngageoint/geowave/issues/17)

- Add maven profile for generation of an executable jar for geotools datastore ingest / add geotools plugin datastores to dependencies [\#3](https://github.com/ngageoint/geowave/issues/3)

- Add a module that can perform end to end system integration testing [\#2](https://github.com/ngageoint/geowave/issues/2)

- Implement a generalized MapReduce ingest process to use for GPX point and line ingest [\#1](https://github.com/ngageoint/geowave/issues/1)

**Fixed bugs:**

- Servlet.class being included in shaded geotools plugin jar [\#61](https://github.com/ngageoint/geowave/issues/61)

- Ensure GeoTools feature collection iterable is closed after ingestion [\#36](https://github.com/ngageoint/geowave/issues/36)

- Ensure CQL filtering is enabled for all GeoTool's data store queries [\#35](https://github.com/ngageoint/geowave/issues/35)

- Zookeeper connection pool / fault handling for geoserver plugin [\#21](https://github.com/ngageoint/geowave/issues/21)

- Geotools datastores / iterators not properly releasing handle on shapefiles during integration tests [\#11](https://github.com/ngageoint/geowave/issues/11)

- Iterator classloader hack breaks with hdfs URI prefix [\#10](https://github.com/ngageoint/geowave/issues/10)

-  3D Geometries are stored as 2D [\#9](https://github.com/ngageoint/geowave/issues/9)

- Transparently handle geometric transformation to EPSG:4326 on ingest for features using other coordinate reference systems [\#5](https://github.com/ngageoint/geowave/issues/5)

**Closed issues:**

- Fix issue related to Calendar to GMT [\#68](https://github.com/ngageoint/geowave/issues/68)

- NPE Exception when Coordinate method for geometry returns null [\#24](https://github.com/ngageoint/geowave/issues/24)

**Merged pull requests:**

- Corrected TimeUtils.calendarToGMTMillis method. Added serveral junit tes... [\#69](https://github.com/ngageoint/geowave/pull/69) ([rhayes-bah](https://github.com/rhayes-bah))

- GEOWAVE-57 [\#64](https://github.com/ngageoint/geowave/pull/64) ([chrisbennight](https://github.com/chrisbennight))

- GEOWAVE-61 Exclude servlet classes from shaded jar [\#62](https://github.com/ngageoint/geowave/pull/62) ([chrisbennight](https://github.com/chrisbennight))

- Geolife input type [\#59](https://github.com/ngageoint/geowave/pull/59) ([chrisbennight](https://github.com/chrisbennight))

- GEOWAVE-55/56 created tavis build matrix/ensure accumulo 1.6.x works [\#58](https://github.com/ngageoint/geowave/pull/58) ([chrisbennight](https://github.com/chrisbennight))

- Added support for locality group caching, entry deletion, and alternativ... [\#52](https://github.com/ngageoint/geowave/pull/52) ([jwomeara](https://github.com/jwomeara))

- T drive datatype for ingest [\#51](https://github.com/ngageoint/geowave/pull/51) ([chrisbennight](https://github.com/chrisbennight))

- provided methods for all GeoTools queries to apply CQL filtering within ... [\#44](https://github.com/ngageoint/geowave/pull/44) ([rfecher](https://github.com/rfecher))

- fixed performance issue with integration tests; used closeable iterators... [\#39](https://github.com/ngageoint/geowave/pull/39) ([rfecher](https://github.com/rfecher))

- Added Accumulo Options which allow you to configure the behavior of the ... [\#38](https://github.com/ngageoint/geowave/pull/38) ([jwomeara](https://github.com/jwomeara))

- Updated the CqlQueryFilterIterator to support URL class loading from hdf... [\#37](https://github.com/ngageoint/geowave/pull/37) ([jwomeara](https://github.com/jwomeara))

- Geowave 1 - ingest framework [\#34](https://github.com/ngageoint/geowave/pull/34) ([rfecher](https://github.com/rfecher))

- introducing travis ci yaml to run integration tests and publish javadocs... [\#33](https://github.com/ngageoint/geowave/pull/33) ([rfecher](https://github.com/rfecher))

- check for empty geometry [\#31](https://github.com/ngageoint/geowave/pull/31) ([chrisbennight](https://github.com/chrisbennight))

- Moved uzaygezen dependency to maven central [\#27](https://github.com/ngageoint/geowave/pull/27) ([chrisbennight](https://github.com/chrisbennight))

- Default WKBWriter to 2 dimensions unless 3rd is detected [\#25](https://github.com/ngageoint/geowave/pull/25) ([chrisbennight](https://github.com/chrisbennight))

- Geowave 9 - looks good to me [\#20](https://github.com/ngageoint/geowave/pull/20) ([rfecher](https://github.com/rfecher))

- Geowave 3 [\#19](https://github.com/ngageoint/geowave/pull/19) ([chrisbennight](https://github.com/chrisbennight))

- Geowave 11 [\#18](https://github.com/ngageoint/geowave/pull/18) ([chrisbennight](https://github.com/chrisbennight))

- formatting on tests and a simple update to ingested ranges regarding bit... [\#8](https://github.com/ngageoint/geowave/pull/8) ([rfecher](https://github.com/rfecher))

- Geowave 5 - transform geometry on ingest to EPSG:4326 [\#7](https://github.com/ngageoint/geowave/pull/7) ([rfecher](https://github.com/rfecher))

- Geowave 2 - integrations tests [\#6](https://github.com/ngageoint/geowave/pull/6) ([rfecher](https://github.com/rfecher))

## [0.7.0](https://github.com/ngageoint/geowave/tree/0.7.0) (2014-06-11)



\* *This Change Log was automatically generated by [github_changelog_generator](https://github.com/skywinder/Github-Changelog-Generator)*