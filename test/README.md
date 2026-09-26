# GeoWave System Integration Test

## About

This module runs GeoWave's end-to-end integration tests (ITs). The ITs are collected in `GeoWaveITSuite` (`src/test/java/org/locationtech/geowave/test/GeoWaveITSuite.java`), which the Maven Failsafe plugin runs during `verify`. Each IT stands up the data store it needs (an embedded or mini cluster where possible), ingests test data, and checks query, statistics, and analytic results against it.

The build requires JDK 21. Use Maven 3.9+ or the committed `./mvnw`.

## Running

ITs are skipped by default. Enable them by selecting a data store with one of the profiles in `test/pom.xml`:

| Profile | Data store |
|---------|------------|
| `accumulo-it-client`, `accumulo-it-server`, `accumulo-it-all`, `accumulo-it-kerberos` | Accumulo (server-side library off, on, both; Kerberos) |
| `hbase-it-client`, `hbase-it-server`, `hbase-it-all` | HBase (server-side library off, on, both) |
| `cassandra-it` | Cassandra |
| `dynamodb-it` | DynamoDB |
| `redis-it` | Redis |
| `rocksdb-it` | RocksDB |
| `filesystem-it` | FileSystem |

`secondary-index-it` enables secondary indexing and can be combined with a store profile, e.g. `-Pfilesystem-it,secondary-index-it`.

From the repository root, the following runs the suite against RocksDB the same way CI does, skipping unit tests:

```
./mvnw verify -am -pl test -Procksdb-it -Dtest=SkipUnitTests -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false -Dspotbugs.skip
```

To run a single IT instead of the whole suite, add `-Dit.test=<ClassName>`, e.g. `-Dit.test=GeoWaveBasicTemporalVectorIT`.

The profiles set the `testStoreType` and `testStoreOptions` system properties, which can also be passed directly (or through the `STORE_TYPE` and `STORE_OPTIONS` environment variables). `testStoreType` is one of `ACCUMULO`, `HBASE`, `CASSANDRA`, `DYNAMODB`, `REDIS`, `ROCKSDB`, or `FILESYSTEM`; if it is not set, the ITs use RocksDB. `testStoreOptions` is a comma-separated list of store options, and `!` separates option sets that should each get a full run.

The Accumulo ITs use a temporary MiniAccumuloCluster unless an existing instance is given with `-DzookeeperUrl=<zookeeperUrl> -Dinstance=<instance> -Dusername=<username> -Dpassword=<password>`. On Windows, MiniAccumuloCluster needs Cygwin, with `<CYGWIN_HOME>/bin` on the `PATH` and a `CYGPATH` environment variable pointing to `<CYGWIN_HOME>/bin/cygpath.exe`.

CI also puts Hadoop's native libraries for the build's `hadoop.version` on `LD_LIBRARY_PATH`; see `.utility/run-tests.sh`.

## Apple Silicon

The ITs, including `rocksdb-it`, run on Apple Silicon Macs, but the build needs x86_64 protoc binaries and two passes; see [Building](https://locationtech.github.io/geowave/latest/devguide.html#building) in the Developer Guide. Build both passes first, then run the ITs without `-am`, which would rebuild the raster and HBase modules with the gRPC modules' protoc:

```
./mvnw verify -pl test -Procksdb-it -Dtest=SkipUnitTests -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false -Dspotbugs.skip -DprotocCommand=/tmp/protoc/protoc-3.17.1-osx-x86_64.exe
```

Hadoop's native libraries are Linux-only, and the ITs do not need them.
