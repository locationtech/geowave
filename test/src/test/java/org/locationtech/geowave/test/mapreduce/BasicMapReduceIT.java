/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.mapreduce;

import java.io.File;
import java.io.FileFilter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.geotools.data.DataStoreFinder;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.adapter.raster.util.ZipUtils;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.adapter.InitializeWithIndicesDataAdapter;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.format.gpx.GpxIngestPlugin;
import org.locationtech.geowave.mapreduce.GeoWaveConfiguratorBase;
import org.locationtech.geowave.mapreduce.GeoWaveWritableInputMapper;
import org.locationtech.geowave.mapreduce.dedupe.GeoWaveDedupeJobRunner;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputFormat;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputKey;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.TestUtils.DimensionalityType;
import org.locationtech.geowave.test.TestUtils.ExpectedResults;
import org.locationtech.geowave.test.annotation.Environments;
import org.locationtech.geowave.test.annotation.Environments.Environment;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.locationtech.geowave.test.basic.AbstractGeoWaveIT;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@RunWith(GeoWaveITRunner.class)
@Environments({Environment.MAP_REDUCE})
public class BasicMapReduceIT extends AbstractGeoWaveIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(BasicMapReduceIT.class);
  protected static final String TEST_DATA_ZIP_RESOURCE_PATH =
      TestUtils.TEST_RESOURCE_PACKAGE + "mapreduce-testdata.zip";
  protected static final String TEST_CASE_GENERAL_GPX_BASE =
      TestUtils.TEST_CASE_BASE + "general_gpx_test_case/";
  protected static final String GENERAL_GPX_FILTER_PACKAGE = TEST_CASE_GENERAL_GPX_BASE + "filter/";
  protected static final String GENERAL_GPX_FILTER_FILE = GENERAL_GPX_FILTER_PACKAGE + "filter.shp";
  protected static final String GENERAL_GPX_INPUT_GPX_DIR =
      TEST_CASE_GENERAL_GPX_BASE + "input_gpx/";
  protected static final String GENERAL_GPX_EXPECTED_RESULTS_DIR =
      TEST_CASE_GENERAL_GPX_BASE + "filter_results/";
  protected static final String OSM_GPX_INPUT_DIR = TestUtils.TEST_CASE_BASE + "osm_gpx_test_case/";

  private static long startMillis;

  @BeforeClass
  public static void extractTestFiles() throws URISyntaxException {
    ZipUtils.unZipFile(
        new File(
            MapReduceTestEnvironment.class.getClassLoader().getResource(
                TEST_DATA_ZIP_RESOURCE_PATH).toURI()),
        TestUtils.TEST_CASE_BASE);

    startMillis = System.currentTimeMillis();
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*         RUNNING BasicMapReduceIT      *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");
  }

  @AfterClass
  public static void reportTest() {
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*      FINISHED BasicMapReduceIT        *");
    LOGGER.warn(
        "*         "
            + ((System.currentTimeMillis() - startMillis) / 1000)
            + "s elapsed.                 *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");
  }

  public static enum ResultCounterType {
    EXPECTED, UNEXPECTED, ERROR
  }

  @GeoWaveTestStore(
      value = {
          GeoWaveStoreType.ACCUMULO,
          GeoWaveStoreType.BIGTABLE,
          GeoWaveStoreType.HBASE,
          GeoWaveStoreType.DYNAMODB,
          GeoWaveStoreType.CASSANDRA,
          GeoWaveStoreType.KUDU,
          GeoWaveStoreType.REDIS,
          GeoWaveStoreType.ROCKSDB})
  protected DataStorePluginOptions dataStorePluginOptions;

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return dataStorePluginOptions;
  }

  @Test
  public void testIngestAndQueryGeneralGpx() throws Exception {
    TestUtils.deleteAll(dataStorePluginOptions);
    MapReduceTestUtils.testMapReduceIngest(
        dataStorePluginOptions,
        DimensionalityType.SPATIAL,
        GENERAL_GPX_INPUT_GPX_DIR);
    final File gpxInputDir = new File(GENERAL_GPX_INPUT_GPX_DIR);
    final File expectedResultsDir = new File(GENERAL_GPX_EXPECTED_RESULTS_DIR);
    final List<URL> expectedResultsResources = new ArrayList<>();
    final Map<String, URL> baseNameToExpectedResultURL = new HashMap<>();

    for (final File file : expectedResultsDir.listFiles(new FileFilter() {

      @Override
      public boolean accept(final File pathname) {
        final Map<String, Object> map = new HashMap<>();
        try {
          map.put("url", pathname.toURI().toURL());
          return DataStoreFinder.getDataStore(map) != null;
        } catch (final IOException e) {
          LOGGER.warn("Cannot read file as GeoTools data store", e);
        }
        return false;
      }
    })) {
      baseNameToExpectedResultURL.put(
          FilenameUtils.getBaseName(file.getName()).replaceAll("_filtered", ""),
          file.toURI().toURL());
    }
    for (final String filename : gpxInputDir.list(new FilenameFilter() {
      @Override
      public boolean accept(final File dir, final String name) {
        return FilenameUtils.isExtension(name, new GpxIngestPlugin().getFileExtensionFilters());
      }
    })) {
      final URL url = baseNameToExpectedResultURL.get(FilenameUtils.getBaseName(filename));
      Assert.assertNotNull(url);
      expectedResultsResources.add(url);
    }
    final ExpectedResults expectedResults =
        TestUtils.getExpectedResults(
            expectedResultsResources.toArray(new URL[expectedResultsResources.size()]));
    runTestJob(
        expectedResults,
        TestUtils.resourceToQuery(new File(GENERAL_GPX_FILTER_FILE).toURI().toURL()),
        null,
        null);
  }

  @Test
  public void testIngestOsmGpxMultipleIndices() throws Exception {
    TestUtils.deleteAll(dataStorePluginOptions);
    // ingest the data set into multiple indices and then try several query
    // methods, by adapter and by index
    MapReduceTestUtils.testMapReduceIngest(
        dataStorePluginOptions,
        DimensionalityType.ALL,
        OSM_GPX_INPUT_DIR);
    final DataTypeAdapter<SimpleFeature>[] adapters = new GpxIngestPlugin().getDataAdapters(null);

    for (final DataTypeAdapter<SimpleFeature> adapter : adapters) {
      if (adapter instanceof InitializeWithIndicesDataAdapter) {
        ((InitializeWithIndicesDataAdapter) adapter).init(TestUtils.DEFAULT_SPATIAL_INDEX);
      }
    }

    final org.locationtech.geowave.core.store.api.DataStore geowaveStore =
        dataStorePluginOptions.createDataStore();
    final Map<String, ExpectedResults> adapterIdToResultsMap = new HashMap<>();
    for (final DataTypeAdapter<SimpleFeature> adapter : adapters) {
      adapterIdToResultsMap.put(
          adapter.getTypeName(),
          TestUtils.getExpectedResults(
              geowaveStore.query(
                  QueryBuilder.newBuilder().addTypeName(adapter.getTypeName()).build())));
    }

    final List<DataTypeAdapter<?>> firstTwoAdapters = new ArrayList<>();
    firstTwoAdapters.add(adapters[0]);
    firstTwoAdapters.add(adapters[1]);

    final ExpectedResults firstTwoAdaptersResults =
        TestUtils.getExpectedResults(
            geowaveStore.query(
                QueryBuilder.newBuilder().addTypeName(adapters[0].getTypeName()).addTypeName(
                    adapters[1].getTypeName()).build()));

    final ExpectedResults fullDataSetResults =
        TestUtils.getExpectedResults(geowaveStore.query(QueryBuilder.newBuilder().build()));

    // just for sanity verify its greater than 0 (ie. that data was actually
    // ingested in the first place)
    Assert.assertTrue(
        "There is no data ingested from OSM GPX test files",
        fullDataSetResults.count > 0);

    // now that we have expected results, run map-reduce export and
    // re-ingest it
    testMapReduceExportAndReingest(DimensionalityType.ALL);
    // first try each adapter individually
    for (final DataTypeAdapter<SimpleFeature> adapter : adapters) {
      final ExpectedResults expResults = adapterIdToResultsMap.get(adapter.getTypeName());

      if (expResults.count > 0) {
        LOGGER.error("Running test for adapter " + adapter.getTypeName());
        runTestJob(expResults, null, new DataTypeAdapter[] {adapter}, null);
      }
    }

    // then try the first 2 adapters, and may as well try with both indices
    // set (should be the default behavior anyways)
    runTestJob(
        firstTwoAdaptersResults,
        null,
        new DataTypeAdapter[] {adapters[0], adapters[1]},
        null);

    // now try all adapters and the spatial temporal index, the result
    // should be the full data set
    runTestJob(fullDataSetResults, null, adapters, TestUtils.DEFAULT_SPATIAL_TEMPORAL_INDEX);

    // and finally run with nothing set, should be the full data set
    runTestJob(fullDataSetResults, null, null, null);
  }

  private void testMapReduceExportAndReingest(final DimensionalityType dimensionalityType)
      throws Exception {
    MapReduceTestUtils.testMapReduceExportAndReingest(
        dataStorePluginOptions,
        dataStorePluginOptions,
        dimensionalityType);
  }

  @SuppressFBWarnings(value = "DM_GC", justification = "Memory usage kept low for travis-ci")
  private void runTestJob(
      final ExpectedResults expectedResults,
      final QueryConstraints query,
      final DataTypeAdapter<?>[] adapters,
      final Index index) throws Exception {
    final TestJobRunner jobRunner = new TestJobRunner(dataStorePluginOptions, expectedResults);
    jobRunner.setMinInputSplits(MapReduceTestUtils.MIN_INPUT_SPLITS);
    jobRunner.setMaxInputSplits(MapReduceTestUtils.MAX_INPUT_SPLITS);
    final QueryBuilder<?, ?> bldr = QueryBuilder.newBuilder();
    if (query != null) {
      bldr.constraints(query);
    }
    if ((index != null)) {
      bldr.indexName(index.getName());
    }
    final Configuration conf = MapReduceTestUtils.getConfiguration();

    MapReduceTestUtils.filterConfiguration(conf);
    if ((adapters != null) && (adapters.length > 0)) {
      Arrays.stream(adapters).forEach(a -> bldr.addTypeName(a.getTypeName()));
    }
    jobRunner.setQuery(bldr.build());
    final int res = ToolRunner.run(conf, jobRunner, new String[] {});
    Assert.assertEquals(0, res);
    // for travis-ci to run, we want to limit the memory consumption
    System.gc();
  }

  private static class TestJobRunner extends GeoWaveDedupeJobRunner {
    private final ExpectedResults expectedResults;

    public TestJobRunner(
        final DataStorePluginOptions pluginOptions,
        final ExpectedResults expectedResults) {
      super(pluginOptions);
      this.expectedResults = expectedResults;
    }

    @Override
    protected String getHdfsOutputBase() {
      return MapReduceTestEnvironment.getInstance().getHdfsBaseDirectory();
    }

    @Override
    public int runJob() throws Exception {
      final boolean job1Success = (super.runJob() == 0);
      Assert.assertTrue(job1Success);
      // after the first job there should be a sequence file with the
      // filtered results which should match the expected results
      // resources

      final Job job = Job.getInstance(super.getConf());

      final Configuration conf = job.getConfiguration();
      MapReduceTestUtils.filterConfiguration(conf);
      final ByteBuffer buf = ByteBuffer.allocate((8 * expectedResults.hashedCentroids.size()) + 4);
      buf.putInt(expectedResults.hashedCentroids.size());
      for (final Long hashedCentroid : expectedResults.hashedCentroids) {
        buf.putLong(hashedCentroid);
      }
      conf.set(
          MapReduceTestUtils.EXPECTED_RESULTS_KEY,
          ByteArrayUtils.byteArrayToString(buf.array()));

      GeoWaveInputFormat.setStoreOptions(conf, dataStoreOptions);
      job.setJarByClass(this.getClass());

      job.setJobName("GeoWave Test (" + dataStoreOptions.getGeoWaveNamespace() + ")");
      job.setInputFormatClass(SequenceFileInputFormat.class);
      job.setMapperClass(VerifyExpectedResultsMapper.class);
      job.setMapOutputKeyClass(NullWritable.class);
      job.setMapOutputValueClass(NullWritable.class);
      job.setOutputFormatClass(NullOutputFormat.class);
      job.setNumReduceTasks(0);
      job.setSpeculativeExecution(false);
      FileInputFormat.setInputPaths(job, getHdfsOutputPath());

      final boolean job2success = job.waitForCompletion(true);
      final Counters jobCounters = job.getCounters();
      final Counter expectedCnt = jobCounters.findCounter(ResultCounterType.EXPECTED);
      Assert.assertNotNull(expectedCnt);
      Assert.assertEquals(expectedResults.count, expectedCnt.getValue());
      final Counter errorCnt = jobCounters.findCounter(ResultCounterType.ERROR);
      if (errorCnt != null) {
        Assert.assertEquals(0L, errorCnt.getValue());
      }
      final Counter unexpectedCnt = jobCounters.findCounter(ResultCounterType.UNEXPECTED);
      if (unexpectedCnt != null) {
        Assert.assertEquals(0L, unexpectedCnt.getValue());
      }
      return job2success ? 0 : 1;
    }
  }

  private static class VerifyExpectedResultsMapper extends
      GeoWaveWritableInputMapper<NullWritable, NullWritable> {
    private Set<Long> expectedHashedCentroids = new HashSet<>();

    @Override
    protected void mapNativeValue(
        final GeoWaveInputKey key,
        final Object value,
        final Mapper<GeoWaveInputKey, ObjectWritable, NullWritable, NullWritable>.Context context)
        throws IOException, InterruptedException {
      ResultCounterType resultType = ResultCounterType.ERROR;
      if (value instanceof SimpleFeature) {
        final SimpleFeature result = (SimpleFeature) value;
        final Geometry geometry = (Geometry) result.getDefaultGeometry();
        if (!geometry.isEmpty()) {
          resultType =
              expectedHashedCentroids.contains(TestUtils.hashCentroid(geometry))
                  ? ResultCounterType.EXPECTED
                  : ResultCounterType.UNEXPECTED;
        }
      }
      context.getCounter(resultType).increment(1);
    }

    @Override
    protected void setup(
        final Mapper<GeoWaveInputKey, ObjectWritable, NullWritable, NullWritable>.Context context)
        throws IOException, InterruptedException {
      super.setup(context);
      final Configuration config = GeoWaveConfiguratorBase.getConfiguration(context);
      final String expectedResults = config.get(MapReduceTestUtils.EXPECTED_RESULTS_KEY);
      if (expectedResults != null) {
        expectedHashedCentroids = new HashSet<>();
        final byte[] expectedResultsBinary = ByteArrayUtils.byteArrayFromString(expectedResults);
        final ByteBuffer buf = ByteBuffer.wrap(expectedResultsBinary);
        final int count = buf.getInt();
        for (int i = 0; i < count; i++) {
          expectedHashedCentroids.add(buf.getLong());
        }
      }
    }
  }
}
