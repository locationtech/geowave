/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test;

import java.awt.image.BufferedImage;
import java.awt.image.WritableRaster;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import javax.ws.rs.core.Response;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.util.VersionUtil;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.junit.Assert;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.cli.parser.ManualOperationParams;
import org.locationtech.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider.SpatialIndexBuilder;
import org.locationtech.geowave.core.geotime.ingest.SpatialOptions;
import org.locationtech.geowave.core.geotime.ingest.SpatialTemporalDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.ingest.SpatialTemporalDimensionalityTypeProvider.SpatialTemporalIndexBuilder;
import org.locationtech.geowave.core.geotime.ingest.SpatialTemporalOptions;
import org.locationtech.geowave.core.geotime.store.query.ExplicitSpatialQuery;
import org.locationtech.geowave.core.geotime.store.query.ExplicitSpatialTemporalQuery;
import org.locationtech.geowave.core.geotime.store.query.OptimalCQLQuery;
import org.locationtech.geowave.core.geotime.store.query.SpatialQuery;
import org.locationtech.geowave.core.geotime.store.query.SpatialTemporalQuery;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.geotime.util.TWKBReader;
import org.locationtech.geowave.core.geotime.util.TWKBWriter;
import org.locationtech.geowave.core.geotime.util.TimeUtils;
import org.locationtech.geowave.core.ingest.local.LocalInputCommandLineOptions;
import org.locationtech.geowave.core.ingest.operations.ConfigAWSCommand;
import org.locationtech.geowave.core.ingest.operations.LocalToGeowaveCommand;
import org.locationtech.geowave.core.ingest.operations.options.IngestFormatPluginOptions;
import org.locationtech.geowave.core.ingest.spark.SparkCommandLineOptions;
import org.locationtech.geowave.core.ingest.spark.SparkIngestDriver;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.cli.config.AddIndexCommand;
import org.locationtech.geowave.core.store.cli.config.AddStoreCommand;
import org.locationtech.geowave.core.store.cli.remote.ListStatsCommand;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.cli.remote.options.IndexPluginOptions;
import org.locationtech.geowave.core.store.cli.remote.options.VisibilityOptions;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.ParseException;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.And;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.ParameterException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class TestUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestUtils.class);

  public static enum DimensionalityType {
    SPATIAL("spatial", DEFAULT_SPATIAL_INDEX),
    SPATIAL_TEMPORAL("spatial_temporal", DEFAULT_SPATIAL_TEMPORAL_INDEX),
    ALL("spatial,spatial_temporal",
        new Index[] {DEFAULT_SPATIAL_INDEX, DEFAULT_SPATIAL_TEMPORAL_INDEX});
    private final String dimensionalityArg;
    private final Index[] indices;

    private DimensionalityType(final String dimensionalityArg, final Index index) {
      this(dimensionalityArg, new Index[] {index});
    }

    private DimensionalityType(final String dimensionalityArg, final Index[] indices) {
      this.dimensionalityArg = dimensionalityArg;
      this.indices = indices;
    }

    public String getDimensionalityArg() {
      return dimensionalityArg;
    }

    public Index[] getDefaultIndices() {
      return indices;
    }
  }

  public static final File TEMP_DIR = new File("./target/temp");

  public static final String TEST_FILTER_START_TIME_ATTRIBUTE_NAME = "StartTime";
  public static final String TEST_FILTER_END_TIME_ATTRIBUTE_NAME = "EndTime";
  public static final String TEST_NAMESPACE = "mil_nga_giat_geowave_test";
  public static final String TEST_NAMESPACE_BAD = "mil_nga_giat_geowave_test_BAD";
  public static final String TEST_RESOURCE_PACKAGE = "org/locationtech/geowave/test/";
  public static final String TEST_CASE_BASE = "data/";

  public static final Index DEFAULT_SPATIAL_INDEX = new SpatialIndexBuilder().createIndex();
  public static final Index DEFAULT_SPATIAL_TEMPORAL_INDEX =
      new SpatialTemporalIndexBuilder().createIndex();
  // CRS for Web Mercator
  public static String CUSTOM_CRSCODE = "EPSG:3857";

  public static final CoordinateReferenceSystem CUSTOM_CRS;

  public static final double DOUBLE_EPSILON = 1E-8d;

  static {
    try {
      CUSTOM_CRS = CRS.decode(CUSTOM_CRSCODE, true);
    } catch (final FactoryException e) {
      LOGGER.error("Unable to decode " + CUSTOM_CRSCODE + "CRS", e);
      throw new RuntimeException("Unable to initialize " + CUSTOM_CRSCODE + " CRS");
    }
  }

  public static Index createWebMercatorSpatialIndex() {
    final SpatialDimensionalityTypeProvider sdp = new SpatialDimensionalityTypeProvider();
    final SpatialOptions so = sdp.createOptions();
    so.setCrs(CUSTOM_CRSCODE);
    final Index primaryIndex = sdp.createIndex(so);
    return primaryIndex;
  }

  public static Index createWebMercatorSpatialTemporalIndex() {
    final SpatialTemporalDimensionalityTypeProvider p =
        new SpatialTemporalDimensionalityTypeProvider();
    final SpatialTemporalOptions o = p.createOptions();
    o.setCrs(CUSTOM_CRSCODE);
    final Index primaryIndex = p.createIndex(o);
    return primaryIndex;
  }

  public static final String S3_INPUT_PATH = "s3://geowave-test/data/gdelt";
  public static final String S3URL = "s3.amazonaws.com";

  public static boolean isYarn() {
    return VersionUtil.compareVersions(VersionInfo.getVersion(), "2.2.0") >= 0;
  }

  public static void testLocalIngest(
      final DataStorePluginOptions dataStore,
      final DimensionalityType dimensionalityType,
      final String ingestFilePath,
      final int nthreads) throws Exception {
    testLocalIngest(dataStore, dimensionalityType, ingestFilePath, "geotools-vector", nthreads);
  }

  public static void testLocalIngest(
      final DataStorePluginOptions dataStore,
      final DimensionalityType dimensionalityType,
      final String ingestFilePath) throws Exception {
    testLocalIngest(dataStore, dimensionalityType, ingestFilePath, "geotools-vector", 1);
  }

  public static boolean isSet(final String str) {
    return (str != null) && !str.isEmpty();
  }

  public static void deleteAll(final DataStorePluginOptions dataStore) {
    dataStore.createDataStore().delete(QueryBuilder.newBuilder().build());
  }

  public static void testLocalIngest(
      final DataStorePluginOptions dataStore,
      final DimensionalityType dimensionalityType,
      final String ingestFilePath,
      final String format,
      final int nthreads) throws Exception {
    testLocalIngest(dataStore, dimensionalityType, null, ingestFilePath, format, nthreads);
  }

  public static void testLocalIngest(
      final DataStorePluginOptions dataStore,
      final DimensionalityType dimensionalityType,
      final String crsCode,
      final String ingestFilePath,
      final String format,
      final int nthreads) throws Exception {

    // ingest a shapefile (geotools type) directly into GeoWave using the
    // ingest framework's main method and pre-defined commandline arguments

    // Ingest Formats
    final IngestFormatPluginOptions ingestFormatOptions = new IngestFormatPluginOptions();
    ingestFormatOptions.selectPlugin(format);

    // Indexes
    final String[] indexTypes = dimensionalityType.getDimensionalityArg().split(",");
    final List<IndexPluginOptions> indexOptions = new ArrayList<>(indexTypes.length);
    for (final String indexType : indexTypes) {
      final IndexPluginOptions indexOption = new IndexPluginOptions();
      indexOption.selectPlugin(indexType);
      if (crsCode != null) {
        if (indexOption.getDimensionalityOptions() instanceof SpatialOptions) {
          ((SpatialOptions) indexOption.getDimensionalityOptions()).setCrs(crsCode);
        } else {
          ((SpatialTemporalOptions) indexOption.getDimensionalityOptions()).setCrs(crsCode);
        }
      }
      indexOptions.add(indexOption);
    }
    final File configFile = File.createTempFile("test_stats", null);
    final ManualOperationParams params = new ManualOperationParams();

    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);
    final StringBuilder indexParam = new StringBuilder();
    for (int i = 0; i < indexOptions.size(); i++) {
      final AddIndexCommand addIndex = new AddIndexCommand();
      addIndex.setParameters("test-index" + i);
      addIndex.setPluginOptions(indexOptions.get(i));
      addIndex.execute(params);
      indexParam.append("test-index" + i + ",");
    }
    // Create the command and execute.
    final LocalToGeowaveCommand localIngester = new LocalToGeowaveCommand();
    localIngester.setPluginFormats(ingestFormatOptions);
    localIngester.setParameters(ingestFilePath, "test-store", indexParam.toString());
    localIngester.setThreads(nthreads);

    final AddStoreCommand addStore = new AddStoreCommand();
    addStore.setParameters("test-store");
    addStore.setPluginOptions(dataStore);
    addStore.execute(params);
    localIngester.execute(params);
    verifyStats(dataStore);
  }

  public static void testS3LocalIngest(
      final DataStorePluginOptions dataStore,
      final DimensionalityType dimensionalityType,
      final String s3Url,
      final String ingestFilePath,
      final String format,
      final int nthreads) throws Exception {

    // ingest a shapefile (geotools type) directly into GeoWave using the
    // ingest framework's main method and pre-defined commandline arguments

    // Ingest Formats
    final IngestFormatPluginOptions ingestFormatOptions = new IngestFormatPluginOptions();
    ingestFormatOptions.selectPlugin(format);

    // Indexes
    final String[] indexTypes = dimensionalityType.getDimensionalityArg().split(",");
    final List<IndexPluginOptions> indexOptions = new ArrayList<>(indexTypes.length);
    for (final String indexType : indexTypes) {
      final IndexPluginOptions indexOption = new IndexPluginOptions();
      indexOption.selectPlugin(indexType);
      indexOptions.add(indexOption);
    }

    final File configFile = File.createTempFile("test_s3_local_ingest", null);
    final ManualOperationParams operationParams = new ManualOperationParams();
    operationParams.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    final StringBuilder indexParam = new StringBuilder();
    for (int i = 0; i < indexOptions.size(); i++) {
      final AddIndexCommand addIndex = new AddIndexCommand();
      addIndex.setParameters("test-index" + i);
      addIndex.setPluginOptions(indexOptions.get(i));
      addIndex.execute(operationParams);
      indexParam.append("test-index" + i + ",");
    }

    final ConfigAWSCommand configS3 = new ConfigAWSCommand();
    configS3.setS3UrlParameter(s3Url);
    configS3.execute(operationParams);

    // Create the command and execute.
    final LocalToGeowaveCommand localIngester = new LocalToGeowaveCommand();
    localIngester.setPluginFormats(ingestFormatOptions);
    localIngester.setParameters(ingestFilePath, "test-store", indexParam.toString());
    localIngester.setThreads(nthreads);

    final AddStoreCommand addStore = new AddStoreCommand();
    addStore.setParameters("test-store");
    addStore.setPluginOptions(dataStore);
    addStore.execute(operationParams);
    localIngester.execute(operationParams);

    verifyStats(dataStore);
  }

  public static void testSparkIngest(
      final DataStorePluginOptions dataStore,
      final DimensionalityType dimensionalityType,
      final String format) throws Exception {
    testSparkIngest(dataStore, dimensionalityType, S3URL, S3_INPUT_PATH, format);
  }

  public static void testSparkIngest(
      final DataStorePluginOptions dataStore,
      final DimensionalityType dimensionalityType,
      final String s3Url,
      final String ingestFilePath,
      final String format) throws Exception {

    // ingest a shapefile (geotools type) directly into GeoWave using the
    // ingest framework's main method and pre-defined commandline arguments

    // Indexes
    final String indexes = dimensionalityType.getDimensionalityArg();
    final File configFile = File.createTempFile("test_spark_ingest", null);
    final ManualOperationParams operationParams = new ManualOperationParams();
    operationParams.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);

    final ConfigAWSCommand configS3 = new ConfigAWSCommand();
    configS3.setS3UrlParameter(s3Url);
    configS3.execute(operationParams);

    final LocalInputCommandLineOptions localOptions = new LocalInputCommandLineOptions();
    localOptions.setFormats(format);

    final SparkCommandLineOptions sparkOptions = new SparkCommandLineOptions();
    sparkOptions.setAppName("SparkIngestTest");
    sparkOptions.setMaster("local");
    sparkOptions.setHost("localhost");

    // Create the command and execute.
    final SparkIngestDriver sparkIngester = new SparkIngestDriver();
    final Properties props = new Properties();
    dataStore.save(props, DataStorePluginOptions.getStoreNamespace("test"));
    final AddStoreCommand addStore = new AddStoreCommand();
    addStore.setParameters("test");
    addStore.setPluginOptions(dataStore);
    addStore.execute(operationParams);

    final String[] indexTypes = dimensionalityType.getDimensionalityArg().split(",");
    for (final String indexType : indexTypes) {
      final IndexPluginOptions pluginOptions = new IndexPluginOptions();
      pluginOptions.selectPlugin(indexType);
      pluginOptions.save(props, IndexPluginOptions.getIndexNamespace(indexType));
      final AddIndexCommand addIndex = new AddIndexCommand();
      addIndex.setParameters(indexType);
      addIndex.setPluginOptions(pluginOptions);
      addIndex.execute(operationParams);
    }
    props.setProperty(ConfigAWSCommand.AWS_S3_ENDPOINT_URL, s3Url);

    sparkIngester.runOperation(
        configFile,
        localOptions,
        "test",
        indexes,
        new VisibilityOptions(),
        sparkOptions,
        ingestFilePath);

    verifyStats(dataStore);
  }

  private static void verifyStats(final DataStorePluginOptions dataStore) throws Exception {
    final ListStatsCommand listStats = new ListStatsCommand();
    listStats.setParameters("test", null);

    final File configFile = File.createTempFile("test_stats", null);
    final ManualOperationParams params = new ManualOperationParams();

    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);
    final AddStoreCommand addStore = new AddStoreCommand();
    addStore.setParameters("test");
    addStore.setPluginOptions(dataStore);
    addStore.execute(params);
    try {
      listStats.execute(params);
    } catch (final ParameterException e) {
      throw new RuntimeException(e);
    }
  }

  public static long hashCentroid(final Geometry geometry) {
    final Point centroid = geometry.getCentroid();
    return Double.doubleToLongBits(centroid.getX()) + Double.doubleToLongBits(centroid.getY() * 31);
  }

  public static class ExpectedResults {
    public Set<Long> hashedCentroids;
    public int count;

    @SuppressFBWarnings({"URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD"})
    public ExpectedResults(final Set<Long> hashedCentroids, final int count) {
      this.hashedCentroids = hashedCentroids;
      this.count = count;
    }
  }

  public static ExpectedResults getExpectedResults(final CloseableIterator<?> results)
      throws IOException {
    final Set<Long> hashedCentroids = new HashSet<>();
    int expectedResultCount = 0;
    try {
      while (results.hasNext()) {
        final Object obj = results.next();
        if (obj instanceof SimpleFeature) {
          expectedResultCount++;
          final SimpleFeature feature = (SimpleFeature) obj;
          hashedCentroids.add(hashCentroid((Geometry) feature.getDefaultGeometry()));
        }
      }
    } finally {
      results.close();
    }
    return new ExpectedResults(hashedCentroids, expectedResultCount);
  }

  public static ExpectedResults getExpectedResults(final URL[] expectedResultsResources)
      throws IOException {
    return getExpectedResults(expectedResultsResources, null);
  }

  public static MathTransform transformFromCrs(final CoordinateReferenceSystem crs) {
    MathTransform mathTransform = null;
    if (crs != null) {
      try {
        mathTransform = CRS.findMathTransform(GeometryUtils.getDefaultCRS(), crs, true);
      } catch (final FactoryException e) {
        LOGGER.warn("Unable to create coordinate reference system transform", e);
      }
    }
    return mathTransform;
  }

  public static ExpectedResults getExpectedResults(
      final URL[] expectedResultsResources,
      final CoordinateReferenceSystem crs) throws IOException {
    final Map<String, Object> map = new HashMap<>();
    DataStore dataStore = null;
    final Set<Long> hashedCentroids = new HashSet<>();
    int expectedResultCount = 0;
    final MathTransform mathTransform = transformFromCrs(crs);
    final TWKBWriter writer = new TWKBWriter();
    final TWKBReader reader = new TWKBReader();
    for (final URL expectedResultsResource : expectedResultsResources) {
      map.put("url", expectedResultsResource);
      SimpleFeatureIterator featureIterator = null;
      try {
        dataStore = DataStoreFinder.getDataStore(map);
        if (dataStore == null) {
          LOGGER.error("Could not get dataStore instance, getDataStore returned null");
          throw new IOException("Could not get dataStore instance, getDataStore returned null");
        }
        final SimpleFeatureCollection expectedResults =
            dataStore.getFeatureSource(dataStore.getNames().get(0)).getFeatures();

        expectedResultCount += expectedResults.size();
        // unwrap the expected results into a set of features IDs so its
        // easy to check against
        featureIterator = expectedResults.features();
        while (featureIterator.hasNext()) {
          final SimpleFeature feature = featureIterator.next();
          final Geometry geometry = (Geometry) feature.getDefaultGeometry();

          // TODO: Geometry has to be serialized and deserialized here
          // to make the centroid match the one coming out of the
          // database.
          final long centroid =
              hashCentroid(
                  reader.read(
                      writer.write(
                          mathTransform != null ? JTS.transform(geometry, mathTransform)
                              : geometry)));
          hashedCentroids.add(centroid);
        }
      } catch (MismatchedDimensionException | TransformException | ParseException e) {
        LOGGER.warn("Unable to transform geometry", e);
        Assert.fail("Unable to transform geometry to CRS: " + crs.toString());
      } finally {
        IOUtils.closeQuietly(featureIterator);
        if (dataStore != null) {
          dataStore.dispose();
        }
      }
    }
    return new ExpectedResults(hashedCentroids, expectedResultCount);
  }

  public static QueryConstraints resourceToQuery(final URL filterResource) throws IOException {
    return featureToQuery(resourceToFeature(filterResource), null, null, true);
  }

  public static QueryConstraints resourceToQuery(
      final URL filterResource,
      final Pair<String, String> optimalCqlQueryGeometryAndTimeFields,
      final boolean useDuring) throws IOException {
    return featureToQuery(
        resourceToFeature(filterResource),
        optimalCqlQueryGeometryAndTimeFields,
        null,
        useDuring);
  }

  public static SimpleFeature resourceToFeature(final URL filterResource) throws IOException {
    final Map<String, Object> map = new HashMap<>();
    DataStore dataStore = null;
    map.put("url", filterResource);
    final SimpleFeature savedFilter;
    SimpleFeatureIterator sfi = null;
    try {
      dataStore = DataStoreFinder.getDataStore(map);
      if (dataStore == null) {
        LOGGER.error("Could not get dataStore instance, getDataStore returned null");
        throw new IOException("Could not get dataStore instance, getDataStore returned null");
      }
      // just grab the first feature and use it as a filter
      sfi = dataStore.getFeatureSource(dataStore.getNames().get(0)).getFeatures().features();
      savedFilter = sfi.next();

    } finally {
      if (sfi != null) {
        sfi.close();
      }
      if (dataStore != null) {
        dataStore.dispose();
      }
    }
    return savedFilter;
  }

  public static QueryConstraints featureToQuery(
      final SimpleFeature savedFilter,
      final Pair<String, String> optimalCqlQueryGeometryAndTimeField,
      final String crsCode,
      final boolean useDuring) {
    final Geometry filterGeometry = (Geometry) savedFilter.getDefaultGeometry();
    final Object startObj = savedFilter.getAttribute(TEST_FILTER_START_TIME_ATTRIBUTE_NAME);
    final Object endObj = savedFilter.getAttribute(TEST_FILTER_END_TIME_ATTRIBUTE_NAME);

    if ((startObj != null) && (endObj != null)) {
      // if we can resolve start and end times, make it a spatial temporal
      // query
      Date startDate = null, endDate = null;
      if (startObj instanceof Calendar) {
        startDate = ((Calendar) startObj).getTime();
      } else if (startObj instanceof Date) {
        startDate = (Date) startObj;
      }
      if (endObj instanceof Calendar) {
        endDate = ((Calendar) endObj).getTime();
      } else if (endObj instanceof Date) {
        endDate = (Date) endObj;
      }
      if ((startDate != null) && (endDate != null)) {
        if (optimalCqlQueryGeometryAndTimeField != null) {
          final FilterFactory2 factory = CommonFactoryFinder.getFilterFactory2();
          Filter timeConstraint;
          if (useDuring) {
            timeConstraint =
                TimeUtils.toDuringFilter(
                    startDate.getTime(),
                    endDate.getTime(),
                    optimalCqlQueryGeometryAndTimeField.getRight());
          } else {
            timeConstraint =
                TimeUtils.toFilter(
                    startDate.getTime(),
                    endDate.getTime(),
                    optimalCqlQueryGeometryAndTimeField.getRight(),
                    optimalCqlQueryGeometryAndTimeField.getRight());
          }

          final And expression =
              factory.and(
                  GeometryUtils.geometryToSpatialOperator(
                      filterGeometry,
                      optimalCqlQueryGeometryAndTimeField.getLeft()),
                  timeConstraint);
          return new OptimalCQLQuery(expression);
        }
        return new SpatialTemporalQuery(
            new ExplicitSpatialTemporalQuery(startDate, endDate, filterGeometry, crsCode));
      }
    }
    if (optimalCqlQueryGeometryAndTimeField != null) {
      return new OptimalCQLQuery(
          GeometryUtils.geometryToSpatialOperator(
              filterGeometry,
              optimalCqlQueryGeometryAndTimeField.getLeft()));
    }
    // otherwise just return a spatial query
    return new SpatialQuery(new ExplicitSpatialQuery(filterGeometry, crsCode));
  }

  protected static void replaceParameters(final Map<String, String> values, final File file)
      throws IOException {
    {
      String str = FileUtils.readFileToString(file);
      for (final Entry<String, String> entry : values.entrySet()) {
        str = str.replaceAll(entry.getKey(), entry.getValue());
      }
      FileUtils.deleteQuietly(file);
      FileUtils.write(file, str);
    }
  }

  /** @param testName Name of the test that we are starting. */
  public static void printStartOfTest(final Logger logger, final String testName) {
    // Format
    final String paddedName = StringUtils.center("RUNNING " + testName, 37);
    // Print
    logger.warn("-----------------------------------------");
    logger.warn("*                                       *");
    logger.warn("* " + paddedName + " *");
    logger.warn("*                                       *");
    logger.warn("-----------------------------------------");
  }

  /**
   * @param testName Name of the test that we are starting.
   * @param startMillis The time (millis) that the test started.
   */
  public static void printEndOfTest(
      final Logger logger,
      final String testName,
      final long startMillis) {
    // Get Elapsed Time
    final double elapsedS = (System.currentTimeMillis() - startMillis) / 1000.;
    // Format
    final String paddedName = StringUtils.center("FINISHED " + testName, 37);
    final String paddedElapsed = StringUtils.center(elapsedS + "s elapsed.", 37);
    // Print
    logger.warn("-----------------------------------------");
    logger.warn("*                                       *");
    logger.warn("* " + paddedName + " *");
    logger.warn("* " + paddedElapsed + " *");
    logger.warn("*                                       *");
    logger.warn("-----------------------------------------");
  }

  /**
   * @param bi sample
   * @param ref reference
   * @param minPctError used for testing subsampling - to ensure we are properly subsampling we want
   *        there to be some error if subsampling is aggressive (10 pixels)
   * @param maxPctError used for testing subsampling - we want to ensure at most we are off by this
   *        percentile
   */
  public static void testTileAgainstReference(
      final BufferedImage actual,
      final BufferedImage expected,
      final double minPctError,
      final double maxPctError) {
    Assert.assertEquals(expected.getWidth(), actual.getWidth());
    Assert.assertEquals(expected.getHeight(), actual.getHeight());
    final int totalPixels = expected.getWidth() * expected.getHeight();
    final int minErrorPixels = (int) Math.round(minPctError * totalPixels);
    final int maxErrorPixels = (int) Math.round(maxPctError * totalPixels);
    int errorPixels = 0;
    // test under default style
    for (int x = 0; x < expected.getWidth(); x++) {
      for (int y = 0; y < expected.getHeight(); y++) {
        if (actual.getRGB(x, y) != expected.getRGB(x, y)) {
          errorPixels++;
          if (errorPixels > maxErrorPixels) {
            Assert.fail(
                String.format(
                    "[%d,%d] failed to match ref=%d gen=%d",
                    x,
                    y,
                    expected.getRGB(x, y),
                    actual.getRGB(x, y)));
          }
        }
      }
    }
    if (errorPixels < minErrorPixels) {
      Assert.fail(
          String.format(
              "Subsampling did not work as expected; error pixels (%d) did not exceed the minimum threshold (%d)",
              errorPixels,
              minErrorPixels));
    }

    if (errorPixels > 0) {
      System.out.println(
          ((float) errorPixels / (float) totalPixels) + "% pixels differed from expected");
    }
  }

  private static int i = 0;

  public static double getTileValue(final int x, final int y, final int b, final int tileSize) {
    // just use an arbitrary 'r'
    return getTileValue(x, y, b, 3, tileSize);
  }

  public static void fillTestRasters(
      final WritableRaster raster1,
      final WritableRaster raster2,
      final int tileSize) {
    // for raster1 do the following:
    // set every even row in bands 0 and 1
    // set every value incorrectly in band 2
    // set no values in band 3 and set every value in 4

    // for raster2 do the following:
    // set no value in band 0 and 4
    // set every odd row in band 1
    // set every value in bands 2 and 3

    // for band 5, set the lower 2x2 samples for raster 1 and the rest for
    // raster 2
    // for band 6, set the upper quadrant samples for raster 1 and the rest
    // for raster 2
    // for band 7, set the lower 2x2 samples to the wrong value for raster 1
    // and the expected value for raster 2 and set everything but the upper
    // quadrant for raster 2
    for (int x = 0; x < tileSize; x++) {
      for (int y = 0; y < tileSize; y++) {

        // just use x and y to arbitrarily end up with some wrong value
        // that can be ingested
        final double wrongValue = (getTileValue(y, x, y, tileSize) * 3) + 1;
        if ((x < 2) && (y < 2)) {
          raster1.setSample(x, y, 5, getTileValue(x, y, 5, tileSize));
          raster1.setSample(x, y, 7, wrongValue);
          raster2.setSample(x, y, 7, getTileValue(x, y, 7, tileSize));
        } else {
          raster2.setSample(x, y, 5, getTileValue(x, y, 5, tileSize));
        }
        if ((x > ((tileSize * 3) / 4)) && (y > ((tileSize * 3) / 4))) {
          raster1.setSample(x, y, 6, getTileValue(x, y, 6, tileSize));
        } else {
          raster2.setSample(x, y, 6, getTileValue(x, y, 6, tileSize));
          raster2.setSample(x, y, 7, getTileValue(x, y, 7, tileSize));
        }
        if ((y % 2) == 0) {
          raster1.setSample(x, y, 0, getTileValue(x, y, 0, tileSize));
          raster1.setSample(x, y, 1, getTileValue(x, y, 1, tileSize));
        }
        raster1.setSample(x, y, 2, wrongValue);

        raster1.setSample(x, y, 4, getTileValue(x, y, 4, tileSize));
        if ((y % 2) != 0) {
          raster2.setSample(x, y, 1, getTileValue(x, y, 1, tileSize));
        }
        raster2.setSample(x, y, 2, TestUtils.getTileValue(x, y, 2, tileSize));

        raster2.setSample(x, y, 3, getTileValue(x, y, 3, tileSize));
      }
    }
  }

  private static Random rng = null;

  public static double getTileValue(
      final int x,
      final int y,
      final int b,
      final int r,
      final int tileSize) {
    // make this some random but repeatable and vary the scale
    final double resultOfFunction = randomFunction(x, y, b, r, tileSize);
    // this is meant to just vary the scale
    if ((r % 2) == 0) {
      return resultOfFunction;
    } else {
      if (rng == null) {
        rng = new Random((long) resultOfFunction);
      } else {
        rng.setSeed((long) resultOfFunction);
      }

      return rng.nextDouble() * resultOfFunction;
    }
  }

  private static double randomFunction(
      final int x,
      final int y,
      final int b,
      final int r,
      final int tileSize) {
    return (((x + (y * tileSize)) * .1) / (b + 1)) + r;
  }

  @Deprecated
  public static void assert200(final String msg, final int responseCode) {
    Assert.assertEquals(msg, 200, responseCode);
  }

  @Deprecated
  public static void assert400(final String msg, final int responseCode) {
    Assert.assertEquals(msg, 400, responseCode);
  }

  @Deprecated
  public static void assert404(final String msg, final int responseCode) {
    Assert.assertEquals(msg, 404, responseCode);
  }

  /**
   * Asserts that the response has the expected Status Code. The assertion message is formatted to
   * include the provided string.
   *
   * @param msg String message to include in the assertion message.
   * @param expectedCode Integer HTTP Status code to expect from the response.
   * @param response The Response object on which .getStatus() will be performed.
   */
  public static void assertStatusCode(
      final String msg,
      final int expectedCode,
      final Response response) {
    final String assertionMsg =
        msg + String.format(": A %s response code should be received", expectedCode);
    Assert.assertEquals(assertionMsg, expectedCode, response.getStatus());
  }

  /**
   * Asserts that the response has the expected Status Code. The assertion message automatically
   * formatted.
   *
   * @param expectedCode Integer HTTP Status code to expect from the response.
   * @param response The Response object on which .getStatus() will be performed.
   */
  // Overload method with option to automatically generate assertion message.
  public static void assertStatusCode(final int expectedCode, final Response response) {
    assertStatusCode("REST call", expectedCode, response);
  }

  public static StoreTestEnvironment getTestEnvironment(final String type) {
    for (final GeoWaveStoreType t : GeoWaveStoreType.values()) {
      if (t.getTestEnvironment().getDataStoreFactory().getType().equals(type)) {
        return t.getTestEnvironment();
      }
    }
    return null;
  }
}
