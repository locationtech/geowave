/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.basic;

import static org.junit.Assert.assertEquals;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math.util.MathUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.locationtech.geowave.adapter.raster.util.ZipUtils;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.adapter.vector.export.VectorLocalExportCommand;
import org.locationtech.geowave.adapter.vector.export.VectorLocalExportOptions;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.cli.parser.ManualOperationParams;
import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.InternalGeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.query.OptimalCQLQuery;
import org.locationtech.geowave.core.geotime.store.statistics.BoundingBoxStatistic;
import org.locationtech.geowave.core.geotime.store.statistics.BoundingBoxStatistic.BoundingBoxValue;
import org.locationtech.geowave.core.geotime.util.TimeDescriptors;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.TransientAdapterStore;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.api.AggregationQuery;
import org.locationtech.geowave.core.store.api.AggregationQueryBuilder;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.FieldStatistic;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticQuery;
import org.locationtech.geowave.core.store.api.StatisticQueryBuilder;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.callback.IngestCallback;
import org.locationtech.geowave.core.store.cli.store.AddStoreCommand;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.data.CommonIndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.ingest.GeoWaveData;
import org.locationtech.geowave.core.store.ingest.LocalFileIngestPlugin;
import org.locationtech.geowave.core.store.memory.MemoryAdapterStore;
import org.locationtech.geowave.core.store.query.aggregate.CommonIndexAggregation;
import org.locationtech.geowave.core.store.query.constraints.DataIdQuery;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.statistics.DefaultStatisticsProvider;
import org.locationtech.geowave.core.store.statistics.StatisticId;
import org.locationtech.geowave.core.store.statistics.StatisticsIngestCallback;
import org.locationtech.geowave.format.geotools.vector.GeoToolsVectorDataStoreIngestPlugin;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.TestUtils.DimensionalityType;
import org.locationtech.geowave.test.TestUtils.ExpectedResults;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Lists;
import jersey.repackaged.com.google.common.collect.Maps;

public abstract class AbstractGeoWaveBasicVectorIT extends AbstractGeoWaveIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractGeoWaveBasicVectorIT.class);
  protected static final String TEST_DATA_ZIP_RESOURCE_PATH =
      TestUtils.TEST_RESOURCE_PACKAGE + "basic-testdata.zip";
  protected static final String TEST_FILTER_PACKAGE = TestUtils.TEST_CASE_BASE + "filter/";
  protected static final String HAIL_TEST_CASE_PACKAGE =
      TestUtils.TEST_CASE_BASE + "hail_test_case/";
  protected static final String HAIL_SHAPEFILE_FILE = HAIL_TEST_CASE_PACKAGE + "hail.shp";
  protected static final String TORNADO_TRACKS_TEST_CASE_PACKAGE =
      TestUtils.TEST_CASE_BASE + "tornado_tracks_test_case/";
  protected static final String TORNADO_TRACKS_SHAPEFILE_FILE =
      TORNADO_TRACKS_TEST_CASE_PACKAGE + "tornado_tracks.shp";
  protected static final String HAIL_EXPECTED_BOX_TEMPORAL_FILTER_RESULTS_FILE =
      HAIL_TEST_CASE_PACKAGE + "hail-box-temporal-filter.shp";
  protected static final String HAIL_EXPECTED_POLYGON_TEMPORAL_FILTER_RESULTS_FILE =
      HAIL_TEST_CASE_PACKAGE + "hail-polygon-temporal-filter.shp";
  protected static final String TORNADO_TRACKS_EXPECTED_BOX_TEMPORAL_FILTER_RESULTS_FILE =
      TORNADO_TRACKS_TEST_CASE_PACKAGE + "tornado_tracks-box-temporal-filter.shp";
  protected static final String TORNADO_TRACKS_EXPECTED_POLYGON_TEMPORAL_FILTER_RESULTS_FILE =
      TORNADO_TRACKS_TEST_CASE_PACKAGE + "tornado_tracks-polygon-temporal-filter.shp";
  protected static final String TEST_BOX_TEMPORAL_FILTER_FILE =
      TEST_FILTER_PACKAGE + "Box-Temporal-Filter.shp";
  protected static final String TEST_POLYGON_TEMPORAL_FILTER_FILE =
      TEST_FILTER_PACKAGE + "Polygon-Temporal-Filter.shp";
  protected static final String HAIL_EXPECTED_BOX_FILTER_RESULTS_FILE =
      HAIL_TEST_CASE_PACKAGE + "hail-box-filter.shp";
  protected static final String HAIL_EXPECTED_POLYGON_FILTER_RESULTS_FILE =
      HAIL_TEST_CASE_PACKAGE + "hail-polygon-filter.shp";

  protected static final String TORNADO_TRACKS_EXPECTED_BOX_FILTER_RESULTS_FILE =
      TORNADO_TRACKS_TEST_CASE_PACKAGE + "tornado_tracks-box-filter.shp";
  protected static final String TORNADO_TRACKS_EXPECTED_POLYGON_FILTER_RESULTS_FILE =
      TORNADO_TRACKS_TEST_CASE_PACKAGE + "tornado_tracks-polygon-filter.shp";

  protected static final String TEST_BOX_FILTER_FILE = TEST_FILTER_PACKAGE + "Box-Filter.shp";
  protected static final String TEST_POLYGON_FILTER_FILE =
      TEST_FILTER_PACKAGE + "Polygon-Filter.shp";
  protected static final String TEST_LOCAL_EXPORT_DIRECTORY = "export";
  private static final String TEST_BASE_EXPORT_FILE_NAME = "basicIT-export.avro";
  protected static final String CQL_DELETE_STR = "STATE = 'TX'";

  private static final SimpleDateFormat CQL_DATE_FORMAT =
      new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss'Z'");

  @BeforeClass
  public static void extractTestFiles() throws URISyntaxException {
    ZipUtils.unZipFile(
        new File(
            AbstractGeoWaveBasicVectorIT.class.getClassLoader().getResource(
                TEST_DATA_ZIP_RESOURCE_PATH).toURI()),
        TestUtils.TEST_CASE_BASE);
  }

  protected void testQuery(
      final URL savedFilterResource,
      final URL[] expectedResultsResources,
      final String queryDescription) throws Exception {
    // test the query with an unspecified index
    testQuery(savedFilterResource, expectedResultsResources, null, queryDescription);
  }

  protected void testQuery(
      final URL savedFilterResource,
      final URL[] expectedResultsResources,
      final Index index,
      final String queryDescription) throws Exception {
    testQuery(
        savedFilterResource,
        expectedResultsResources,
        null,
        index,
        queryDescription,
        null,
        false);
  }

  protected void testQuery(
      final URL savedFilterResource,
      final URL[] expectedResultsResources,
      final Pair<String, String> optimalCqlQueryGeometryAndTimeFields,
      final Index index,
      final String queryDescription,
      final CoordinateReferenceSystem crs,
      final boolean countDuplicates) throws IOException {
    LOGGER.info("querying " + queryDescription);

    final DataStore geowaveStore = getDataStorePluginOptions().createDataStore();
    // this file is the filtered dataset (using the previous file as a
    // filter) so use it to ensure the query worked
    final QueryConstraints constraints =
        TestUtils.resourceToQuery(savedFilterResource, optimalCqlQueryGeometryAndTimeFields, true);
    QueryBuilder<?, ?> bldr = QueryBuilder.newBuilder();
    if (index != null) {
      bldr = bldr.indexName(index.getName());
    }
    try (final CloseableIterator<?> actualResults =
        geowaveStore.query(bldr.constraints(constraints).build())) {
      final ExpectedResults expectedResults =
          TestUtils.getExpectedResults(expectedResultsResources, crs);
      int totalResults = 0;
      final List<Long> actualCentroids = new ArrayList<>();
      while (actualResults.hasNext()) {
        final Object obj = actualResults.next();
        if (obj instanceof SimpleFeature) {
          final SimpleFeature result = (SimpleFeature) obj;
          final long actualHashCentroid =
              TestUtils.hashCentroid((Geometry) result.getDefaultGeometry());
          Assert.assertTrue(
              "Actual result '" + result.toString() + "' not found in expected result set",
              expectedResults.hashedCentroids.contains(actualHashCentroid));
          actualCentroids.add(actualHashCentroid);
          totalResults++;
        } else {
          TestUtils.deleteAll(getDataStorePluginOptions());
          Assert.fail("Actual result '" + obj.toString() + "' is not of type Simple Feature.");
        }
      }
      for (final long l : actualCentroids) {
        expectedResults.hashedCentroids.remove(l);
      }
      for (final long l : expectedResults.hashedCentroids) {
        LOGGER.error("Missing expected hashed centroid: " + l);
      }
      if (expectedResults.count != totalResults) {
        TestUtils.deleteAll(getDataStorePluginOptions());
      }
      Assert.assertEquals(expectedResults.count, totalResults);

      final PersistentAdapterStore adapterStore = getDataStorePluginOptions().createAdapterStore();
      long statisticsResult = 0;
      int duplicates = 0;
      final InternalDataAdapter<?>[] adapters = adapterStore.getAdapters();
      for (final InternalDataAdapter<?> internalDataAdapter : adapters) {
        AggregationQueryBuilder<?, Long, ?, ?> aggBldr = AggregationQueryBuilder.newBuilder();
        if (index != null) {
          aggBldr = aggBldr.indexName(index.getName());
        }
        aggBldr = aggBldr.constraints(constraints);
        if (countDuplicates) {
          aggBldr.aggregate(
              internalDataAdapter.getTypeName(),
              (Aggregation) new DuplicateCountAggregation());
          final DuplicateCount countResult =
              (DuplicateCount) geowaveStore.aggregate((AggregationQuery) aggBldr.build());
          if (countResult != null) {
            duplicates += countResult.count;
          }
        }
        aggBldr.count(internalDataAdapter.getTypeName());
        final Long countResult = geowaveStore.aggregate(aggBldr.build());
        // results should already be aggregated, there should be
        // exactly one value in this iterator
        Assert.assertNotNull(countResult);
        statisticsResult += countResult;
      }

      Assert.assertEquals(expectedResults.count, statisticsResult - duplicates);
    }
  }

  public static class DuplicateCountAggregation implements
      CommonIndexAggregation<Persistable, DuplicateCount> {
    private final Set<ByteArray> visitedDataIds = new HashSet<>();
    long count = 0;

    @Override
    public void aggregate(
        final DataTypeAdapter<CommonIndexedPersistenceEncoding> adapter,
        final CommonIndexedPersistenceEncoding entry) {
      if (!entry.isDuplicated()) {
        return;
      }
      if (visitedDataIds.contains(new ByteArray(entry.getDataId()))) {
        // only aggregate when you find a duplicate entry
        count++;
      }
      visitedDataIds.add(new ByteArray(entry.getDataId()));
    }

    @Override
    public void clearResult() {
      count = 0;
      visitedDataIds.clear();
    }

    @Override
    public Persistable getParameters() {
      return null;
    }

    @Override
    public void setParameters(final Persistable parameters) {}

    @Override
    public DuplicateCount merge(final DuplicateCount result1, final DuplicateCount result2) {
      int dupes = 0;
      for (final ByteArray d : result1.visitedDataIds) {
        if (result2.visitedDataIds.contains(d)) {
          dupes++;
        }
      }
      result1.visitedDataIds.addAll(result2.visitedDataIds);
      result1.count += result2.count;
      // this is very important, it covers counting duplicates across
      // regions, which is the inadequacy of the aggregation in the
      // first place when there are duplicates
      result1.count += dupes;
      return result1;
    }

    @Override
    public DuplicateCount getResult() {
      return new DuplicateCount(count, visitedDataIds);
    }

    @Override
    public byte[] resultToBinary(final DuplicateCount result) {
      int bufferSize = 12;
      for (final ByteArray visited : visitedDataIds) {
        bufferSize += 4;
        bufferSize += visited.getBytes().length;
      }
      final ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
      buffer.putLong(count);
      buffer.putInt(visitedDataIds.size());

      for (final ByteArray visited : visitedDataIds) {
        buffer.putInt(visited.getBytes().length);
        buffer.put(visited.getBytes());
      }
      return buffer.array();
    }

    @Override
    public DuplicateCount resultFromBinary(final byte[] binary) {
      final ByteBuffer buffer = ByteBuffer.wrap(binary);
      final long count = buffer.getLong();
      final int size = buffer.getInt();
      final Set<ByteArray> visitedDataIds = new HashSet<>(size);
      for (int i = 0; i < size; i++) {
        final byte[] dataId = new byte[buffer.getInt()];
        buffer.get(dataId);
        visitedDataIds.add(new ByteArray(dataId));
      }
      return new DuplicateCount(count, visitedDataIds);
    }

    @Override
    public byte[] toBinary() {
      return new byte[] {};
    }

    @Override
    public void fromBinary(final byte[] bytes) {}
  }

  public static class DuplicateCount {
    private long count;
    private Set<ByteArray> visitedDataIds = new HashSet<>();

    public DuplicateCount() {
      super();
    }

    public DuplicateCount(final long count, final Set<ByteArray> visitedDataIds) {
      this.count = count;
      this.visitedDataIds = visitedDataIds;
    }
  }

  protected void testDeleteDataId(final URL savedFilterResource, final Index index)
      throws Exception {
    LOGGER.warn("deleting by data ID from " + index.getName() + " index");

    boolean success = false;
    final DataStore geowaveStore = getDataStorePluginOptions().createDataStore();
    final QueryConstraints query = TestUtils.resourceToQuery(savedFilterResource);
    final CloseableIterator<?> actualResults;

    // Run the spatial query
    actualResults =
        geowaveStore.query(
            QueryBuilder.newBuilder().indexName(index.getName()).constraints(query).build());

    // Grab the first one
    SimpleFeature testFeature = null;
    if (actualResults.hasNext()) {
      final Object obj = actualResults.next();
      if ((testFeature == null) && (obj instanceof SimpleFeature)) {
        testFeature = (SimpleFeature) obj;
      }
    }
    actualResults.close();

    // Delete it by data ID
    if (testFeature != null) {
      final ByteArray dataId = new ByteArray(testFeature.getID());

      if (geowaveStore.delete(
          QueryBuilder.newBuilder().addTypeName(
              testFeature.getFeatureType().getTypeName()).indexName(index.getName()).constraints(
                  new DataIdQuery(dataId.getBytes())).build())) {
        success =
            !hasAtLeastOne(
                geowaveStore.query(
                    QueryBuilder.newBuilder().addTypeName(
                        testFeature.getFeatureType().getTypeName()).indexName(
                            index.getName()).constraints(
                                new DataIdQuery(dataId.getBytes())).build()));
      }
    }
    Assert.assertTrue("Unable to delete entry by data ID and adapter ID", success);
  }

  protected void testDeleteByBasicQuery(final URL savedFilterResource, final Index index)
      throws Exception {
    LOGGER.info("bulk deleting via spatial query");

    final DataStore geowaveStore = getDataStorePluginOptions().createDataStore();

    // Run the query for this delete to get the expected count
    final QueryConstraints query = TestUtils.resourceToQuery(savedFilterResource);

    deleteInternal(geowaveStore, index, query);
  }

  protected void testDeleteCQL(final String cqlStr, final Index index) throws Exception {
    LOGGER.info("bulk deleting using CQL: '" + cqlStr + "'");

    final DataStore geowaveStore = getDataStorePluginOptions().createDataStore();

    // Retrieve the feature adapter for the CQL query generator
    final PersistentAdapterStore adapterStore = getDataStorePluginOptions().createAdapterStore();

    final InternalDataAdapter<?>[] adapters = adapterStore.getAdapters();
    for (final InternalDataAdapter<?> adapter : adapters) {
      // Create the CQL query
      final QueryConstraints query =
          OptimalCQLQuery.createOptimalQuery(
              cqlStr,
              (InternalGeotoolsFeatureDataAdapter<SimpleFeature>) adapter,
              null,
              null);

      deleteInternal(geowaveStore, index, query);
    }
  }

  protected void deleteInternal(
      final DataStore geowaveStore,
      final Index index,
      final QueryConstraints query) {
    // Query everything
    QueryBuilder<?, ?> bldr = QueryBuilder.newBuilder();
    if (index != null) {
      bldr.indexName(index.getName());
    }
    CloseableIterator<?> queryResults = geowaveStore.query(bldr.build());

    int allFeatures = 0;
    while (queryResults.hasNext()) {
      final Object obj = queryResults.next();
      if (obj instanceof SimpleFeature) {
        allFeatures++;
      }
    }
    queryResults.close();

    LOGGER.warn("Total count in table before delete: " + allFeatures);

    // Run the query for this delete to get the expected count
    bldr = QueryBuilder.newBuilder().constraints(query);
    if (index != null) {
      bldr.indexName(index.getName());
    }
    queryResults = geowaveStore.query(bldr.build());
    int expectedFeaturesToDelete = 0;
    while (queryResults.hasNext()) {
      final Object obj = queryResults.next();
      if (obj instanceof SimpleFeature) {
        expectedFeaturesToDelete++;
      }
    }
    queryResults.close();

    LOGGER.warn(expectedFeaturesToDelete + " features to delete...");
    // Do the delete
    bldr = QueryBuilder.newBuilder().constraints(query);
    if (index != null) {
      bldr.indexName(index.getName());
    }
    final boolean deleteResults = geowaveStore.delete(bldr.build());
    LOGGER.warn("Bulk delete results: " + (deleteResults ? "Success" : "Failure"));

    // Query again - should be zero remaining
    bldr = QueryBuilder.newBuilder().constraints(query);
    if (index != null) {
      bldr.indexName(index.getName());
    }
    queryResults = geowaveStore.query(bldr.build());

    final int initialQueryFeatures = expectedFeaturesToDelete;
    int remainingFeatures = 0;
    while (queryResults.hasNext()) {
      final Object obj = queryResults.next();
      if (obj instanceof SimpleFeature) {
        remainingFeatures++;
      }
    }
    queryResults.close();

    final int deletedFeatures = initialQueryFeatures - remainingFeatures;

    LOGGER.warn(deletedFeatures + " features bulk deleted.");
    LOGGER.warn(remainingFeatures + " features not deleted.");

    Assert.assertTrue(
        "Unable to delete all features in bulk delete, there are "
            + remainingFeatures
            + " not deleted",
        remainingFeatures == 0);
    // Now for the final check, query everything again
    bldr = QueryBuilder.newBuilder();
    if (index != null) {
      bldr.indexName(index.getName());
    }
    queryResults = geowaveStore.query(bldr.build());

    int finalFeatures = 0;
    while (queryResults.hasNext()) {
      final Object obj = queryResults.next();
      if (obj instanceof SimpleFeature) {
        finalFeatures++;
      }
    }
    queryResults.close();

    LOGGER.warn("Total count in table after delete: " + finalFeatures);
    LOGGER.warn("<before> - <after> = " + (allFeatures - finalFeatures));

    Assert.assertTrue(
        "Unable to delete all features in bulk delete",
        (allFeatures - finalFeatures) == deletedFeatures);
  }

  private static boolean hasAtLeastOne(final CloseableIterator<?> it) {
    try {
      return it.hasNext();
    } finally {
      it.close();
    }
  }

  protected void testStats(
      final URL[] inputFiles,
      final boolean multithreaded,
      final Index... indices) {
    testStats(inputFiles, multithreaded, null, indices);
  }

  protected void testSpatialTemporalLocalExportAndReingestWithCQL(
      final URL filterUrl,
      final int numThreads,
      final boolean pointsOnly,
      final DimensionalityType dimensionalityType) throws Exception {
    final File exportDir =
        exportWithCQL(getDataStorePluginOptions(), filterUrl, dimensionalityType);
    TestUtils.testLocalIngest(
        getDataStorePluginOptions(),
        dimensionalityType,
        null,
        exportDir.getAbsolutePath(),
        "avro",
        numThreads,
        false);
    try {
      URL[] expectedResultsUrls;
      if (pointsOnly) {
        expectedResultsUrls =
            new URL[] {new File(HAIL_EXPECTED_BOX_TEMPORAL_FILTER_RESULTS_FILE).toURI().toURL()};
      } else {
        expectedResultsUrls =
            new URL[] {
                new File(HAIL_EXPECTED_BOX_TEMPORAL_FILTER_RESULTS_FILE).toURI().toURL(),
                new File(TORNADO_TRACKS_EXPECTED_BOX_TEMPORAL_FILTER_RESULTS_FILE).toURI().toURL()};
      }

      testQuery(
          new File(TEST_BOX_TEMPORAL_FILTER_FILE).toURI().toURL(),
          expectedResultsUrls,
          "reingested bounding box and time range");
    } catch (final Exception e) {
      e.printStackTrace();
      TestUtils.deleteAll(getDataStorePluginOptions());
      Assert.fail(
          "Error occurred on reingested dataset while testing a bounding box and time range query of spatial temporal index: '"
              + e.getLocalizedMessage()
              + '\'');
    }
  }

  protected static File exportWithCQL(
      final DataStorePluginOptions dataStoreOptions,
      final URL filterUrl,
      final DimensionalityType dimensionalityType) throws Exception {
    Geometry filterGeometry = null;
    Date startDate = null, endDate = null;
    if (filterUrl != null) {
      final SimpleFeature savedFilter = TestUtils.resourceToFeature(filterUrl);

      filterGeometry = (Geometry) savedFilter.getDefaultGeometry();
      final Object startObj =
          savedFilter.getAttribute(TestUtils.TEST_FILTER_START_TIME_ATTRIBUTE_NAME);
      final Object endObj = savedFilter.getAttribute(TestUtils.TEST_FILTER_END_TIME_ATTRIBUTE_NAME);
      if ((startObj != null) && (endObj != null)) {
        // if we can resolve start and end times, make it a spatial temporal
        // query
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
      }
    }
    final PersistentAdapterStore adapterStore = dataStoreOptions.createAdapterStore();
    final VectorLocalExportCommand exportCommand = new VectorLocalExportCommand();
    final VectorLocalExportOptions options = exportCommand.getOptions();
    final File exportDir = new File(TestUtils.TEMP_DIR, TEST_LOCAL_EXPORT_DIRECTORY);
    FileUtils.deleteDirectory(exportDir);
    if (!exportDir.mkdirs()) {
      LOGGER.warn("Unable to create directory: " + exportDir.getAbsolutePath());
    }

    exportCommand.setParameters("test");

    final File configFile = File.createTempFile("test_export", null);
    final ManualOperationParams params = new ManualOperationParams();

    params.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);
    final AddStoreCommand addStore = new AddStoreCommand();
    addStore.setParameters("test");
    addStore.setPluginOptions(dataStoreOptions);
    addStore.execute(params);
    options.setBatchSize(10000);
    final InternalDataAdapter<?>[] adapters = adapterStore.getAdapters();
    for (final InternalDataAdapter<?> adapter : adapters) {
      options.setTypeNames(Lists.newArrayList(adapter.getTypeName()));
      if ((adapter.getAdapter() instanceof GeotoolsFeatureDataAdapter)
          && (filterGeometry != null)
          && (startDate != null)
          && (endDate != null)) {
        final GeotoolsFeatureDataAdapter gtAdapter =
            (GeotoolsFeatureDataAdapter) adapter.getAdapter();
        final TimeDescriptors timeDesc = gtAdapter.getTimeDescriptors();

        String startTimeAttribute;
        if (timeDesc.getStartRange() != null) {
          startTimeAttribute = timeDesc.getStartRange().getLocalName();
        } else {
          startTimeAttribute = timeDesc.getTime().getLocalName();
        }
        final String endTimeAttribute;
        if (timeDesc.getEndRange() != null) {
          endTimeAttribute = timeDesc.getEndRange().getLocalName();
        } else {
          endTimeAttribute = timeDesc.getTime().getLocalName();
        }
        final String geometryAttribute =
            gtAdapter.getFeatureType().getGeometryDescriptor().getLocalName();

        final Envelope env = filterGeometry.getEnvelopeInternal();
        final double east = env.getMaxX();
        final double west = env.getMinX();
        final double south = env.getMinY();
        final double north = env.getMaxY();
        final String cqlPredicate =
            String.format(
                "BBOX(\"%s\",%f,%f,%f,%f) AND \"%s\" <= '%s' AND \"%s\" >= '%s'",
                geometryAttribute,
                west,
                south,
                east,
                north,
                startTimeAttribute,
                CQL_DATE_FORMAT.format(endDate),
                endTimeAttribute,
                CQL_DATE_FORMAT.format(startDate));
        options.setCqlFilter(cqlPredicate);
      }

      options.setOutputFile(
          new File(exportDir, adapter.getTypeName() + TEST_BASE_EXPORT_FILE_NAME));
      exportCommand.execute(params);
    }
    TestUtils.deleteAll(dataStoreOptions);
    return exportDir;
  }

  @SuppressWarnings("unchecked")
  protected void testStats(
      final URL[] inputFiles,
      final boolean multithreaded,
      final CoordinateReferenceSystem crs,
      final Index... indices) {
    // In the multithreaded case, only test min/max and count. Stats will be
    // ingested/ in a different order and will not match.
    final LocalFileIngestPlugin<SimpleFeature> localFileIngest =
        new GeoToolsVectorDataStoreIngestPlugin(Filter.INCLUDE);
    final Map<String, StatisticsCache> statsCache = new HashMap<>();
    final String[] indexNames =
        Arrays.stream(indices).map(i -> i.getName()).toArray(i -> new String[i]);
    for (final URL inputFile : inputFiles) {
      LOGGER.warn(
          "Calculating stats from file '"
              + inputFile.getPath()
              + "' - this may take several minutes...");
      try (final CloseableIterator<GeoWaveData<SimpleFeature>> dataIterator =
          localFileIngest.toGeoWaveData(inputFile, indexNames)) {
        final TransientAdapterStore adapterCache =
            new MemoryAdapterStore(localFileIngest.getDataAdapters());
        while (dataIterator.hasNext()) {
          final GeoWaveData<SimpleFeature> data = dataIterator.next();
          final DataTypeAdapter<SimpleFeature> adapter = data.getAdapter(adapterCache);
          // it should be a statistical data adapter
          if (adapter instanceof DefaultStatisticsProvider) {
            StatisticsCache cachedValues = statsCache.get(adapter.getTypeName());
            if (cachedValues == null) {
              cachedValues = new StatisticsCache(adapter, crs);
              statsCache.put(adapter.getTypeName(), cachedValues);
            }
            cachedValues.entryIngested(data.getValue());
          }
        }
      }
    }
    final DataStatisticsStore statsStore = getDataStorePluginOptions().createDataStatisticsStore();
    final PersistentAdapterStore adapterStore = getDataStorePluginOptions().createAdapterStore();
    final InternalDataAdapter<?>[] adapters = adapterStore.getAdapters();
    for (final InternalDataAdapter<?> internalDataAdapter : adapters) {
      final FeatureDataAdapter adapter = (FeatureDataAdapter) internalDataAdapter.getAdapter();
      final StatisticsCache cachedValue = statsCache.get(adapter.getTypeName());
      Assert.assertNotNull(cachedValue);
      final Set<Entry<Statistic<?>, Map<ByteArray, StatisticValue<?>>>> expectedStats =
          cachedValue.statsCache.entrySet();
      int statsCount = 0;
      try (CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> statsIterator =
          statsStore.getDataTypeStatistics(adapter, null, null)) {
        while (statsIterator.hasNext()) {
          statsIterator.next();
          statsCount++;
        }
      }
      try (CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> statsIterator =
          statsStore.getFieldStatistics(adapter, null, null, null)) {
        while (statsIterator.hasNext()) {
          statsIterator.next();
          statsCount++;
        }
      }
      Assert.assertEquals(
          "The number of stats for data adapter '"
              + adapter.getTypeName()
              + "' do not match count expected",
          expectedStats.size(),
          statsCount);
      for (final Entry<Statistic<?>, Map<ByteArray, StatisticValue<?>>> expectedStat : expectedStats) {
        for (final Entry<ByteArray, StatisticValue<?>> expectedValues : expectedStat.getValue().entrySet()) {
          StatisticValue<Object> actual;
          if (expectedValues.getKey().equals(StatisticValue.NO_BIN)) {
            actual =
                statsStore.getStatisticValue(
                    (Statistic<StatisticValue<Object>>) expectedStat.getKey());
          } else {
            actual =
                statsStore.getStatisticValue(
                    (Statistic<StatisticValue<Object>>) expectedStat.getKey(),
                    expectedValues.getKey());
          }
          assertEquals(expectedValues.getValue().getValue(), actual.getValue());
        }
      }
      // finally check the one stat that is more manually calculated -
      // the bounding box
      StatisticQuery<BoundingBoxValue, Envelope> query =
          StatisticQueryBuilder.newBuilder(BoundingBoxStatistic.STATS_TYPE).fieldName(
              adapter.getFeatureType().getGeometryDescriptor().getLocalName()).typeName(
                  adapter.getTypeName()).build();
      BoundingBoxValue bboxStat =
          getDataStorePluginOptions().createDataStore().aggregateStatistics(query);
      validateBBox(bboxStat.getValue(), cachedValue);

      // now make sure it works without giving field name because there is only one geometry field
      // anyways
      query =
          StatisticQueryBuilder.newBuilder(BoundingBoxStatistic.STATS_TYPE).typeName(
              adapter.getTypeName()).build();
      bboxStat = getDataStorePluginOptions().createDataStore().aggregateStatistics(query);
      validateBBox(bboxStat.getValue(), cachedValue);

      final StatisticId<BoundingBoxValue> bboxStatId =
          FieldStatistic.generateStatisticId(
              adapter.getTypeName(),
              BoundingBoxStatistic.STATS_TYPE,
              adapter.getFeatureType().getGeometryDescriptor().getLocalName(),
              Statistic.INTERNAL_TAG);

      Assert.assertTrue(
          "Unable to remove individual stat",
          statsStore.removeStatistic(statsStore.getStatisticById(bboxStatId)));

      Assert.assertNull(
          "Individual stat was not successfully removed",
          statsStore.getStatisticById(bboxStatId));
    }

  }

  private static void validateBBox(final Envelope bboxStat, final StatisticsCache cachedValue) {
    Assert.assertNotNull(bboxStat);
    Assert.assertEquals(
        "The min X of the bounding box stat does not match the expected value",
        cachedValue.minX,
        bboxStat.getMinX(),
        MathUtils.EPSILON);
    Assert.assertEquals(
        "The min Y of the bounding box stat does not match the expected value",
        cachedValue.minY,
        bboxStat.getMinY(),
        MathUtils.EPSILON);
    Assert.assertEquals(
        "The max X of the bounding box stat does not match the expected value",
        cachedValue.maxX,
        bboxStat.getMaxX(),
        MathUtils.EPSILON);
    Assert.assertEquals(
        "The max Y of the bounding box stat does not match the expected value",
        cachedValue.maxY,
        bboxStat.getMaxY(),
        MathUtils.EPSILON);
  }

  protected static class StatisticsCache implements IngestCallback<SimpleFeature> {
    // assume a bounding box statistic exists and calculate the value
    // separately to ensure calculation works
    private double minX = Double.MAX_VALUE;
    private double minY = Double.MAX_VALUE;
    private double maxX = -Double.MAX_VALUE;
    private double maxY = -Double.MAX_VALUE;
    protected final Map<Statistic<?>, Map<ByteArray, StatisticValue<?>>> statsCache =
        new HashMap<>();
    private final DataTypeAdapter<SimpleFeature> adapter;

    // otherwise use the statistics interface to calculate every statistic
    // and compare results to what is available in the statistics data store
    private StatisticsCache(
        final DataTypeAdapter<SimpleFeature> adapter,
        final CoordinateReferenceSystem crs) {
      this.adapter = adapter;
      final List<Statistic<?>> stats = ((DefaultStatisticsProvider) adapter).getDefaultStatistics();
      for (final Statistic<?> stat : stats) {
        if (stat instanceof BoundingBoxStatistic) {
          ((BoundingBoxStatistic) stat).setSourceCrs(crs);
        }
        statsCache.put(stat, Maps.newHashMap());
      }
    }

    @Override
    public void entryIngested(final SimpleFeature entry, final GeoWaveRow... geowaveRows) {
      for (final Statistic<?> stat : statsCache.keySet()) {
        ByteArray[] bins;
        if (stat.getBinningStrategy() == null) {
          bins = new ByteArray[] {StatisticValue.NO_BIN};
        } else {
          bins = stat.getBinningStrategy().getBins(adapter, entry, geowaveRows);
        }
        final Map<ByteArray, StatisticValue<?>> binValues = statsCache.get(stat);
        for (final ByteArray bin : bins) {
          if (!binValues.containsKey(bin)) {
            binValues.put(bin, stat.createEmpty());
          }
          final StatisticValue<?> value = binValues.get(bin);
          if (value instanceof StatisticsIngestCallback) {
            ((StatisticsIngestCallback) value).entryIngested(adapter, entry, geowaveRows);
          }
        }
      }
      final Geometry geometry = ((Geometry) entry.getDefaultGeometry());
      if ((geometry != null) && !geometry.isEmpty()) {
        minX = Math.min(minX, geometry.getEnvelopeInternal().getMinX());
        minY = Math.min(minY, geometry.getEnvelopeInternal().getMinY());
        maxX = Math.max(maxX, geometry.getEnvelopeInternal().getMaxX());
        maxY = Math.max(maxY, geometry.getEnvelopeInternal().getMaxY());
      }
    }
  }
}
