/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.mapreduce.splits;

import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.query.ExplicitSpatialQuery;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.memory.MemoryAdapterStore;
import org.locationtech.geowave.core.store.operations.RowReader;
import org.locationtech.geowave.core.store.query.constraints.EverythingQuery;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.options.CommonQueryOptions;
import org.locationtech.geowave.core.store.query.options.FilterByTypeQueryOptions;
import org.locationtech.geowave.core.store.query.options.QuerySingleIndex;
import org.locationtech.geowave.examples.ingest.SimpleIngest;
import org.locationtech.geowave.mapreduce.MapReduceDataStore;
import org.locationtech.geowave.mapreduce.MapReduceDataStoreOperations;
import org.locationtech.geowave.service.rest.GeoWaveOperationServiceWrapper;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.annotation.Environments;
import org.locationtech.geowave.test.annotation.Environments.Environment;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.locationtech.geowave.test.basic.AbstractGeoWaveIT;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(GeoWaveITRunner.class)
@Environments({Environment.MAP_REDUCE})
public class SplitsProviderIT extends AbstractGeoWaveIT {

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

  private static final Logger LOGGER =
      LoggerFactory.getLogger(GeoWaveOperationServiceWrapper.class);
  private static long startMillis;
  private static final String testName = "SplitsProviderIT";

  private static final SimpleFeatureType sft = SimpleIngest.createPointFeatureType();
  private static final Index idx = SimpleIngest.createSpatialIndex();
  private static final GeotoolsFeatureDataAdapter fda = SimpleIngest.createDataAdapter(sft);

  enum Distribution {
    UNIFORM, BIMODAL, SKEWED
  }

  @BeforeClass
  public static void setup() {
    startMillis = System.currentTimeMillis();
    TestUtils.printStartOfTest(LOGGER, testName);
  }

  @AfterClass
  public static void reportTest() {
    TestUtils.printEndOfTest(LOGGER, testName, startMillis);
  }

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return dataStorePluginOptions;
  }

  private void ingestWithDistribution(final Distribution distr) {
    final DataStore dataStore = dataStorePluginOptions.createDataStore();
    dataStore.addType(fda, idx);
    try (final Writer<SimpleFeature> writer = dataStore.createWriter(fda.getTypeName())) {

      switch (distr) {
        case UNIFORM:
          createUniformFeatures(new SimpleFeatureBuilder(sft), writer, 100000);
          break;
        case BIMODAL:
          createBimodalFeatures(new SimpleFeatureBuilder(sft), writer, 400000);
          break;
        case SKEWED:
        default:
          createSkewedFeatures(new SimpleFeatureBuilder(sft), writer, 700000);
          break;
      }
    }
  }

  @Test
  public void testUniform() {
    ingestWithDistribution(Distribution.UNIFORM);
    final QueryConstraints query =
        new ExplicitSpatialQuery(
            new GeometryFactory().toGeometry(new Envelope(-180, 180, -90, 90)));
    assertTrue(getSplitsMSE(query, 12, 12) < 0.1);
  }

  @Test
  public void testBimodal() {
    ingestWithDistribution(Distribution.BIMODAL);
    QueryConstraints query =
        new ExplicitSpatialQuery(
            new GeometryFactory().toGeometry(new Envelope(-180, 180, -90, 90)));
    assertTrue(getSplitsMSE(query, 12, 12) < 0.1);

    query =
        new ExplicitSpatialQuery(
            new GeometryFactory().toGeometry(new Envelope(-120, -60, -90, 90)));
    assertTrue(getSplitsMSE(query, 12, 12) < 0.1);

    query =
        new ExplicitSpatialQuery(new GeometryFactory().toGeometry(new Envelope(-20, 20, -90, 90)));
    assertTrue(getSplitsMSE(query, 12, 12) < 0.1);
  }

  @Test
  public void testSkewed() {
    ingestWithDistribution(Distribution.SKEWED);
    QueryConstraints query =
        new ExplicitSpatialQuery(
            new GeometryFactory().toGeometry(new Envelope(-180, 180, -90, 90)));
    assertTrue(getSplitsMSE(query, 12, 12) < 0.1);

    query =
        new ExplicitSpatialQuery(
            new GeometryFactory().toGeometry(new Envelope(-180, -140, -90, 90)));
    assertTrue(getSplitsMSE(query, 12, 12) < 0.1);

    query =
        new ExplicitSpatialQuery(new GeometryFactory().toGeometry(new Envelope(0, 180, -90, 90)));
    assertTrue(getSplitsMSE(query, 12, 12) < 0.1);
  }

  private double getSplitsMSE(
      final QueryConstraints query,
      final int minSplits,
      final int maxSplits) {

    // get splits and create reader for each RangeLocationPair, then summing
    // up the rows for each split

    List<InputSplit> splits = null;
    final MapReduceDataStore dataStore =
        (MapReduceDataStore) dataStorePluginOptions.createDataStore();
    final PersistentAdapterStore as = dataStorePluginOptions.createAdapterStore();
    final InternalAdapterStore ias = dataStorePluginOptions.createInternalAdapterStore();
    final MapReduceDataStoreOperations ops =
        (MapReduceDataStoreOperations) dataStorePluginOptions.createDataStoreOperations();
    final IndexStore is = dataStorePluginOptions.createIndexStore();
    final AdapterIndexMappingStore aim = dataStorePluginOptions.createAdapterIndexMappingStore();
    final DataStatisticsStore stats = dataStorePluginOptions.createDataStatisticsStore();

    final MemoryAdapterStore mas = new MemoryAdapterStore();
    mas.addAdapter(fda);
    try {
      splits =
          dataStore.getSplits(
              new CommonQueryOptions(),
              new FilterByTypeQueryOptions<>(new String[] {fda.getTypeName()}),
              new QuerySingleIndex(idx.getName()),
              new EverythingQuery(),
              mas,
              aim,
              stats,
              ias,
              is,
              new JobContextImpl(new Configuration(), new JobID()),
              minSplits,
              maxSplits);
    } catch (final IOException e) {
      LOGGER.error("IOException thrown when calling getSplits", e);
    } catch (final InterruptedException e) {
      LOGGER.error("InterruptedException thrown when calling getSplits", e);
    }

    final double[] observed = new double[splits.size()];

    int totalCount = 0;
    int currentSplit = 0;

    for (final InputSplit split : splits) {
      int countPerSplit = 0;
      if (GeoWaveInputSplit.class.isAssignableFrom(split.getClass())) {
        final GeoWaveInputSplit gwSplit = (GeoWaveInputSplit) split;
        for (final String indexName : gwSplit.getIndexNames()) {
          final SplitInfo splitInfo = gwSplit.getInfo(indexName);
          for (final RangeLocationPair p : splitInfo.getRangeLocationPairs()) {
            final RecordReaderParams readerParams =
                new RecordReaderParams(
                    splitInfo.getIndex(),
                    as,
                    ias,
                    new short[] {ias.getAdapterId(fda.getTypeName())},
                    null,
                    null,
                    null,
                    splitInfo.isMixedVisibility(),
                    splitInfo.isAuthorizationsLimiting(),
                    splitInfo.isClientsideRowMerging(),
                    p.getRange(),
                    null,
                    null);
            try (RowReader<?> reader = ops.createReader(readerParams)) {
              while (reader.hasNext()) {
                reader.next();
                countPerSplit++;
              }
            } catch (final Exception e) {
              LOGGER.error("Exception thrown when calling createReader", e);
            }
          }
        }
      }
      totalCount += countPerSplit;
      observed[currentSplit] = countPerSplit;
      currentSplit++;
    }

    final double expected = 1.0 / splits.size();

    double sum = 0;

    for (int i = 0; i < observed.length; i++) {
      sum += Math.pow((observed[i] / totalCount) - expected, 2);
    }

    return sum / splits.size();
  }

  public static void createUniformFeatures(
      final SimpleFeatureBuilder pointBuilder,
      final Writer<SimpleFeature> writer,
      final int firstFeatureId) {

    int featureId = firstFeatureId;
    for (int longitude = -180; longitude <= 180; longitude += 1) {
      for (int latitude = -90; latitude <= 90; latitude += 1) {
        pointBuilder.set(
            "geometry",
            GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(longitude, latitude)));
        pointBuilder.set("TimeStamp", new Date());
        pointBuilder.set("Latitude", latitude);
        pointBuilder.set("Longitude", longitude);
        // Note since trajectoryID and comment are marked as nillable we
        // don't need to set them (they default ot null).

        final SimpleFeature sft = pointBuilder.buildFeature(String.valueOf(featureId));
        writer.write(sft);
        featureId++;
      }
    }
  }

  public static void createBimodalFeatures(
      final SimpleFeatureBuilder pointBuilder,
      final Writer<SimpleFeature> writer,
      final int firstFeatureId) {

    int featureId = firstFeatureId;
    for (double longitude = -180.0; longitude <= 0.0; longitude += 1.0) {
      if (longitude == -90) {
        continue;
      }
      for (double latitude = -180.0; latitude <= 0.0; latitude +=
          (Math.abs(-90.0 - longitude) / 10.0)) {
        pointBuilder.set(
            "geometry",
            GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(longitude, latitude)));
        pointBuilder.set("TimeStamp", new Date());
        pointBuilder.set("Latitude", latitude);
        pointBuilder.set("Longitude", longitude);
        // Note since trajectoryID and comment are marked as nillable we
        // don't need to set them (they default ot null).

        final SimpleFeature sft = pointBuilder.buildFeature(String.valueOf(featureId));
        writer.write(sft);
        featureId++;
      }
    }

    for (double longitude = 0.0; longitude <= 180.0; longitude += 1.0) {
      if (longitude == 90) {
        continue;
      }
      for (double latitude = 0.0; latitude <= 180.0; latitude +=
          (Math.abs(90.0 - longitude) / 10.0)) {
        pointBuilder.set(
            "geometry",
            GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(longitude, latitude)));
        pointBuilder.set("TimeStamp", new Date());
        pointBuilder.set("Latitude", latitude);
        pointBuilder.set("Longitude", longitude);
        // Note since trajectoryID and comment are marked as nillable we
        // don't need to set them (they default ot null).

        final SimpleFeature sft = pointBuilder.buildFeature(String.valueOf(featureId));
        writer.write(sft);
        featureId++;
      }
    }
  }

  public static void createSkewedFeatures(
      final SimpleFeatureBuilder pointBuilder,
      final Writer<SimpleFeature> writer,
      final int firstFeatureId) {

    int featureId = firstFeatureId;
    for (double longitude = -180.0; longitude <= 180.0; longitude += 1.0) {
      for (double latitude = -90.0; latitude <= 90.0; latitude += ((longitude + 181.0) / 10.0)) {
        pointBuilder.set(
            "geometry",
            GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(longitude, latitude)));
        pointBuilder.set("TimeStamp", new Date());
        pointBuilder.set("Latitude", latitude);
        pointBuilder.set("Longitude", longitude);
        // Note since trajectoryID and comment are marked as nillable we
        // don't need to set them (they default ot null).

        final SimpleFeature sft = pointBuilder.buildFeature(String.valueOf(featureId));
        writer.write(sft);
        featureId++;
      }
    }
  }
}
