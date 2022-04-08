/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.basic;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.core.geotime.binning.ComplexGeometryBinningOption;
import org.locationtech.geowave.core.geotime.binning.SpatialBinningType;
import org.locationtech.geowave.core.geotime.store.query.api.VectorQueryBuilder;
import org.locationtech.geowave.core.geotime.store.statistics.binning.SpatialFieldValueBinningStrategy;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.FloatCompareUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.BinConstraints;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.IngestOptions;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.ingest.LocalFileIngestPlugin;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic;
import org.locationtech.geowave.core.store.statistics.field.NumericStatsStatistic;
import org.locationtech.geowave.core.store.statistics.field.Stats;
import org.locationtech.geowave.core.store.statistics.field.StatsAccumulator;
import org.locationtech.geowave.format.geotools.vector.AbstractFieldRetypingSource;
import org.locationtech.geowave.format.geotools.vector.GeoToolsVectorDataOptions;
import org.locationtech.geowave.format.geotools.vector.GeoToolsVectorDataStoreIngestPlugin;
import org.locationtech.geowave.format.geotools.vector.RetypingVectorDataPlugin;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.TestUtils.DimensionalityType;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.Name;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.ImmutableMap;

@RunWith(GeoWaveITRunner.class)
public class GeoWaveSpatialBinningStatisticsIT extends AbstractGeoWaveBasicVectorIT {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(GeoWaveSpatialBinningStatisticsIT.class);
  private static long startMillis;
  @GeoWaveTestStore(
      value = {
          GeoWaveTestStore.GeoWaveStoreType.ACCUMULO,
          GeoWaveTestStore.GeoWaveStoreType.BIGTABLE,
          GeoWaveTestStore.GeoWaveStoreType.CASSANDRA,
          GeoWaveTestStore.GeoWaveStoreType.DYNAMODB,
          GeoWaveTestStore.GeoWaveStoreType.FILESYSTEM,
          GeoWaveTestStore.GeoWaveStoreType.HBASE,
          GeoWaveTestStore.GeoWaveStoreType.KUDU,
          GeoWaveTestStore.GeoWaveStoreType.REDIS,
          GeoWaveTestStore.GeoWaveStoreType.ROCKSDB})
  protected DataStorePluginOptions dataStoreOptions;

  private final static Map<SpatialBinningType, Double> TYPE_TO_ERROR_THRESHOLD =
      ImmutableMap.of(
          SpatialBinningType.GEOHASH,
          1E-13,
          SpatialBinningType.S2,
          // 0.01 seems ok except for tests applying geometric constraints
          0.03,
          SpatialBinningType.H3,
          // H3 approximations can just be *bad*
          0.25);
  private static Envelope[] TEST_ENVELOPES =
      new Envelope[] {
          new Envelope(-105, -104, 31.75, 32.75),
          new Envelope(-99, -94, 31.5, 33.25),
          new Envelope(-94, -93, 34, 35)};

  private final static String POLYGON_RESOURCE_LOCATION =
      TestUtils.TEST_RESOURCE_PACKAGE + "multi-polygon-test.geojson";

  private final static String POLYGON_FILE_LOCATION =
      TestUtils.TEST_CASE_BASE + "multi-polygon-test.geojson";
  private final static double STATS_COMPARE_EPSILON = 1E-10;

  @BeforeClass
  public static void reportTestStart() {
    startMillis = System.currentTimeMillis();
    LOGGER.warn("-----------------------------------------------");
    LOGGER.warn("*                                             *");
    LOGGER.warn("* RUNNING GeoWaveSpatialBinningStatisticsIT *");
    LOGGER.warn("*                                             *");
    LOGGER.warn("-----------------------------------------------");
  }

  @BeforeClass
  public static void copyPolygonFile() throws IOException {
    final File output = new File(POLYGON_FILE_LOCATION);
    if (!output.exists()) {
      Files.copy(
          GeoWaveSpatialBinningStatisticsIT.class.getClassLoader().getResourceAsStream(
              POLYGON_RESOURCE_LOCATION),
          Paths.get(output.toURI()));
    }
  }

  @AfterClass
  public static void reportTestFinish() {
    LOGGER.warn("------------------------------------------------");
    LOGGER.warn("*                                              *");
    LOGGER.warn("* FINISHED GeoWaveSpatialBinningStatisticsIT *");
    LOGGER.warn(
        "*                {}s elapsed.                  *",
        ((System.currentTimeMillis() - startMillis) / 1000));
    LOGGER.warn("*                                              *");
    LOGGER.warn("------------------------------------------------");
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Test
  public void testLineGeometry() throws MalformedURLException, IOException {
    final DataStore store = dataStoreOptions.createDataStore();
    new GeoToolsVectorDataOptions();
    store.ingest(
        TORNADO_TRACKS_SHAPEFILE_FILE,
        IngestOptions.newBuilder().threads(4).format(
            (LocalFileIngestPlugin) new GeoToolsVectorDataStoreIngestPlugin(
                new StringToIntRetypingPlugin())).build(),
        DimensionalityType.SPATIAL_TEMPORAL.getDefaultIndices());
    final SimpleFeatureType featureType =
        ((FeatureDataAdapter) store.getTypes()[0]).getFeatureType();
    testGeometry(featureType, store);
    testNumericStat(featureType, store);
  }

  @Test
  public void testPointGeometry() throws MalformedURLException, IOException {
    final DataStore store = dataStoreOptions.createDataStore();
    store.ingest(
        HAIL_SHAPEFILE_FILE,
        IngestOptions.newBuilder().threads(4).build(),
        DimensionalityType.SPATIAL_AND_SPATIAL_TEMPORAL.getDefaultIndices());
    final SimpleFeatureType featureType =
        ((FeatureDataAdapter) store.getTypes()[0]).getFeatureType();
    testGeometry(featureType, store);
    testNumericStat(featureType, store);
  }

  @Test
  public void testPolygonGeometry() {
    final DataStore store = dataStoreOptions.createDataStore();
    store.ingest(
        POLYGON_FILE_LOCATION,
        IngestOptions.newBuilder().threads(4).build(),
        DimensionalityType.SPATIAL.getDefaultIndices());
    testGeometry(((FeatureDataAdapter) store.getTypes()[0]).getFeatureType(), store);
  }

  private static void testGeometry(final SimpleFeatureType featureType, final DataStore store) {
    final String geometryField = featureType.getGeometryDescriptor().getLocalName();
    final List<CountStatistic> stats = new ArrayList<>();
    for (final SpatialBinningType type : SpatialBinningType.values()) {
      for (final ComplexGeometryBinningOption complexGeometryOption : ComplexGeometryBinningOption.values()) {
        for (int precision = 1; precision < 4; precision++) {
          // S2 is more than twice as granular in its use of power of 2 "levels" as opposed to only
          // using the granularity of a character for geohash and H3
          // so double the precision for S2 to make it similar in scale
          final int finalPrecision = SpatialBinningType.S2.equals(type) ? precision * 2 : precision;
          final CountStatistic count = new CountStatistic(featureType.getTypeName());
          final SpatialFieldValueBinningStrategy strategy =
              new SpatialFieldValueBinningStrategy(geometryField);
          strategy.setComplexGeometry(complexGeometryOption);
          strategy.setPrecision(finalPrecision);
          strategy.setType(type);
          count.setTag(String.format("%s-%d-%s", type, finalPrecision, complexGeometryOption));
          count.setBinningStrategy(strategy);
          stats.add(count);
        }
      }
    }
    store.addStatistic(stats.toArray(new Statistic[stats.size()]));
    final CountStatistic referenceCountStat = new CountStatistic(featureType.getTypeName());
    store.addStatistic(referenceCountStat);
    final Long expectedCount = store.getStatisticValue(referenceCountStat);
    Assert.assertTrue("Must be at least one entry", expectedCount > 0);
    // sanity check scaling
    stats.stream().filter(
        s -> ((SpatialFieldValueBinningStrategy) s.getBinningStrategy()).getComplexGeometry().equals(
            ComplexGeometryBinningOption.USE_FULL_GEOMETRY_SCALE_BY_OVERLAP)).forEach(
                s -> Assert.assertEquals(
                    String.format(
                        "%s failed scaled geometry",
                        ((SpatialFieldValueBinningStrategy) s.getBinningStrategy()).getDefaultTag()),
                    expectedCount,
                    store.getStatisticValue(s),
                    expectedCount
                        * TYPE_TO_ERROR_THRESHOLD.get(
                            ((SpatialFieldValueBinningStrategy) s.getBinningStrategy()).getType())));
    // sanity check centroids
    stats.stream().filter(
        s -> ((SpatialFieldValueBinningStrategy) s.getBinningStrategy()).getComplexGeometry().equals(
            ComplexGeometryBinningOption.USE_CENTROID_ONLY)).forEach(
                s -> Assert.assertEquals(
                    String.format(
                        "%s failed centroids at precision %d",
                        ((SpatialFieldValueBinningStrategy) s.getBinningStrategy()).getType(),
                        ((SpatialFieldValueBinningStrategy) s.getBinningStrategy()).getPrecision()),
                    expectedCount,
                    store.getStatisticValue(s)));
    // best way to sanity check full geometry is to perhaps check every bin count for centroid only
    // and for full geometry scale by overlap and make sure bin-by-bin every one of the full
    // geometry bins contains at least the count for either of the other 2 approaches (although
    // technically a centroid may be a bin that the full geometry doesn't even intersect so this is
    // not always a fair expectation but it'll suffice, particular when are precision only goes to 4
    // in this test

    final Map<BinningStrategyKey, Map<ByteArray, Long>> perBinResults = new HashMap<>();
    stats.stream().forEach(s -> {
      final Map<ByteArray, Long> results = new HashMap<>();;
      perBinResults.put(
          new BinningStrategyKey((SpatialFieldValueBinningStrategy) s.getBinningStrategy()),
          results);
      try (CloseableIterator<Pair<ByteArray, Long>> it = store.getBinnedStatisticValues(s)) {
        while (it.hasNext()) {
          final Pair<ByteArray, Long> bin = it.next();
          Assert.assertFalse(results.containsKey(bin.getKey()));
          results.put(bin.getKey(), bin.getValue());
        }
      }
    });
    perBinResults.entrySet().stream().filter(
        e -> ComplexGeometryBinningOption.USE_FULL_GEOMETRY.equals(e.getKey().option)).forEach(
            entry -> {
              // get both the other complex binning options with matching type and precision and
              // make sure this full geometry count is at least the others for each bin
              final Map<ByteArray, Long> centroidResults =
                  perBinResults.get(
                      new BinningStrategyKey(
                          entry.getKey().type,
                          entry.getKey().precision,
                          ComplexGeometryBinningOption.USE_CENTROID_ONLY));
              final Map<ByteArray, Long> scaledResults =
                  perBinResults.get(
                      new BinningStrategyKey(
                          entry.getKey().type,
                          entry.getKey().precision,
                          ComplexGeometryBinningOption.USE_FULL_GEOMETRY_SCALE_BY_OVERLAP));
              entry.getValue().forEach((bin, count) -> {
                // make sure the scaled results exists for this bin, but is less than or equal to
                // this count
                final Long scaledResult = scaledResults.get(bin);
                Assert.assertNotNull(
                    String.format(
                        "Scaled result doesn't exist for %s (%d) at bin %s",
                        entry.getKey().type,
                        entry.getKey().precision,
                        entry.getKey().type.binToString(bin.getBytes())),
                    scaledResult);
                Assert.assertTrue(
                    String.format(
                        "Scaled result is greater than the full geometry for %s (%d) at bin %s",
                        entry.getKey().type,
                        entry.getKey().precision,
                        entry.getKey().type.binToString(bin.getBytes())),
                    scaledResult <= count);
                final Long centroidResult = centroidResults.get(bin);
                Assert.assertTrue(
                    String.format(
                        "Centroid result is greater than the full geometry for %s (%d) at bin %s",
                        entry.getKey().type,
                        entry.getKey().precision,
                        entry.getKey().type.binToString(bin.getBytes())),
                    (centroidResult == null) || (centroidResult <= count));
              });
            });
  }

  private static void testNumericStat(final SimpleFeatureType featureType, final DataStore store)
      throws MalformedURLException, IOException {
    final Geometry[] geometryFilters =
        new Geometry[] {
            (Geometry) TestUtils.resourceToFeature(
                new File(TEST_POLYGON_FILTER_FILE).toURI().toURL()).getDefaultGeometry(),
            (Geometry) TestUtils.resourceToFeature(
                new File(TEST_BOX_FILTER_FILE).toURI().toURL()).getDefaultGeometry(),
            (Geometry) TestUtils.resourceToFeature(
                new File(TEST_POLYGON_TEMPORAL_FILTER_FILE).toURI().toURL()).getDefaultGeometry(),
            (Geometry) TestUtils.resourceToFeature(
                new File(TEST_BOX_TEMPORAL_FILTER_FILE).toURI().toURL()).getDefaultGeometry(),};

    // Note: this test is only applicable for the hail (points) and tornado (lines) types
    final String geometryField = featureType.getGeometryDescriptor().getLocalName();
    // we're using a tree map just to make iteration ordered, predictable, and sensible
    final Map<BinningStrategyKey, NumericStatsStatistic> stats =
        new TreeMap<>(Comparator.comparing(BinningStrategyKey::getName));
    // because each gridding system will be overly inclusive, we need to determine the appropriate
    // over-inclusive reference geometry per gridding system to reliably verify results
    final Map<BinningStrategyKey, Geometry[]> referenceGeometries = new HashMap<>();
    for (final SpatialBinningType type : SpatialBinningType.values()) {
      for (int precision = 1; precision < 4; precision++) {
        // S2 is more than twice as granular in its use of power of 2 "levels" as opposed to only
        // using the granularity of a character for geohash and H3
        // so double the precision for S2 to make it similar in scale
        final int finalPrecision = SpatialBinningType.S2.equals(type) ? precision * 2 : precision;
        final NumericStatsStatistic stat =
            new NumericStatsStatistic(featureType.getTypeName(), "LOSS");
        final SpatialFieldValueBinningStrategy strategy =
            new SpatialFieldValueBinningStrategy(geometryField);
        strategy.setPrecision(finalPrecision);
        strategy.setType(type);
        stat.setTag(String.format("Loss-Stats-%s-%d", type, finalPrecision));
        stat.setBinningStrategy(strategy);
        final BinningStrategyKey key = new BinningStrategyKey(strategy);
        stats.put(key, stat);
        final Geometry[] refGeoms = new Geometry[TEST_ENVELOPES.length + geometryFilters.length];
        for (int i = 0; i < TEST_ENVELOPES.length; i++) {
          refGeoms[i] = GeometryUtils.GEOMETRY_FACTORY.toGeometry(TEST_ENVELOPES[i]);
          final ByteArray[] bins = type.getSpatialBins(refGeoms[i], finalPrecision);
          for (final ByteArray bin : bins) {
            refGeoms[i] = refGeoms[i].union(type.getBinGeometry(bin, finalPrecision));
          }
        }
        for (int i = 0; i < geometryFilters.length; i++) {
          final int refGeomIdx = i + TEST_ENVELOPES.length;
          refGeoms[refGeomIdx] = geometryFilters[i];
          final ByteArray[] bins = type.getSpatialBins(refGeoms[refGeomIdx], finalPrecision);
          for (final ByteArray bin : bins) {
            refGeoms[refGeomIdx] =
                refGeoms[refGeomIdx].union(type.getBinGeometry(bin, finalPrecision));
          }
        }
        referenceGeometries.put(key, refGeoms);
      }
    }
    store.addStatistic(stats.values().toArray(new Statistic[stats.size()]));

    // just iterate through all the data to sum up loss as a whole and per area
    final Map<BinningStrategyKey, StatsAccumulator[]> statAccsPerStrategy = new HashMap<>();
    final StatsAccumulator referenceFullScanStatsAccumulator = new StatsAccumulator();
    for (final BinningStrategyKey key : stats.keySet()) {
      final StatsAccumulator[] referenceStatsAccumulators =
          new StatsAccumulator[TEST_ENVELOPES.length + geometryFilters.length];
      for (int i = 0; i < referenceStatsAccumulators.length; i++) {
        referenceStatsAccumulators[i] = new StatsAccumulator();
      }
      statAccsPerStrategy.put(key, referenceStatsAccumulators);
    }
    try (CloseableIterator<SimpleFeature> it =
        store.query(
            VectorQueryBuilder.newBuilder().addTypeName(featureType.getTypeName()).build())) {
      while (it.hasNext()) {
        final SimpleFeature f = it.next();
        // considering centroids are being used for the hashing in this case, just use centroids for
        // this reference
        final Point centroid = ((Geometry) f.getDefaultGeometry()).getCentroid();
        // turns out some of the centroids are "exactly" on the border of hashes, this disambiguates
        // the border (essentially rounding it up)
        final Point centroidOffset =
            GeometryUtils.GEOMETRY_FACTORY.createPoint(
                new Coordinate(
                    centroid.getX() + STATS_COMPARE_EPSILON,
                    centroid.getY() + STATS_COMPARE_EPSILON));
        final double loss = ((Number) f.getAttribute("LOSS")).doubleValue();
        referenceFullScanStatsAccumulator.add(loss);
        for (final BinningStrategyKey key : stats.keySet()) {
          final StatsAccumulator[] referenceStatsAccumulators = statAccsPerStrategy.get(key);
          final Geometry[] refGeoms = referenceGeometries.get(key);
          for (int i = 0; i < refGeoms.length; i++) {
            if (refGeoms[i].contains(centroidOffset)) {
              referenceStatsAccumulators[i].add(loss);
            }
          }
        }
      }
    }
    final Stats referenceFullScanStats = referenceFullScanStatsAccumulator.snapshot();
    final Map<BinningStrategyKey, Stats[]> referenceStatsPerStrategy = new HashMap<>();
    statAccsPerStrategy.forEach((k, v) -> {
      referenceStatsPerStrategy.put(
          k,
          Arrays.stream(v).map(a -> a.snapshot()).toArray(Stats[]::new));
    });
    for (final Entry<BinningStrategyKey, NumericStatsStatistic> entry : stats.entrySet()) {
      final NumericStatsStatistic stat = entry.getValue();
      final Stats[] referenceStats =
          ArrayUtils.add(referenceStatsPerStrategy.get(entry.getKey()), referenceFullScanStats);
      final Stats[] perBinStats = new Stats[referenceStats.length];
      final Stats[] statValue = new Stats[referenceStats.length];
      fillStats(
          perBinStats,
          statValue,
          perBinStats.length - 1,
          stat,
          store,
          BinConstraints.allBins());

      for (int i = 0; i < TEST_ENVELOPES.length; i++) {
        fillStats(
            perBinStats,
            statValue,
            i,
            stat,
            store,
            BinConstraints.ofObject(TEST_ENVELOPES[i]));
      }
      for (int i = 0; i < geometryFilters.length; i++) {
        fillStats(
            perBinStats,
            statValue,
            i + TEST_ENVELOPES.length,
            stat,
            store,
            BinConstraints.ofObject(geometryFilters[i]));
      }
      final double geometricErrorThreshold = TYPE_TO_ERROR_THRESHOLD.get(entry.getKey().type);
      for (int i = 0; i < perBinStats.length; i++) {
        // now just assert that the reference value equals the accumulated value which equals the
        // aggregated "getStatisticValue"

        // for the full scan we can make an exact assertion (to the level of precision of floating
        // point error)

        // for the geometrically constrained assertions we'll need to assert based on the provided
        // error thresholds of the binning strategy (eg. H3 has very poor approximations for
        // line/poly to h3 coords which come into play for the geometrically constrained assertions)
        final boolean isGeometricallyConstrained = (i != (perBinStats.length - 1));
        if (isGeometricallyConstrained) {
          Assert.assertEquals(
              String.format(
                  "Per Bin Stats [%d] count doesn't match full scan for %s (%d)",
                  i,
                  entry.getKey().type,
                  entry.getKey().precision),
              1.0,
              getRatio(referenceStats[i].count(), perBinStats[i].count()),
              geometricErrorThreshold);
          Assert.assertEquals(
              String.format(
                  "getStatisticValue [%d] count doesn't match full scan for %s (%d)",
                  i,
                  entry.getKey().type,
                  entry.getKey().precision),
              1.0,
              getRatio(referenceStats[i].count(), statValue[i].count()),
              geometricErrorThreshold);
          Assert.assertEquals(
              String.format(
                  "Per Bin Stats [%d] mean doesn't match full scan for %s (%d)",
                  i,
                  entry.getKey().type,
                  entry.getKey().precision),
              1.0,
              getRatio(referenceStats[i].mean(), perBinStats[i].mean()),
              geometricErrorThreshold);
          Assert.assertEquals(
              String.format(
                  "Per Bin Stats [%d] variance doesn't match full scan for %s (%d)",
                  i,
                  entry.getKey().type,
                  entry.getKey().precision),
              1.0,
              getRatio(referenceStats[i].populationVariance(), perBinStats[i].populationVariance()),
              geometricErrorThreshold);
          Assert.assertEquals(
              String.format(
                  "getStatisticValue [%d] mean doesn't match full scan for %s (%d)",
                  i,
                  entry.getKey().type,
                  entry.getKey().precision),
              1.0,
              getRatio(referenceStats[i].mean(), statValue[i].mean()),
              geometricErrorThreshold);
          Assert.assertEquals(
              String.format(
                  "getStatisticValue [%d] variance doesn't match full scan for %s (%d)",
                  i,
                  entry.getKey().type,
                  entry.getKey().precision),
              1.0,
              getRatio(referenceStats[i].populationVariance(), statValue[i].populationVariance()),
              geometricErrorThreshold);
        } else {
          Assert.assertEquals(
              String.format(
                  "Per Bin Stats [%d] count doesn't match full scan for %s (%d)",
                  i,
                  entry.getKey().type,
                  entry.getKey().precision),
              referenceStats[i].count(),
              perBinStats[i].count());
          Assert.assertEquals(
              String.format(
                  "getStatisticValue [%d] count doesn't match full scan for %s (%d)",
                  i,
                  entry.getKey().type,
                  entry.getKey().precision),
              referenceStats[i].count(),
              statValue[i].count());
          Assert.assertEquals(
              String.format(
                  "Per Bin Stats [%d] mean doesn't match full scan for %s (%d)",
                  i,
                  entry.getKey().type,
                  entry.getKey().precision),
              referenceStats[i].mean(),
              perBinStats[i].mean(),
              STATS_COMPARE_EPSILON);
          Assert.assertEquals(
              String.format(
                  "Per Bin Stats [%d] variance doesn't match full scan for %s (%d)",
                  i,
                  entry.getKey().type,
                  entry.getKey().precision),
              referenceStats[i].populationVariance(),
              perBinStats[i].populationVariance(),
              STATS_COMPARE_EPSILON);
          Assert.assertEquals(
              String.format(
                  "getStatisticValue [%d] mean doesn't match full scan for %s (%d)",
                  i,
                  entry.getKey().type,
                  entry.getKey().precision),
              referenceStats[i].mean(),
              statValue[i].mean(),
              STATS_COMPARE_EPSILON);
          Assert.assertEquals(
              String.format(
                  "getStatisticValue [%d] variance doesn't match full scan for %s (%d)",
                  i,
                  entry.getKey().type,
                  entry.getKey().precision),
              referenceStats[i].populationVariance(),
              statValue[i].populationVariance(),
              STATS_COMPARE_EPSILON);
        }
      }
    }
  }

  private static double getRatio(final double x, final double y) {
    if (FloatCompareUtils.checkDoublesEqual(y, 0.0)) {
      if (FloatCompareUtils.checkDoublesEqual(x, 0.0)) {
        return 1.0;
      }
      return 0.0;
    }
    return x / y;
  }

  private static void fillStats(
      final Stats[] perBinStats,
      final Stats[] statValue,
      final int i,
      final NumericStatsStatistic stat,
      final DataStore store,
      final BinConstraints constraints) {
    try (CloseableIterator<Pair<ByteArray, Stats>> it =
        store.getBinnedStatisticValues(stat, constraints)) {
      perBinStats[i] = accumulatePerBinStats(it);
    }
    statValue[i] = store.getStatisticValue(stat, constraints);
  }

  private static Stats accumulatePerBinStats(final CloseableIterator<Pair<ByteArray, Stats>> it) {
    final StatsAccumulator acc = new StatsAccumulator();
    while (it.hasNext()) {
      final Pair<ByteArray, Stats> pair = it.next();
      acc.addAll(pair.getRight());
    }
    return acc.snapshot();
  }

  private static class BinningStrategyKey {
    private final SpatialBinningType type;
    private final int precision;
    private final ComplexGeometryBinningOption option;

    private BinningStrategyKey(final SpatialFieldValueBinningStrategy binningStrategy) {
      type = binningStrategy.getType();
      precision = binningStrategy.getPrecision();
      option = binningStrategy.getComplexGeometry();
    }

    private BinningStrategyKey(
        final SpatialBinningType type,
        final int precision,
        final ComplexGeometryBinningOption option) {
      super();
      this.type = type;
      this.precision = precision;
      this.option = option;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = (prime * result) + ((option == null) ? 0 : option.hashCode());
      result = (prime * result) + precision;
      result = (prime * result) + ((type == null) ? 0 : type.hashCode());
      return result;
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final BinningStrategyKey other = (BinningStrategyKey) obj;
      if (option != other.option) {
        return false;
      }
      if (precision != other.precision) {
        return false;
      }
      if (type != other.type) {
        return false;
      }
      return true;
    }

    public String getName() {
      return String.format("%s-%d-%s", type, precision, option);
    }
  }

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return dataStoreOptions;
  }

  private static class StringToIntRetypingPlugin implements RetypingVectorDataPlugin {

    @Override
    public RetypingVectorDataSource getRetypingSource(final SimpleFeatureType type) {
      return new StringToIntRetypingSource(type);
    }

  }
  private static class StringToIntRetypingSource extends AbstractFieldRetypingSource {
    private final SimpleFeatureType type;

    private StringToIntRetypingSource(final SimpleFeatureType type) {
      super();
      this.type = type;
    }

    @Override
    public SimpleFeatureType getRetypedSimpleFeatureType() {
      final SimpleFeatureTypeBuilder typeOutBuilder = new SimpleFeatureTypeBuilder();

      // Manually set the basics and replace the date fields
      typeOutBuilder.setCRS(type.getCoordinateReferenceSystem());
      typeOutBuilder.setDescription(type.getDescription());
      typeOutBuilder.setName(type.getName());
      for (final AttributeDescriptor att : type.getAttributeDescriptors()) {
        if ("LOSS".equals(att.getLocalName())) {
          typeOutBuilder.add(att.getLocalName(), Integer.class);
        } else {
          typeOutBuilder.add(att);
        }
      }

      return typeOutBuilder.buildFeatureType();
    }

    @Override
    public String getFeatureId(final SimpleFeature original) {
      return original.getID();
    }

    @Override
    public Object retypeAttributeValue(final Object value, final Name attributeName) {
      if ("LOSS".equals(attributeName.getLocalPart())) {
        return Integer.parseInt(value.toString());
      }
      return value;
    }
  }
}
