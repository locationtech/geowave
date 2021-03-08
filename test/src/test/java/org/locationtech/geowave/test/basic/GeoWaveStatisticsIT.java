/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.basic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.Range;
import org.apache.commons.lang3.tuple.Pair;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.locationtech.geowave.core.geotime.index.dimension.TemporalBinningStrategy.Unit;
import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.query.api.VectorQueryBuilder;
import org.locationtech.geowave.core.geotime.store.statistics.BoundingBoxStatistic;
import org.locationtech.geowave.core.geotime.store.statistics.BoundingBoxStatistic.BoundingBoxValue;
import org.locationtech.geowave.core.geotime.store.statistics.TimeRangeStatistic;
import org.locationtech.geowave.core.geotime.store.statistics.binning.TimeRangeFieldValueBinningStrategy;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;
import org.locationtech.geowave.core.store.adapter.statistics.histogram.TDigestNumericHistogram;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Query;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticBinningStrategy;
import org.locationtech.geowave.core.store.api.StatisticQueryBuilder;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic.CountValue;
import org.locationtech.geowave.core.store.statistics.binning.CompositeBinningStrategy;
import org.locationtech.geowave.core.store.statistics.binning.NumericRangeFieldValueBinningStrategy;
import org.locationtech.geowave.core.store.statistics.field.BloomFilterStatistic;
import org.locationtech.geowave.core.store.statistics.field.NumericRangeStatistic;
import org.locationtech.geowave.core.store.statistics.field.NumericRangeStatistic.NumericRangeValue;
import org.locationtech.geowave.core.store.statistics.field.NumericStatsStatistic;
import org.locationtech.geowave.core.store.statistics.field.TDigestNumericHistogramStatistic;
import org.locationtech.geowave.examples.ingest.SimpleIngest;
import org.locationtech.geowave.test.GeoWaveITRunner;
import org.locationtech.geowave.test.TestUtils;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore;
import org.locationtech.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;
import org.locationtech.jts.geom.Envelope;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.extra.Interval;
import com.google.common.hash.BloomFilter;
import com.google.common.math.DoubleMath;
import com.google.common.math.Stats;
import jersey.repackaged.com.google.common.collect.Iterators;

@RunWith(GeoWaveITRunner.class)
public class GeoWaveStatisticsIT extends AbstractGeoWaveBasicVectorIT {
  private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveStatisticsIT.class);

  @GeoWaveTestStore(
      value = {
          GeoWaveStoreType.ACCUMULO,
          GeoWaveStoreType.BIGTABLE,
          GeoWaveStoreType.CASSANDRA,
          GeoWaveStoreType.DYNAMODB,
          GeoWaveStoreType.HBASE,
          GeoWaveStoreType.KUDU,
          GeoWaveStoreType.REDIS,
          GeoWaveStoreType.ROCKSDB,
          GeoWaveStoreType.FILESYSTEM})
  protected DataStorePluginOptions dataStore;

  private static long startMillis;

  @BeforeClass
  public static void reportTestStart() {
    startMillis = System.currentTimeMillis();
    LOGGER.warn("-------------------------------");
    LOGGER.warn("*                             *");
    LOGGER.warn("* RUNNING GeoWaveStatisticsIT *");
    LOGGER.warn("*                             *");
    LOGGER.warn("-------------------------------");
  }

  @AfterClass
  public static void reportTestFinish() {
    LOGGER.warn("--------------------------------");
    LOGGER.warn("*                              *");
    LOGGER.warn("* FINISHED GeoWaveStatisticsIT *");
    LOGGER.warn(
        "*        "
            + ((System.currentTimeMillis() - startMillis) / 1000)
            + "s elapsed.          *");
    LOGGER.warn("*                              *");
    LOGGER.warn("--------------------------------");
  }

  @Before
  public void initialize() throws MismatchedIndexToAdapterMapping, IOException {
    final DataStore ds = dataStore.createDataStore();
    final SimpleFeatureType sft = SimpleIngest.createPointFeatureType();
    final Index idx = SimpleIngest.createSpatialIndex();
    final GeotoolsFeatureDataAdapter<SimpleFeature> fda = SimpleIngest.createDataAdapter(sft);
    final List<SimpleFeature> features =
        SimpleIngest.getGriddedFeatures(new SimpleFeatureBuilder(sft), 8675309);
    LOGGER.info(
        String.format("Beginning to ingest a uniform grid of %d features", features.size()));
    int ingestedFeatures = 0;
    final int featuresPer5Percent = features.size() / 20;
    ds.addType(fda, idx);

    try (Writer<Object> writer = ds.createWriter(fda.getTypeName())) {
      for (final SimpleFeature feat : features) {
        ingestedFeatures++;
        if ((ingestedFeatures % featuresPer5Percent) == 0) {
          // just write 5 percent of the grid
          writer.write(feat);
        }
      }
    }
  }

  @After
  public void cleanupWorkspace() {
    TestUtils.deleteAll(dataStore);
  }

  @Test
  public void testAddStatistic() {
    final DataStore ds = dataStore.createDataStore();

    final NumericRangeStatistic longitudeRange =
        new NumericRangeStatistic(SimpleIngest.FEATURE_NAME, "Longitude");
    final NumericRangeStatistic latitudeRange =
        new NumericRangeStatistic(SimpleIngest.FEATURE_NAME, "Latitude");
    final TimeRangeStatistic timeRange =
        new TimeRangeStatistic(SimpleIngest.FEATURE_NAME, "TimeStamp");
    final NumericStatsStatistic latitudeStats =
        new NumericStatsStatistic(SimpleIngest.FEATURE_NAME, "Latitude");
    final BloomFilterStatistic latitudeBloomFilter =
        new BloomFilterStatistic(SimpleIngest.FEATURE_NAME, "Latitude");
    final TDigestNumericHistogramStatistic latitudeTdigest =
        new TDigestNumericHistogramStatistic(SimpleIngest.FEATURE_NAME, "Latitude");
    ds.addStatistic(longitudeRange, timeRange, latitudeStats, latitudeBloomFilter, latitudeTdigest);
    ds.addEmptyStatistic(latitudeRange);

    try (CloseableIterator<NumericRangeValue> iterator =
        ds.queryStatistics(
            StatisticQueryBuilder.newBuilder(NumericRangeStatistic.STATS_TYPE).typeName(
                SimpleIngest.FEATURE_NAME).fieldName("Longitude").build())) {
      assertTrue(iterator.hasNext());
      final NumericRangeValue value = iterator.next();
      assertEquals(-165.0, value.getMin(), 0.1);
      assertEquals(180.0, value.getMax(), 0.1);
      assertFalse(iterator.hasNext());
    }

    try (CloseableIterator<NumericRangeValue> iterator =
        ds.queryStatistics(
            StatisticQueryBuilder.newBuilder(NumericRangeStatistic.STATS_TYPE).typeName(
                SimpleIngest.FEATURE_NAME).fieldName("Latitude").build())) {
      // We only calculated stats for Longitude
      assertTrue(iterator.hasNext());
      assertFalse(iterator.next().isSet());
      assertFalse(iterator.hasNext());
    }
    final Interval interval = ds.getStatisticValue(timeRange);
    try (CloseableIterator<SimpleFeature> it = ds.query(VectorQueryBuilder.newBuilder().build())) {
      long min = Long.MAX_VALUE, max = Long.MIN_VALUE;
      while (it.hasNext()) {
        final long time = ((Date) it.next().getAttribute("TimeStamp")).getTime();
        min = Math.min(min, time);
        max = Math.max(max, time);
      }

      assertEquals(min, interval.getStart().toEpochMilli());
      assertEquals(max, interval.getEnd().toEpochMilli());
    }
    final Stats stats = ds.getStatisticValue(latitudeStats);
    assertEquals(20L, stats.count());
    assertEquals(-90.0, stats.min(), 0.1);
    assertEquals(85.0, stats.max(), 0.1);
    assertEquals(-0.5, stats.mean(), 0.1);
    assertEquals(53.47, stats.populationStandardDeviation(), 0.1);
    final BloomFilter<CharSequence> bloomFilter = ds.getStatisticValue(latitudeBloomFilter);
    boolean expectLat = true;
    for (double lat = -90; lat <= 90; lat += 5) {
      if (expectLat) {
        assertTrue(bloomFilter.mightContain(Double.toString(lat)));
      } else {
        assertFalse(bloomFilter.mightContain(Double.toString(lat)));
      }
      // there are 37 iterations (180 / 5 + 1) and 20 inserted rows, so it doesn't always skip back
      // and forth each iteration, 3 times it stays true at these latitudes
      if (!DoubleMath.fuzzyEquals(-40, lat, 0.1)
          && !DoubleMath.fuzzyEquals(25, lat, 0.1)
          && !DoubleMath.fuzzyEquals(80, lat, 0.1)) {
        expectLat = !expectLat;
      }
    }
    final TDigestNumericHistogram tdigest = ds.getStatisticValue(latitudeTdigest);
    assertEquals(20L, tdigest.getTotalCount(), 0.1);
    assertEquals(-90.0, tdigest.getMinValue(), 0.1);
    assertEquals(85.0, tdigest.getMaxValue(), 0.1);
    assertEquals(0.0, tdigest.quantile(0.5), 0.1);
  }

  @Test
  public void testAddStatisticWithBinningStrategy() {
    final DataStore ds = dataStore.createDataStore();

    final NumericRangeStatistic longitudeRange =
        new NumericRangeStatistic(SimpleIngest.FEATURE_NAME, "Longitude");
    // binning by the same as the statistic should be easy to sanity check
    longitudeRange.setBinningStrategy(new NumericRangeFieldValueBinningStrategy("Longitude"));
    final NumericRangeStatistic latitudeRange =
        new NumericRangeStatistic(SimpleIngest.FEATURE_NAME, "Latitude");
    latitudeRange.setBinningStrategy(new NumericRangeFieldValueBinningStrategy(45, "Latitude"));

    final TimeRangeStatistic timeRangeHourBin =
        new TimeRangeStatistic(SimpleIngest.FEATURE_NAME, "TimeStamp");
    timeRangeHourBin.setBinningStrategy(
        new TimeRangeFieldValueBinningStrategy(Unit.HOUR, "TimeStamp"));
    timeRangeHourBin.setTag("hour");
    final TimeRangeStatistic timeRangeDayBin =
        new TimeRangeStatistic(SimpleIngest.FEATURE_NAME, "TimeStamp");
    timeRangeDayBin.setBinningStrategy(
        new TimeRangeFieldValueBinningStrategy(Unit.DAY, "TimeStamp"));
    timeRangeDayBin.setTag("day");
    final TimeRangeStatistic timeRangeWeekBin =
        new TimeRangeStatistic(SimpleIngest.FEATURE_NAME, "TimeStamp");
    timeRangeWeekBin.setBinningStrategy(
        new TimeRangeFieldValueBinningStrategy(Unit.WEEK, "TimeStamp"));
    timeRangeWeekBin.setTag("week");
    final TimeRangeStatistic timeRangeMonthBin =
        new TimeRangeStatistic(SimpleIngest.FEATURE_NAME, "TimeStamp");
    timeRangeMonthBin.setBinningStrategy(
        new TimeRangeFieldValueBinningStrategy(Unit.MONTH, "TimeStamp"));
    timeRangeMonthBin.setTag("month");
    final TimeRangeStatistic timeRangeYearBin =
        new TimeRangeStatistic(SimpleIngest.FEATURE_NAME, "TimeStamp");
    timeRangeYearBin.setBinningStrategy(
        new TimeRangeFieldValueBinningStrategy(Unit.YEAR, "TimeStamp"));
    timeRangeYearBin.setTag("year");

    final CountStatistic countByGridUsingMultifield = new CountStatistic(SimpleIngest.FEATURE_NAME);
    countByGridUsingMultifield.setTag("multifield-latlon");
    countByGridUsingMultifield.setBinningStrategy(
        new NumericRangeFieldValueBinningStrategy(45, "Latitude", "Longitude"));
    final CountStatistic countByGridUsingComposite = new CountStatistic(SimpleIngest.FEATURE_NAME);
    countByGridUsingComposite.setTag("composite-latlon");
    countByGridUsingComposite.setBinningStrategy(
        new CompositeBinningStrategy(
            new NumericRangeFieldValueBinningStrategy(45, 22.5, "Latitude"),
            new NumericRangeFieldValueBinningStrategy(90, 45, "Longitude")));
    long min = Long.MAX_VALUE, max = Long.MIN_VALUE;
    try (CloseableIterator<SimpleFeature> it = ds.query(VectorQueryBuilder.newBuilder().build())) {

      while (it.hasNext()) {
        final long time = ((Date) it.next().getAttribute("TimeStamp")).getTime();
        min = Math.min(min, time);
        max = Math.max(max, time);
      }
    }
    final Interval overallInterval =
        Interval.of(Instant.ofEpochMilli(min), Instant.ofEpochMilli(max));
    ds.addStatistic(
        longitudeRange,
        latitudeRange,
        timeRangeHourBin,
        timeRangeDayBin,
        timeRangeWeekBin,
        timeRangeMonthBin,
        timeRangeYearBin,
        countByGridUsingMultifield,
        countByGridUsingComposite);
    Range<Double> rangeValue = ds.getStatisticValue(longitudeRange);
    assertEquals(-165.0, rangeValue.getMinimum(), 0.1);
    assertEquals(180.0, rangeValue.getMaximum(), 0.1);

    rangeValue = ds.getStatisticValue(latitudeRange);
    assertEquals(-90.0, rangeValue.getMinimum(), 0.1);
    assertEquals(85.0, rangeValue.getMaximum(), 0.1);


    // Verify count statistic exists
    final Statistic<CountValue> countStat =
        ds.getDataTypeStatistic(
            CountStatistic.STATS_TYPE,
            SimpleIngest.FEATURE_NAME,
            Statistic.INTERNAL_TAG);
    assertNotNull(countStat);

    // Verify value exists
    Long countValue = ds.getStatisticValue(countStat);
    assertEquals(new Long(20), countValue);

    countValue = ds.getStatisticValue(countByGridUsingMultifield);
    assertEquals(new Long(20), countValue);

    countValue = ds.getStatisticValue(countByGridUsingComposite);
    assertEquals(new Long(20), countValue);

    try (CloseableIterator<Pair<ByteArray, Range<Double>>> iterator =
        ds.getBinnedStatisticValues(longitudeRange)) {
      int count = 0;

      while (iterator.hasNext()) {
        final Pair<ByteArray, Range<Double>> binValue = iterator.next();

        final Range<Double> binRange =
            ((NumericRangeFieldValueBinningStrategy) longitudeRange.getBinningStrategy()).getRange(
                binValue.getKey());

        assertEquals(1, binRange.getMaximum() - binRange.getMinimum(), 0.1);
        assertTrue(binRange.containsRange(binValue.getValue()));
        count++;
      }
      assertEquals(20, count);
    }
    try (CloseableIterator<Pair<ByteArray, Range<Double>>> iterator =
        ds.getBinnedStatisticValues(latitudeRange)) {
      int count = 0;

      while (iterator.hasNext()) {
        final Pair<ByteArray, Range<Double>> binValue = iterator.next();

        final Range<Double> binRange =
            ((NumericRangeFieldValueBinningStrategy) latitudeRange.getBinningStrategy()).getRange(
                binValue.getKey());

        assertEquals(45, binRange.getMaximum() - binRange.getMinimum(), 0.1);
        assertTrue(binRange.containsRange(binValue.getValue()));
        count++;
      }
      assertEquals(4, count);
    }
    try (CloseableIterator<Pair<ByteArray, Range<Double>>> iterator =
        ds.getBinnedStatisticValues(latitudeRange)) {
      int count = 0;

      while (iterator.hasNext()) {
        final Pair<ByteArray, Range<Double>> binValue = iterator.next();

        final Range<Double> binRange =
            ((NumericRangeFieldValueBinningStrategy) latitudeRange.getBinningStrategy()).getRange(
                binValue.getKey());

        assertEquals(45, binRange.getMaximum() - binRange.getMinimum(), 0.1);
        assertTrue(binRange.containsRange(binValue.getValue()));
        count++;
      }
      assertEquals(4, count);
    }
    try (CloseableIterator<Pair<ByteArray, Interval>> iterator =
        ds.getBinnedStatisticValues(timeRangeHourBin)) {
      int count = 0;
      while (iterator.hasNext()) {
        final Pair<ByteArray, Interval> binValue = iterator.next();

        final Interval binRange =
            ((TimeRangeFieldValueBinningStrategy) timeRangeHourBin.getBinningStrategy()).getInterval(
                binValue.getKey());
        assertEquals(Duration.ofHours(1L), binRange.toDuration());
        assertTrue(binRange.encloses(binValue.getValue()));
        assertTrue(overallInterval.encloses(binValue.getValue()));
        count++;
      }
      assertEquals(20, count);
    }
    try (CloseableIterator<Pair<ByteArray, Interval>> iterator =
        ds.getBinnedStatisticValues(timeRangeDayBin)) {
      int count = 0;
      while (iterator.hasNext()) {
        final Pair<ByteArray, Interval> binValue = iterator.next();

        final Interval binRange =
            ((TimeRangeFieldValueBinningStrategy) timeRangeDayBin.getBinningStrategy()).getInterval(
                binValue.getKey());
        assertEquals(Duration.ofDays(1), binRange.toDuration());
        assertTrue(binRange.encloses(binValue.getValue()));
        assertTrue(overallInterval.encloses(binValue.getValue()));
        count++;
      }
      assertEquals(20, count);
    }
    try (CloseableIterator<Pair<ByteArray, Interval>> iterator =
        ds.getBinnedStatisticValues(timeRangeWeekBin)) {
      int count = 0;
      while (iterator.hasNext()) {
        final Pair<ByteArray, Interval> binValue = iterator.next();

        final Interval binRange =
            ((TimeRangeFieldValueBinningStrategy) timeRangeWeekBin.getBinningStrategy()).getInterval(
                binValue.getKey());

        assertEquals(Duration.ofDays(7), binRange.toDuration());
        assertTrue(binRange.encloses(binValue.getValue()));
        assertTrue(overallInterval.encloses(binValue.getValue()));
        count++;
      }
      assertEquals(20, count);
    }
    try (CloseableIterator<Pair<ByteArray, Interval>> iterator =
        ds.getBinnedStatisticValues(timeRangeMonthBin)) {
      int count = 0;
      while (iterator.hasNext()) {
        final Pair<ByteArray, Interval> binValue = iterator.next();

        final Interval binRange =
            ((TimeRangeFieldValueBinningStrategy) timeRangeMonthBin.getBinningStrategy()).getInterval(
                binValue.getKey());
        final Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(binValue.getValue().getStart().toEpochMilli());

        assertEquals(
            Duration.ofDays(cal.getActualMaximum(Calendar.DAY_OF_MONTH)),
            binRange.toDuration());
        assertTrue(binRange.encloses(binValue.getValue()));
        assertTrue(overallInterval.encloses(binValue.getValue()));
        count++;
      }
      assertEquals(12, count);
    }
    try (CloseableIterator<Pair<ByteArray, Interval>> iterator =
        ds.getBinnedStatisticValues(timeRangeYearBin)) {
      int count = 0;
      while (iterator.hasNext()) {
        final Pair<ByteArray, Interval> binValue = iterator.next();

        final Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(binValue.getValue().getStart().toEpochMilli());
        final Interval binRange =
            ((TimeRangeFieldValueBinningStrategy) timeRangeYearBin.getBinningStrategy()).getInterval(
                binValue.getKey());
        assertEquals(
            Duration.ofDays(cal.getActualMaximum(Calendar.DAY_OF_YEAR)),
            binRange.toDuration());
        assertTrue(binRange.encloses(binValue.getValue()));
        assertTrue(overallInterval.encloses(binValue.getValue()));
        count++;
      }
      assertEquals(1, count);
    }
    try (CloseableIterator<Pair<ByteArray, Long>> iterator =
        ds.getBinnedStatisticValues(countByGridUsingMultifield)) {
      int count = 0;
      while (iterator.hasNext()) {
        final Pair<ByteArray, Long> binValue = iterator.next();

        final Map<String, Range<Double>> rangePerField =
            ((NumericRangeFieldValueBinningStrategy) countByGridUsingMultifield.getBinningStrategy()).getRanges(
                binValue.getKey());

        assertEquals(1L, binValue.getValue().longValue());
        assertEquals(2, rangePerField.size());
        final Range<Double> latRange = rangePerField.get("Latitude");
        final Range<Double> lonRange = rangePerField.get("Longitude");
        // this ensures the interval is 45
        assertEquals(45, latRange.getMaximum() - latRange.getMinimum(), 0.1);
        assertEquals(45, lonRange.getMaximum() - lonRange.getMinimum(), 0.1);
        // this ensures the offset is 0
        assertEquals(0.0, latRange.getMinimum() % 45.0, 0.1);
        assertEquals(0.0, lonRange.getMinimum() % 45.0, 0.1);
        count++;
      }
      assertEquals(20, count);
    }
    try (CloseableIterator<Pair<ByteArray, Long>> iterator =
        ds.getBinnedStatisticValues(countByGridUsingComposite)) {
      int count = 0;
      int totalCount = 0;
      while (iterator.hasNext()) {
        final Pair<ByteArray, Long> binValue = iterator.next();

        totalCount += binValue.getValue();
        final Pair<StatisticBinningStrategy, ByteArray>[] bins =
            ((CompositeBinningStrategy) countByGridUsingComposite.getBinningStrategy()).getSubBins(
                binValue.getKey());
        assertEquals(2, bins.length);
        final Range<Double> latRange =
            ((NumericRangeFieldValueBinningStrategy) bins[0].getLeft()).getRange(
                bins[0].getRight());
        final Range<Double> lonRange =
            ((NumericRangeFieldValueBinningStrategy) bins[1].getLeft()).getRange(
                bins[1].getRight());
        // this ensures the interval is 45 and 90 respectively
        assertEquals(45, latRange.getMaximum() - latRange.getMinimum(), 0.1);
        assertEquals(90, lonRange.getMaximum() - lonRange.getMinimum(), 0.1);
        // this ensures the offset is 22.5 and 45 respectively
        assertEquals(22.5, Math.abs(latRange.getMinimum() % 45.0), 0.1);
        assertEquals(45.0, Math.abs(lonRange.getMinimum() % 90.0), 0.1);
        count++;
      }
      assertEquals(16, count);
      assertEquals(20, totalCount);
    }
  }

  @Test
  public void testRemoveStatistic() {
    final DataStore ds = dataStore.createDataStore();

    // Verify count statistic exists
    Statistic<CountValue> countStat =
        ds.getDataTypeStatistic(
            CountStatistic.STATS_TYPE,
            SimpleIngest.FEATURE_NAME,
            Statistic.INTERNAL_TAG);
    assertNotNull(countStat);

    // Verify value exists
    Long count = ds.getStatisticValue(countStat);
    assertEquals(new Long(20), count);

    // Verify query
    try (CloseableIterator<CountValue> iterator =
        ds.queryStatistics(
            StatisticQueryBuilder.newBuilder(CountStatistic.STATS_TYPE).typeName(
                SimpleIngest.FEATURE_NAME).build())) {
      assertTrue(iterator.hasNext());
      final CountValue value = iterator.next();
      assertEquals(new Long(20), value.getValue());
      assertFalse(iterator.hasNext());
    }

    ds.removeStatistic(countStat);

    // Verify statistic value was removed
    count = ds.getStatisticValue(countStat);
    assertEquals(count.longValue(), 0L);

    // Verify query
    try (CloseableIterator<CountValue> iterator =
        ds.queryStatistics(
            StatisticQueryBuilder.newBuilder(CountStatistic.STATS_TYPE).typeName(
                SimpleIngest.FEATURE_NAME).build())) {
      assertFalse(iterator.hasNext());
    }


    // Verify statistic is no longer present
    countStat =
        ds.getDataTypeStatistic(
            CountStatistic.STATS_TYPE,
            SimpleIngest.FEATURE_NAME,
            Statistic.INTERNAL_TAG);
    assertNull(countStat);
  }

  @Test
  public void testRecalcStatistic() {
    final DataStore ds = dataStore.createDataStore();

    // Get bounding box statistic
    final Statistic<BoundingBoxValue> bboxStat =
        ds.getFieldStatistic(
            BoundingBoxStatistic.STATS_TYPE,
            SimpleIngest.FEATURE_NAME,
            SimpleIngest.GEOMETRY_FIELD,
            Statistic.INTERNAL_TAG);
    assertNotNull(bboxStat);

    // Get the value
    Envelope bbox = ds.getStatisticValue(bboxStat);
    assertEquals(-165.0, bbox.getMinX(), 0.1);
    assertEquals(180.0, bbox.getMaxX(), 0.1);
    assertEquals(-90.0, bbox.getMinY(), 0.1);
    assertEquals(85.0, bbox.getMaxY(), 0.1);

    // Delete half of the data
    final VectorQueryBuilder bldr = VectorQueryBuilder.newBuilder();
    final Query<?> query =
        bldr.addTypeName(SimpleIngest.FEATURE_NAME).constraints(
            bldr.constraintsFactory().cqlConstraints("Longitude > 0")).build();
    assertTrue(ds.delete(query));

    // Verify the value was unchanged
    bbox = ds.getStatisticValue(bboxStat);
    assertEquals(-165.0, bbox.getMinX(), 0.1);
    assertEquals(180.0, bbox.getMaxX(), 0.1);
    assertEquals(-90.0, bbox.getMinY(), 0.1);
    assertEquals(85.0, bbox.getMaxY(), 0.1);

    // Recalculate the stat
    ds.recalcStatistic(bboxStat);

    // Verify the value was updated
    bbox = ds.getStatisticValue(bboxStat);
    assertEquals(-165.0, bbox.getMinX(), 0.1);
    assertEquals(0, bbox.getMaxX(), 0.1);
    assertEquals(-60.0, bbox.getMinY(), 0.1);
    assertEquals(80.0, bbox.getMaxY(), 0.1);
  }

  @Test
  public void testMergeStats() {
    final DataStore ds = dataStore.createDataStore();

    // Create many statistic values by performing single writes
    final SimpleFeatureBuilder builder =
        new SimpleFeatureBuilder(SimpleIngest.createPointFeatureType());
    int featureId = 9000000;
    for (int i = 0; i < 50; i++) {
      try (Writer<Object> writer = ds.createWriter(SimpleIngest.FEATURE_NAME)) {
        writer.write(SimpleIngest.createRandomFeature(builder, featureId++));
      }
    }

    // Verify count value
    final Statistic<CountValue> countStat =
        ds.getDataTypeStatistic(
            CountStatistic.STATS_TYPE,
            SimpleIngest.FEATURE_NAME,
            Statistic.INTERNAL_TAG);
    assertNotNull(countStat);

    // Verify value exists
    Long count = ds.getStatisticValue(countStat);
    assertEquals(new Long(70), count);

    // Merge stats
    final DataStoreOperations operations = dataStore.createDataStoreOperations();
    final DataStatisticsStore statsStore = dataStore.createDataStatisticsStore();
    assertTrue(operations.mergeStats(statsStore));

    // Verify value is still correct
    count = ds.getStatisticValue(countStat);
    assertEquals(new Long(70), count);

    // Verify there is only 1 metadata entry for it
    final MetadataQuery query =
        new MetadataQuery(
            countStat.getId().getUniqueId().getBytes(),
            countStat.getId().getGroupId().getBytes(),
            false);
    try (CloseableIterator<GeoWaveMetadata> iter =
        operations.createMetadataReader(MetadataType.STATISTIC_VALUES).query(query)) {
      final int valueCount = Iterators.size(iter);
      assertTrue(valueCount == 1);
    }
  }

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return dataStore;
  }
}
