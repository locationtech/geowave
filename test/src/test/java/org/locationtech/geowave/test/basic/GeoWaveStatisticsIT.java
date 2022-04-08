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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.time.Instant;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
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
import org.locationtech.geowave.core.index.sfc.tiered.TieredSFCIndexStrategy;
import org.locationtech.geowave.core.index.sfc.xz.XZHierarchicalIndexStrategy;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.statistics.histogram.NumericHistogram;
import org.locationtech.geowave.core.store.api.BinConstraints;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeStatistic;
import org.locationtech.geowave.core.store.api.FieldStatistic;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.IndexStatistic;
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
import org.locationtech.geowave.core.store.statistics.InternalStatisticsHelper;
import org.locationtech.geowave.core.store.statistics.StatisticsRegistry;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic;
import org.locationtech.geowave.core.store.statistics.adapter.CountStatistic.CountValue;
import org.locationtech.geowave.core.store.statistics.binning.CompositeBinningStrategy;
import org.locationtech.geowave.core.store.statistics.binning.NumericRangeFieldValueBinningStrategy;
import org.locationtech.geowave.core.store.statistics.field.BloomFilterStatistic;
import org.locationtech.geowave.core.store.statistics.field.NumericHistogramStatistic;
import org.locationtech.geowave.core.store.statistics.field.NumericRangeStatistic;
import org.locationtech.geowave.core.store.statistics.field.NumericRangeStatistic.NumericRangeValue;
import org.locationtech.geowave.core.store.statistics.field.NumericStatsStatistic;
import org.locationtech.geowave.core.store.statistics.field.Stats;
import org.locationtech.geowave.core.store.statistics.index.IndexMetaDataSetStatistic.IndexMetaDataSetValue;
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
import com.beust.jcommander.Parameter;
import com.google.common.hash.BloomFilter;
import com.google.common.math.DoubleMath;
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
  public void initialize() throws IOException {
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
    final NumericHistogramStatistic latitudeHistogram =
        new NumericHistogramStatistic(SimpleIngest.FEATURE_NAME, "Latitude");
    ds.addStatistic(
        longitudeRange,
        timeRange,
        latitudeStats,
        latitudeBloomFilter,
        latitudeHistogram);
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
    final NumericHistogram histogram = ds.getStatisticValue(latitudeHistogram);
    assertEquals(20L, histogram.getTotalCount(), 0.1);
    assertEquals(-90.0, histogram.getMinValue(), 0.1);
    assertEquals(85.0, histogram.getMaxValue(), 0.1);
    assertEquals(0.0, histogram.quantile(0.5), 0.1);
  }

  @Test
  public void testInternalStatistics() throws IllegalArgumentException, IllegalAccessException,
      NoSuchFieldException, SecurityException {
    final PersistentAdapterStore adapterStore = dataStore.createAdapterStore();
    final DataStatisticsStore statsStore = dataStore.createDataStatisticsStore();
    final InternalAdapterStore internalAdapterStore = dataStore.createInternalAdapterStore();

    final Index index = SimpleIngest.createSpatialIndex();
    final Collection<Short> adapterIds =
        Collections.singletonList(internalAdapterStore.getAdapterId(SimpleIngest.FEATURE_NAME));
    final IndexMetaDataSetValue ims =
        InternalStatisticsHelper.getIndexMetadata(index, adapterIds, adapterStore, statsStore);
    assertEquals(2, ims.getValue().size());
    assertTrue(ims.getValue().get(0) instanceof TieredSFCIndexStrategy.TierIndexMetaData);
    // the tiered strategy should be empty so it should look like the original empty metadata
    assertEquals(
        SimpleIngest.createSpatialIndex().getIndexStrategy().createMetaData().get(0).toString(),
        ((TieredSFCIndexStrategy.TierIndexMetaData) ims.getValue().get(0)).toString());
    // to avoid opening up accessors in code we just grab the field via reflection in this test
    final Field pointCurveField =
        XZHierarchicalIndexStrategy.XZHierarchicalIndexMetaData.class.getDeclaredField(
            "pointCurveCount");
    pointCurveField.setAccessible(true);
    final Field xzCurveField =
        XZHierarchicalIndexStrategy.XZHierarchicalIndexMetaData.class.getDeclaredField(
            "xzCurveCount");
    xzCurveField.setAccessible(true);
    assertTrue(
        ims.getValue().get(1) instanceof XZHierarchicalIndexStrategy.XZHierarchicalIndexMetaData);
    assertEquals(20, pointCurveField.getInt(ims.getValue().get(1)));
    assertEquals(0, xzCurveField.getInt(ims.getValue().get(1)));
    // duplicate count should be empty
    assertEquals(
        0L,
        InternalStatisticsHelper.getDuplicateCounts(
            index,
            adapterIds,
            adapterStore,
            statsStore).getValue().longValue());
    // differing visibility count should be empty
    assertEquals(
        0L,
        InternalStatisticsHelper.getDifferingVisibilityCounts(
            index,
            adapterIds,
            adapterStore,
            statsStore).getValue().longValue());
    // visibility count should have 20 empty visibilities
    final Map<ByteArray, Long> visMap =
        InternalStatisticsHelper.getVisibilityCounts(
            index,
            adapterIds,
            adapterStore,
            statsStore).getValue();
    assertEquals(1, visMap.size());
    assertEquals(20L, visMap.get(new ByteArray("")).longValue());
  }

  @Test
  public void testAddStatisticWithBinningStrategy() {
    DataStore ds = dataStore.createDataStore();

    NumericRangeStatistic longitudeRange =
        new NumericRangeStatistic(SimpleIngest.FEATURE_NAME, "Longitude");
    // binning by the same as the statistic should be easy to sanity check
    longitudeRange.setBinningStrategy(new NumericRangeFieldValueBinningStrategy("Longitude"));
    NumericRangeStatistic latitudeRange =
        new NumericRangeStatistic(SimpleIngest.FEATURE_NAME, "Latitude");
    latitudeRange.setBinningStrategy(new NumericRangeFieldValueBinningStrategy(45, "Latitude"));

    TimeRangeStatistic timeRangeHourBin =
        new TimeRangeStatistic(SimpleIngest.FEATURE_NAME, "TimeStamp");
    timeRangeHourBin.setBinningStrategy(
        new TimeRangeFieldValueBinningStrategy(Unit.HOUR, "TimeStamp"));
    timeRangeHourBin.setTag("hour");
    TimeRangeStatistic timeRangeDayBin =
        new TimeRangeStatistic(SimpleIngest.FEATURE_NAME, "TimeStamp");
    timeRangeDayBin.setBinningStrategy(
        new TimeRangeFieldValueBinningStrategy(Unit.DAY, "TimeStamp"));
    timeRangeDayBin.setTag("day");
    TimeRangeStatistic timeRangeWeekBin =
        new TimeRangeStatistic(SimpleIngest.FEATURE_NAME, "TimeStamp");
    timeRangeWeekBin.setBinningStrategy(
        new TimeRangeFieldValueBinningStrategy(Unit.WEEK, "TimeStamp"));
    timeRangeWeekBin.setTag("week");
    TimeRangeStatistic timeRangeMonthBin =
        new TimeRangeStatistic(SimpleIngest.FEATURE_NAME, "TimeStamp");
    timeRangeMonthBin.setBinningStrategy(
        new TimeRangeFieldValueBinningStrategy(Unit.MONTH, "TimeStamp"));
    timeRangeMonthBin.setTag("month");
    TimeRangeStatistic timeRangeYearBin =
        new TimeRangeStatistic(SimpleIngest.FEATURE_NAME, "TimeStamp");
    timeRangeYearBin.setBinningStrategy(
        new TimeRangeFieldValueBinningStrategy(Unit.YEAR, "TimeStamp"));
    timeRangeYearBin.setTag("year");

    CountStatistic countByGridUsingMultifield = new CountStatistic(SimpleIngest.FEATURE_NAME);
    countByGridUsingMultifield.setTag("multifield-latlon");
    countByGridUsingMultifield.setBinningStrategy(
        new NumericRangeFieldValueBinningStrategy(45, "Latitude", "Longitude"));
    CountStatistic countByGridUsingComposite = new CountStatistic(SimpleIngest.FEATURE_NAME);
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
    // let's make sure seralization/deserialization works for stats
    ds = dataStore.createDataStore();
    longitudeRange =
        (NumericRangeStatistic) ds.getFieldStatistic(
            longitudeRange.getStatisticType(),
            longitudeRange.getTypeName(),
            longitudeRange.getFieldName(),
            longitudeRange.getTag());
    latitudeRange =
        (NumericRangeStatistic) ds.getFieldStatistic(
            latitudeRange.getStatisticType(),
            latitudeRange.getTypeName(),
            latitudeRange.getFieldName(),
            latitudeRange.getTag());
    timeRangeHourBin =
        (TimeRangeStatistic) ds.getFieldStatistic(
            timeRangeHourBin.getStatisticType(),
            timeRangeHourBin.getTypeName(),
            timeRangeHourBin.getFieldName(),
            timeRangeHourBin.getTag());
    timeRangeDayBin =
        (TimeRangeStatistic) ds.getFieldStatistic(
            timeRangeDayBin.getStatisticType(),
            timeRangeDayBin.getTypeName(),
            timeRangeDayBin.getFieldName(),
            timeRangeDayBin.getTag());
    timeRangeWeekBin =
        (TimeRangeStatistic) ds.getFieldStatistic(
            timeRangeWeekBin.getStatisticType(),
            timeRangeWeekBin.getTypeName(),
            timeRangeWeekBin.getFieldName(),
            timeRangeWeekBin.getTag());
    timeRangeMonthBin =
        (TimeRangeStatistic) ds.getFieldStatistic(
            timeRangeMonthBin.getStatisticType(),
            timeRangeMonthBin.getTypeName(),
            timeRangeMonthBin.getFieldName(),
            timeRangeMonthBin.getTag());
    timeRangeYearBin =
        (TimeRangeStatistic) ds.getFieldStatistic(
            timeRangeYearBin.getStatisticType(),
            timeRangeYearBin.getTypeName(),
            timeRangeYearBin.getFieldName(),
            timeRangeYearBin.getTag());
    countByGridUsingMultifield =
        (CountStatistic) ds.getDataTypeStatistic(
            countByGridUsingMultifield.getStatisticType(),
            countByGridUsingMultifield.getTypeName(),
            countByGridUsingMultifield.getTag());
    countByGridUsingComposite =
        (CountStatistic) ds.getDataTypeStatistic(
            countByGridUsingComposite.getStatisticType(),
            countByGridUsingComposite.getTypeName(),
            countByGridUsingComposite.getTag());
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
    assertTimeBinning(ds, timeRangeHourBin, 20, (i) -> Duration.ofHours(1L), overallInterval);
    assertTimeBinning(ds, timeRangeDayBin, 20, (i) -> Duration.ofDays(1L), overallInterval);
    assertTimeBinning(ds, timeRangeWeekBin, 20, (i) -> Duration.ofDays(7L), overallInterval);
    assertTimeBinning(ds, timeRangeMonthBin, 12, (i) -> {
      final Calendar cal = Calendar.getInstance();
      cal.setTimeInMillis(i.getStart().toEpochMilli());
      return Duration.ofDays(cal.getActualMaximum(Calendar.DAY_OF_MONTH));
    }, overallInterval);
    assertTimeBinning(ds, timeRangeYearBin, 1, (i) -> {
      final Calendar cal = Calendar.getInstance();
      cal.setTimeInMillis(i.getStart().toEpochMilli());
      return Duration.ofDays(cal.getActualMaximum(Calendar.DAY_OF_YEAR));
    }, overallInterval);


    final Set<ByteArray> multiFieldFilteredExpectedResults = new HashSet<>();
    int multiFieldFilteredExpectedCount = 0;
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
        if (latRange.isOverlappedBy(Range.is(12.0))
            && lonRange.isOverlappedBy(Range.between(-89.0, 89.0))) {
          multiFieldFilteredExpectedResults.add(binValue.getKey());
          multiFieldFilteredExpectedCount += binValue.getValue();
        }
        count++;
      }
      assertEquals(20, count);
    }
    // now query by object constraints on the gridded bins
    try (CloseableIterator<Pair<ByteArray, Long>> iterator =
        ds.getBinnedStatisticValues(
            countByGridUsingMultifield,
            BinConstraints.ofObject(
                new Pair[] {
                    Pair.of("Latitude", Double.valueOf(12.0)),
                    Pair.of("Longitude", Range.between(-89.0, 89.0))}))) {
      final Set<ByteArray> multiFieldFilteredActualResults = new HashSet<>();
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
        assertEquals(0.0, latRange.getMinimum(), 0.1);
        assertEquals(45.0, latRange.getMaximum(), 0.1);
        assertEquals(45, lonRange.getMaximum() - lonRange.getMinimum(), 0.1);
        assertTrue(lonRange.getMaximum() < 90.1);
        assertTrue(lonRange.getMinimum() > -90.1);
        // this ensures the offset is 0
        assertEquals(0.0, latRange.getMinimum() % 45.0, 0.1);
        assertEquals(0.0, lonRange.getMinimum() % 45.0, 0.1);
        count += binValue.getValue();
        multiFieldFilteredActualResults.add(binValue.getKey());
      }
      assertEquals(multiFieldFilteredExpectedCount, count);
      assertTrue(multiFieldFilteredExpectedResults.containsAll(multiFieldFilteredActualResults));
      assertTrue(multiFieldFilteredActualResults.containsAll(multiFieldFilteredExpectedResults));
    }

    final Set<ByteArray> compositeFilteredExpectedResults = new HashSet<>();
    int compositeFilteredExpectedCount = 0;
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

        if (latRange.isOverlappedBy(Range.between(-44.0, 44.0))
            && lonRange.isOverlappedBy(Range.between(-179.0, 89.0))) {
          compositeFilteredExpectedResults.add(binValue.getKey());
          compositeFilteredExpectedCount += binValue.getValue();
        }
      }
      assertEquals(16, count);
      assertEquals(20, totalCount);
    }
    try (CloseableIterator<Pair<ByteArray, Long>> iterator =
        ds.getBinnedStatisticValues(
            countByGridUsingComposite,
            BinConstraints.ofObject(
                new Range[] {Range.between(-44.0, 44.0), Range.between(-179.0, 89.0)}))) {
      final Set<ByteArray> compositeFilteredActualResults = new HashSet<>();
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
        assertTrue(latRange.getMaximum() < 67.6);
        assertTrue(latRange.getMinimum() > -67.6);
        assertTrue(lonRange.getMaximum() < 135.1);
        assertTrue(lonRange.getMinimum() > -225.1);
        compositeFilteredActualResults.add(binValue.getKey());
      }

      assertTrue(compositeFilteredExpectedResults.containsAll(compositeFilteredActualResults));
      assertTrue(compositeFilteredActualResults.containsAll(compositeFilteredExpectedResults));
      assertEquals(compositeFilteredExpectedCount, totalCount);
    }
  }

  private static void assertTimeBinning(
      final DataStore ds,
      final TimeRangeStatistic stat,
      final int expectedCount,
      final Function<Interval, Duration> expectedDuration,
      final Interval overallInterval) {

    try (
        CloseableIterator<Pair<ByteArray, Interval>> iterator = ds.getBinnedStatisticValues(stat)) {
      int count = 0;
      while (iterator.hasNext()) {
        final Pair<ByteArray, Interval> binValue = iterator.next();

        final Interval binRange =
            ((TimeRangeFieldValueBinningStrategy) stat.getBinningStrategy()).getInterval(
                binValue.getKey());
        assertEquals(expectedDuration.apply(binValue.getValue()), binRange.toDuration());
        assertTrue(binRange.encloses(binValue.getValue()));
        assertTrue(overallInterval.encloses(binValue.getValue()));
        count++;
      }
      assertEquals(expectedCount, count);
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
  public void testMergeStats() throws IOException {
    internalTestMergeStats();
    cleanupWorkspace();
    // because this has intermittently failed in the past, lets run it several times and make sure
    // it passes regularly
    for (int i = 1; i < 10; i++) {
      initialize();
      internalTestMergeStats();
      cleanupWorkspace();
    }
  }

  private void internalTestMergeStats() {
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
    if (count == 0) {
      count = ds.getStatisticValue(countStat);
    }
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

  @Test
  public void testStatisticParameters() {
    assertNoFinalParameters(Statistic.class);
    assertNoFinalParameters(IndexStatistic.class);
    assertNoFinalParameters(DataTypeStatistic.class);
    assertNoFinalParameters(FieldStatistic.class);
    assertNoFinalParameters(StatisticBinningStrategy.class);
    final List<? extends Statistic<?>> statistics =
        StatisticsRegistry.instance().getAllRegisteredStatistics();
    for (final Statistic<?> statistic : statistics) {
      assertNoFinalParameters(statistic.getClass());
    }
    final List<StatisticBinningStrategy> binningStrategies =
        StatisticsRegistry.instance().getAllRegisteredBinningStrategies();
    for (final StatisticBinningStrategy binningStrategy : binningStrategies) {
      assertNoFinalParameters(binningStrategy.getClass());
    }
  }

  private void assertNoFinalParameters(final Class<?> clazz) {
    for (final Field field : clazz.getDeclaredFields()) {
      if (field.isAnnotationPresent(Parameter.class)) {
        assertFalse(
            clazz.getName() + " contains final CLI Parameter: " + field.getName(),
            Modifier.isFinal(field.getModifiers()));
      }
    }
  }

  @Override
  protected DataStorePluginOptions getDataStorePluginOptions() {
    return dataStore;
  }
}
