/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.referencing.CRS;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.adapter.vector.plugin.transaction.StatisticsCache;
import org.locationtech.geowave.core.geotime.index.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.index.SpatialOptions;
import org.locationtech.geowave.core.geotime.index.SpatialTemporalDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.index.SpatialTemporalOptions;
import org.locationtech.geowave.core.geotime.store.query.TemporalConstraints;
import org.locationtech.geowave.core.geotime.store.query.TemporalConstraintsSet;
import org.locationtech.geowave.core.geotime.store.query.TemporalRange;
import org.locationtech.geowave.core.geotime.store.statistics.BoundingBoxStatistic;
import org.locationtech.geowave.core.geotime.store.statistics.BoundingBoxStatistic.BoundingBoxValue;
import org.locationtech.geowave.core.geotime.store.statistics.TimeRangeStatistic;
import org.locationtech.geowave.core.geotime.store.statistics.TimeRangeStatistic.TimeRangeValue;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.geotime.util.TimeDescriptors;
import org.locationtech.geowave.core.geotime.util.TimeDescriptors.TimeDescriptorConfiguration;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.numeric.MultiDimensionalNumericData;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.query.constraints.BasicQueryByClass;
import org.locationtech.geowave.core.store.query.constraints.Constraints;
import org.locationtech.geowave.core.store.statistics.StatisticId;
import org.locationtech.geowave.core.store.statistics.StatisticType;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.PrecisionModel;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.MathTransform;
import com.google.common.primitives.Bytes;

public class QueryIndexHelperTest {
  private static final Index SPATIAL_INDEX =
      SpatialDimensionalityTypeProvider.createIndexFromOptions(new SpatialOptions());
  private static final Index SPATIAL_TEMPORAL_INDEX =
      SpatialTemporalDimensionalityTypeProvider.createIndexFromOptions(
          new SpatialTemporalOptions());
  final ByteArray dataAdapterId = new ByteArray("123");

  SimpleFeatureType rangeType;
  SimpleFeatureType singleType;
  SimpleFeatureType geoType;
  SimpleFeatureType geoMercType;

  final TimeDescriptors geoTimeDescriptors = new TimeDescriptors();
  final TimeDescriptors rangeTimeDescriptors = new TimeDescriptors();
  final TimeDescriptors singleTimeDescriptors = new TimeDescriptors();

  final GeometryFactory factory = new GeometryFactory(new PrecisionModel(PrecisionModel.FIXED));

  Date startTime, endTime;
  Object[] singleDefaults, rangeDefaults, geoDefaults;

  MathTransform transform;

  @Before
  public void setup() throws SchemaException, ParseException, FactoryException {

    startTime = DateUtilities.parseISO("2005-05-15T20:32:56Z");
    endTime = DateUtilities.parseISO("2005-05-20T20:32:56Z");

    geoType =
        DataUtilities.createType(
            "geostuff",
            "geometry:Geometry:srid=4326,pop:java.lang.Long,pid:String");

    geoMercType =
        DataUtilities.createType(
            "geostuff",
            "geometry:Geometry:srid=3785,pop:java.lang.Long,pid:String");

    rangeType =
        DataUtilities.createType(
            "geostuff",
            "geometry:Geometry:srid=4326,start:Date,end:Date,pop:java.lang.Long,pid:String");

    singleType =
        DataUtilities.createType(
            "geostuff",
            "geometry:Geometry:srid=4326,when:Date,pop:java.lang.Long,pid:String");

    transform =
        CRS.findMathTransform(
            geoMercType.getCoordinateReferenceSystem(),
            geoType.getCoordinateReferenceSystem(),
            true);

    final TimeDescriptorConfiguration rangeConfig = new TimeDescriptorConfiguration();
    rangeConfig.configureFromType(rangeType);
    rangeTimeDescriptors.update(rangeType, rangeConfig);
    final TimeDescriptorConfiguration singleTimeConfig = new TimeDescriptorConfiguration();
    singleTimeConfig.configureFromType(singleType);
    singleTimeDescriptors.update(singleType, singleTimeConfig);

    List<AttributeDescriptor> descriptors = rangeType.getAttributeDescriptors();
    rangeDefaults = new Object[descriptors.size()];
    int p = 0;
    for (final AttributeDescriptor descriptor : descriptors) {
      rangeDefaults[p++] = descriptor.getDefaultValue();
    }

    descriptors = singleType.getAttributeDescriptors();
    singleDefaults = new Object[descriptors.size()];
    p = 0;
    for (final AttributeDescriptor descriptor : descriptors) {
      singleDefaults[p++] = descriptor.getDefaultValue();
    }

    descriptors = geoType.getAttributeDescriptors();
    geoDefaults = new Object[descriptors.size()];
    p = 0;
    for (final AttributeDescriptor descriptor : descriptors) {
      geoDefaults[p++] = descriptor.getDefaultValue();
    }
  }

  @Test
  public void testGetTemporalConstraintsForSingleClippedRange() throws ParseException {

    final Date stime = DateUtilities.parseISO("2005-05-14T20:32:56Z");
    final Date etime = DateUtilities.parseISO("2005-05-18T20:32:56Z");
    final Date stime1 = DateUtilities.parseISO("2005-05-18T20:32:56Z");
    final Date etime1 = DateUtilities.parseISO("2005-05-19T20:32:56Z");

    final TestStatisticsCache statsCache = new TestStatisticsCache();
    final TimeRangeStatistic whenStats = new TimeRangeStatistic(singleType.getTypeName(), "when");
    final TimeRangeValue whenValue = whenStats.createEmpty();
    statsCache.putFieldStatistic(TimeRangeStatistic.STATS_TYPE, "when", whenValue);

    final TemporalConstraintsSet constraintsSet = new TemporalConstraintsSet();
    constraintsSet.getConstraintsFor("when").add(new TemporalRange(stime, etime));

    final FeatureDataAdapter singleDataAdapter = new FeatureDataAdapter(singleType);
    final SimpleFeature notIntersectSingle1 = createSingleTimeFeature(startTime);

    whenValue.entryIngested(singleDataAdapter, notIntersectSingle1);

    final SimpleFeature notIntersectSingle = createSingleTimeFeature(endTime);

    whenValue.entryIngested(singleDataAdapter, notIntersectSingle);

    final TemporalConstraintsSet resultConstraintsSet =
        QueryIndexHelper.clipIndexedTemporalConstraints(
            statsCache,
            singleTimeDescriptors,
            constraintsSet);

    final TemporalConstraints constraints = resultConstraintsSet.getConstraintsFor("when");

    assertEquals(1, constraints.getRanges().size());
    assertEquals(startTime, constraints.getStartRange().getStartTime());
    assertEquals(etime, constraints.getStartRange().getEndTime());

    final TemporalConstraintsSet constraintsSet1 = new TemporalConstraintsSet();
    constraintsSet1.getConstraintsFor("when").add(new TemporalRange(stime1, etime1));

    final TemporalConstraintsSet resultConstraintsSet1 =
        QueryIndexHelper.clipIndexedTemporalConstraints(
            statsCache,
            singleTimeDescriptors,
            constraintsSet1);

    final TemporalConstraints constraints1 = resultConstraintsSet1.getConstraintsFor("when");

    assertEquals(1, constraints1.getRanges().size());
    assertEquals(stime1, constraints1.getStartRange().getStartTime());
    assertEquals(etime1, constraints1.getStartRange().getEndTime());
  }

  @Test
  public void testGetTemporalConstraintsForRangeClippedFullRange() throws ParseException {

    final TestStatisticsCache statsCache = new TestStatisticsCache();
    final TimeRangeStatistic startStats = new TimeRangeStatistic("type", "start");
    final TimeRangeValue startValue = startStats.createEmpty();
    statsCache.putFieldStatistic(TimeRangeStatistic.STATS_TYPE, "start", startValue);

    final TimeRangeStatistic endStats = new TimeRangeStatistic("type", "end");
    final TimeRangeValue endValue = endStats.createEmpty();
    statsCache.putFieldStatistic(TimeRangeStatistic.STATS_TYPE, "end", endValue);

    final Date statsStart1 = DateUtilities.parseISO("2005-05-18T20:32:56Z");
    final Date statsStart2 = DateUtilities.parseISO("2005-05-20T20:32:56Z");
    final Date statsEnd1 = DateUtilities.parseISO("2005-05-21T20:32:56Z");
    final Date statsEnd2 = DateUtilities.parseISO("2005-05-24T20:32:56Z");

    final SimpleFeature firstRangFeature = createFeature(statsStart1, statsEnd1);
    FeatureDataAdapter adapter = new FeatureDataAdapter(firstRangFeature.getFeatureType());

    startValue.entryIngested(adapter, firstRangFeature);

    endValue.entryIngested(adapter, firstRangFeature);

    final SimpleFeature secondRangFeature = createFeature(statsStart2, statsEnd2);

    startValue.entryIngested(adapter, secondRangFeature);

    endValue.entryIngested(adapter, secondRangFeature);

    final Date stime = DateUtilities.parseISO("2005-05-18T20:32:56Z");
    final Date etime = DateUtilities.parseISO("2005-05-19T20:32:56Z");

    final TemporalConstraintsSet constraintsSet = new TemporalConstraintsSet();
    constraintsSet.getConstraintsForRange("start", "end").add(
        new TemporalRange(new Date(0), etime));

    final TemporalConstraintsSet resultConstraintsSet =
        QueryIndexHelper.clipIndexedTemporalConstraints(
            statsCache,
            rangeTimeDescriptors,
            constraintsSet);

    final TemporalConstraints constraints =
        resultConstraintsSet.getConstraintsForRange("start", "end");

    assertEquals(1, constraints.getRanges().size());
    assertEquals(stime, constraints.getStartRange().getStartTime());
    assertEquals(etime, constraints.getStartRange().getEndTime());
  }

  @Test
  public void testComposeQueryWithTimeRange() throws ParseException {

    final TestStatisticsCache statsCache = new TestStatisticsCache();
    final TimeRangeStatistic startStats = new TimeRangeStatistic("type", "start");
    final TimeRangeValue startValue = startStats.createEmpty();
    statsCache.putFieldStatistic(TimeRangeStatistic.STATS_TYPE, "start", startValue);

    final TimeRangeStatistic endStats = new TimeRangeStatistic("type", "end");
    final TimeRangeValue endValue = endStats.createEmpty();
    statsCache.putFieldStatistic(TimeRangeStatistic.STATS_TYPE, "end", endValue);

    final Date statsStart1 = DateUtilities.parseISO("2005-05-18T20:32:56Z");
    final Date statsStart2 = DateUtilities.parseISO("2005-05-20T20:32:56Z");
    final Date statsEnd1 = DateUtilities.parseISO("2005-05-21T20:32:56Z");
    final Date statsEnd2 = DateUtilities.parseISO("2005-05-24T20:32:56Z");

    final SimpleFeature firstRangFeature = createFeature(statsStart1, statsEnd1);
    FeatureDataAdapter adapter = new FeatureDataAdapter(firstRangFeature.getFeatureType());

    startValue.entryIngested(adapter, firstRangFeature);

    endValue.entryIngested(adapter, firstRangFeature);

    final SimpleFeature secondRangFeature = createFeature(statsStart2, statsEnd2);

    startValue.entryIngested(adapter, secondRangFeature);

    endValue.entryIngested(adapter, secondRangFeature);

    final Date stime = DateUtilities.parseISO("2005-05-18T20:32:56Z");
    final Date etime = DateUtilities.parseISO("2005-05-19T20:32:56Z");

    final TemporalConstraintsSet constraintsSet = new TemporalConstraintsSet();
    constraintsSet.getConstraintsForRange("start", "end").add(new TemporalRange(stime, etime));

    final BasicQueryByClass query =
        new BasicQueryByClass(
            QueryIndexHelper.composeConstraints(
                statsCache,
                rangeType,
                rangeTimeDescriptors,
                factory.toGeometry(
                    factory.createPoint(new Coordinate(27.25, 41.25)).getEnvelopeInternal()),
                constraintsSet));

    final List<MultiDimensionalNumericData> nd = query.getIndexConstraints(SPATIAL_TEMPORAL_INDEX);
    assertEquals(stime.getTime(), nd.get(0).getDataPerDimension()[2].getMin().longValue());
    assertEquals(etime.getTime(), nd.get(0).getDataPerDimension()[2].getMax().longValue());

    final BasicQueryByClass query1 =
        new BasicQueryByClass(
            QueryIndexHelper.composeConstraints(
                statsCache,
                rangeType,
                rangeTimeDescriptors,
                factory.toGeometry(
                    factory.createPoint(new Coordinate(27.25, 41.25)).getEnvelopeInternal()),
                null));

    final List<MultiDimensionalNumericData> nd1 =
        query1.getIndexConstraints(SPATIAL_TEMPORAL_INDEX);
    assertEquals(statsStart1.getTime(), nd1.get(0).getDataPerDimension()[2].getMin().longValue());
    assertEquals(statsEnd2.getTime(), nd1.get(0).getDataPerDimension()[2].getMax().longValue());
  }

  @Test
  public void testComposeQueryWithOutTimeRange() {

    final TestStatisticsCache statsCache = new TestStatisticsCache();
    final BoundingBoxStatistic geoStats = new BoundingBoxStatistic("type", "geometry");
    final BoundingBoxValue value = geoStats.createEmpty();
    statsCache.putFieldStatistic(BoundingBoxStatistic.STATS_TYPE, "geometry", value);

    final SimpleFeature firstFeature =
        createGeoFeature(factory.createPoint(new Coordinate(22.25, 42.25)));
    FeatureDataAdapter adapter = new FeatureDataAdapter(firstFeature.getFeatureType());

    value.entryIngested(adapter, firstFeature);

    final SimpleFeature secondFeature =
        createGeoFeature(factory.createPoint(new Coordinate(27.25, 41.25)));

    value.entryIngested(adapter, secondFeature);

    final Envelope bounds = new Envelope(21.23, 26.23, 41.75, 43.1);

    final BasicQueryByClass query =
        new BasicQueryByClass(
            QueryIndexHelper.composeConstraints(
                statsCache,
                geoType,
                geoTimeDescriptors,
                new GeometryFactory().toGeometry(bounds),
                null));

    final List<MultiDimensionalNumericData> nd = query.getIndexConstraints(SPATIAL_INDEX);
    assertEquals(21.23, nd.get(0).getDataPerDimension()[0].getMin(), 0.0001);
    assertEquals(26.23, nd.get(0).getDataPerDimension()[0].getMax(), 0.0001);
    assertEquals(41.75, nd.get(0).getDataPerDimension()[1].getMin(), 0.0001);
    assertEquals(43.1, nd.get(0).getDataPerDimension()[1].getMax(), 0.0001);
  }

  @Test
  public void testGetBBOX() {
    final TestStatisticsCache statsCache = new TestStatisticsCache();
    final BoundingBoxStatistic geoStats = new BoundingBoxStatistic("type", "geometry");
    final BoundingBoxValue value = geoStats.createEmpty();
    statsCache.putFieldStatistic(BoundingBoxStatistic.STATS_TYPE, "geometry", value);

    final SimpleFeature firstFeature =
        createGeoFeature(factory.createPoint(new Coordinate(22.25, 42.25)));
    FeatureDataAdapter adapter = new FeatureDataAdapter(firstFeature.getFeatureType());

    value.entryIngested(adapter, firstFeature);

    final SimpleFeature secondFeature =
        createGeoFeature(factory.createPoint(new Coordinate(27.25, 41.25)));

    value.entryIngested(adapter, secondFeature);

    final Envelope bounds = new Envelope(21.23, 26.23, 41.75, 43.1);

    final Geometry bbox =
        QueryIndexHelper.clipIndexedBBOXConstraints(
            statsCache,
            geoType,
            geoType.getCoordinateReferenceSystem(),
            new GeometryFactory().toGeometry(bounds));

    final Envelope env = bbox.getEnvelopeInternal();

    assertEquals(22.25, env.getMinX(), 0.0001);
    assertEquals(26.23, env.getMaxX(), 0.0001);
    assertEquals(41.75, env.getMinY(), 0.0001);
    assertEquals(42.25, env.getMaxY(), 0.0001);
  }

  @Test
  public void testBBOXStatReprojection() {

    // create a EPSG:3785 feature (units in meters)
    final SimpleFeature mercFeat =
        createGeoMercFeature(factory.createPoint(new Coordinate(19971868.8804, 20037508.3428)));

    // convert from EPSG:3785 to EPSG:4326 (convert to degrees lon/lat)
    // approximately 180.0, 85.0
    final SimpleFeature defaultCRSFeat = GeometryUtils.crsTransform(mercFeat, geoType, transform);

    final BoundingBoxStatistic bboxStat =
        new BoundingBoxStatistic(
            geoType.getTypeName(),
            geoType.getGeometryDescriptor().getLocalName(),
            geoMercType.getCoordinateReferenceSystem(),
            geoType.getCoordinateReferenceSystem());

    final BoundingBoxValue bboxValue = bboxStat.createEmpty();
    bboxValue.entryIngested(new FeatureDataAdapter(geoType), mercFeat);

    final Coordinate coord = ((Point) defaultCRSFeat.getDefaultGeometry()).getCoordinate();

    // coordinate should match reprojected feature
    assertEquals(coord.x, bboxValue.getMinX(), 0.0001);
    assertEquals(coord.x, bboxValue.getMaxX(), 0.0001);
    assertEquals(coord.y, bboxValue.getMinY(), 0.0001);
    assertEquals(coord.y, bboxValue.getMaxY(), 0.0001);
  }

  private SimpleFeature createGeoFeature(final Geometry geo) {
    final SimpleFeature instance =
        SimpleFeatureBuilder.build(geoType, geoDefaults, UUID.randomUUID().toString());
    instance.setAttribute("pop", Long.valueOf(100));
    instance.setAttribute("pid", UUID.randomUUID().toString());
    instance.setAttribute("geometry", geo);
    return instance;
  }

  private SimpleFeature createGeoMercFeature(final Geometry geo) {
    final SimpleFeature instance =
        SimpleFeatureBuilder.build(geoMercType, geoDefaults, UUID.randomUUID().toString());
    instance.setAttribute("pop", Long.valueOf(100));
    instance.setAttribute("pid", UUID.randomUUID().toString());
    instance.setAttribute("geometry", geo);
    return instance;
  }

  private SimpleFeature createSingleTimeFeature(final Date time) {
    final SimpleFeature instance =
        SimpleFeatureBuilder.build(singleType, singleDefaults, UUID.randomUUID().toString());
    instance.setAttribute("pop", Long.valueOf(100));
    instance.setAttribute("pid", UUID.randomUUID().toString());
    instance.setAttribute("when", time);
    instance.setAttribute("geometry", factory.createPoint(new Coordinate(27.25, 41.25)));
    return instance;
  }

  @Test
  public void testComposeSubsetConstraints() throws ParseException {

    final TestStatisticsCache statsCache = new TestStatisticsCache();
    final TimeRangeStatistic startStats = new TimeRangeStatistic("type", "start");
    final TimeRangeValue startValue = startStats.createEmpty();
    statsCache.putFieldStatistic(TimeRangeStatistic.STATS_TYPE, "start", startValue);

    final TimeRangeStatistic endStats = new TimeRangeStatistic("type", "end");
    final TimeRangeValue endValue = endStats.createEmpty();
    statsCache.putFieldStatistic(TimeRangeStatistic.STATS_TYPE, "end", endValue);

    final Date statsStart1 = DateUtilities.parseISO("2005-05-18T20:32:56Z");
    final Date statsStart2 = DateUtilities.parseISO("2005-05-20T20:32:56Z");
    final Date statsEnd1 = DateUtilities.parseISO("2005-05-21T20:32:56Z");
    final Date statsEnd2 = DateUtilities.parseISO("2005-05-24T20:32:56Z");

    final SimpleFeature firstRangFeature = createFeature(statsStart1, statsEnd1);
    FeatureDataAdapter adapter = new FeatureDataAdapter(firstRangFeature.getFeatureType());

    startValue.entryIngested(adapter, firstRangFeature);

    endValue.entryIngested(adapter, firstRangFeature);

    final SimpleFeature secondRangFeature = createFeature(statsStart2, statsEnd2);

    startValue.entryIngested(adapter, secondRangFeature);

    endValue.entryIngested(adapter, secondRangFeature);

    final Date stime = DateUtilities.parseISO("2005-05-18T20:32:56Z");
    final Date etime = DateUtilities.parseISO("2005-05-19T20:32:56Z");

    final TemporalConstraintsSet constraintsSet = new TemporalConstraintsSet();
    constraintsSet.getConstraintsForRange("start", "end").add(new TemporalRange(stime, etime));

    final Constraints constraints =
        QueryIndexHelper.composeTimeBoundedConstraints(
            rangeType,
            rangeTimeDescriptors,
            constraintsSet);
    final List<MultiDimensionalNumericData> nd =
        constraints.getIndexConstraints(SPATIAL_TEMPORAL_INDEX);
    assertTrue(nd.isEmpty());

    final BoundingBoxStatistic geoStats = new BoundingBoxStatistic("type", "geometry");
    final BoundingBoxValue geoValue = geoStats.createEmpty();
    statsCache.putFieldStatistic(BoundingBoxStatistic.STATS_TYPE, "geometry", geoValue);

    final SimpleFeature firstFeature =
        createGeoFeature(factory.createPoint(new Coordinate(22.25, 42.25)));

    geoValue.entryIngested(adapter, firstFeature);

    final SimpleFeature secondFeature =
        createGeoFeature(factory.createPoint(new Coordinate(27.25, 41.25)));
    geoValue.entryIngested(adapter, secondFeature);

    final Constraints constraints1 =
        QueryIndexHelper.composeConstraints(
            statsCache,
            rangeType,
            rangeTimeDescriptors,
            null,
            constraintsSet);
    final List<MultiDimensionalNumericData> nd1 =
        constraints1.getIndexConstraints(SPATIAL_TEMPORAL_INDEX);
    assertTrue(nd1.isEmpty());
    /*
     * assertEquals( stime.getTime(), (long) nd1.get( 0).getDataPerDimension()[2].getMin());
     * assertEquals( etime.getTime(), (long) nd1.get( 0).getDataPerDimension()[2].getMax());
     */

    final TemporalConstraintsSet constraintsSet2 = new TemporalConstraintsSet();
    constraintsSet2.getConstraintsForRange("start", "end").add(
        new TemporalRange(statsStart1, statsEnd2));
    final Constraints constraints2 =
        QueryIndexHelper.composeTimeBoundedConstraints(
            rangeType,
            rangeTimeDescriptors,
            constraintsSet2);
    final List<MultiDimensionalNumericData> nd2 =
        constraints2.getIndexConstraints(SPATIAL_TEMPORAL_INDEX);
    assertTrue(nd2.isEmpty());
  }

  private SimpleFeature createFeature(final Date sTime, final Date eTime) {
    final SimpleFeature instance =
        SimpleFeatureBuilder.build(rangeType, rangeDefaults, UUID.randomUUID().toString());
    instance.setAttribute("pop", Long.valueOf(100));
    instance.setAttribute("pid", UUID.randomUUID().toString());
    instance.setAttribute("start", sTime);
    instance.setAttribute("end", eTime);
    instance.setAttribute("geometry", factory.createPoint(new Coordinate(27.25, 41.25)));
    return instance;
  }

  private static class TestStatisticsCache extends StatisticsCache {

    public TestStatisticsCache() {
      super(null, null);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V extends StatisticValue<R>, R> V getFieldStatistic(
        final StatisticType<V> statisticType,
        final String fieldName) {
      if (statisticType == null || fieldName == null) {
        return null;
      }
      ByteArray key =
          new ByteArray(
              Bytes.concat(
                  statisticType.getBytes(),
                  StatisticId.UNIQUE_ID_SEPARATOR,
                  fieldName.getBytes()));
      if (cache.containsKey(key)) {
        return (V) cache.get(key);
      }
      cache.put(key, null);
      return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V extends StatisticValue<R>, R> V getAdapterStatistic(
        final StatisticType<V> statisticType) {
      ByteArray key = statisticType;
      if (cache.containsKey(key)) {
        return (V) cache.get(key);
      }
      cache.put(key, null);
      return null;
    }

    public void putFieldStatistic(
        final StatisticType<?> statisticType,
        final String fieldName,
        final StatisticValue<?> value) {
      ByteArray key =
          new ByteArray(
              Bytes.concat(
                  statisticType.getBytes(),
                  StatisticId.UNIQUE_ID_SEPARATOR,
                  fieldName.getBytes()));
      cache.put(key, value);
    }

    public void putAdapterStatistic(
        final StatisticType<?> statisticType,
        final StatisticValue<?> value) {
      cache.put(statisticType, value);
    }

  }
}
