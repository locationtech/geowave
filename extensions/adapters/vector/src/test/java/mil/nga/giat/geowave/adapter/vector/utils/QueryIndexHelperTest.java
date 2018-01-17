/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.adapter.vector.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.referencing.CRS;
import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.operation.MathTransform;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.PrecisionModel;

import mil.nga.giat.geowave.adapter.vector.stats.FeatureBoundingBoxStatistics;
import mil.nga.giat.geowave.adapter.vector.stats.FeatureTimeRangeStatistics;
import mil.nga.giat.geowave.adapter.vector.util.FeatureDataUtils;
import mil.nga.giat.geowave.adapter.vector.util.QueryIndexHelper;
import mil.nga.giat.geowave.adapter.vector.utils.TimeDescriptors.TimeDescriptorConfiguration;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialOptions;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialTemporalDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialTemporalOptions;
import mil.nga.giat.geowave.core.geotime.store.query.TemporalConstraints;
import mil.nga.giat.geowave.core.geotime.store.query.TemporalConstraintsSet;
import mil.nga.giat.geowave.core.geotime.store.query.TemporalRange;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.BasicQuery;
import mil.nga.giat.geowave.core.store.query.BasicQuery.Constraints;

public class QueryIndexHelperTest
{
	private static final PrimaryIndex SPATIAL_INDEX = new SpatialDimensionalityTypeProvider()
			.createPrimaryIndex(new SpatialOptions());
	private static final PrimaryIndex SPATIAL_TEMPORAL_INDEX = new SpatialTemporalDimensionalityTypeProvider()
			.createPrimaryIndex(new SpatialTemporalOptions());
	final ByteArrayId dataAdapterId = new ByteArrayId(
			"123");

	SimpleFeatureType rangeType;
	SimpleFeatureType singleType;
	SimpleFeatureType geoType;
	SimpleFeatureType geoMercType;

	final TimeDescriptors geoTimeDescriptors = new TimeDescriptors();
	final TimeDescriptors rangeTimeDescriptors = new TimeDescriptors();
	final TimeDescriptors singleTimeDescriptors = new TimeDescriptors();

	final GeometryFactory factory = new GeometryFactory(
			new PrecisionModel(
					PrecisionModel.FIXED));

	Date startTime, endTime;

	Object[] singleDefaults, rangeDefaults, geoDefaults;

	MathTransform transform;

	@Before
	public void setup()
			throws SchemaException,
			ParseException,
			FactoryException {

		startTime = DateUtilities.parseISO("2005-05-15T20:32:56Z");
		endTime = DateUtilities.parseISO("2005-05-20T20:32:56Z");

		geoType = DataUtilities.createType(
				"geostuff",
				"geometry:Geometry:srid=4326,pop:java.lang.Long,pid:String");

		geoMercType = DataUtilities.createType(
				"geostuff",
				"geometry:Geometry:srid=3785,pop:java.lang.Long,pid:String");

		rangeType = DataUtilities.createType(
				"geostuff",
				"geometry:Geometry:srid=4326,start:Date,end:Date,pop:java.lang.Long,pid:String");

		singleType = DataUtilities.createType(
				"geostuff",
				"geometry:Geometry:srid=4326,when:Date,pop:java.lang.Long,pid:String");

		transform = CRS.findMathTransform(
				geoMercType.getCoordinateReferenceSystem(),
				geoType.getCoordinateReferenceSystem(),
				true);

		final TimeDescriptorConfiguration rangeConfig = new TimeDescriptorConfiguration();
		rangeConfig.configureFromType(rangeType);
		rangeTimeDescriptors.update(
				rangeType,
				rangeConfig);
		final TimeDescriptorConfiguration singleTimeConfig = new TimeDescriptorConfiguration();
		singleTimeConfig.configureFromType(singleType);
		singleTimeDescriptors.update(
				singleType,
				singleTimeConfig);

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
	public void testGetTemporalConstraintsForSingleClippedRange()
			throws ParseException {

		final Date stime = DateUtilities.parseISO("2005-05-14T20:32:56Z");
		final Date etime = DateUtilities.parseISO("2005-05-18T20:32:56Z");
		final Date stime1 = DateUtilities.parseISO("2005-05-18T20:32:56Z");
		final Date etime1 = DateUtilities.parseISO("2005-05-19T20:32:56Z");

		final Map<ByteArrayId, DataStatistics<SimpleFeature>> statsMap = new HashMap<>();
		final FeatureTimeRangeStatistics whenStats = new FeatureTimeRangeStatistics(
				dataAdapterId,
				"when");
		statsMap.put(
				FeatureTimeRangeStatistics.composeId("when"),
				whenStats);

		final TemporalConstraintsSet constraintsSet = new TemporalConstraintsSet();
		constraintsSet.getConstraintsFor(
				"when").add(
				new TemporalRange(
						stime,
						etime));

		final SimpleFeature notIntersectSingle1 = createSingleTimeFeature(startTime);

		whenStats.entryIngested(
				null,
				notIntersectSingle1);

		final SimpleFeature notIntersectSingle = createSingleTimeFeature(endTime);

		whenStats.entryIngested(
				null,
				notIntersectSingle);

		final TemporalConstraintsSet resultConstraintsSet = QueryIndexHelper.clipIndexedTemporalConstraints(
				statsMap,
				singleTimeDescriptors,
				constraintsSet);

		final TemporalConstraints constraints = resultConstraintsSet.getConstraintsFor("when");

		assertEquals(
				1,
				constraints.getRanges().size());
		assertEquals(
				startTime,
				constraints.getStartRange().getStartTime());
		assertEquals(
				etime,
				constraints.getStartRange().getEndTime());

		final TemporalConstraintsSet constraintsSet1 = new TemporalConstraintsSet();
		constraintsSet1.getConstraintsFor(
				"when").add(
				new TemporalRange(
						stime1,
						etime1));

		final TemporalConstraintsSet resultConstraintsSet1 = QueryIndexHelper.clipIndexedTemporalConstraints(
				statsMap,
				singleTimeDescriptors,
				constraintsSet1);

		final TemporalConstraints constraints1 = resultConstraintsSet1.getConstraintsFor("when");

		assertEquals(
				1,
				constraints1.getRanges().size());
		assertEquals(
				stime1,
				constraints1.getStartRange().getStartTime());
		assertEquals(
				etime1,
				constraints1.getStartRange().getEndTime());
	}

	@Test
	public void testGetTemporalConstraintsForRangeClippedFullRange()
			throws ParseException {

		final Map<ByteArrayId, DataStatistics<SimpleFeature>> statsMap = new HashMap<>();
		final FeatureTimeRangeStatistics startStats = new FeatureTimeRangeStatistics(
				dataAdapterId,
				"start");
		statsMap.put(
				FeatureTimeRangeStatistics.composeId("start"),
				startStats);

		final FeatureTimeRangeStatistics endStats = new FeatureTimeRangeStatistics(
				dataAdapterId,
				"end");
		statsMap.put(
				FeatureTimeRangeStatistics.composeId("end"),
				endStats);

		final Date statsStart1 = DateUtilities.parseISO("2005-05-18T20:32:56Z");
		final Date statsStart2 = DateUtilities.parseISO("2005-05-20T20:32:56Z");
		final Date statsEnd1 = DateUtilities.parseISO("2005-05-21T20:32:56Z");
		final Date statsEnd2 = DateUtilities.parseISO("2005-05-24T20:32:56Z");

		final SimpleFeature firstRangFeature = createFeature(
				statsStart1,
				statsEnd1);

		startStats.entryIngested(
				null,
				firstRangFeature);

		endStats.entryIngested(
				null,
				firstRangFeature);

		final SimpleFeature secondRangFeature = createFeature(
				statsStart2,
				statsEnd2);

		startStats.entryIngested(
				null,
				secondRangFeature);

		endStats.entryIngested(
				null,
				secondRangFeature);

		final Date stime = DateUtilities.parseISO("2005-05-18T20:32:56Z");
		final Date etime = DateUtilities.parseISO("2005-05-19T20:32:56Z");

		final TemporalConstraintsSet constraintsSet = new TemporalConstraintsSet();
		constraintsSet.getConstraintsForRange(
				"start",
				"end").add(
				new TemporalRange(
						new Date(
								0),
						etime));

		final TemporalConstraintsSet resultConstraintsSet = QueryIndexHelper.clipIndexedTemporalConstraints(
				statsMap,
				rangeTimeDescriptors,
				constraintsSet);

		final TemporalConstraints constraints = resultConstraintsSet.getConstraintsForRange(
				"start",
				"end");

		assertEquals(
				1,
				constraints.getRanges().size());
		assertEquals(
				stime,
				constraints.getStartRange().getStartTime());
		assertEquals(
				etime,
				constraints.getStartRange().getEndTime());
	}

	@Test
	public void testComposeQueryWithTimeRange()
			throws ParseException {

		final Map<ByteArrayId, DataStatistics<SimpleFeature>> statsMap = new HashMap<>();
		final FeatureTimeRangeStatistics startStats = new FeatureTimeRangeStatistics(
				dataAdapterId,
				"start");
		statsMap.put(
				FeatureTimeRangeStatistics.composeId("start"),
				startStats);

		final FeatureTimeRangeStatistics endStats = new FeatureTimeRangeStatistics(
				dataAdapterId,
				"end");
		statsMap.put(
				FeatureTimeRangeStatistics.composeId("end"),
				endStats);

		final Date statsStart1 = DateUtilities.parseISO("2005-05-18T20:32:56Z");
		final Date statsStart2 = DateUtilities.parseISO("2005-05-20T20:32:56Z");
		final Date statsEnd1 = DateUtilities.parseISO("2005-05-21T20:32:56Z");
		final Date statsEnd2 = DateUtilities.parseISO("2005-05-24T20:32:56Z");

		final SimpleFeature firstRangFeature = createFeature(
				statsStart1,
				statsEnd1);

		startStats.entryIngested(
				null,
				firstRangFeature);

		endStats.entryIngested(
				null,
				firstRangFeature);

		final SimpleFeature secondRangFeature = createFeature(
				statsStart2,
				statsEnd2);

		startStats.entryIngested(
				null,
				secondRangFeature);

		endStats.entryIngested(
				null,
				secondRangFeature);

		final Date stime = DateUtilities.parseISO("2005-05-18T20:32:56Z");
		final Date etime = DateUtilities.parseISO("2005-05-19T20:32:56Z");

		final TemporalConstraintsSet constraintsSet = new TemporalConstraintsSet();
		constraintsSet.getConstraintsForRange(
				"start",
				"end").add(
				new TemporalRange(
						stime,
						etime));

		final BasicQuery query = new BasicQuery(
				QueryIndexHelper.composeConstraints(
						rangeType,
						rangeTimeDescriptors,
						statsMap,
						factory.toGeometry(factory.createPoint(
								new Coordinate(
										27.25,
										41.25)).getEnvelopeInternal()),
						constraintsSet));

		final List<MultiDimensionalNumericData> nd = query.getIndexConstraints(SPATIAL_TEMPORAL_INDEX);
		assertEquals(
				stime.getTime(),
				(long) nd.get(
						0).getDataPerDimension()[2].getMin());
		assertEquals(
				etime.getTime(),
				(long) nd.get(
						0).getDataPerDimension()[2].getMax());

		final BasicQuery query1 = new BasicQuery(
				QueryIndexHelper.composeConstraints(
						rangeType,
						rangeTimeDescriptors,
						statsMap,
						factory.toGeometry(factory.createPoint(
								new Coordinate(
										27.25,
										41.25)).getEnvelopeInternal()),
						null));

		final List<MultiDimensionalNumericData> nd1 = query1.getIndexConstraints(SPATIAL_TEMPORAL_INDEX);
		assertEquals(
				statsStart1.getTime(),
				(long) nd1.get(
						0).getDataPerDimension()[2].getMin());
		assertEquals(
				statsEnd2.getTime(),
				(long) nd1.get(
						0).getDataPerDimension()[2].getMax());
	}

	@Test
	public void testComposeQueryWithOutTimeRange() {
		final Map<ByteArrayId, DataStatistics<SimpleFeature>> statsMap = new HashMap<>();
		final FeatureBoundingBoxStatistics geoStats = new FeatureBoundingBoxStatistics(
				dataAdapterId,
				"geometry");
		statsMap.put(
				FeatureBoundingBoxStatistics.composeId("geometry"),
				geoStats);

		final SimpleFeature firstFeature = createGeoFeature(factory.createPoint(new Coordinate(
				22.25,
				42.25)));

		geoStats.entryIngested(
				null,
				firstFeature);

		final SimpleFeature secondFeature = createGeoFeature(factory.createPoint(new Coordinate(
				27.25,
				41.25)));

		geoStats.entryIngested(
				null,
				secondFeature);

		final Envelope bounds = new Envelope(
				21.23,
				26.23,
				41.75,
				43.1);

		final BasicQuery query = new BasicQuery(
				QueryIndexHelper.composeConstraints(
						geoType,
						geoTimeDescriptors,
						statsMap,
						new GeometryFactory().toGeometry(bounds),
						null));

		final List<MultiDimensionalNumericData> nd = query.getIndexConstraints(SPATIAL_INDEX);
		assertEquals(
				21.23,
				nd.get(
						0).getDataPerDimension()[0].getMin(),
				0.0001);
		assertEquals(
				26.23,
				nd.get(
						0).getDataPerDimension()[0].getMax(),
				0.0001);
		assertEquals(
				41.75,
				nd.get(
						0).getDataPerDimension()[1].getMin(),
				0.0001);
		assertEquals(
				43.1,
				nd.get(
						0).getDataPerDimension()[1].getMax(),
				0.0001);

	}

	@Test
	public void testGetBBOX() {
		final Map<ByteArrayId, DataStatistics<SimpleFeature>> statsMap = new HashMap<>();
		final FeatureBoundingBoxStatistics geoStats = new FeatureBoundingBoxStatistics(
				dataAdapterId,
				"geometry");
		statsMap.put(
				FeatureBoundingBoxStatistics.composeId("geometry"),
				geoStats);

		final SimpleFeature firstFeature = createGeoFeature(factory.createPoint(new Coordinate(
				22.25,
				42.25)));

		geoStats.entryIngested(
				null,
				firstFeature);

		final SimpleFeature secondFeature = createGeoFeature(factory.createPoint(new Coordinate(
				27.25,
				41.25)));

		geoStats.entryIngested(
				null,
				secondFeature);

		final Envelope bounds = new Envelope(
				21.23,
				26.23,
				41.75,
				43.1);

		final Geometry bbox = QueryIndexHelper.clipIndexedBBOXConstraints(
				geoType,
				new GeometryFactory().toGeometry(bounds),
				statsMap);

		final Envelope env = bbox.getEnvelopeInternal();

		assertEquals(
				22.25,
				env.getMinX(),
				0.0001);
		assertEquals(
				26.23,
				env.getMaxX(),
				0.0001);
		assertEquals(
				41.75,
				env.getMinY(),
				0.0001);
		assertEquals(
				42.25,
				env.getMaxY(),
				0.0001);

	}

	@Test
	public void testBBOXStatReprojection() {

		// create a EPSG:3785 feature (units in meters)
		final SimpleFeature mercFeat = createGeoMercFeature(factory.createPoint(new Coordinate(
				19971868.8804,
				20037508.3428)));

		// convert from EPSG:3785 to EPSG:4326 (convert to degrees lon/lat)
		// approximately 180.0, 85.0
		final SimpleFeature defaultCRSFeat = FeatureDataUtils.crsTransform(
				mercFeat,
				geoType,
				transform);

		final FeatureBoundingBoxStatistics geoStats = new FeatureBoundingBoxStatistics(
				dataAdapterId,
				"geometry",
				geoType,
				transform);

		geoStats.entryIngested(
				null,
				mercFeat);

		final Coordinate coord = ((Point) defaultCRSFeat.getDefaultGeometry()).getCoordinate();

		// coordinate should match reprojected feature
		assertEquals(
				coord.x,
				geoStats.getMinX(),
				0.0001);
		assertEquals(
				coord.x,
				geoStats.getMaxX(),
				0.0001);
		assertEquals(
				coord.y,
				geoStats.getMinY(),
				0.0001);
		assertEquals(
				coord.y,
				geoStats.getMaxY(),
				0.0001);

	}

	private SimpleFeature createGeoFeature(
			final Geometry geo ) {
		final SimpleFeature instance = SimpleFeatureBuilder.build(
				geoType,
				geoDefaults,
				UUID.randomUUID().toString());
		instance.setAttribute(
				"pop",
				Long.valueOf(100));
		instance.setAttribute(
				"pid",
				UUID.randomUUID().toString());
		instance.setAttribute(
				"geometry",
				geo);
		return instance;
	}

	private SimpleFeature createGeoMercFeature(
			final Geometry geo ) {
		final SimpleFeature instance = SimpleFeatureBuilder.build(
				geoMercType,
				geoDefaults,
				UUID.randomUUID().toString());
		instance.setAttribute(
				"pop",
				Long.valueOf(100));
		instance.setAttribute(
				"pid",
				UUID.randomUUID().toString());
		instance.setAttribute(
				"geometry",
				geo);
		return instance;
	}

	private SimpleFeature createSingleTimeFeature(
			final Date time ) {
		final SimpleFeature instance = SimpleFeatureBuilder.build(
				singleType,
				singleDefaults,
				UUID.randomUUID().toString());
		instance.setAttribute(
				"pop",
				Long.valueOf(100));
		instance.setAttribute(
				"pid",
				UUID.randomUUID().toString());
		instance.setAttribute(
				"when",
				time);
		instance.setAttribute(
				"geometry",
				factory.createPoint(new Coordinate(
						27.25,
						41.25)));
		return instance;
	}

	@Test
	public void testComposeSubsetConstraints()
			throws ParseException {

		final Map<ByteArrayId, DataStatistics<SimpleFeature>> statsMap = new HashMap<>();
		final FeatureTimeRangeStatistics startStats = new FeatureTimeRangeStatistics(
				dataAdapterId,
				"start");
		statsMap.put(
				FeatureTimeRangeStatistics.composeId("start"),
				startStats);

		final FeatureTimeRangeStatistics endStats = new FeatureTimeRangeStatistics(
				dataAdapterId,
				"end");
		statsMap.put(
				FeatureTimeRangeStatistics.composeId("end"),
				endStats);

		final Date statsStart1 = DateUtilities.parseISO("2005-05-18T20:32:56Z");
		final Date statsStart2 = DateUtilities.parseISO("2005-05-20T20:32:56Z");
		final Date statsEnd1 = DateUtilities.parseISO("2005-05-21T20:32:56Z");
		final Date statsEnd2 = DateUtilities.parseISO("2005-05-24T20:32:56Z");

		final SimpleFeature firstRangFeature = createFeature(
				statsStart1,
				statsEnd1);

		startStats.entryIngested(
				null,
				firstRangFeature);

		endStats.entryIngested(
				null,
				firstRangFeature);

		final SimpleFeature secondRangFeature = createFeature(
				statsStart2,
				statsEnd2);

		startStats.entryIngested(
				null,
				secondRangFeature);

		endStats.entryIngested(
				null,
				secondRangFeature);

		final Date stime = DateUtilities.parseISO("2005-05-18T20:32:56Z");
		final Date etime = DateUtilities.parseISO("2005-05-19T20:32:56Z");

		final TemporalConstraintsSet constraintsSet = new TemporalConstraintsSet();
		constraintsSet.getConstraintsForRange(
				"start",
				"end").add(
				new TemporalRange(
						stime,
						etime));

		final Constraints constraints = QueryIndexHelper.composeTimeBoundedConstraints(
				rangeType,
				rangeTimeDescriptors,
				statsMap,
				constraintsSet);
		final List<MultiDimensionalNumericData> nd = constraints.getIndexConstraints(SPATIAL_TEMPORAL_INDEX
				.getIndexStrategy());
		assertTrue(nd.isEmpty());

		final FeatureBoundingBoxStatistics geoStats = new FeatureBoundingBoxStatistics(
				dataAdapterId,
				"geometry");
		statsMap.put(
				FeatureBoundingBoxStatistics.composeId("geometry"),
				geoStats);

		final SimpleFeature firstFeature = createGeoFeature(factory.createPoint(new Coordinate(
				22.25,
				42.25)));

		geoStats.entryIngested(
				null,
				firstFeature);

		final SimpleFeature secondFeature = createGeoFeature(factory.createPoint(new Coordinate(
				27.25,
				41.25)));
		geoStats.entryIngested(
				null,
				secondFeature);

		final Constraints constraints1 = QueryIndexHelper.composeConstraints(
				rangeType,
				rangeTimeDescriptors,
				statsMap,
				null,
				constraintsSet);
		final List<MultiDimensionalNumericData> nd1 = constraints1.getIndexConstraints(SPATIAL_TEMPORAL_INDEX
				.getIndexStrategy());
		assertTrue(nd1.isEmpty());
		/*
		 * assertEquals( stime.getTime(), (long) nd1.get(
		 * 0).getDataPerDimension()[2].getMin()); assertEquals( etime.getTime(),
		 * (long) nd1.get( 0).getDataPerDimension()[2].getMax());
		 */

		final TemporalConstraintsSet constraintsSet2 = new TemporalConstraintsSet();
		constraintsSet2.getConstraintsForRange(
				"start",
				"end").add(
				new TemporalRange(
						statsStart1,
						statsEnd2));
		final Constraints constraints2 = QueryIndexHelper.composeTimeBoundedConstraints(
				rangeType,
				rangeTimeDescriptors,
				statsMap,
				constraintsSet2);
		final List<MultiDimensionalNumericData> nd2 = constraints2.getIndexConstraints(SPATIAL_TEMPORAL_INDEX
				.getIndexStrategy());
		assertTrue(nd2.isEmpty());
	}

	private SimpleFeature createFeature(
			final Date sTime,
			final Date eTime ) {
		final SimpleFeature instance = SimpleFeatureBuilder.build(
				rangeType,
				rangeDefaults,
				UUID.randomUUID().toString());
		instance.setAttribute(
				"pop",
				Long.valueOf(100));
		instance.setAttribute(
				"pid",
				UUID.randomUUID().toString());
		instance.setAttribute(
				"start",
				sTime);
		instance.setAttribute(
				"end",
				eTime);
		instance.setAttribute(
				"geometry",
				factory.createPoint(new Coordinate(
						27.25,
						41.25)));
		return instance;
	}
}
