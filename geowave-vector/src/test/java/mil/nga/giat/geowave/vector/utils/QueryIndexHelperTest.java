package mil.nga.giat.geowave.vector.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.store.index.IndexType;
import mil.nga.giat.geowave.store.query.BasicQuery;
import mil.nga.giat.geowave.store.query.BasicQuery.Constraints;
import mil.nga.giat.geowave.store.query.TemporalConstraints;
import mil.nga.giat.geowave.store.query.TemporalConstraintsSet;
import mil.nga.giat.geowave.store.query.TemporalRange;
import mil.nga.giat.geowave.vector.stats.FeatureBoundingBoxStatistics;
import mil.nga.giat.geowave.vector.stats.FeatureTimeRangeStatistics;
import mil.nga.giat.geowave.vector.util.QueryIndexHelper;

import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;

public class QueryIndexHelperTest
{
	final ByteArrayId dataAdapterId = new ByteArrayId(
			"123");

	SimpleFeatureType rangeType;
	SimpleFeatureType singleType;
	SimpleFeatureType geoType;

	final TimeDescriptors geoTimeDescriptors = new TimeDescriptors();
	final TimeDescriptors rangeTimeDescriptors = new TimeDescriptors();
	final TimeDescriptors singleTimeDescriptors = new TimeDescriptors();

	final GeometryFactory factory = new GeometryFactory(
			new PrecisionModel(
					PrecisionModel.FIXED));

	Date startTime, endTime;

	Object[] singleDefaults, rangeDefaults, geoDefaults;

	@Before
	public void setup()
			throws SchemaException,
			ParseException {

		startTime = DateUtilities.parseISO("2005-05-15T20:32:56Z");
		endTime = DateUtilities.parseISO("2005-05-20T20:32:56Z");

		geoType = DataUtilities.createType(
				"geostuff",
				"geometry:Geometry:srid=4326,pop:java.lang.Long,pid:String");

		rangeType = DataUtilities.createType(
				"geostuff",
				"geometry:Geometry:srid=4326,start:Date,end:Date,pop:java.lang.Long,pid:String");

		singleType = DataUtilities.createType(
				"geostuff",
				"geometry:Geometry:srid=4326,when:Date,pop:java.lang.Long,pid:String");

		rangeTimeDescriptors.inferType(rangeType);
		singleTimeDescriptors.inferType(singleType);

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

		final Map<ByteArrayId, DataStatistics<SimpleFeature>> statsMap = new HashMap<ByteArrayId, DataStatistics<SimpleFeature>>();
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

		final Map<ByteArrayId, DataStatistics<SimpleFeature>> statsMap = new HashMap<ByteArrayId, DataStatistics<SimpleFeature>>();
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
		constraintsSet.getConstraintsFor(
				"start").add(
				new TemporalRange(
						new Date(
								0),
						etime));
		constraintsSet.getConstraintsFor(
				"end").add(
				new TemporalRange(
						etime,
						new Date(
								Long.MAX_VALUE)));

		final TemporalConstraintsSet resultConstraintsSet = QueryIndexHelper.clipIndexedTemporalConstraints(
				statsMap,
				rangeTimeDescriptors,
				constraintsSet);

		final TemporalConstraints constraints = resultConstraintsSet.getConstraintsFor("start_end");

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

		final Map<ByteArrayId, DataStatistics<SimpleFeature>> statsMap = new HashMap<ByteArrayId, DataStatistics<SimpleFeature>>();
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
		constraintsSet.getConstraintsFor(
				"start_end").add(
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

		final MultiDimensionalNumericData nd = query.getIndexConstraints(IndexType.SPATIAL_TEMPORAL_VECTOR.createDefaultIndexStrategy());
		assertEquals(
				stime.getTime(),
				(long) nd.getDataPerDimension()[2].getMin());
		assertEquals(
				etime.getTime(),
				(long) nd.getDataPerDimension()[2].getMax());

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

		final MultiDimensionalNumericData nd1 = query1.getIndexConstraints(IndexType.SPATIAL_TEMPORAL_VECTOR.createDefaultIndexStrategy());
		assertEquals(
				statsStart1.getTime(),
				(long) nd1.getDataPerDimension()[2].getMin());
		assertEquals(
				statsEnd2.getTime(),
				(long) nd1.getDataPerDimension()[2].getMax());
	}

	@Test
	public void testComposeQueryWithOutTimeRange() {
		final Map<ByteArrayId, DataStatistics<SimpleFeature>> statsMap = new HashMap<ByteArrayId, DataStatistics<SimpleFeature>>();
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

		final MultiDimensionalNumericData nd = query.getIndexConstraints(IndexType.SPATIAL_VECTOR.createDefaultIndexStrategy());
		assertEquals(
				21.23,
				nd.getDataPerDimension()[0].getMin(),
				0.0001);
		assertEquals(
				26.23,
				nd.getDataPerDimension()[0].getMax(),
				0.0001);
		assertEquals(
				41.75,
				nd.getDataPerDimension()[1].getMin(),
				0.0001);
		assertEquals(
				43.1,
				nd.getDataPerDimension()[1].getMax(),
				0.0001);

		final BasicQuery query1 = new BasicQuery(
				QueryIndexHelper.composeConstraints(
						geoType,
						geoTimeDescriptors,
						statsMap,
						null,
						null));

		final MultiDimensionalNumericData nd1 = query1.getIndexConstraints(IndexType.SPATIAL_VECTOR.createDefaultIndexStrategy());
		assertEquals(
				22.25,
				nd1.getDataPerDimension()[0].getMin(),
				0.0001);
		assertEquals(
				27.25,
				nd1.getDataPerDimension()[0].getMax(),
				0.0001);
		assertEquals(
				41.25,
				nd1.getDataPerDimension()[1].getMin(),
				0.0001);
		assertEquals(
				42.25,
				nd1.getDataPerDimension()[1].getMax(),
				0.0001);

	}

	@Test
	public void testGetBBOX() {
		final Map<ByteArrayId, DataStatistics<SimpleFeature>> statsMap = new HashMap<ByteArrayId, DataStatistics<SimpleFeature>>();
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

		final Map<ByteArrayId, DataStatistics<SimpleFeature>> statsMap = new HashMap<ByteArrayId, DataStatistics<SimpleFeature>>();
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
		constraintsSet.getConstraintsFor(
				"start_end").add(
				new TemporalRange(
						stime,
						etime));

		final Constraints constraints = QueryIndexHelper.composeTimeBoundedConstraints(
				rangeType,
				rangeTimeDescriptors,
				statsMap,
				constraintsSet);
		final MultiDimensionalNumericData nd = constraints.getIndexConstraints(IndexType.SPATIAL_TEMPORAL_VECTOR.createDefaultIndexStrategy());
		assertEquals(
				stime.getTime(),
				(long) nd.getDataPerDimension()[2].getMin());
		assertEquals(
				etime.getTime(),
				(long) nd.getDataPerDimension()[2].getMax());

		final TemporalConstraintsSet constraintsSet2 = new TemporalConstraintsSet();
		constraintsSet2.getConstraintsFor(
				"start_end").add(
				new TemporalRange(
						statsStart1,
						statsEnd2));
		final Constraints constraints2 = QueryIndexHelper.composeTimeBoundedConstraints(
				rangeType,
				rangeTimeDescriptors,
				statsMap,
				constraintsSet2);
		final MultiDimensionalNumericData nd2 = constraints2.getIndexConstraints(IndexType.SPATIAL_TEMPORAL_VECTOR.createDefaultIndexStrategy());
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
