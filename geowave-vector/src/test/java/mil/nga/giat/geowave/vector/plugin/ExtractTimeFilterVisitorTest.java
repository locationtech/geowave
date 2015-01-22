package mil.nga.giat.geowave.vector.plugin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;
import java.util.Date;

import mil.nga.giat.geowave.store.query.TemporalConstraints;
import mil.nga.giat.geowave.store.query.TemporalRange;
import mil.nga.giat.geowave.vector.utils.DateUtilities;
import mil.nga.giat.geowave.vector.utils.TimeDescriptors;

import org.geotools.data.Query;
import org.geotools.feature.NameImpl;
import org.geotools.feature.type.AttributeDescriptorImpl;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.geotools.gml3.v3_2.gco.GCOSchema;
import org.junit.Test;
import org.opengis.filter.Filter;

public class ExtractTimeFilterVisitorTest
{
	private final TimeDescriptors TIMEDESCRIPTOR = new TimeDescriptors(
			new AttributeDescriptorImpl(
					GCOSchema.DATE_PROPERTYTYPE_TYPE,
					new NameImpl(
							"when"),
					100,
					485903457,
					false,
					new Date()));

	@Test
	public void testAfter()
			throws CQLException,
			ParseException {
		final ExtractTimeFilterVisitor visitor = new ExtractTimeFilterVisitor();
		final Date time = DateUtilities.parseISO("2005-05-19T20:32:56Z");
		Filter filter = CQL.toFilter("when after 2005-05-19T20:32:56Z");
		Query query = new Query(
				"type",
				filter);
		TemporalConstraints range = (TemporalConstraints) query.getFilter().accept(
				visitor,
				null);
		assertNotNull(range);
		assertEquals(
				time,
				range.getStartRange().getStartTime());

		final ExtractTimeFilterVisitor visitorWithDescriptor = new ExtractTimeFilterVisitor(
				TIMEDESCRIPTOR);

		range = (TemporalConstraints) query.getFilter().accept(
				visitorWithDescriptor,
				null);
		assertNotNull(range);
		assertEquals(
				time,
				range.getStartRange().getStartTime());

		filter = CQL.toFilter("sometime after 2005-05-19T20:32:56Z");
		query = new Query(
				"type",
				filter);
		range = (TemporalConstraints) query.getFilter().accept(
				visitorWithDescriptor,
				null);
		assertNotNull(range);
		assertTrue(range.isEmpty());
	}

	@Test
	public void testGreaterThan()
			throws CQLException,
			ParseException {
		final ExtractTimeFilterVisitor visitor = new ExtractTimeFilterVisitor();
		final Date stime = DateUtilities.parseISO("2005-05-19T20:32:56Z");
		final Date etime = DateUtilities.parseISO("2005-05-19T21:32:56Z");
		Filter filter = ECQL.toFilter("when > 2005-05-19T20:32:56Z");
		Query query = new Query(
				"type",
				filter);
		TemporalConstraints range = (TemporalConstraints) query.getFilter().accept(
				visitor,
				null);
		assertNotNull(range);
		assertEquals(
				stime,
				range.getStartRange().getStartTime());
		assertEquals(
				TemporalRange.END_TIME,
				range.getEndRange().getEndTime());

		final ExtractTimeFilterVisitor visitorWithDescriptor = new ExtractTimeFilterVisitor(
				TIMEDESCRIPTOR);
		filter = ECQL.toFilter("sometime > 2005-05-19T20:32:56Z");
		query = new Query(
				"type",
				filter);
		range = (TemporalConstraints) query.getFilter().accept(
				visitorWithDescriptor,
				null);
		assertNotNull(range);
		assertTrue(range.isEmpty());

	}

	@Test
	public void testLessThan()
			throws CQLException,
			ParseException {
		final ExtractTimeFilterVisitor visitor = new ExtractTimeFilterVisitor();
		final Date etime = DateUtilities.parseISO("2005-05-19T21:32:56Z");
		Filter filter = ECQL.toFilter("when < 2005-05-19T21:32:56Z");
		Query query = new Query(
				"type",
				filter);
		TemporalConstraints range = (TemporalConstraints) query.getFilter().accept(
				visitor,
				null);
		assertNotNull(range);
		assertEquals(
				TemporalRange.START_TIME,
				range.getStartRange().getStartTime());
		assertEquals(
				etime,
				range.getEndRange().getEndTime());

		final ExtractTimeFilterVisitor visitorWithDescriptor = new ExtractTimeFilterVisitor(
				TIMEDESCRIPTOR);
		filter = ECQL.toFilter("sometime < 2005-05-19T20:32:56Z");
		query = new Query(
				"type",
				filter);
		range = (TemporalConstraints) query.getFilter().accept(
				visitorWithDescriptor,
				null);
		assertNotNull(range);
		assertTrue(range.isEmpty());

	}

	@Test
	public void testLessAndGreaterThan()
			throws CQLException,
			ParseException {
		final ExtractTimeFilterVisitor visitor = new ExtractTimeFilterVisitor();
		final Date etime = DateUtilities.parseISO("2005-05-19T21:32:56Z");
		final Date stime = DateUtilities.parseISO("2005-05-19T20:32:56Z");
		Filter filter = ECQL.toFilter("when > 2005-05-19T21:32:56Z and when < 2005-05-19T20:32:56Z");
		Query query = new Query(
				"type",
				filter);
		TemporalConstraints range = (TemporalConstraints) query.getFilter().accept(
				visitor,
				null);
		assertNotNull(range);
		assertEquals(
				TemporalRange.START_TIME,
				range.getStartRange().getStartTime());
		assertEquals(
				TemporalRange.END_TIME,
				range.getEndRange().getEndTime());

		filter = ECQL.toFilter("when < 2005-05-19T21:32:56Z and when > 2005-05-19T20:32:56Z");
		query = new Query(
				"type",
				filter);
		range = (TemporalConstraints) query.getFilter().accept(
				visitor,
				null);
		assertNotNull(range);
		assertEquals(
				stime,
				range.getStartRange().getStartTime());
		assertEquals(
				etime,
				range.getEndRange().getEndTime());

		final ExtractTimeFilterVisitor visitorWithDescriptor = new ExtractTimeFilterVisitor(
				TIMEDESCRIPTOR);
		filter = ECQL.toFilter("sometime < 2005-05-19T20:32:56Z and when > 2005-05-19T20:32:56Z");
		query = new Query(
				"type",
				filter);
		range = (TemporalConstraints) query.getFilter().accept(
				visitorWithDescriptor,
				null);
		assertEquals(
				stime,
				range.getStartRange().getStartTime());
		assertEquals(
				TemporalRange.END_TIME,
				range.getEndRange().getEndTime());

		filter = ECQL.toFilter("when < 2005-05-19T20:32:56Z and sometime > 2005-05-19T20:32:56Z");
		query = new Query(
				"type",
				filter);
		range = (TemporalConstraints) query.getFilter().accept(
				visitorWithDescriptor,
				null);
		assertEquals(
				TemporalRange.START_TIME,
				range.getStartRange().getStartTime());
		assertEquals(
				stime,
				range.getEndRange().getEndTime());

		filter = ECQL.toFilter("sometime < 2005-05-19T20:32:56Z and sometime > 2005-05-19T20:32:56Z");
		query = new Query(
				"type",
				filter);
		range = (TemporalConstraints) query.getFilter().accept(
				visitorWithDescriptor,
				null);

		assertNotNull(range);
		assertTrue(range.isEmpty());

	}

	@Test
	public void testEqual()
			throws CQLException,
			ParseException {
		final ExtractTimeFilterVisitor visitor = new ExtractTimeFilterVisitor();
		final Date etime = DateUtilities.parseISO("2005-05-19T21:32:56Z");
		Filter filter = ECQL.toFilter("when = 2005-05-19T21:32:56Z");
		Query query = new Query(
				"type",
				filter);
		TemporalConstraints range = (TemporalConstraints) query.getFilter().accept(
				visitor,
				null);
		assertNotNull(range);
		assertEquals(
				etime,
				range.getStartRange().getStartTime());
		assertEquals(
				etime,
				range.getEndRange().getEndTime());

		final ExtractTimeFilterVisitor visitorWithDescriptor = new ExtractTimeFilterVisitor(
				TIMEDESCRIPTOR);
		filter = ECQL.toFilter("sometime = 2005-05-19T20:32:56Z");
		query = new Query(
				"type",
				filter);
		range = (TemporalConstraints) query.getFilter().accept(
				visitorWithDescriptor,
				null);
		assertNotNull(range);
		assertTrue(range.isEmpty());

	}

	@Test
	public void testDuring()
			throws CQLException,
			ParseException {
		final ExtractTimeFilterVisitor visitor = new ExtractTimeFilterVisitor();
		final Date stime = DateUtilities.parseISO("2005-05-19T20:32:56Z");
		final Date etime = DateUtilities.parseISO("2005-05-19T21:32:56Z");
		Filter filter = CQL.toFilter("when during 2005-05-19T20:32:56Z/2005-05-19T21:32:56Z");
		Query query = new Query(
				"type",
				filter);
		TemporalConstraints range = (TemporalConstraints) query.getFilter().accept(
				visitor,
				null);
		assertNotNull(range);
		assertEquals(
				stime,
				range.getStartRange().getStartTime());
		assertEquals(
				etime,
				range.getStartRange().getEndTime());

		final ExtractTimeFilterVisitor visitorWithDescriptor = new ExtractTimeFilterVisitor(
				TIMEDESCRIPTOR);

		range = (TemporalConstraints) query.getFilter().accept(
				visitor,
				null);
		assertNotNull(range);
		assertEquals(
				stime,
				range.getStartRange().getStartTime());
		assertEquals(
				etime,
				range.getStartRange().getEndTime());

		filter = CQL.toFilter("sometime during 2005-05-19T20:32:56Z/2005-05-19T21:32:56Z");
		query = new Query(
				"type",
				filter);
		range = (TemporalConstraints) query.getFilter().accept(
				visitorWithDescriptor,
				null);
		assertNotNull(range);
		assertTrue(range.isEmpty());
	}

	@Test
	public void testBefore()
			throws CQLException,
			ParseException {
		final ExtractTimeFilterVisitor visitor = new ExtractTimeFilterVisitor();
		final Date etime = DateUtilities.parseISO("2005-05-19T20:32:56Z");
		Filter filter = CQL.toFilter("when before 2005-05-19T20:32:56Z");
		Query query = new Query(
				"type",
				filter);
		TemporalConstraints range = (TemporalConstraints) query.getFilter().accept(
				visitor,
				null);
		assertNotNull(range);
		assertEquals(
				TemporalRange.START_TIME,
				range.getStartRange().getStartTime());
		assertEquals(
				etime,
				range.getStartRange().getEndTime());

		final ExtractTimeFilterVisitor visitorWithDescriptor = new ExtractTimeFilterVisitor(
				TIMEDESCRIPTOR);
		range = (TemporalConstraints) query.getFilter().accept(
				visitorWithDescriptor,
				null);
		assertNotNull(range);
		assertEquals(
				TemporalRange.START_TIME,
				range.getStartRange().getStartTime());
		assertEquals(
				etime,
				range.getStartRange().getEndTime());

		filter = CQL.toFilter("sometime before 2005-05-19T20:32:56Z");
		query = new Query(
				"type",
				filter);
		range = (TemporalConstraints) query.getFilter().accept(
				visitorWithDescriptor,
				null);
		assertNotNull(range);
		assertTrue(range.isEmpty());
	}

	@Test
	public void testBeforeOrDuring()
			throws CQLException,
			ParseException {
		final ExtractTimeFilterVisitor visitor = new ExtractTimeFilterVisitor();
		final Date stime = DateUtilities.parseISO("2005-05-19T21:32:56Z");
		Filter filter = CQL.toFilter("when BEFORE OR DURING 2005-05-19T20:32:56Z/2005-05-19T21:32:56Z");
		Query query = new Query(
				"type",
				filter);
		TemporalConstraints range = (TemporalConstraints) query.getFilter().accept(
				visitor,
				null);
		assertNotNull(range);
		assertEquals(
				TemporalRange.START_TIME,
				range.getStartRange().getStartTime());
		assertEquals(
				stime,
				range.getStartRange().getEndTime());

		final ExtractTimeFilterVisitor visitorWithDescriptor = new ExtractTimeFilterVisitor(
				TIMEDESCRIPTOR);
		range = (TemporalConstraints) query.getFilter().accept(
				visitorWithDescriptor,
				null);
		assertNotNull(range);
		assertEquals(
				TemporalRange.START_TIME,
				range.getStartRange().getStartTime());
		assertEquals(
				stime,
				range.getStartRange().getEndTime());

		filter = CQL.toFilter("sometime BEFORE OR DURING 2005-05-19T20:32:56Z/2005-05-19T21:32:56Z");
		query = new Query(
				"type",
				filter);
		range = (TemporalConstraints) query.getFilter().accept(
				visitorWithDescriptor,
				null);
		assertNotNull(range);
		assertTrue(range.isEmpty());
	}

	@Test
	public void testDuringOrAfter()
			throws CQLException,
			ParseException {
		final ExtractTimeFilterVisitor visitor = new ExtractTimeFilterVisitor();
		final Date stime = DateUtilities.parseISO("2005-05-19T20:32:56Z");
		final Date etime = DateUtilities.parseISO("2005-05-19T21:32:56Z");
		Filter filter = CQL.toFilter("when DURING OR AFTER 2005-05-19T20:32:56Z/2005-05-19T21:32:56Z");
		Query query = new Query(
				"type",
				filter);
		TemporalConstraints range = (TemporalConstraints) query.getFilter().accept(
				visitor,
				null);
		assertNotNull(range);
		assertEquals(
				stime,
				range.getStartRange().getStartTime());
		assertEquals(
				TemporalRange.END_TIME,
				range.getStartRange().getEndTime());

		final ExtractTimeFilterVisitor visitorWithDescriptor = new ExtractTimeFilterVisitor(
				TIMEDESCRIPTOR);

		range = (TemporalConstraints) query.getFilter().accept(
				visitorWithDescriptor,
				null);
		assertNotNull(range);
		assertEquals(
				stime,
				range.getStartRange().getStartTime());
		assertEquals(
				TemporalRange.END_TIME,
				range.getStartRange().getEndTime());

		filter = CQL.toFilter("sometime DURING OR AFTER 2005-05-19T20:32:56Z/2005-05-19T21:32:56Z");
		query = new Query(
				"type",
				filter);
		range = (TemporalConstraints) query.getFilter().accept(
				visitorWithDescriptor,
				null);
		assertTrue(range.isEmpty());

	}

	@Test
	public void testAndOverlap()
			throws CQLException,
			ParseException {
		final ExtractTimeFilterVisitor visitor = new ExtractTimeFilterVisitor();
		final Date sTime = DateUtilities.parseISO("2005-05-19T20:32:56Z");
		final Date eTime = DateUtilities.parseISO("2005-05-20T20:32:56Z");
		Filter filter = CQL.toFilter("when before 2005-05-20T20:32:56Z and when after 2005-05-19T20:32:56Z");
		Query query = new Query(
				"type",
				filter);
		TemporalConstraints range = (TemporalConstraints) query.getFilter().accept(
				visitor,
				null);
		assertNotNull(range);
		assertEquals(
				sTime,
				range.getStartRange().getStartTime());
		assertEquals(
				eTime,
				range.getStartRange().getEndTime());

		final ExtractTimeFilterVisitor visitorWithDescriptor = new ExtractTimeFilterVisitor(
				TIMEDESCRIPTOR);

		range = (TemporalConstraints) query.getFilter().accept(
				visitorWithDescriptor,
				null);
		assertNotNull(range);
		assertEquals(
				sTime,
				range.getStartRange().getStartTime());
		assertEquals(
				eTime,
				range.getStartRange().getEndTime());

		filter = CQL.toFilter("sometime before 2005-05-20T20:32:56Z and when after 2005-05-19T20:32:56Z");
		query = new Query(
				"type",
				filter);
		range = (TemporalConstraints) query.getFilter().accept(
				visitorWithDescriptor,
				null);
		assertNotNull(range);
		assertEquals(
				sTime,
				range.getStartRange().getStartTime());
		assertEquals(
				TemporalRange.END_TIME,
				range.getStartRange().getEndTime());

		filter = CQL.toFilter("when before 2005-05-20T20:32:56Z and sometime after 2005-05-19T20:32:56Z");
		query = new Query(
				"type",
				filter);
		range = (TemporalConstraints) query.getFilter().accept(
				visitorWithDescriptor,
				null);
		assertNotNull(range);
		assertEquals(
				TemporalRange.START_TIME,
				range.getStartRange().getStartTime());
		assertEquals(
				eTime,
				range.getStartRange().getEndTime());

		filter = CQL.toFilter("sometime before 2005-05-20T20:32:56Z and sometime after 2005-05-19T20:32:56Z");
		query = new Query(
				"type",
				filter);
		range = (TemporalConstraints) query.getFilter().accept(
				visitorWithDescriptor,
				null);
		assertNotNull(range);
		assertTrue(range.isEmpty());
	}

	@Test
	public void testAndNoOverlap()
			throws CQLException,
			ParseException {
		final ExtractTimeFilterVisitor visitor = new ExtractTimeFilterVisitor();
		final Filter filter = CQL.toFilter("when before 2005-05-17T20:32:56Z and when after 2005-05-19T20:32:56Z");
		final Query query = new Query(
				"type",
				filter);
		final TemporalConstraints range = (TemporalConstraints) query.getFilter().accept(
				visitor,
				null);
		assertNotNull(range);
		assertTrue(range.isEmpty());
	}

	@Test
	public void testOr()
			throws CQLException,
			ParseException {
		final ExtractTimeFilterVisitor visitor = new ExtractTimeFilterVisitor();
		final Date sTime2 = DateUtilities.parseISO("2005-05-19T20:32:56Z");
		final Date eTime1 = DateUtilities.parseISO("2005-05-17T20:32:56Z");
		Filter filter = CQL.toFilter("when before 2005-05-17T20:32:56Z or when after 2005-05-19T20:32:56Z");
		Query query = new Query(
				"type",
				filter);
		TemporalConstraints range = (TemporalConstraints) query.getFilter().accept(
				visitor,
				null);
		assertNotNull(range);
		assertEquals(
				eTime1,
				range.getStartRange().getEndTime());
		assertEquals(
				sTime2,
				range.getRanges().get(
						1).getStartTime());

		final ExtractTimeFilterVisitor visitorWithDescriptor = new ExtractTimeFilterVisitor(
				TIMEDESCRIPTOR);
		filter = CQL.toFilter("when before 2005-05-17T20:32:56Z or sometime after 2005-05-19T20:32:56Z");
		query = new Query(
				"type",
				filter);
		range = (TemporalConstraints) query.getFilter().accept(
				visitorWithDescriptor,
				null);
		assertNotNull(range);
		assertEquals(
				eTime1,
				range.getStartRange().getEndTime());
		assertEquals(
				TemporalRange.START_TIME,
				range.getEndRange().getStartTime());

		filter = CQL.toFilter("sometime before 2005-05-17T20:32:56Z or when after 2005-05-19T20:32:56Z");
		query = new Query(
				"type",
				filter);
		range = (TemporalConstraints) query.getFilter().accept(
				visitorWithDescriptor,
				null);
		assertNotNull(range);
		assertEquals(
				TemporalRange.END_TIME,
				range.getStartRange().getEndTime());
		assertEquals(
				sTime2,
				range.getEndRange().getStartTime());

	}

	@Test
	public void testNotBetween()
			throws CQLException,
			ParseException {
		final ExtractTimeFilterVisitor visitor = new ExtractTimeFilterVisitor();
		final Date sTime2 = DateUtilities.parseISO("2005-05-19T20:32:56Z");
		final Date eTime1 = DateUtilities.parseISO("2005-05-17T20:32:56Z");
		final Filter filter = CQL.toFilter("not (when before 2005-05-17T20:32:56Z or when after 2005-05-19T20:32:56Z)");
		final Query query = new Query(
				"type",
				filter);
		final TemporalConstraints range = (TemporalConstraints) query.getFilter().accept(
				visitor,
				null);
		assertNotNull(range);
		assertEquals(
				eTime1,
				range.getStartRange().getStartTime());
		assertEquals(
				new Date(
						sTime2.getTime() - 1),
				range.getStartRange().getEndTime());
	}

	@Test
	public void testNotOutliers()
			throws CQLException,
			ParseException {
		final ExtractTimeFilterVisitor visitor = new ExtractTimeFilterVisitor();
		final Date sTime = DateUtilities.parseISO("2005-05-19T20:32:56Z");
		final Date eTime = DateUtilities.parseISO("2005-05-20T20:32:56Z");
		Filter filter = CQL.toFilter("not (when before 2005-05-20T20:32:56Z and when after 2005-05-19T20:32:56Z)");
		Query query = new Query(
				"type",
				filter);
		TemporalConstraints range = (TemporalConstraints) query.getFilter().accept(
				visitor,
				null);
		assertNotNull(range);
		assertEquals(
				TemporalRange.START_TIME,
				range.getStartRange().getStartTime());
		assertEquals(
				new Date(
						sTime.getTime() - 1),
				range.getStartRange().getEndTime());
		assertEquals(
				eTime,
				range.getRanges().get(
						1).getStartTime());
		assertEquals(
				TemporalRange.END_TIME,
				range.getRanges().get(
						1).getEndTime());

		final ExtractTimeFilterVisitor visitorWithDescriptor = new ExtractTimeFilterVisitor(
				TIMEDESCRIPTOR);

		filter = CQL.toFilter("not (sometime before 2005-05-20T20:32:56Z and when after 2005-05-19T20:32:56Z)");
		query = new Query(
				"type",
				filter);
		range = (TemporalConstraints) query.getFilter().accept(
				visitorWithDescriptor,
				null);
		assertNotNull(range);
		assertEquals(
				new Date(
						sTime.getTime() - 1),
				range.getEndRange().getEndTime());
		assertEquals(
				TemporalRange.START_TIME,
				range.getEndRange().getStartTime());

	}

}
