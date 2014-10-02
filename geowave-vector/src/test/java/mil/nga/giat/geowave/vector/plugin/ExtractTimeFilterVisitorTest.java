package mil.nga.giat.geowave.vector.plugin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;
import java.util.Date;

import mil.nga.giat.geowave.store.query.TemporalConstraints;
import mil.nga.giat.geowave.store.query.TemporalRange;
import mil.nga.giat.geowave.vector.plugin.ExtractTimeFilterVisitor;
import mil.nga.giat.geowave.vector.utils.DateUtilities;

import org.geotools.data.Query;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.junit.Test;
import org.opengis.filter.Filter;

public class ExtractTimeFilterVisitorTest {

	@Test
	public void testAfter() throws CQLException, ParseException {
		ExtractTimeFilterVisitor visitor = new ExtractTimeFilterVisitor();
		Date time = DateUtilities.parseISO("2005-05-19T20:32:56Z");
		Filter filter = CQL.toFilter("when after 2005-05-19T20:32:56Z");
		Query query = new Query("type", filter);
		TemporalConstraints range = (TemporalConstraints) query.getFilter()
				.accept(visitor, null);
		assertNotNull(range);
		assertEquals(time, range.getStartRange().getStartTime());
	}

	@Test
	public void testDuring() throws CQLException, ParseException {
		ExtractTimeFilterVisitor visitor = new ExtractTimeFilterVisitor();
		Date stime = DateUtilities.parseISO("2005-05-19T20:32:56Z");
		Date etime = DateUtilities.parseISO("2005-05-19T21:32:56Z");
		Filter filter = CQL
				.toFilter("when during 2005-05-19T20:32:56Z/2005-05-19T21:32:56Z");
		Query query = new Query("type", filter);
		TemporalConstraints range = (TemporalConstraints) query.getFilter()
				.accept(visitor, null);
		assertNotNull(range);
		assertEquals(stime, range.getStartRange().getStartTime());
		assertEquals(etime, range.getStartRange().getEndTime());
	}

	@Test
	public void testBefore() throws CQLException, ParseException {
		ExtractTimeFilterVisitor visitor = new ExtractTimeFilterVisitor();
		Date etime = DateUtilities.parseISO("2005-05-19T20:32:56Z");
		Filter filter = CQL.toFilter("when before 2005-05-19T20:32:56Z");
		Query query = new Query("type", filter);
		TemporalConstraints range = (TemporalConstraints) query.getFilter()
				.accept(visitor, null);
		assertNotNull(range);
		assertEquals(TemporalRange.START_TIME, range.getStartRange()
				.getStartTime());
		assertEquals(etime, range.getStartRange().getEndTime());
	}

	@Test
	public void testBeforeOrDuring() throws CQLException, ParseException {
		ExtractTimeFilterVisitor visitor = new ExtractTimeFilterVisitor();
		Date stime = DateUtilities.parseISO("2005-05-19T21:32:56Z");
		Filter filter = CQL
				.toFilter("when BEFORE OR DURING 2005-05-19T20:32:56Z/2005-05-19T21:32:56Z");
		Query query = new Query("type", filter);
		TemporalConstraints range = (TemporalConstraints) query.getFilter()
				.accept(visitor, null);
		assertNotNull(range);
		assertEquals(TemporalRange.START_TIME, range.getStartRange()
				.getStartTime());
		assertEquals(stime, range.getStartRange().getEndTime());
	}

	@Test
	public void testDuringOrAfter() throws CQLException, ParseException {
		ExtractTimeFilterVisitor visitor = new ExtractTimeFilterVisitor();
		Date stime = DateUtilities.parseISO("2005-05-19T20:32:56Z");
		Date etime = DateUtilities.parseISO("2005-05-19T21:32:56Z");
		Filter filter = CQL
				.toFilter("when DURING OR AFTER 2005-05-19T20:32:56Z/2005-05-19T21:32:56Z");
		Query query = new Query("type", filter);
		TemporalConstraints range = (TemporalConstraints) query.getFilter()
				.accept(visitor, null);
		assertNotNull(range);
		assertEquals(stime, range.getStartRange().getStartTime());
		assertEquals(TemporalRange.END_TIME, range.getStartRange().getEndTime());
	}

	@Test
	public void testAndOverlap() throws CQLException, ParseException {
		ExtractTimeFilterVisitor visitor = new ExtractTimeFilterVisitor();
		Date sTime = DateUtilities.parseISO("2005-05-19T20:32:56Z");
		Date eTime = DateUtilities.parseISO("2005-05-20T20:32:56Z");
		Filter filter = CQL
				.toFilter("when before 2005-05-20T20:32:56Z and when after 2005-05-19T20:32:56Z");
		Query query = new Query("type", filter);
		TemporalConstraints range = (TemporalConstraints) query.getFilter()
				.accept(visitor, null);
		assertNotNull(range);
		assertEquals(sTime, range.getStartRange().getStartTime());
		assertEquals(eTime, range.getStartRange().getEndTime());
	}

	@Test
	public void testAndNoOverlap() throws CQLException, ParseException {
		ExtractTimeFilterVisitor visitor = new ExtractTimeFilterVisitor();
		Filter filter = CQL
				.toFilter("when before 2005-05-17T20:32:56Z and when after 2005-05-19T20:32:56Z");
		Query query = new Query("type", filter);
		TemporalConstraints range = (TemporalConstraints) query.getFilter()
				.accept(visitor, null);
		assertNotNull(range);
		assertTrue(range.isEmpty());
	}

	@Test
	public void testOr() throws CQLException, ParseException {
		ExtractTimeFilterVisitor visitor = new ExtractTimeFilterVisitor();
		Date sTime2 = DateUtilities.parseISO("2005-05-19T20:32:56Z");
		Date eTime1 = DateUtilities.parseISO("2005-05-17T20:32:56Z");
		Filter filter = CQL
				.toFilter("when before 2005-05-17T20:32:56Z or when after 2005-05-19T20:32:56Z");
		Query query = new Query("type", filter);
		TemporalConstraints range = (TemporalConstraints) query.getFilter()
				.accept(visitor, null);
		assertNotNull(range);
		assertEquals(eTime1, range.getStartRange().getEndTime());
		assertEquals(sTime2, range.getRanges().get(1).getStartTime());
	}

	@Test
	public void testNotBetween() throws CQLException, ParseException {
		ExtractTimeFilterVisitor visitor = new ExtractTimeFilterVisitor();
		Date sTime2 = DateUtilities.parseISO("2005-05-19T20:32:56Z");
		Date eTime1 = DateUtilities.parseISO("2005-05-17T20:32:56Z");
		Filter filter = CQL
				.toFilter("not (when before 2005-05-17T20:32:56Z or when after 2005-05-19T20:32:56Z)");
		Query query = new Query("type", filter);
		TemporalConstraints range = (TemporalConstraints) query.getFilter()
				.accept(visitor, null);
		assertNotNull(range);
		assertEquals(eTime1, range.getStartRange().getStartTime());
		assertEquals(new Date(sTime2.getTime() - 1), range.getStartRange()
				.getEndTime());
	}

	@Test
	public void testNotOutliers() throws CQLException, ParseException {
		ExtractTimeFilterVisitor visitor = new ExtractTimeFilterVisitor();
		Date sTime = DateUtilities.parseISO("2005-05-19T20:32:56Z");
		Date eTime = DateUtilities.parseISO("2005-05-20T20:32:56Z");
		Filter filter = CQL
				.toFilter("not (when before 2005-05-20T20:32:56Z and when after 2005-05-19T20:32:56Z)");
		Query query = new Query("type", filter);
		TemporalConstraints range = (TemporalConstraints) query.getFilter()
				.accept(visitor, null);
		assertNotNull(range);
		assertEquals(TemporalRange.START_TIME, range.getStartRange()
				.getStartTime());
		assertEquals(new Date(sTime.getTime() - 1), range.getStartRange()
				.getEndTime());
		assertEquals(eTime, range.getRanges().get(1).getStartTime());
		assertEquals(TemporalRange.END_TIME, range.getRanges().get(1)
				.getEndTime());
	}

}
