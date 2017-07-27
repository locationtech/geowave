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
package mil.nga.giat.geowave.adapter.vector.plugin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;
import java.util.Date;

import mil.nga.giat.geowave.adapter.vector.plugin.ExtractTimeFilterVisitor;
import mil.nga.giat.geowave.adapter.vector.utils.DateUtilities;
import mil.nga.giat.geowave.core.geotime.store.query.TemporalConstraints;
import mil.nga.giat.geowave.core.geotime.store.query.TemporalConstraintsSet;
import mil.nga.giat.geowave.core.geotime.store.query.TemporalRange;

import org.geotools.data.Query;
import org.geotools.filter.FilterFactoryImpl;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.junit.Before;
import org.junit.Test;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory;

public class ExtractTimeFilterVisitorTest
{
	final ExtractTimeFilterVisitor visitorWithDescriptor = new ExtractTimeFilterVisitor();
	final ExtractTimeFilterVisitor visitorWithDescriptorForRange = new ExtractTimeFilterVisitor();

	@Before
	public void setup() {
		visitorWithDescriptorForRange.addRangeVariables(
				"start",
				"end");
	}

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
				new Date(
						time.getTime() + 1),
				range.getStartRange().getStartTime());

		range = (TemporalConstraints) query.getFilter().accept(
				visitorWithDescriptor,
				null);
		assertNotNull(range);
		assertEquals(
				new Date(
						time.getTime() + 1),
				range.getStartRange().getStartTime());
		assertEquals(
				"when",
				range.getName());
	}

	@Test
	public void testGreaterThan()
			throws CQLException,
			ParseException {
		final ExtractTimeFilterVisitor visitor = new ExtractTimeFilterVisitor();
		final Date stimeNotEqual = new Date(
				DateUtilities.parseISO(
						"2005-05-19T20:32:56Z").getTime() + 1);
		final Date stime = DateUtilities.parseISO("2005-05-19T20:32:56Z");
		Filter filter = ECQL.toFilter("when > 2005-05-19T20:32:56Z");
		Query query = new Query(
				"type",
				filter);
		TemporalConstraints range = (TemporalConstraints) query.getFilter().accept(
				visitor,
				null);
		assertNotNull(range);
		assertEquals(
				stimeNotEqual,
				range.getStartRange().getStartTime());
		assertEquals(
				TemporalRange.END_TIME,
				range.getEndRange().getEndTime());
		assertEquals(
				"when",
				range.getName());

		filter = ECQL.toFilter("2005-05-19T20:32:56Z < when");
		query = new Query(
				"type",
				filter);
		range = (TemporalConstraints) query.getFilter().accept(
				visitor,
				null);
		assertNotNull(range);
		assertEquals(
				stimeNotEqual,
				range.getStartRange().getStartTime());
		assertEquals(
				TemporalRange.END_TIME,
				range.getEndRange().getEndTime());
		assertEquals(
				"when",
				range.getName());

		filter = ECQL.toFilter("2005-05-19T20:32:56Z <= when");
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
				TemporalRange.END_TIME,
				range.getEndRange().getEndTime());
		assertEquals(
				"when",
				range.getName());
	}

	@Test
	public void testMixedRanges()
			throws CQLException,
			ParseException {
		final Date stime = new Date(
				DateUtilities.parseISO(
						"2005-05-19T20:32:56Z").getTime() + 1);
		final Date etime = new Date(
				DateUtilities.parseISO(
						"2005-05-20T20:32:56Z").getTime() - 1);

		Filter filter = ECQL.toFilter("start > 2005-05-19T20:32:56Z and end < 2005-05-20T20:32:56Z");
		FilterFactory factory = new FilterFactoryImpl();
		filter = factory.and(
				Filter.INCLUDE,
				filter);
		Query query = new Query(
				"type",
				filter);
		TemporalConstraintsSet rangeSet = (TemporalConstraintsSet) query.getFilter().accept(
				visitorWithDescriptor,
				null);
		assertNotNull(rangeSet);
		assertTrue(!rangeSet.isEmpty());
		assertEquals(
				stime,
				rangeSet.getConstraintsFor(
						"start").getStartRange().getStartTime());
		assertEquals(
				etime,
				rangeSet.getConstraintsFor(
						"end").getEndRange().getEndTime());

		final Date stime1 = new Date(
				DateUtilities.parseISO(
						"2005-05-17T20:32:56Z").getTime() + 1);
		final Date etime1 = new Date(
				DateUtilities.parseISO(
						"2005-05-18T20:32:56Z").getTime() - 1);
		filter = ECQL
				.toFilter("(start < 2005-05-18T20:32:56Z and end > 2005-05-17T20:32:56Z) or (start < 2005-05-20T20:32:56Z and end > 2005-05-19T20:32:56Z)");
		filter = factory.and(
				Filter.INCLUDE,
				filter);
		query = new Query(
				"type",
				filter);
		rangeSet = (TemporalConstraintsSet) query.getFilter().accept(
				visitorWithDescriptorForRange,
				null);
		assertNotNull(rangeSet);
		assertTrue(!rangeSet.isEmpty());
		assertEquals(
				stime1,
				rangeSet.getConstraintsFor(
						"start_end").getStartRange().getStartTime());
		assertEquals(
				etime1,
				rangeSet.getConstraintsFor(
						"start_end").getStartRange().getEndTime());
		assertEquals(
				stime,
				rangeSet.getConstraintsFor(
						"start_end").getEndRange().getStartTime());
		assertEquals(
				etime,
				rangeSet.getConstraintsFor(
						"start_end").getEndRange().getEndTime());

		// Open ended query
		filter = ECQL.toFilter("start < 2005-05-20T20:32:56Z and end > 2005-05-19T20:32:56Z");
		filter = factory.and(
				Filter.INCLUDE,
				filter);
		query = new Query(
				"type",
				filter);
		rangeSet = (TemporalConstraintsSet) query.getFilter().accept(
				visitorWithDescriptor,
				null);
		assertNotNull(rangeSet);
		assertTrue(!rangeSet.isEmpty());
		assertEquals(
				TemporalRange.START_TIME,
				rangeSet.getConstraintsFor(
						"start_end").getStartRange().getStartTime());
		assertEquals(
				TemporalRange.END_TIME,
				rangeSet.getConstraintsFor(
						"start_end").getEndRange().getEndTime());

	}

	@Test
	public void testLessThan()
			throws CQLException,
			ParseException {
		final ExtractTimeFilterVisitor visitor = new ExtractTimeFilterVisitor();
		final Date etimeNotEqual = new Date(
				DateUtilities.parseISO(
						"2005-05-19T21:32:56Z").getTime() - 1);
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
				etimeNotEqual,
				range.getEndRange().getEndTime());
		assertEquals(
				"when",
				range.getName());

		filter = ECQL.toFilter(" 2005-05-19T21:32:56Z > when");
		query = new Query(
				"type",
				filter);
		range = (TemporalConstraints) query.getFilter().accept(
				visitor,
				null);
		assertNotNull(range);
		assertEquals(
				TemporalRange.START_TIME,
				range.getStartRange().getStartTime());
		assertEquals(
				etimeNotEqual,
				range.getEndRange().getEndTime());
		assertEquals(
				"when",
				range.getName());

		filter = ECQL.toFilter(" 2005-05-19T21:32:56Z >= when");
		query = new Query(
				"type",
				filter);
		range = (TemporalConstraints) query.getFilter().accept(
				visitor,
				null);
		assertNotNull(range);
		assertEquals(
				TemporalRange.START_TIME,
				range.getStartRange().getStartTime());
		assertEquals(
				etime,
				range.getEndRange().getEndTime());
		assertEquals(
				"when",
				range.getName());

	}

	@Test
	public void testLessAndGreaterThan()
			throws CQLException,
			ParseException {
		final ExtractTimeFilterVisitor visitor = new ExtractTimeFilterVisitor();
		final Date etime = new Date(
				DateUtilities.parseISO(
						"2005-05-19T21:32:56Z").getTime() - 1);
		final Date stime = new Date(
				DateUtilities.parseISO(
						"2005-05-19T20:32:56Z").getTime() + 1);
		Filter filter = ECQL.toFilter("when > 2005-05-19T21:32:56Z and when < 2005-05-19T20:32:56Z");
		Query query = new Query(
				"type",
				filter);
		TemporalConstraintsSet rangeSet = (TemporalConstraintsSet) query.getFilter().accept(
				visitor,
				null);
		assertNotNull(rangeSet);
		assertEquals(
				TemporalRange.START_TIME,
				rangeSet.getConstraintsFor(
						"when").getStartRange().getStartTime());
		assertEquals(
				TemporalRange.END_TIME,
				rangeSet.getConstraintsFor(
						"when").getEndRange().getEndTime());

		filter = ECQL.toFilter("when < 2005-05-19T21:32:56Z and when > 2005-05-19T20:32:56Z");
		query = new Query(
				"type",
				filter);
		rangeSet = (TemporalConstraintsSet) query.getFilter().accept(
				visitor,
				null);
		assertNotNull(rangeSet);
		assertEquals(
				stime,
				rangeSet.getConstraintsFor(
						"when").getStartRange().getStartTime());
		assertEquals(
				etime,
				rangeSet.getConstraintsFor(
						"when").getEndRange().getEndTime());

		filter = ECQL.toFilter("sometime < 2005-05-19T21:32:56Z and when > 2005-05-19T20:32:56Z");
		query = new Query(
				"type",
				filter);
		rangeSet = (TemporalConstraintsSet) query.getFilter().accept(
				visitorWithDescriptor,
				null);
		assertEquals(
				stime,
				rangeSet.getConstraintsFor(
						"when").getStartRange().getStartTime());
		assertEquals(
				TemporalRange.END_TIME,
				rangeSet.getConstraintsFor(
						"when").getEndRange().getEndTime());
		assertEquals(
				TemporalRange.START_TIME,
				rangeSet.getConstraintsFor(
						"sometime").getStartRange().getStartTime());
		assertEquals(
				etime,
				rangeSet.getConstraintsFor(
						"sometime").getEndRange().getEndTime());

		filter = ECQL.toFilter("when < 2005-05-19T21:32:56Z and sometime > 2005-05-19T20:32:56Z");
		query = new Query(
				"type",
				filter);
		rangeSet = (TemporalConstraintsSet) query.getFilter().accept(
				visitorWithDescriptor,
				null);
		assertEquals(
				TemporalRange.START_TIME,
				rangeSet.getConstraintsFor(
						"when").getStartRange().getStartTime());
		assertEquals(
				etime,
				rangeSet.getConstraintsFor(
						"when").getEndRange().getEndTime());
		assertEquals(
				stime,
				rangeSet.getConstraintsFor(
						"sometime").getStartRange().getStartTime());
		assertEquals(
				TemporalRange.END_TIME,
				rangeSet.getConstraintsFor(
						"sometime").getEndRange().getEndTime());

		filter = ECQL.toFilter("2005-05-19T21:32:56Z > when and  2005-05-19T20:32:56Z < sometime");
		query = new Query(
				"type",
				filter);
		rangeSet = (TemporalConstraintsSet) query.getFilter().accept(
				visitorWithDescriptor,
				null);
		assertEquals(
				TemporalRange.START_TIME,
				rangeSet.getConstraintsFor(
						"when").getStartRange().getStartTime());
		assertEquals(
				etime,
				rangeSet.getConstraintsFor(
						"when").getEndRange().getEndTime());
		assertEquals(
				stime,
				rangeSet.getConstraintsFor(
						"sometime").getStartRange().getStartTime());
		assertEquals(
				TemporalRange.END_TIME,
				rangeSet.getConstraintsFor(
						"sometime").getEndRange().getEndTime());

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

	}

	@Test
	public void testDuring()
			throws CQLException,
			ParseException {
		final ExtractTimeFilterVisitor visitor = new ExtractTimeFilterVisitor();
		final Date stime = new Date(
				DateUtilities.parseISO(
						"2005-05-19T20:32:56Z").getTime() + 1);
		final Date etime = new Date(
				DateUtilities.parseISO(
						"2005-05-19T21:32:56Z").getTime() - 1);
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
	}

	@Test
	public void testBefore()
			throws CQLException,
			ParseException {
		final ExtractTimeFilterVisitor visitor = new ExtractTimeFilterVisitor();
		final Date etime = new Date(
				DateUtilities.parseISO(
						"2005-05-19T20:32:56Z").getTime() - 1);
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
	}

	@Test
	public void testBeforeOrDuring()
			throws CQLException,
			ParseException {
		final ExtractTimeFilterVisitor visitor = new ExtractTimeFilterVisitor();
		final Date stime = new Date(
				DateUtilities.parseISO(
						"2005-05-19T21:32:56Z").getTime() - 1);
		Filter filter = CQL.toFilter("when BEFORE OR DURING 2005-05-19T20:32:56Z/2005-05-19T21:32:56Z");
		Query query = new Query(
				"type",
				filter);
		TemporalConstraintsSet rangeSet = (TemporalConstraintsSet) query.getFilter().accept(
				visitor,
				null);
		assertNotNull(rangeSet);
		assertEquals(
				TemporalRange.START_TIME,
				rangeSet.getConstraintsFor(
						"when").getStartRange().getStartTime());
		assertEquals(
				stime,
				rangeSet.getConstraintsFor(
						"when").getEndRange().getEndTime());

		rangeSet = (TemporalConstraintsSet) query.getFilter().accept(
				visitorWithDescriptor,
				null);
		assertNotNull(rangeSet);
		assertEquals(
				TemporalRange.START_TIME,
				rangeSet.getConstraintsFor(
						"when").getStartRange().getStartTime());
		assertEquals(
				stime,
				rangeSet.getConstraintsFor(
						"when").getEndRange().getEndTime());
	}

	@Test
	public void testDuringOrAfter()
			throws CQLException,
			ParseException {
		final ExtractTimeFilterVisitor visitor = new ExtractTimeFilterVisitor();
		final Date stime = new Date(
				DateUtilities.parseISO(
						"2005-05-19T20:32:56Z").getTime() + 1);
		Filter filter = CQL.toFilter("when DURING OR AFTER 2005-05-19T20:32:56Z/2005-05-19T21:32:56Z");
		Query query = new Query(
				"type",
				filter);
		TemporalConstraintsSet rangeSet = (TemporalConstraintsSet) query.getFilter().accept(
				visitor,
				null);
		assertNotNull(rangeSet);
		assertEquals(
				stime,
				rangeSet.getConstraintsFor(
						"when").getStartRange().getStartTime());
		assertEquals(
				TemporalRange.END_TIME,
				rangeSet.getConstraintsFor(
						"when").getEndRange().getEndTime());

		rangeSet = (TemporalConstraintsSet) query.getFilter().accept(
				visitorWithDescriptor,
				null);
		assertNotNull(rangeSet);
		assertEquals(
				stime,
				rangeSet.getConstraintsFor(
						"when").getStartRange().getStartTime());
		assertEquals(
				TemporalRange.END_TIME,
				rangeSet.getConstraintsFor(
						"when").getEndRange().getEndTime());

	}

	@Test
	public void testAndOverlap()
			throws CQLException,
			ParseException {
		final ExtractTimeFilterVisitor visitor = new ExtractTimeFilterVisitor();
		final Date sTime = new Date(
				DateUtilities.parseISO(
						"2005-05-19T20:32:56Z").getTime() + 1);
		final Date eTime = new Date(
				DateUtilities.parseISO(
						"2005-05-20T20:32:56Z").getTime() - 1);
		Filter filter = CQL.toFilter("when before 2005-05-20T20:32:56Z and when after 2005-05-19T20:32:56Z");
		Query query = new Query(
				"type",
				filter);
		TemporalConstraintsSet rangeSet = (TemporalConstraintsSet) query.getFilter().accept(
				visitor,
				null);
		assertNotNull(rangeSet);
		assertEquals(
				sTime,
				rangeSet.getConstraintsFor(
						"when").getStartRange().getStartTime());
		assertEquals(
				eTime,
				rangeSet.getConstraintsFor(
						"when").getStartRange().getEndTime());

		filter = CQL.toFilter("sometime before 2005-05-20T20:32:56Z and when after 2005-05-19T20:32:56Z");
		query = new Query(
				"type",
				filter);
		rangeSet = (TemporalConstraintsSet) query.getFilter().accept(
				visitorWithDescriptor,
				null);
		assertNotNull(rangeSet);
		assertEquals(
				sTime,
				rangeSet.getConstraintsFor(
						"when").getStartRange().getStartTime());
		assertEquals(
				TemporalRange.END_TIME,
				rangeSet.getConstraintsFor(
						"when").getStartRange().getEndTime());

		filter = CQL.toFilter("when before 2005-05-20T20:32:56Z and sometime after 2005-05-19T20:32:56Z");
		query = new Query(
				"type",
				filter);
		rangeSet = (TemporalConstraintsSet) query.getFilter().accept(
				visitorWithDescriptor,
				null);
		assertNotNull(rangeSet);
		assertFalse(rangeSet.isEmpty());
		assertEquals(
				TemporalRange.START_TIME,
				rangeSet.getConstraintsFor(
						"when").getStartRange().getStartTime());
		assertEquals(
				eTime,
				rangeSet.getConstraintsFor(
						"when").getStartRange().getEndTime());

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
		TemporalConstraintsSet rangeSet = (TemporalConstraintsSet) query.getFilter().accept(
				visitor,
				null);
		assertNotNull(rangeSet);
		assertTrue(rangeSet.isEmpty());
	}

	@Test
	public void testOr()
			throws CQLException,
			ParseException {
		final ExtractTimeFilterVisitor visitor = new ExtractTimeFilterVisitor();
		final Date sTime2 = new Date(
				DateUtilities.parseISO(
						"2005-05-19T20:32:56Z").getTime() + 1);
		final Date eTime1 = new Date(
				DateUtilities.parseISO(
						"2005-05-17T20:32:56Z").getTime() - 1);
		Filter filter = CQL.toFilter("when before 2005-05-17T20:32:56Z or when after 2005-05-19T20:32:56Z");
		Query query = new Query(
				"type",
				filter);
		TemporalConstraintsSet rangeSet = (TemporalConstraintsSet) query.getFilter().accept(
				visitor,
				null);
		assertNotNull(rangeSet);
		assertEquals(
				eTime1,
				rangeSet.getConstraintsFor(
						"when").getStartRange().getEndTime());
		assertEquals(
				sTime2,
				rangeSet.getConstraintsFor(
						"when").getRanges().get(
						1).getStartTime());

		// test mixed
		filter = CQL.toFilter("when before 2005-05-17T20:32:56Z or sometime after 2005-05-19T20:32:56Z");
		query = new Query(
				"type",
				filter);
		rangeSet = (TemporalConstraintsSet) query.getFilter().accept(
				visitorWithDescriptor,
				null);
		assertNotNull(rangeSet);
		assertEquals(
				eTime1,
				rangeSet.getConstraintsFor(
						"when").getStartRange().getEndTime());
		assertEquals(
				TemporalRange.START_TIME,
				rangeSet.getConstraintsFor(
						"when").getEndRange().getStartTime());
		assertEquals(
				TemporalRange.END_TIME,
				rangeSet.getConstraintsFor(
						"sometime").getStartRange().getEndTime());
		assertEquals(
				sTime2,
				rangeSet.getConstraintsFor(
						"sometime").getEndRange().getStartTime());

	}

	@Test
	public void testNotBetween()
			throws CQLException,
			ParseException {
		final ExtractTimeFilterVisitor visitor = new ExtractTimeFilterVisitor();
		final Date sTime2 = new Date(
				DateUtilities.parseISO(
						"2005-05-19T20:32:56Z").getTime() + 1);
		final Date eTime1 = new Date(
				DateUtilities.parseISO(
						"2005-05-17T20:32:56Z").getTime() - 1);
		final Filter filter = CQL.toFilter("not (when before 2005-05-17T20:32:56Z or when after 2005-05-19T20:32:56Z)");
		final Query query = new Query(
				"type",
				filter);
		TemporalConstraintsSet rangeSet = (TemporalConstraintsSet) query.getFilter().accept(
				visitor,
				null);
		assertNotNull(rangeSet);
		assertEquals(
				eTime1,
				rangeSet.getConstraintsFor(
						"when").getStartRange().getStartTime());
		assertEquals(
				new Date(
						sTime2.getTime() - 1),
				rangeSet.getConstraintsFor(
						"when").getStartRange().getEndTime());
	}

	@Test
	public void testNotOutliers()
			throws CQLException,
			ParseException {
		final ExtractTimeFilterVisitor visitor = new ExtractTimeFilterVisitor();
		final Date sTime = new Date(
				DateUtilities.parseISO(
						"2005-05-19T20:32:56Z").getTime() + 1);
		final Date eTime = new Date(
				DateUtilities.parseISO(
						"2005-05-20T20:32:56Z").getTime() - 1);
		Filter filter = CQL.toFilter("not (when before 2005-05-20T20:32:56Z and when after 2005-05-19T20:32:56Z)");
		Query query = new Query(
				"type",
				filter);
		TemporalConstraintsSet rangeSet = (TemporalConstraintsSet) query.getFilter().accept(
				visitor,
				null);
		assertNotNull(rangeSet);
		assertEquals(
				TemporalRange.START_TIME,
				rangeSet.getConstraintsFor(
						"when").getStartRange().getStartTime());
		assertEquals(
				new Date(
						sTime.getTime() - 1),
				rangeSet.getConstraintsFor(
						"when").getStartRange().getEndTime());
		assertEquals(
				eTime,
				rangeSet.getConstraintsFor(
						"when").getRanges().get(
						1).getStartTime());
		assertEquals(
				TemporalRange.END_TIME,
				rangeSet.getConstraintsFor(
						"when").getRanges().get(
						1).getEndTime());

		filter = CQL.toFilter("not (sometime before 2005-05-20T20:32:56Z and when after 2005-05-19T20:32:56Z)");
		query = new Query(
				"type",
				filter);
		rangeSet = (TemporalConstraintsSet) query.getFilter().accept(
				visitorWithDescriptor,
				null);
		assertNotNull(rangeSet);
		assertEquals(
				new Date(
						sTime.getTime() - 1),
				rangeSet.getConstraintsFor(
						"when").getEndRange().getEndTime());
		assertEquals(
				TemporalRange.START_TIME,
				rangeSet.getConstraintsFor(
						"when").getEndRange().getStartTime());

	}

}
