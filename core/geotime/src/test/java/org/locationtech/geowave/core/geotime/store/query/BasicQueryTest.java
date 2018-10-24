/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *   
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.core.geotime.store.query;

import static org.junit.Assert.assertEquals;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Test;
import org.locationtech.geowave.core.geotime.index.dimension.TimeDefinition;
import org.locationtech.geowave.core.geotime.index.dimension.TemporalBinningStrategy.Unit;
import org.locationtech.geowave.core.geotime.ingest.SpatialTemporalDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.ingest.SpatialTemporalOptions;
import org.locationtech.geowave.core.geotime.store.dimension.TimeField;
import org.locationtech.geowave.core.geotime.store.dimension.Time.TimeRange;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.sfc.data.NumericRange;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.data.CommonIndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.data.PersistentDataset;
import org.locationtech.geowave.core.store.data.PersistentValue;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.index.CommonIndexValue;
import org.locationtech.geowave.core.store.query.constraints.BasicQuery;
import org.locationtech.geowave.core.store.query.constraints.BasicQuery.ConstraintData;
import org.locationtech.geowave.core.store.query.constraints.BasicQuery.ConstraintSet;
import org.locationtech.geowave.core.store.query.constraints.BasicQuery.Constraints;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;
import org.locationtech.geowave.core.store.query.filter.BasicQueryFilter.BasicQueryCompareOperation;

public class BasicQueryTest
{
	SimpleDateFormat df = new SimpleDateFormat(
			"yyyy-MM-dd'T'HH:mm:ssz");

	private CommonIndexedPersistenceEncoding createData(
			final Date start,
			final Date end ) {
		final PersistentDataset<CommonIndexValue> commonData = new PersistentDataset<CommonIndexValue>();

		commonData.addValue(
				new TimeField(
						Unit.YEAR).getFieldName(),
				new TimeRange(
						start.getTime(),
						end.getTime(),
						new byte[0]));

		return new CommonIndexedPersistenceEncoding(
				(short) 1,
				new ByteArray(
						"1"),
				new ByteArray(
						"1"),
				new ByteArray(
						"1"),
				1,
				commonData,
				new PersistentDataset<byte[]>());
	}

	public void performOp(
			final BasicQueryCompareOperation op,
			final boolean[] expectedResults )
			throws ParseException {
		// query time range
		ConstraintData constrainData = new ConstraintData(
				new NumericRange(
						df.parse(
								"2017-02-22T12:00:00GMT-00:00").getTime(),
						df.parse(
								"2017-02-22T13:00:00GMT-00:00").getTime()),
				true);
		Constraints constaints = new Constraints(
				new ConstraintSet(
						TimeDefinition.class,
						constrainData));
		final BasicQuery query = new BasicQuery(
				constaints,
				op);

		final CommonIndexedPersistenceEncoding[] data = new CommonIndexedPersistenceEncoding[] {

			// same exact time range as the query
			createData(
					df.parse("2017-02-22T12:00:00GMT-00:00"),
					df.parse("2017-02-22T13:00:00GMT-00:00")),

			// partial overlap
			createData(
					df.parse("2017-02-22T11:00:00GMT-00:00"),
					df.parse("2017-02-22T12:30:00GMT-00:00")),

			// time range completely within the query
			createData(
					df.parse("2017-02-22T12:30:00GMT-00:00"),
					df.parse("2017-02-22T12:50:00GMT-00:00")),

			// time range touching each other
			createData(
					df.parse("2017-02-22T11:00:00GMT-00:00"),
					df.parse("2017-02-22T12:00:00GMT-00:00")),

			// no intersection between ranges
			createData(
					df.parse("2017-02-22T11:00:00GMT-00:00"),
					df.parse("2017-02-22T11:59:00GMT-00:00")),

			// time range contains complete query range
			createData(
					df.parse("2017-02-22T11:00:00GMT-00:00"),
					df.parse("2017-02-22T14:00:00GMT-00:00"))
		};
		final Index index = new SpatialTemporalDimensionalityTypeProvider().createIndex(new SpatialTemporalOptions());
		int pos = 0;
		for (final CommonIndexedPersistenceEncoding dataItem : data) {
			for (final QueryFilter filter : query.createFilters(index)) {
				assertEquals(
						"result: " + pos,
						expectedResults[pos++],
						filter.accept(
								index.getIndexModel(),
								dataItem));
			}
		}
	}

	@Test
	public void testContains()
			throws ParseException {
		performOp(
				BasicQueryCompareOperation.CONTAINS,
				new boolean[] {
					true,
					false,
					true,
					false,
					false,
					false
				});
	}

	@Test
	public void testOverlaps()
			throws ParseException {
		performOp(
				BasicQueryCompareOperation.OVERLAPS,
				new boolean[] {
					false,
					true,
					false,
					false,
					false,
					false
				});
	}

	@Test
	public void testIntersects()
			throws ParseException {
		performOp(
				BasicQueryCompareOperation.INTERSECTS,
				new boolean[] {
					true,
					true,
					true,
					true,
					false,
					true
				});
	}

	@Test
	public void testEquals()
			throws ParseException {
		performOp(
				BasicQueryCompareOperation.EQUALS,
				new boolean[] {
					true,
					false,
					false,
					false,
					false,
					false
				});
	}

	@Test
	public void testDisjoint()
			throws ParseException {
		performOp(
				BasicQueryCompareOperation.DISJOINT,
				new boolean[] {
					false,
					false,
					false,
					false,
					true,
					false
				});
	}

	@Test
	public void testWithin()
			throws ParseException {
		performOp(
				BasicQueryCompareOperation.WITHIN,
				new boolean[] {
					true,
					false,
					false,
					false,
					false,
					true
				});
	}

	@Test
	public void testCrosses()
			throws ParseException {
		performOp(
				BasicQueryCompareOperation.CROSSES,
				new boolean[] {
					false,
					false,
					false,
					false,
					false,
					false
				});
	}

	@Test
	public void testTouches()
			throws ParseException {
		performOp(
				BasicQueryCompareOperation.TOUCHES,
				new boolean[] {
					false,
					false,
					false,
					true,
					false,
					false
				});
	}
}
