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
package org.locationtech.geowave.core.store.query.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.locationtech.geowave.core.index.sfc.data.BasicNumericDataset;
import org.locationtech.geowave.core.index.sfc.data.NumericData;
import org.locationtech.geowave.core.index.sfc.data.NumericRange;
import org.locationtech.geowave.core.index.sfc.data.NumericValue;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;
import org.locationtech.geowave.core.store.query.BasicQueryTest;
import org.locationtech.geowave.core.store.query.filter.BasicQueryFilter.BasicQueryCompareOperation;

public class DistributedQueryFilterTest
{

	@Test
	public void test() {
		List<QueryFilter> filters = new ArrayList<>();
		filters.add(new BasicQueryFilter(
				new BasicNumericDataset(
						new NumericData[] {
							new NumericValue(
									0.4)
						}),
				new NumericDimensionField[] {
					new BasicQueryTest.ExampleDimensionOne()
				},
				BasicQueryCompareOperation.CONTAINS));
		filters.add(new DedupeFilter());
		FilterList list = new FilterList(
				false,
				filters);
		list.fromBinary(list.toBinary());
		assertFalse(list.logicalAnd);
		assertEquals(
				((BasicQueryFilter) list.filters.get(0)).compareOp,
				BasicQueryCompareOperation.CONTAINS);
		assertEquals(
				((BasicQueryFilter) list.filters.get(0)).constraints,
				new BasicNumericDataset(
						new NumericData[] {
							new NumericRange(
									0.4,
									0.4)
						}));

		filters = new ArrayList<>();
		filters.add(new BasicQueryFilter(
				new BasicNumericDataset(
						new NumericData[] {
							new NumericValue(
									0.5)
						}),
				new NumericDimensionField[] {
					new BasicQueryTest.ExampleDimensionOne()
				},
				BasicQueryCompareOperation.INTERSECTS));
		filters.add(new DedupeFilter());
		list = new FilterList(
				true,
				filters);
		list.fromBinary(list.toBinary());
		assertTrue(list.logicalAnd);
		assertEquals(
				((BasicQueryFilter) list.filters.get(0)).compareOp,
				BasicQueryCompareOperation.INTERSECTS);
		assertEquals(
				((BasicQueryFilter) list.filters.get(0)).constraints,
				new BasicNumericDataset(
						new NumericData[] {
							new NumericRange(
									0.5,
									0.5)
						}));
	}

}
