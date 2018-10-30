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
package org.locationtech.geowave.adapter.vector.query.cql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.geotools.data.Query;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.junit.Test;
import org.locationtech.geowave.core.geotime.util.PropertyConstraintSet;
import org.locationtech.geowave.core.geotime.util.PropertyFilterVisitor;
import org.locationtech.geowave.core.store.index.numeric.NumberRangeFilter;
import org.locationtech.geowave.core.store.index.numeric.NumericEqualsConstraint;
import org.locationtech.geowave.core.store.index.numeric.NumericLessThanConstraint;
import org.locationtech.geowave.core.store.index.numeric.NumericQueryConstraint;
import org.locationtech.geowave.core.store.index.text.TextExactMatchFilter;
import org.locationtech.geowave.core.store.index.text.TextQueryConstraint;
import org.opengis.filter.Filter;

public class PropertyFilterVisitorTest
{
	@Test
	public void testNumbersTypes()
			throws CQLException {
		final Filter filter = CQL
				.toFilter("a < 9 and c = 12 and e >= 11 and f <= 12 and g > 13 and h between 4 and 6 and k > 4 and k < 6 and l >= 4 and l <= 6");
		final Query query = new Query(
				"type",
				filter);

		final PropertyFilterVisitor visitor = new PropertyFilterVisitor();

		final PropertyConstraintSet constraints = (PropertyConstraintSet) query.getFilter().accept(
				visitor,
				null);
		NumberRangeFilter nf = (NumberRangeFilter) ((NumericLessThanConstraint) constraints.getConstraintsByName("a"))
				.getFilter();
		assertTrue(nf.getLowerValue().doubleValue() == Double.MIN_VALUE);
		assertEquals(
				9,
				nf.getUpperValue().longValue());
		assertFalse(nf.isInclusiveHigh());
		assertTrue(nf.isInclusiveLow());

		nf = (NumberRangeFilter) ((NumericQueryConstraint) constraints.getConstraintsByName("e")).getFilter();
		assertEquals(
				11,
				nf.getLowerValue().longValue());
		assertTrue(nf.getUpperValue().doubleValue() == Double.MAX_VALUE);
		assertTrue(nf.isInclusiveHigh());
		assertTrue(nf.isInclusiveLow());

		nf = (NumberRangeFilter) ((NumericEqualsConstraint) constraints.getConstraintsByName("c")).getFilter();
		assertEquals(
				12,
				nf.getLowerValue().longValue());
		assertEquals(
				12,
				nf.getUpperValue().longValue());
		assertTrue(nf.isInclusiveHigh());
		assertTrue(nf.isInclusiveLow());

		nf = (NumberRangeFilter) ((NumericQueryConstraint) constraints.getConstraintsByName("g")).getFilter();
		assertEquals(
				13,
				nf.getLowerValue().longValue());
		assertTrue(nf.getUpperValue().doubleValue() == Double.MAX_VALUE);

		assertTrue(nf.isInclusiveHigh());
		assertFalse(nf.isInclusiveLow());

		nf = (NumberRangeFilter) ((NumericQueryConstraint) constraints.getConstraintsByName("f")).getFilter();
		assertEquals(
				12,
				nf.getUpperValue().longValue());
		assertTrue(nf.getLowerValue().doubleValue() == Double.MIN_VALUE);
		assertTrue(nf.isInclusiveHigh());
		assertTrue(nf.isInclusiveLow());

		nf = (NumberRangeFilter) ((NumericQueryConstraint) constraints.getConstraintsByName("h")).getFilter();
		assertEquals(
				4,
				nf.getLowerValue().longValue());
		assertEquals(
				6,
				nf.getUpperValue().longValue());
		assertTrue(nf.isInclusiveHigh());
		assertTrue(nf.isInclusiveLow());

		nf = (NumberRangeFilter) ((NumericQueryConstraint) constraints.getConstraintsByName("k")).getFilter();
		assertEquals(
				4,
				nf.getLowerValue().longValue());
		assertEquals(
				6,
				nf.getUpperValue().longValue());
		assertFalse(nf.isInclusiveHigh());
		assertFalse(nf.isInclusiveLow());

		nf = (NumberRangeFilter) ((NumericQueryConstraint) constraints.getConstraintsByName("l")).getFilter();
		assertEquals(
				4,
				nf.getLowerValue().longValue());
		assertEquals(
				6,
				nf.getUpperValue().longValue());
		assertTrue(nf.isInclusiveHigh());
		assertTrue(nf.isInclusiveLow());

	}

	@Test
	public void testTextTypes()
			throws CQLException {
		final Filter filter = CQL.toFilter("b = '10'");
		final Query query = new Query(
				"type",
				filter);

		final PropertyFilterVisitor visitor = new PropertyFilterVisitor();

		final PropertyConstraintSet constraints = (PropertyConstraintSet) query.getFilter().accept(
				visitor,
				null);
		final TextExactMatchFilter tf = (TextExactMatchFilter) ((TextQueryConstraint) constraints
				.getConstraintsByName("b")).getFilter();
		assertEquals(
				"10",
				tf.getMatchValue());
		assertTrue(tf.isCaseSensitive());

	}
}
