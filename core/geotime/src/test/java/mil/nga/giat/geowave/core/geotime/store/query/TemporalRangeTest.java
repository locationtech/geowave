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
package mil.nga.giat.geowave.core.geotime.store.query;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Date;

import mil.nga.giat.geowave.core.geotime.store.query.TemporalRange;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;

import org.junit.Test;

public class TemporalRangeTest
{

	@Test
	public void test() {
		TemporalRange range = new TemporalRange(
				new Date(
						100),
				new Date(
						1000));
		assertFalse(range.isWithin(new Date(
				10)));
		assertFalse(range.isWithin(new Date(
				100000)));
		assertTrue(range.isWithin(new Date(
				800)));

		assertFalse(range.isWithin(new NumericRange(
				20,
				99)));
		assertFalse(range.isWithin(new NumericRange(
				1001,
				9900)));
		assertTrue(range.isWithin(new NumericRange(
				998,
				9900)));
		assertTrue(range.isWithin(new NumericRange(
				20,
				199)));
		assertTrue(range.isWithin(new NumericRange(
				150,
				199)));

		assertTrue(check(
				new NumericRange(
						-1,
						1),
				new NumericRange(
						-1,
						1)));

		assertFalse(check(
				new NumericRange(
						9,
						19),
				new NumericRange(
						20,
						30)));
		assertTrue(check(
				new NumericRange(
						11,
						21),
				new NumericRange(
						20,
						30)));
		assertTrue(check(
				new NumericRange(
						20,
						30),
				new NumericRange(
						20,
						30)));
		assertFalse(check(
				new NumericRange(
						9,
						19),
				new NumericRange(
						20,
						30)));
		assertTrue(check(
				new NumericRange(
						11,
						21),
				new NumericRange(
						20,
						30)));
		assertTrue(check(
				new NumericRange(
						21,
						29),
				new NumericRange(
						20,
						30)));
		assertTrue(check(
				new NumericRange(
						20,
						30),
				new NumericRange(
						21,
						29)));
		assertTrue(check(
				new NumericRange(
						20,
						30),
				new NumericRange(
						11,
						21)));
		assertFalse(check(
				new NumericRange(
						20,
						30),
				new NumericRange(
						9,
						19)));

		assertTrue(check(
				new NumericRange(
						-3,
						-1),
				new NumericRange(
						-2,
						0)));
		assertTrue(check(
				new NumericRange(
						-2,
						0),
				new NumericRange(
						-3,
						-1)));
		assertFalse(check(
				new NumericRange(
						-3,
						1),
				new NumericRange(
						2,
						4)));
		assertTrue(check(
				new NumericRange(
						-3,
						1),
				new NumericRange(
						-2,
						0)));
		assertTrue(check(
				new NumericRange(
						-2,
						0),
				new NumericRange(
						-3,
						1)));
		assertTrue(check(
				new NumericRange(
						-2,
						0),
				new NumericRange(
						-3,
						-1)));
		assertTrue(check(
				new NumericRange(
						-3,
						-1),
				new NumericRange(
						-2,
						0)));
		assertTrue(check(
				new NumericRange(
						-2,
						0),
				new NumericRange(
						-1,
						1)));
		assertTrue(check(
				new NumericRange(
						-1,
						3),
				new NumericRange(
						0,
						2)));
		assertFalse(check(
				new NumericRange(
						-1,
						-0.5),
				new NumericRange(
						0,
						2)));
		assertTrue(check(
				new NumericRange(
						0,
						2),
				new NumericRange(
						-1,
						3)));
		assertTrue(check(
				new NumericRange(
						0,
						2),
				new NumericRange(
						-1,
						3)));
		assertFalse(check(
				new NumericRange(
						-1,
						2),
				new NumericRange(
						3,
						4)));
		assertFalse(check(
				new NumericRange(
						-1,
						2),
				new NumericRange(
						3,
						6)));
		assertTrue(check(
				new NumericRange(
						-1,
						2),
				new NumericRange(
						1,
						4)));

	}

	public static boolean check(
			NumericRange r1,
			NumericRange r2 ) {
		double t0 = r1.getMax() - r2.getMin();
		double t1 = r2.getMax() - r1.getMin();
		return !(Math.abs(t0 - t1) > (t0 + t1));

	}
}
