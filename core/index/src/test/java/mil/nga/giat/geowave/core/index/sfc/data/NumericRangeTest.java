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
package mil.nga.giat.geowave.core.index.sfc.data;

import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;

import org.junit.Assert;
import org.junit.Test;

public class NumericRangeTest
{

	private double MINIMUM = 20;
	private double MAXIMUM = 100;
	private double CENTROID = 60;
	private double DELTA = 1e-15;

	@Test
	public void testNumericRangeValues() {

		NumericRange numericRange = new NumericRange(
				MINIMUM,
				MAXIMUM);

		Assert.assertEquals(
				MINIMUM,
				numericRange.getMin(),
				DELTA);
		Assert.assertEquals(
				MAXIMUM,
				numericRange.getMax(),
				DELTA);
		Assert.assertEquals(
				CENTROID,
				numericRange.getCentroid(),
				DELTA);
		Assert.assertTrue(numericRange.isRange());

	}
}
