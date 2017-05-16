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
package mil.nga.giat.geowave.core.geotime.index.dimension;

import java.util.Calendar;
import java.util.TimeZone;

import mil.nga.giat.geowave.core.geotime.index.dimension.TemporalBinningStrategy.Unit;
import mil.nga.giat.geowave.core.index.dimension.bin.BinningStrategy;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TimeDefinitionTest
{

	private double DELTA = 1e-15;

	@Before
	public void setTimezoneToGMT() {
		TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
	}

	@Test
	public void testTimeDefinitionMaxBinByDay() {

		final double expectedMin = 0.0;
		final double expectedMax = 86399999;

		final Calendar calendar = Calendar.getInstance();

		calendar.set(
				Calendar.HOUR_OF_DAY,
				23);
		calendar.set(
				Calendar.MINUTE,
				59);
		calendar.set(
				Calendar.SECOND,
				59);
		calendar.set(
				Calendar.MILLISECOND,
				999);

		BinningStrategy bin = getStrategyByUnit(Unit.DAY);

		Assert.assertEquals(
				expectedMin,
				bin.getBinMin(),
				DELTA);
		Assert.assertEquals(
				expectedMax,
				bin.getBinMax(),
				DELTA);
		Assert.assertEquals(
				bin.getBinMax(),
				bin.getBinnedValue(
						calendar.getTimeInMillis()).getNormalizedValue(),
				DELTA);

	}

	@Test
	public void testTimeDefinitionMaxBinByMonth() {

		final double expectedMin = 0.0;
		final double expectedMax = 2678399999.0;

		final Calendar calendar = Calendar.getInstance();

		calendar.set(
				Calendar.MONTH,
				6);
		calendar.set(
				Calendar.DATE,
				31);
		calendar.set(
				Calendar.HOUR_OF_DAY,
				23);
		calendar.set(
				Calendar.MINUTE,
				59);
		calendar.set(
				Calendar.SECOND,
				59);
		calendar.set(
				Calendar.MILLISECOND,
				999);

		BinningStrategy bin = getStrategyByUnit(Unit.MONTH);

		Assert.assertEquals(
				expectedMin,
				bin.getBinMin(),
				DELTA);
		Assert.assertEquals(
				expectedMax,
				bin.getBinMax(),
				DELTA);
		Assert.assertEquals(
				bin.getBinMax(),
				bin.getBinnedValue(
						calendar.getTimeInMillis()).getNormalizedValue(),
				DELTA);

	}

	@Test
	public void testTimeDefinitionMinBinByMonth() {

		final double expectedMin = 0.0;
		final double expectedMax = 2678399999.0;

		final Calendar calendar = Calendar.getInstance();

		calendar.set(
				Calendar.MONTH,
				6);
		calendar.set(
				Calendar.DATE,
				1);
		calendar.set(
				Calendar.HOUR_OF_DAY,
				0);
		calendar.set(
				Calendar.MINUTE,
				0);
		calendar.set(
				Calendar.SECOND,
				0);
		calendar.set(
				Calendar.MILLISECOND,
				0);

		BinningStrategy bin = getStrategyByUnit(Unit.MONTH);

		Assert.assertEquals(
				expectedMin,
				bin.getBinMin(),
				DELTA);
		Assert.assertEquals(
				expectedMax,
				bin.getBinMax(),
				DELTA);
		Assert.assertEquals(
				bin.getBinMin(),
				bin.getBinnedValue(
						calendar.getTimeInMillis()).getNormalizedValue(),
				DELTA);

	}

	@Test
	public void testTimeDefinitionMaxBinByYEAR() {

		final double expectedMin = 0.0;
		final double expectedMax = 31622399999.0;

		final Calendar calendar = Calendar.getInstance();

		calendar.set(
				Calendar.YEAR,
				2012);
		calendar.set(
				Calendar.MONTH,
				11);
		calendar.set(
				Calendar.DATE,
				31);
		calendar.set(
				Calendar.HOUR_OF_DAY,
				23);
		calendar.set(
				Calendar.MINUTE,
				59);
		calendar.set(
				Calendar.SECOND,
				59);
		calendar.set(
				Calendar.MILLISECOND,
				999);

		BinningStrategy bin = getStrategyByUnit(Unit.YEAR);

		Assert.assertEquals(
				expectedMin,
				bin.getBinMin(),
				DELTA);
		Assert.assertEquals(
				expectedMax,
				bin.getBinMax(),
				DELTA);
		Assert.assertEquals(
				bin.getBinMax(),
				bin.getBinnedValue(
						calendar.getTimeInMillis()).getNormalizedValue(),
				DELTA);
	}

	@Test
	public void testTimeDefinitionBinByHour() {

		final double expectedMin = 0.0;
		final double expectedMax = 3599999.0;

		final Calendar calendar = Calendar.getInstance();

		calendar.set(
				Calendar.MINUTE,
				59);
		calendar.set(
				Calendar.SECOND,
				59);
		calendar.set(
				Calendar.MILLISECOND,
				999);

		BinningStrategy bin = getStrategyByUnit(Unit.HOUR);

		Assert.assertEquals(
				expectedMin,
				bin.getBinMin(),
				DELTA);
		Assert.assertEquals(
				expectedMax,
				bin.getBinMax(),
				DELTA);
		Assert.assertEquals(
				bin.getBinMax(),
				bin.getBinnedValue(
						calendar.getTimeInMillis()).getNormalizedValue(),
				DELTA);

		calendar.set(
				Calendar.MINUTE,
				0);
		calendar.set(
				Calendar.SECOND,
				0);
		calendar.set(
				Calendar.MILLISECOND,
				0);

		Assert.assertEquals(
				bin.getBinMin(),
				bin.getBinnedValue(
						calendar.getTimeInMillis()).getNormalizedValue(),
				DELTA);
	}

	@Test
	public void testTimeDefinitionBinByMinute() {

		final double expectedMin = 0.0;
		final double expectedMax = 59999.0;

		final Calendar calendar = Calendar.getInstance();

		calendar.set(
				Calendar.SECOND,
				59);
		calendar.set(
				Calendar.MILLISECOND,
				999);

		BinningStrategy bin = getStrategyByUnit(Unit.MINUTE);

		Assert.assertEquals(
				expectedMin,
				bin.getBinMin(),
				DELTA);
		Assert.assertEquals(
				expectedMax,
				bin.getBinMax(),
				DELTA);
		Assert.assertEquals(
				bin.getBinMax(),
				bin.getBinnedValue(
						calendar.getTimeInMillis()).getNormalizedValue(),
				DELTA);

		calendar.set(
				Calendar.SECOND,
				0);
		calendar.set(
				Calendar.MILLISECOND,
				0);

		Assert.assertEquals(
				bin.getBinMin(),
				bin.getBinnedValue(
						calendar.getTimeInMillis()).getNormalizedValue(),
				DELTA);
	}

	@Test
	public void testTimeDefinitionMaxBinByDecade() {

		final double expectedMin = 0.0;
		final double expectedMax = 315619199999.0;

		final Calendar calendar = Calendar.getInstance();

		calendar.set(
				Calendar.YEAR,
				2009);
		calendar.set(
				Calendar.MONTH,
				11);
		calendar.set(
				Calendar.DATE,
				31);
		calendar.set(
				Calendar.HOUR_OF_DAY,
				23);
		calendar.set(
				Calendar.MINUTE,
				59);
		calendar.set(
				Calendar.SECOND,
				59);
		calendar.set(
				Calendar.MILLISECOND,
				999);

		BinningStrategy bin = getStrategyByUnit(Unit.DECADE);

		Assert.assertEquals(
				expectedMin,
				bin.getBinMin(),
				DELTA);
		Assert.assertEquals(
				expectedMax,
				bin.getBinMax(),
				DELTA);
		Assert.assertEquals(
				bin.getBinMax(),
				bin.getBinnedValue(
						calendar.getTimeInMillis()).getNormalizedValue(),
				DELTA);
	}

	@Test
	public void testTimeDefinitionMaxBinByWeek() {

		final double expectedMin = 0.0;
		final double expectedMax = 604799999.0;

		BinningStrategy bin = getStrategyByUnit(Unit.WEEK);

		final Calendar calendar = Calendar.getInstance();

		calendar.set(
				Calendar.DAY_OF_WEEK,
				calendar.getActualMaximum(Calendar.DAY_OF_WEEK));
		calendar.set(
				Calendar.HOUR_OF_DAY,
				23);
		calendar.set(
				Calendar.MINUTE,
				59);
		calendar.set(
				Calendar.SECOND,
				59);
		calendar.set(
				Calendar.MILLISECOND,
				999);

		Assert.assertEquals(
				expectedMin,
				bin.getBinMin(),
				DELTA);
		Assert.assertEquals(
				expectedMax,
				bin.getBinMax(),
				DELTA);
		Assert.assertEquals(
				bin.getBinMax(),
				bin.getBinnedValue(
						calendar.getTimeInMillis()).getNormalizedValue(),
				DELTA);
	}

	private BinningStrategy getStrategyByUnit(
			Unit unit ) {
		return new TimeDefinition(
				unit).getBinningStrategy();
	}

}
