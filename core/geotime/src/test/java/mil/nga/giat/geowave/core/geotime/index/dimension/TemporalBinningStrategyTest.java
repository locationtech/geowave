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
import mil.nga.giat.geowave.core.index.dimension.bin.BinRange;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TemporalBinningStrategyTest
{
	@Before
	public void setTimezoneToGMT() {
		TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
	}

	@Test
	public void testLargeNumberOfDayBins() {
		internalTestBinsMatchExpectedCount(
				250000,
				Unit.DAY,
				123456789876L);
	}

	@Test
	public void testLargeNumberOfMonthBins() {
		internalTestBinsMatchExpectedCount(
				250000,
				Unit.MONTH,
				9876543210L);
	}

	@Test
	public void testLargeNumberOfYearBins() {
		// for years, use 250,000 to keep milli time values less than max long
		internalTestBinsMatchExpectedCount(
				250000,
				Unit.YEAR,
				0L);
	}

	@Test
	public void testLargeNumberOfHourBins() {
		internalTestBinsMatchExpectedCount(
				250000,
				Unit.HOUR,
				0L);
	}

	@Test
	public void testLargeNumberOfMinuteBins() {
		internalTestBinsMatchExpectedCount(
				250000,
				Unit.MINUTE,
				0L);
	}

	private void internalTestBinsMatchExpectedCount(
			final int binCount,
			final Unit unit,
			final long arbitraryTime ) {
		final BinRange[] ranges = getBinRangesUsingFullExtents(
				binCount,
				unit,
				arbitraryTime);
		Assert.assertEquals(
				binCount,
				ranges.length);
	}

	private BinRange[] getBinRangesUsingFullExtents(
			final int binCount,
			final Unit unit,
			final long arbitraryTime ) {
		final Calendar startCal = Calendar.getInstance();
		final long time = arbitraryTime; // hopefully these approaches work for
											// any arbitrary time, but allow a
											// caller to set the specific time
											// so tests are all entirely
											// reproducible
		startCal.setTimeInMillis(time);
		final Calendar endCal = Calendar.getInstance();
		endCal.setTimeInMillis(time);
		final TemporalBinningStrategy binStrategy = new TemporalBinningStrategy(
				unit);
		binStrategy.setToEpoch(startCal);
		binStrategy.setToEpoch(endCal);
		endCal.add(
				unit.toCalendarEnum(),
				binCount);
		return binStrategy.getNormalizedRanges(new NumericRange(
				startCal.getTimeInMillis(),
				(double) endCal.getTimeInMillis() - 1));
	}

	@Test
	public void testFullExtentOnSingleBin() {
		final BinRange[] ranges = getBinRangesUsingFullExtents(
				1,
				Unit.MONTH,
				543210987654321L);

		Assert.assertEquals(
				1,
				ranges.length);
		Assert.assertTrue(ranges[0].isFullExtent());
	}

	@Test
	public void testFullExtentOnMultipleBins() {
		final Calendar startCal = Calendar.getInstance();
		final long time = 3456789012345L;
		startCal.setTimeInMillis(time);
		final Calendar endCal = Calendar.getInstance();
		// theoretically should get 3 bins back the first and last not having
		// full extent and the middle one being full extent
		endCal.setTimeInMillis(time + (TemporalBinningStrategy.MILLIS_PER_DAY * 2));
		final TemporalBinningStrategy binStrategy = new TemporalBinningStrategy(
				Unit.DAY);

		BinRange[] ranges = binStrategy.getNormalizedRanges(new NumericRange(
				startCal.getTimeInMillis(),
				endCal.getTimeInMillis()));
		Assert.assertEquals(
				3,
				ranges.length);

		Assert.assertTrue(!ranges[0].isFullExtent());

		Assert.assertTrue(ranges[1].isFullExtent());

		Assert.assertTrue(!ranges[2].isFullExtent());

		final Calendar startCalOnEpoch = Calendar.getInstance();

		startCalOnEpoch.setTimeInMillis(time);
		binStrategy.setToEpoch(startCalOnEpoch);

		ranges = binStrategy.getNormalizedRanges(new NumericRange(
				startCalOnEpoch.getTimeInMillis(),
				endCal.getTimeInMillis()));
		Assert.assertEquals(
				3,
				ranges.length);
		// now the first element should be full extent
		Assert.assertTrue(ranges[0].isFullExtent());

		Assert.assertTrue(ranges[1].isFullExtent());

		Assert.assertTrue(!ranges[2].isFullExtent());

		final Calendar endCalOnMax = Calendar.getInstance();
		// theoretically should get 3 bins back the first and last not having
		// full extent and the middle one being full extent
		endCalOnMax.setTimeInMillis(time + (TemporalBinningStrategy.MILLIS_PER_DAY * 3));
		binStrategy.setToEpoch(endCalOnMax);
		endCalOnMax.add(
				Calendar.MILLISECOND,
				-1);
		ranges = binStrategy.getNormalizedRanges(new NumericRange(
				startCal.getTimeInMillis(),
				endCalOnMax.getTimeInMillis()));
		Assert.assertEquals(
				3,
				ranges.length);

		Assert.assertTrue(!ranges[0].isFullExtent());

		Assert.assertTrue(ranges[1].isFullExtent());

		// now the last element should be full extent
		Assert.assertTrue(ranges[2].isFullExtent());
	}

	@Test
	public void testStartOnEpochMinusOneAndEndOnEpoch() {
		final Calendar startCal = Calendar.getInstance();
		// final long time = 675849302912837456L; //this value would cause it to
		// fail because we lose precision in coverting to a double (the mantissa
		// of a double value is 52 bits and therefore the max long that it can
		// accurately represent is 2^52 before the ulp of the double becomes
		// greater than 1)
		final long time = 6758493029128L;
		startCal.setTimeInMillis(time);
		startCal.set(
				Calendar.MONTH,
				0);// make sure its a month after one with 31 days
		final TemporalBinningStrategy binStrategy = new TemporalBinningStrategy(
				Unit.MONTH);
		binStrategy.setToEpoch(startCal);
		final Calendar endCal = Calendar.getInstance();
		endCal.setTimeInMillis(time);
		endCal.set(
				Calendar.MONTH,
				0);// make sure its a month after one with 31 days
		binStrategy.setToEpoch(endCal);
		final BinRange[] ranges = binStrategy.getNormalizedRanges(new NumericRange(
				startCal.getTimeInMillis() - 1,
				endCal.getTimeInMillis()));

		Assert.assertEquals(
				2,
				ranges.length);

		// the first range should be the max possible value and both the min and
		// max of the range should be equal
		Assert.assertTrue(ranges[0].getNormalizedMax() == binStrategy.getBinMax());
		Assert.assertTrue(ranges[0].getNormalizedMin() == ranges[0].getNormalizedMax());

		// the second range should be the min possible value and both the min
		// and max of the range should be equal
		Assert.assertTrue(ranges[1].getNormalizedMin() == binStrategy.getBinMin());
		Assert.assertTrue(ranges[0].getNormalizedMax() == ranges[0].getNormalizedMin());
	}

	@Test
	public void testStartAndEndEqual() {
		final long time = 123987564019283L;
		final TemporalBinningStrategy binStrategy = new TemporalBinningStrategy(
				Unit.YEAR);
		final BinRange[] ranges = binStrategy.getNormalizedRanges(new NumericRange(
				time,
				time));

		Assert.assertEquals(
				1,
				ranges.length);
		// both the min and max of the range should be equal
		Assert.assertTrue(ranges[0].getNormalizedMin() == ranges[0].getNormalizedMax());
	}

	@Test
	public void testEndLessThanStart() {
		final long time = 123987564019283L;
		final TemporalBinningStrategy binStrategy = new TemporalBinningStrategy(
				Unit.YEAR);
		final BinRange[] ranges = binStrategy.getNormalizedRanges(new NumericRange(
				time,
				time - 1));

		Assert.assertEquals(
				0,
				ranges.length);
	}

	@Test
	public void testFeb28ToMarch1NonLeapYear() {
		final long time = 47920164930285667L;
		final Calendar startCal = Calendar.getInstance();
		startCal.setTimeInMillis(time);
		startCal.set(
				Calendar.MONTH,
				1);
		startCal.set(
				Calendar.YEAR,
				2015);
		startCal.set(
				Calendar.DAY_OF_MONTH,
				28);

		final Calendar endCal = Calendar.getInstance();
		endCal.setTimeInMillis(time);
		endCal.set(
				Calendar.MONTH,
				2);
		endCal.set(
				Calendar.YEAR,
				2015);
		endCal.set(
				Calendar.DAY_OF_MONTH,
				1);
		// test the day boundaries first - going from feb28 to march 1 should
		// give 2 bins
		TemporalBinningStrategy binStrategy = new TemporalBinningStrategy(
				Unit.DAY);
		BinRange[] ranges = binStrategy.getNormalizedRanges(new NumericRange(
				startCal.getTimeInMillis(),
				endCal.getTimeInMillis()));

		Assert.assertEquals(
				2,
				ranges.length);

		// now test the month boundaries - adding a day to feb28 for the end
		// time should give 2 bins
		binStrategy = new TemporalBinningStrategy(
				Unit.MONTH);
		ranges = binStrategy.getNormalizedRanges(new NumericRange(
				startCal.getTimeInMillis(),
				startCal.getTimeInMillis() + (TemporalBinningStrategy.MILLIS_PER_DAY)));

		Assert.assertEquals(
				2,
				ranges.length);
	}

	@Test
	public void testFeb28ToMarch1LeapYear() {
		final long time = 29374659120374656L;
		final Calendar startCal = Calendar.getInstance();
		startCal.setTimeInMillis(time);
		startCal.set(
				Calendar.MONTH,
				1);
		startCal.set(
				Calendar.YEAR,
				2016);
		startCal.set(
				Calendar.DAY_OF_MONTH,
				28);

		final Calendar endCal = Calendar.getInstance();
		endCal.setTimeInMillis(time);
		endCal.set(
				Calendar.MONTH,
				2);
		endCal.set(
				Calendar.YEAR,
				2016);
		endCal.set(
				Calendar.DAY_OF_MONTH,
				1);
		// test the day boundaries first - going from feb28 to march 1 should
		// give 3 bins
		TemporalBinningStrategy binStrategy = new TemporalBinningStrategy(
				Unit.DAY);
		BinRange[] ranges = binStrategy.getNormalizedRanges(new NumericRange(
				startCal.getTimeInMillis(),
				endCal.getTimeInMillis()));

		Assert.assertEquals(
				3,
				ranges.length);

		// now test the month boundaries - adding a day to feb28 for the end
		// time should give 1 bin, adding 2 days should give 2 bins
		binStrategy = new TemporalBinningStrategy(
				Unit.MONTH);
		ranges = binStrategy.getNormalizedRanges(new NumericRange(
				startCal.getTimeInMillis(),
				startCal.getTimeInMillis() + (TemporalBinningStrategy.MILLIS_PER_DAY)));

		Assert.assertEquals(
				1,
				ranges.length);
		// add 2 days and now we should end up with 2 bins
		ranges = binStrategy.getNormalizedRanges(new NumericRange(
				startCal.getTimeInMillis(),
				startCal.getTimeInMillis() + (TemporalBinningStrategy.MILLIS_PER_DAY * 2)));

		Assert.assertEquals(
				2,
				ranges.length);
	}

	@Test
	public void testNonLeapYear() {
		final long time = 75470203439504394L;
		final TemporalBinningStrategy binStrategy = new TemporalBinningStrategy(
				Unit.YEAR);
		final Calendar startCal = Calendar.getInstance();
		startCal.setTimeInMillis(time);
		startCal.set(
				Calendar.YEAR,
				2015);
		binStrategy.setToEpoch(startCal);
		// if we add 365 days to this we should get 2 year bins
		final BinRange[] ranges = binStrategy.getNormalizedRanges(new NumericRange(
				startCal.getTimeInMillis(),
				startCal.getTimeInMillis() + (TemporalBinningStrategy.MILLIS_PER_DAY * 365)));
		Assert.assertEquals(
				2,
				ranges.length);
	}

	@Test
	public void testLeapYear() {
		final long time = 94823024856598633L;
		final TemporalBinningStrategy binStrategy = new TemporalBinningStrategy(
				Unit.YEAR);
		final Calendar startCal = Calendar.getInstance();
		startCal.setTimeInMillis(time);
		startCal.set(
				Calendar.YEAR,
				2016);
		binStrategy.setToEpoch(startCal);
		// if we add 365 days to this we should get 1 year bin
		BinRange[] ranges = binStrategy.getNormalizedRanges(new NumericRange(
				startCal.getTimeInMillis(),
				startCal.getTimeInMillis() + (TemporalBinningStrategy.MILLIS_PER_DAY * 365)));
		Assert.assertEquals(
				1,
				ranges.length);
		// if we add 366 days to this we should get 2 year bins, and the second
		// bin should be the epoch
		ranges = binStrategy.getNormalizedRanges(new NumericRange(
				startCal.getTimeInMillis(),
				startCal.getTimeInMillis() + (TemporalBinningStrategy.MILLIS_PER_DAY * 366)));
		Assert.assertEquals(
				2,
				ranges.length);

		// the second bin should just contain the epoch
		Assert.assertTrue(ranges[1].getNormalizedMin() == ranges[1].getNormalizedMax());
		Assert.assertTrue(ranges[1].getNormalizedMin() == binStrategy.getBinMin());
	}
}
