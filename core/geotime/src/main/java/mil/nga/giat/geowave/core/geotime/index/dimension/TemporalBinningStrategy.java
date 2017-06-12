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

import java.nio.ByteBuffer;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

import mil.nga.giat.geowave.core.index.FloatCompareUtils;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.index.dimension.bin.BinRange;
import mil.nga.giat.geowave.core.index.dimension.bin.BinValue;
import mil.nga.giat.geowave.core.index.dimension.bin.BinningStrategy;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * This class is useful for establishing a consistent binning strategy using a
 * unit of time. Each bin will then be defined by the boundaries of that unit
 * within the timezone given in the constructor. So if the unit is year and the
 * data spreads across 2011-2013, the bins will be 2011, 2012, and 2013. The
 * unit chosen should represent a much more significant range than the average
 * query range (at least 20x larger) for efficiency purposes. So if the average
 * query is for a 24 hour period, the unit should not be a day, but could be
 * perhaps a month or a year (depending on the temporal extent of the dataset).
 */
public class TemporalBinningStrategy implements
		BinningStrategy
{
	public static enum Unit {
		MINUTE(
				Calendar.MINUTE),
		HOUR(
				Calendar.HOUR_OF_DAY),
		DAY(
				Calendar.DAY_OF_MONTH),
		WEEK(
				Calendar.WEEK_OF_YEAR),
		MONTH(
				Calendar.MONTH),
		YEAR(
				Calendar.YEAR),
		DECADE(
				-1);
		// java.util.Calendar does not define a field number for decade
		// use -1 since that value is unused

		private final int calendarEnum;

		private Unit(
				final int calendarEnum ) {
			this.calendarEnum = calendarEnum;
		}

		public int toCalendarEnum() {
			return calendarEnum;
		}

		public static Unit getUnit(
				final int calendarEnum ) {
			for (final Unit u : values()) {
				if (u.calendarEnum == calendarEnum) {
					return u;
				}
			}
			throw new IllegalArgumentException(
					"Calendar enum '" + calendarEnum + "' not found as a valid unit ");
		}

		// converter that will be used later
		public static Unit fromString(
				final String code ) {

			for (final Unit output : Unit.values()) {
				if (output.toString().equalsIgnoreCase(
						code)) {
					return output;
				}
			}

			return null;
		}
	}

	protected static final long MILLIS_PER_DAY = 86400000L;
	private static final NumberFormat TWO_DIGIT_NUMBER = NumberFormat.getIntegerInstance();
	{
		TWO_DIGIT_NUMBER.setMinimumIntegerDigits(2);
		TWO_DIGIT_NUMBER.setMaximumIntegerDigits(2);
	}

	private Unit unit;
	private String timezone;

	public TemporalBinningStrategy() {}

	public TemporalBinningStrategy(
			final Unit unit ) {
		this(
				unit,
				"GMT");
	}

	public TemporalBinningStrategy(
			final Unit unit,
			final String timezone ) {
		this.unit = unit;
		this.timezone = timezone;
	}

	@Override
	public double getBinMin() {
		return 0;
	}

	@Override
	public double getBinMax() {
		return getBinSizeMillis() - 1;
	}

	/**
	 * Method used to bin a raw date in milliseconds to a binned value of the
	 * Binning Strategy.
	 */
	@Override
	public BinValue getBinnedValue(
			final double value ) {
		// convert to a calendar and subtract the epoch for the bin
		final Calendar epochCal = Calendar.getInstance(TimeZone.getTimeZone(timezone));
		epochCal.setTimeInMillis((long) value);
		setToEpoch(epochCal);
		// use the value to get the bin ID (although the epoch should work fine
		// too)
		final Calendar valueCal = Calendar.getInstance(TimeZone.getTimeZone(timezone));
		valueCal.setTimeInMillis((long) value);

		return new BinValue(
				getBinId(valueCal),
				valueCal.getTimeInMillis() - epochCal.getTimeInMillis());
	}

	private long getBinSizeMillis() {
		long binSizeMillis = MILLIS_PER_DAY;
		// use the max possible value for that unit as the bin size
		switch (unit) {
			case DECADE:
				binSizeMillis *= 3653;
				break;
			case YEAR:
			default:
				binSizeMillis *= 366;
				break;
			case MONTH:
				binSizeMillis *= 31;
				break;
			case WEEK:
				binSizeMillis *= 7;
				break;
			case DAY:
				break;
			case HOUR:
				binSizeMillis /= 24;
				break;
			case MINUTE:
				binSizeMillis /= 1440;
				break;
		}
		return binSizeMillis;

	}

	@SuppressFBWarnings(value = {
		"SF_SWITCH_FALLTHROUGH",
		"SF_SWITCH_NO_DEFAULT"
	}, justification = "Fallthrough intentional for time parsing; default case is provided")
	protected void setToEpoch(
			final Calendar value ) {
		// reset appropriate values to 0 based on the unit
		switch (unit) {
			case DECADE:
				value.set(
						Calendar.YEAR,
						((value.get(Calendar.YEAR) / 10) * 10));
				// don't break so that the other fields are also set to the
				// minimum
			case YEAR:
			default:
				value.set(
						Calendar.MONTH,
						value.getActualMinimum(Calendar.MONTH));
				// don't break so that the other fields are also set to the
				// minimum
			case MONTH:
				value.set(
						Calendar.DAY_OF_MONTH,
						value.getActualMinimum(Calendar.DAY_OF_MONTH));
				// don't break so that the other fields are also set to the
				// minimum
			case DAY:
				value.set(
						Calendar.HOUR_OF_DAY,
						value.getActualMinimum(Calendar.HOUR_OF_DAY));
				// don't break so that the other fields are also set to the
				// minimum
			case HOUR:
				value.set(
						Calendar.MINUTE,
						value.getActualMinimum(Calendar.MINUTE));
				// don't break so that the other fields are also set to the
				// minimum
			case MINUTE:
				value.set(
						Calendar.SECOND,
						value.getActualMinimum(Calendar.SECOND));
				value.set(
						Calendar.MILLISECOND,
						value.getActualMinimum(Calendar.MILLISECOND));
				break; // special handling for week
			case WEEK:
				value.set(
						Calendar.DAY_OF_WEEK,
						value.getActualMinimum(Calendar.DAY_OF_WEEK));
				value.set(
						Calendar.HOUR_OF_DAY,
						value.getActualMinimum(Calendar.HOUR_OF_DAY));
				value.set(
						Calendar.MINUTE,
						value.getActualMinimum(Calendar.MINUTE));
				value.set(
						Calendar.SECOND,
						value.getActualMinimum(Calendar.SECOND));
				value.set(
						Calendar.MILLISECOND,
						value.getActualMinimum(Calendar.MILLISECOND));
		}
	}

	@Override
	public int getFixedBinIdSize() {
		switch (unit) {
			case YEAR:
			default:
				return 4;
			case MONTH:
				return 7;
			case WEEK:
				return 7;
			case DAY:
				return 10;
			case HOUR:
				return 13;
			case MINUTE:
				return 16;
		}
	}

	private byte[] getBinId(
			final Calendar value ) {
		// this is assuming we want human-readable bin ID's but alternatively we
		// could consider returning a more compressed representation
		switch (unit) {
			case YEAR:
			default:
				return StringUtils.stringToBinary(Integer.toString(value.get(Calendar.YEAR)));
			case MONTH:
				return StringUtils.stringToBinary((Integer.toString(value.get(Calendar.YEAR)) + "_" + TWO_DIGIT_NUMBER
						.format(value.get(Calendar.MONTH))));
			case WEEK:
				return StringUtils.stringToBinary(Integer.toString(value.get(Calendar.YEAR)) + "_"
						+ TWO_DIGIT_NUMBER.format(value.get(Calendar.WEEK_OF_YEAR)));
			case DAY:
				return StringUtils.stringToBinary((Integer.toString(value.get(Calendar.YEAR)) + "_"
						+ TWO_DIGIT_NUMBER.format(value.get(Calendar.MONTH)) + "_" + TWO_DIGIT_NUMBER.format(value
						.get(Calendar.DAY_OF_MONTH))));
			case HOUR:
				return StringUtils.stringToBinary((Integer.toString(value.get(Calendar.YEAR)) + "_"
						+ TWO_DIGIT_NUMBER.format(value.get(Calendar.MONTH)) + "_"
						+ TWO_DIGIT_NUMBER.format(value.get(Calendar.DAY_OF_MONTH)) + "_" + TWO_DIGIT_NUMBER
						.format(value.get(Calendar.HOUR_OF_DAY))));
			case MINUTE:
				return StringUtils.stringToBinary((Integer.toString(value.get(Calendar.YEAR)) + "_"
						+ TWO_DIGIT_NUMBER.format(value.get(Calendar.MONTH)) + "_"
						+ TWO_DIGIT_NUMBER.format(value.get(Calendar.DAY_OF_MONTH)) + "_"
						+ TWO_DIGIT_NUMBER.format(value.get(Calendar.HOUR_OF_DAY)) + "_" + TWO_DIGIT_NUMBER
						.format(value.get(Calendar.MINUTE))));
		}
	}

	@SuppressFBWarnings(value = {
		"SF_SWITCH_FALLTHROUGH",
		"SF_SWITCH_NO_DEFAULT"
	}, justification = "Fallthrough intentional for time parsing")
	private Calendar getStartEpoch(
			final byte[] binId ) {
		final String str = StringUtils.stringFromBinary(binId);
		final Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(timezone));
		switch (unit) {
			case MINUTE:
				final int minute = Integer.parseInt(str.substring(
						14,
						16));
				cal.set(
						Calendar.MINUTE,
						minute);
			case HOUR:
				final int hour = Integer.parseInt(str.substring(
						11,
						13));
				cal.set(
						Calendar.HOUR_OF_DAY,
						hour);
			case DAY:
				final int day = Integer.parseInt(str.substring(
						8,
						10));
				cal.set(
						Calendar.DAY_OF_MONTH,
						day);
			case MONTH:
				final int month = Integer.parseInt(str.substring(
						5,
						7));
				cal.set(
						Calendar.MONTH,
						month);
			case YEAR:
			default:
				final int year = Integer.parseInt(str.substring(
						0,
						4));
				cal.set(
						Calendar.YEAR,
						year);
				break; // do not automatically fall-through to decade parsing
			case DECADE:
				int decade = Integer.parseInt(str.substring(
						0,
						4));
				decade = (decade / 10) * 10; // int division will truncate ones
				cal.set(
						Calendar.YEAR,
						decade);
				break; // special handling for week
			case WEEK:
				final int yr = Integer.parseInt(str.substring(
						0,
						4));
				cal.set(
						Calendar.YEAR,
						yr);
				final int weekOfYear = Integer.parseInt(str.substring(
						5,
						7));
				cal.set(
						Calendar.WEEK_OF_YEAR,
						weekOfYear);
				break;
		}
		setToEpoch(cal);
		return cal;
	}

	@Override
	public BinRange[] getNormalizedRanges(
			final NumericData range ) {
		if (range.getMax() < range.getMin()) {
			return new BinRange[] {};
		}
		final Calendar startEpoch = Calendar.getInstance(TimeZone.getTimeZone(timezone));
		final long binSizeMillis = getBinSizeMillis();
		// initialize the epoch to the range min and then reset appropriate
		// values to 0 based on the units
		startEpoch.setTimeInMillis((long) range.getMin());
		setToEpoch(startEpoch);
		// now make sure all bin definitions between the start and end bins
		// are covered
		final long startEpochMillis = startEpoch.getTimeInMillis();
		long epochIterator = startEpochMillis;
		final List<BinRange> bins = new ArrayList<BinRange>();
		// track this, so that we can easily declare a range to be the full
		// extent and use the information to perform a more efficient scan
		boolean firstBin = ((long) range.getMin() != startEpochMillis);
		boolean lastBin = false;
		do {
			// because not every year has 366 days, and not every month has 31
			// days we need to reset next epoch to the actual epoch
			final Calendar nextEpochCal = Calendar.getInstance(TimeZone.getTimeZone(timezone));
			// set it to a value in the middle of the bin just to be sure (for
			// example if the bin size does not get to the next epoch as is
			// the case when units are days and the timezone accounts for
			// daylight savings time)
			nextEpochCal.setTimeInMillis(epochIterator + (long) (binSizeMillis * 1.5));
			setToEpoch(nextEpochCal);
			final long nextEpoch = nextEpochCal.getTimeInMillis();
			final long maxOfBin = nextEpoch - 1;
			final Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(timezone));
			cal.setTimeInMillis(epochIterator);
			long startMillis, endMillis;
			boolean fullExtent;
			if ((long) range.getMax() <= maxOfBin) {
				lastBin = true;
				endMillis = (long) range.getMax();
				// its questionable whether we use
				fullExtent = FloatCompareUtils.checkDoublesEqual(
						range.getMax(),
						maxOfBin);
			}
			else {
				endMillis = maxOfBin;
				fullExtent = !firstBin;
			}

			if (firstBin) {
				startMillis = (long) range.getMin();
				firstBin = false;
			}
			else {
				startMillis = epochIterator;
			}
			// we have the millis for range, but to normalize for this bin we
			// need to subtract the epoch of the bin
			bins.add(new BinRange(
					getBinId(cal),
					startMillis - epochIterator,
					endMillis - epochIterator,
					fullExtent));
			epochIterator = nextEpoch;
			// iterate until we reach our end epoch
		}
		while (!lastBin);
		return bins.toArray(new BinRange[bins.size()]);
	}

	@Override
	public byte[] toBinary() {
		final byte[] timeZone = StringUtils.stringToBinary(timezone);
		final ByteBuffer binary = ByteBuffer.allocate(timezone.length() + 4);
		binary.putInt(unit.calendarEnum);
		binary.put(timeZone);
		return binary.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buffer = ByteBuffer.wrap(bytes);
		final int unitCalendarEnum = buffer.getInt();
		final byte[] timeZoneName = new byte[bytes.length - 4];
		buffer.get(timeZoneName);
		unit = Unit.getUnit(unitCalendarEnum);
		timezone = StringUtils.stringFromBinary(timeZoneName);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		final String className = getClass().getName();
		result = (prime * result) + ((className == null) ? 0 : className.hashCode());
		result = (prime * result) + ((timezone == null) ? 0 : timezone.hashCode());
		result = (prime * result) + ((unit == null) ? 0 : unit.calendarEnum);
		return result;
	}

	@Override
	public boolean equals(
			final Object obj ) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final TemporalBinningStrategy other = (TemporalBinningStrategy) obj;
		if (timezone == null) {
			if (other.timezone != null) {
				return false;
			}
		}
		else if (!timezone.equals(other.timezone)) {
			return false;
		}
		if (unit == null) {
			if (other.unit != null) {
				return false;
			}
		}
		else if (unit.calendarEnum != other.unit.calendarEnum) {
			return false;
		}
		return true;
	}

	@Override
	public NumericRange getDenormalizedRanges(
			final BinRange binnedRange ) {
		final Calendar startofEpoch = getStartEpoch(binnedRange.getBinId());
		final long startOfEpochMillis = startofEpoch.getTimeInMillis();
		final long minMillis = startOfEpochMillis + (long) binnedRange.getNormalizedMin();
		final long maxMillis = startOfEpochMillis + (long) binnedRange.getNormalizedMax();
		return new NumericRange(
				minMillis,
				maxMillis);
	}
}
