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
package mil.nga.giat.geowave.core.geotime.store.dimension;

import java.nio.ByteBuffer;

import mil.nga.giat.geowave.core.index.persist.Persistable;
import mil.nga.giat.geowave.core.index.sfc.data.NumericData;
import mil.nga.giat.geowave.core.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.core.index.sfc.data.NumericValue;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;

/**
 * The base interface for time values, could be either a time range (an
 * interval) or a timestamp (an instant)
 * 
 */
public interface Time extends
		Persistable,
		CommonIndexValue
{
	/**
	 * A range of time. This class wraps a start and stop instant in
	 * milliseconds with a visibility tag for the field value.
	 * 
	 */
	public static class TimeRange implements
			Time
	{
		private long startTime;
		private long endTime;
		private byte[] visibility;

		public TimeRange() {}

		public TimeRange(
				final long startTime,
				final long endTime,
				final byte[] visibility ) {
			this.startTime = startTime;
			this.endTime = endTime;
			this.visibility = visibility;
		}

		@Override
		public byte[] getVisibility() {
			return visibility;
		}

		@Override
		public void setVisibility(
				final byte[] visibility ) {
			this.visibility = visibility;
		}

		@Override
		public NumericData toNumericData() {
			return new NumericRange(
					startTime,
					endTime);
		}

		@Override
		public byte[] toBinary() {
			final ByteBuffer bytes = ByteBuffer.allocate(16);
			bytes.putLong(startTime);
			bytes.putLong(endTime);
			return bytes.array();
		}

		@Override
		public void fromBinary(
				final byte[] bytes ) {
			final ByteBuffer buf = ByteBuffer.wrap(bytes);
			startTime = buf.getLong();
			endTime = buf.getLong();
		}

		@Override
		public boolean overlaps(
				final NumericDimensionField[] field,
				final NumericData[] rangeData ) {
			assert (field[0] instanceof TimeField);
			long t0 = (long) Math.ceil(rangeData[0].getMax()) - this.startTime;
			long t1 = this.endTime - (long) Math.floor(rangeData[0].getMin());
			return Math.abs(t0 - t1) <= (t0 + t1);

		}
	}

	/**
	 * An instant of time in milliseconds wrapped with a visibility tag for the
	 * field value.
	 * 
	 */
	public static class Timestamp implements
			Time
	{
		private long time;
		private byte[] visibility;

		public Timestamp() {}

		public Timestamp(
				final long time,
				final byte[] visibility ) {
			this.time = time;
			this.visibility = visibility;
		}

		@Override
		public byte[] getVisibility() {
			return visibility;
		}

		@Override
		public void setVisibility(
				final byte[] visibility ) {
			this.visibility = visibility;
		}

		@Override
		public NumericData toNumericData() {
			return new NumericValue(
					time);
		}

		@Override
		public byte[] toBinary() {
			final ByteBuffer bytes = ByteBuffer.allocate(8);
			bytes.putLong(time);
			return bytes.array();
		}

		@Override
		public void fromBinary(
				final byte[] bytes ) {
			final ByteBuffer buf = ByteBuffer.wrap(bytes);
			time = buf.getLong();
		}

		@Override
		public boolean overlaps(
				final NumericDimensionField[] field,
				final NumericData[] rangeData ) {
			assert (field[0] instanceof TimeField);
			return (long) Math.floor(rangeData[0].getMin()) <= this.time
					&& (long) Math.ceil(rangeData[0].getMax()) >= this.time;
		}
	}

	public NumericData toNumericData();
}
