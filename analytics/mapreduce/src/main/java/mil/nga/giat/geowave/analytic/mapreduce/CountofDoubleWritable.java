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
package mil.nga.giat.geowave.analytic.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Used for (1) representation of collections (2) summation in a combiner (3)
 * and finally, for computation of averages
 */
public class CountofDoubleWritable implements
		Writable,
		WritableComparable
{

	private double value = 0.0;
	private double count = 0.0;

	public CountofDoubleWritable() {

	}

	public CountofDoubleWritable(
			final double value,
			final double count ) {
		set(
				value,
				count);
	}

	@Override
	public void readFields(
			final DataInput in )
			throws IOException {
		value = in.readDouble();
		count = in.readDouble();
	}

	@Override
	public void write(
			final DataOutput out )
			throws IOException {
		out.writeDouble(value);
		out.writeDouble(count);
	}

	public void set(
			final double value,
			final double count ) {
		this.value = value;
		this.count = count;
	}

	public double getValue() {
		return value;
	}

	public double getCount() {
		return count;
	}

	/**
	 * Returns true iff <code>o</code> is a DoubleWritable with the same value.
	 */
	@Override
	public boolean equals(
			final Object o ) {
		if (!(o instanceof CountofDoubleWritable)) {
			return false;
		}
		return compareTo(o) == 0;
	}

	@Override
	public int hashCode() {
		return (int) Double.doubleToLongBits(value / count);
	}

	@Override
	public int compareTo(
			final Object o ) {
		final CountofDoubleWritable other = (CountofDoubleWritable) o;
		final double diff = (value / count) - (other.value / other.count);
		return (Math.abs(diff) < 0.0000001) ? 0 : (diff < 0 ? -1 : 0);
	}

	@Override
	public String toString() {
		return Double.toString(value) + "/" + Double.toString(count);
	}

	/** A Comparator optimized for DoubleWritable. */
	public static class Comparator extends
			WritableComparator implements
			Serializable
	{
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public Comparator() {
			super(
					CountofDoubleWritable.class);
		}

		@Override
		public int compare(
				final byte[] b1,
				final int s1,
				final int l1,
				final byte[] b2,
				final int s2,
				final int l2 ) {
			final double thisValue = readDouble(
					b1,
					s1);
			final double thatValue = readDouble(
					b2,
					s2);
			return Double.compare(
					thisValue,
					thatValue);
		}
	}

	static { // register this comparator
		WritableComparator.define(
				CountofDoubleWritable.class,
				new Comparator());
	}
}
