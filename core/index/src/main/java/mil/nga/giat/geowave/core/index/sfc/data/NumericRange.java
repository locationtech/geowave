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

import java.nio.ByteBuffer;

/**
 * Concrete implementation defining a numeric range associated with a space
 * filling curve.
 * 
 */
public class NumericRange implements
		NumericData
{
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	private double min;
	private double max;

	public NumericRange() {}

	/**
	 * Constructor used to create a IndexRange object
	 * 
	 * @param min
	 *            the minimum bounds of a unique index range
	 * @param max
	 *            the maximum bounds of a unique index range
	 */
	public NumericRange(
			final double min,
			final double max ) {
		this.min = min;
		this.max = max;
	}

	/**
	 * 
	 * @return min the minimum bounds of a index range object
	 */
	@Override
	public double getMin() {
		return min;
	}

	/**
	 * 
	 * @return max the maximum bounds of a index range object
	 */
	@Override
	public double getMax() {
		return max;
	}

	/**
	 * 
	 * @return centroid the center of a unique index range object
	 */
	@Override
	public double getCentroid() {
		return (min + max) / 2;
	}

	/**
	 * Flag to determine if the object is a range
	 */
	@Override
	public boolean isRange() {
		return true;
	}

	@Override
	public String toString() {
		return "NumericRange [min=" + min + ", max=" + max + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(max);
		result = (prime * result) + (int) (temp ^ (temp >>> 32));
		temp = Double.doubleToLongBits(min);
		result = (prime * result) + (int) (temp ^ (temp >>> 32));
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
		// changing this check will fail some unit tests.
		if (!NumericRange.class.isAssignableFrom(obj.getClass())) {
			return false;
		}
		final NumericRange other = (NumericRange) obj;
		return (Math.abs(max - other.max) < NumericValue.EPSILON) && (Math.abs(min - other.min) < NumericValue.EPSILON);
	}

	@Override
	public byte[] toBinary() {
		final ByteBuffer buf = ByteBuffer.allocate(16);
		buf.putDouble(min);
		buf.putDouble(max);
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		min = buf.getDouble();
		max = buf.getDouble();
	}
}
