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
 * Concrete implementation defining a single numeric value associated with a
 * space filling curve.
 * 
 */
public class NumericValue implements
		NumericData
{
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	private double value;

	public NumericValue() {}

	/**
	 * Constructor used to create a new NumericValue object
	 * 
	 * @param value
	 *            the particular numeric value
	 */
	public NumericValue(
			final double value ) {
		this.value = value;
	}

	/**
	 * 
	 * @return value the value of a numeric value object
	 */
	@Override
	public double getMin() {
		return value;
	}

	/**
	 * 
	 * @return value the value of a numeric value object
	 */
	@Override
	public double getMax() {
		return value;
	}

	/**
	 * 
	 * @return value the value of a numeric value object
	 */
	@Override
	public double getCentroid() {
		return value;
	}

	/**
	 * Determines if this object is a range or not
	 */
	@Override
	public boolean isRange() {
		return false;
	}

	@Override
	public String toString() {
		return "NumericRange [value=" + value + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(value);
		result = (prime * result) + (int) (temp ^ (temp >>> 32));
		return result;
	}

	protected static final double EPSILON = 1E-10;

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
		final NumericValue other = (NumericValue) obj;
		return (Math.abs(value - other.value) < EPSILON);
	}

	@Override
	public byte[] toBinary() {
		final ByteBuffer buf = ByteBuffer.allocate(8);
		buf.putDouble(value);
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		value = buf.getDouble();
	}
}
