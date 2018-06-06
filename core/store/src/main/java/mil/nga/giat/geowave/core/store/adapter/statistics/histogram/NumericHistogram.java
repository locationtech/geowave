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
package mil.nga.giat.geowave.core.store.adapter.statistics.histogram;

import java.nio.ByteBuffer;

public interface NumericHistogram
{
	public void merge(
			final NumericHistogram other );

	/**
	 * @param v
	 *            The data point to add to the histogram approximation.
	 */
	public void add(
			final double v );

	/**
	 * Gets an approximate quantile value from the current histogram. Some
	 * popular quantiles are 0.5 (median), 0.95, and 0.98.
	 *
	 * @param q
	 *            The requested quantile, must be strictly within the range
	 *            (0,1).
	 * @return The quantile value.
	 */
	public double quantile(
			final double q );

	/**
	 *
	 * Returns the fraction of all points added which are <= x.
	 *
	 * @return the cumulative distribution function (cdf) result
	 */
	public double cdf(
			final double val );

	/**
	 * Estimate number of values consumed up to provided value.
	 *
	 * @param val
	 * @return the number of estimated points
	 */
	public double sum(
			final double val,
			boolean inclusive );

	/**
	 *
	 * @return the amount of byte buffer space to serialize this histogram
	 */
	public int bufferSize();

	public void toBinary(
			final ByteBuffer buffer );

	public void fromBinary(
			final ByteBuffer buffer );

	public double getMaxValue();

	public double getMinValue();

	public long getTotalCount();
}
