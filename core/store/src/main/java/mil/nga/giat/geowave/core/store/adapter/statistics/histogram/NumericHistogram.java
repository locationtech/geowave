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

	/**
	 * 
	 * @return the total number of consumed values
	 */

	public long getTotalCount();

	/**
	 * 
	 * @return the number of bins used
	 */
	public int getNumBins();

	public void merge(
			final NumericHistogram other );

	/**
	 * @param v
	 *            The data point to add to the histogram approximation.
	 */
	public void add(
			final double v );

	public void add(
			long count,
			double v );

	/**
	 * 
	 * @return The quantiles over the given number of bins.
	 */
	public double[] quantile(
			final int bins );

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
	 * Estimate number of values consumed up to provided value.
	 * 
	 * @param val
	 * @return the number of estimated points
	 */
	public double sum(
			final double val,
			boolean inclusive );

	public double cdf(
			final double val );

	public long[] count(
			final int bins );

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
}
