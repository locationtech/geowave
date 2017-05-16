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

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

/**
 * Dynamic Histogram:
 * 
 * Derived from work for Hive and based on Yael Ben-Haim and Elad Tom-Tov,
 * "A streaming parallel decision tree algorithm", J. Machine Learning Research
 * 11 (2010), pp. 849--872.
 * 
 * Note: the paper refers to a bins as a pair (p,m) where p = lower bound and m
 * = count. Some of the interpolation treats the pair as a coordinate.
 * 
 * Although there are no approximation guarantees, it appears to work well with
 * adequate data and a large number of histogram bins.
 */
public class MinimalBinDistanceHistogram implements
		NumericHistogram
{

	// Class variables
	private int nbins = 1024; // the fix maximum number of bins to maintain
	private long totalCount; // cache to avoid counting all the bins
	private ArrayList<Bin> bins;
	private final Random prng;
	private double maxValue; // the maximum value consumed

	/**
	 * Creates a new histogram object.
	 */
	public MinimalBinDistanceHistogram() {
		totalCount = 0;

		// init the RNG for breaking ties in histogram merging.
		prng = new Random(
				System.currentTimeMillis());

		bins = new ArrayList<Bin>(
				1024);
	}

	/**
	 * Creates a new histogram object.
	 */
	public MinimalBinDistanceHistogram(
			final int size ) {
		totalCount = 0;

		// init the RNG for breaking ties in histogram merging.
		prng = new Random(
				System.currentTimeMillis());

		bins = new ArrayList<Bin>(
				size);
		nbins = size;
	}

	/**
	 * Resets a histogram object to its initial state.
	 */
	public void reset() {
		bins.clear();
		totalCount = 0;
	}

	/**
	 * 
	 * @return the total number of consumed values
	 */

	public long getTotalCount() {
		return totalCount;
	}

	/**
	 * 
	 * @return the number of bins used
	 */
	public int getNumBins() {
		return bins.size();
	}

	/**
	 * 
	 * @param other
	 *            A serialized histogram created by the serialize() method
	 * @see #merge
	 */
	public void merge(
			final NumericHistogram other ) {
		if (other == null) {
			return;
		}

		MinimalBinDistanceHistogram myTypeOfHist = (MinimalBinDistanceHistogram) other;

		totalCount += myTypeOfHist.totalCount;
		maxValue = Math.max(
				myTypeOfHist.maxValue,
				maxValue);
		if ((nbins == 0) || (bins.size() == 0)) {
			// Just make a copy
			bins = new ArrayList<Bin>(
					myTypeOfHist.bins.size());
			for (final Bin coord : myTypeOfHist.bins) {
				bins.add(coord);
			}
			// the constrained bin sizes may not match
			trim();
		}
		else {
			// The aggregation buffer already contains a partial histogram.
			// Merge using Algorithm #2 from the Ben-Haim and
			// Tom-Tov paper.

			final ArrayList<Bin> mergedBins = new ArrayList<Bin>(
					getNumBins() + other.getNumBins());
			mergedBins.addAll(bins);
			for (final Bin oldBin : myTypeOfHist.bins) {
				mergedBins.add(new Bin(
						oldBin.lowerBound,
						oldBin.count));
			}
			Collections.sort(mergedBins);

			bins = mergedBins;

			// Now trim the overstuffed histogram down to the correct number of
			// bins
			trim();
		}
	}

	/**
	 * Adds a new data point to the histogram approximation. Make sure you have
	 * called either allocate() or merge() first. This method implements
	 * Algorithm #1 from Ben-Haim and Tom-Tov,
	 * "A Streaming Parallel Decision Tree Algorithm", JMLR 2010.
	 * 
	 * @param v
	 *            The data point to add to the histogram approximation.
	 */
	public void add(
			final double v ) {
		this.add(
				1,
				v);
	}

	public void add(
			long count,
			double v ) {
		// Binary search to find the closest bucket that v should go into.
		// 'bin' should be interpreted as the bin to shift right in order to
		// accomodate
		// v. As a result, bin is in the range [0,N], where N means that the
		// value v is
		// greater than all the N bins currently in the histogram. It is also
		// possible that
		// a bucket centered at 'v' already exists, so this must be checked in
		// the next step.
		totalCount++;
		maxValue = Math.max(
				maxValue,
				v);
		int bin = 0;
		for (int l = 0, r = bins.size(); l < r;) {
			bin = (l + r) / 2;
			if (bins.get(bin).lowerBound > v) {
				r = bin;
			}
			else {
				if (bins.get(bin).lowerBound < v) {
					l = ++bin;
				}
				else {
					break; // break loop on equal comparator
				}
			}
		}

		// If we found an exact bin match for value v, then just increment that
		// bin's count.
		// Otherwise, we need to insert a new bin and trim the resulting
		// histogram back to size.
		// A possible optimization here might be to set some threshold under
		// which 'v' is just
		// assumed to be equal to the closest bin -- if fabs(v-bins[bin].x) <
		// THRESHOLD, then
		// just increment 'bin'. This is not done now because we don't want to
		// make any
		// assumptions about the range of numeric data being analyzed.
		if ((bin < bins.size()) && Math.abs(bins.get(bin).lowerBound - v) < 1E-12) {
			bins.get(bin).count += count;
		}
		else {
			bins.add(
					bin,
					new Bin(
							v,
							count));

			// Trim the bins down to the correct number of bins.
			if (bins.size() > nbins) {
				trim();
			}
		}

	}

	/**
	 * Trims a histogram down to 'nbins' bins by iteratively merging the closest
	 * bins. If two pairs of bins are equally close to each other, decide
	 * uniformly at random which pair to merge, based on a PRNG.
	 */
	private void trim() {
		while (bins.size() > nbins) {
			// Find the closest pair of bins in terms of x coordinates. Break
			// ties randomly.
			double smallestdiff = bins.get(1).lowerBound - bins.get(0).lowerBound;
			int smallestdiffloc = 0, smallestdiffcount = 1;
			final int s = bins.size() - 1;
			for (int i = 1; i < s; i++) {
				final double diff = bins.get(i + 1).lowerBound - bins.get(i).lowerBound;
				if (diff < smallestdiff) {
					smallestdiff = diff;
					smallestdiffloc = i;
					smallestdiffcount = 1;
				}
				else {
					// HP Fortify "Insecure Randomness" false positive
					// This random number is not used for any purpose
					// related to security or cryptography
					if (((diff - smallestdiff) < 1E-12) && (prng.nextDouble() <= (1.0 / ++smallestdiffcount))) {
						smallestdiffloc = i;
					}
				}
			}

			// Merge the two closest bins into their average x location,
			// weighted by their heights.
			// The height of the new bin is the sum of the heights of the old
			// bins.

			final Bin smallestdiffbin = bins.get(smallestdiffloc);
			final double d = smallestdiffbin.count + bins.get(smallestdiffloc + 1).count;
			smallestdiffbin.lowerBound *= smallestdiffbin.count / d;
			smallestdiffbin.lowerBound += (bins.get(smallestdiffloc + 1).lowerBound / d)
					* bins.get(smallestdiffloc + 1).count;
			smallestdiffbin.count = d;
			// Shift the remaining bins left one position
			bins.remove(smallestdiffloc + 1);
		}
	}

	/**
	 * 
	 * @return The quantiles over the given number of bins.
	 */
	public double[] quantile(
			final int bins ) {
		double increment = 1.0 / (double) bins;
		double[] result = new double[bins];
		double val = increment;
		for (int i = 0; i < bins; i++, val += increment) {
			result[i] = quantile(val);
		}
		return result;
	}

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
			final double q ) {
		assert ((bins != null) && (bins.size() > 0) && (nbins > 0));
		double csum = 0;
		final int binsCount = bins.size();
		for (int b = 0; b < binsCount; b++) {
			csum += bins.get(b).count;
			if ((csum / totalCount) >= q) {
				if (b == 0) {
					return bins.get(b).lowerBound;
				}

				csum -= bins.get(b).count;
				final double r = bins.get(b - 1).lowerBound
						+ ((((q * totalCount) - csum) * (bins.get(b).lowerBound - bins.get(b - 1).lowerBound)) / (bins
								.get(b).count));
				return r;
			}
		}
		return maxValue; // should not get here
	}

	/**
	 * Estimate number of values consumed up to provided value.
	 * 
	 * @param val
	 * @return the number of estimated points
	 */
	public double sum(
			final double val,
			final boolean inclusive ) {
		if (bins.isEmpty()) {
			return 0.0;
		}

		final double minValue = bins.get(0).lowerBound;
		final double range = maxValue - minValue;
		// one value

		if ((range <= 0.0) || (val > maxValue)) {
			return totalCount;
		}
		else if (val < minValue) {
			return 0.0;
		}

		double foundCount = 0;
		int i = 0;
		for (final Bin coord : bins) {
			if (coord.lowerBound < val) {
				foundCount += coord.count;
			}
			else {
				break;
			}
			i++;
		}

		final double upperBoundary = (i < getNumBins()) ? bins.get(i).lowerBound : maxValue;
		final double lowerBoundary = i > 0 ? bins.get(i - 1).lowerBound : 0.0;
		final double upperCount = (i < getNumBins()) ? bins.get(i).count : 0;
		final double lowerCount = i > 0 ? bins.get(i - 1).count : 0;
		foundCount -= lowerCount;

		// from paper 'sum' procedure
		// the paper treats Bins like coordinates, taking the area of histogram
		// (lowerBoundary,0) (lowerBoundary,lowerCount)
		// (upperBoundary,upperCount) (upperBoundary,0)
		// divided by (upperBoundary - lowerBoundary).
		final double mb = lowerCount
				+ (((upperCount - lowerCount) / (upperBoundary - lowerBoundary)) * (val - lowerBoundary));
		final double s = (((lowerCount + mb) / 2.0) * (val - lowerBoundary)) / (upperBoundary - lowerBoundary);
		final double r = foundCount + s + (lowerCount / 2.0);
		return r > 1.0 ? r : (inclusive ? 1.0 : r);
	}

	public double cdf(
			final double val ) {
		return sum(
				val,
				false) / totalCount;
	}

	public long[] count(
			final int bins ) {
		final long[] result = new long[bins];
		double start = this.getMinValue();
		double range = maxValue - start;
		double increment = range / bins;
		start += increment;
		long last = 0;
		for (int bin = 0; bin < bins; bin++, start += increment) {
			final long aggSum = (long) Math.ceil(sum(
					start,
					false));
			result[bin] = aggSum - last;
			last = aggSum;
		}
		return result;
	}

	public int bufferSize() {
		// 20 = 8 bytes for total count, 4 bytes for number of used bins, 4
		// bytes for number of bins, 8 bytes for maxValue
		return (bins.size() * Bin.bufferSize()) + 24;
	}

	public void toBinary(
			final ByteBuffer buffer ) {
		buffer.putLong(totalCount);
		buffer.putDouble(maxValue);
		buffer.putInt(nbins);
		buffer.putInt(bins.size());
		for (final Bin bin : bins) {
			bin.toBuffer(buffer);
		}
	}

	public void fromBinary(
			final ByteBuffer buffer ) {
		totalCount = buffer.getLong();
		maxValue = buffer.getDouble();
		nbins = buffer.getInt();
		final int usedBinCount = buffer.getInt();
		bins.clear();
		bins.ensureCapacity(nbins);
		for (int i = 0; i < usedBinCount; i++) {
			bins.add(new Bin().fromBuffer(buffer));
		}
	}

	/**
	 * The Bin class defines a histogram bin, which is just an (x,y) pair.
	 */
	static class Bin implements
			Comparable<Bin>
	{
		double lowerBound;
		// Counts can be split fractionally
		double count;

		public Bin() {

		}

		public Bin(
				final double lowerBound,
				final double count ) {
			super();
			this.lowerBound = lowerBound;
			this.count = count;
		}

		@Override
		public int compareTo(
				final Bin other ) {
			return Double.compare(
					lowerBound,
					other.lowerBound);
		}

		public void toBuffer(
				final ByteBuffer buffer ) {
			buffer.putDouble(lowerBound);
			buffer.putDouble(count);
		}

		public Bin fromBuffer(
				final ByteBuffer buffer ) {
			lowerBound = buffer.getDouble();
			count = buffer.getDouble();
			return this;
		}

		static int bufferSize() {
			return 16;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			long temp;
			temp = Double.doubleToLongBits(count);
			result = prime * result + (int) (temp ^ (temp >>> 32));
			temp = Double.doubleToLongBits(lowerBound);
			result = prime * result + (int) (temp ^ (temp >>> 32));
			return result;
		}

		@Override
		public boolean equals(
				Object obj ) {
			if (this == obj) return true;
			if (obj == null) return false;
			if (getClass() != obj.getClass()) return false;
			Bin other = (Bin) obj;
			if (Double.doubleToLongBits(count) != Double.doubleToLongBits(other.count)) return false;
			if (Double.doubleToLongBits(lowerBound) != Double.doubleToLongBits(other.lowerBound)) return false;
			return true;
		}
	}

	public double getMaxValue() {
		return maxValue;
	};

	public double getMinValue() {
		return !bins.isEmpty() ? bins.get(0).lowerBound : 0.0;
	};

	public static class MinimalBinDistanceHistogramFactory implements
			NumericHistogramFactory
	{

		@Override
		public NumericHistogram create(
				int bins ) {
			return new MinimalBinDistanceHistogram(
					bins);
		}

		@Override
		public NumericHistogram create(
				int bins,
				double minValue,
				double maxValue ) {
			return new MinimalBinDistanceHistogram(
					bins);
		}
	}
}
