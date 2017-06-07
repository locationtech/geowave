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

import mil.nga.giat.geowave.core.index.FloatCompareUtils;

/**
 * * Fixed number of bins for a histogram. Unless configured, the range will
 * expand dynamically, redistributing the data as necessary into the wider bins.
 * 
 * The advantage of constraining the range of the statistic is to ignore values
 * outside the range, such as erroneous values. Erroneous values force extremes
 * in the histogram. For example, if the expected range of values falls between
 * 0 and 1 and a value of 10000 occurs, then a single bin contains the entire
 * population between 0 and 1, a single bin represents the single value of
 * 10000.
 * 
 */
public class FixedBinNumericHistogram implements
		NumericHistogram
{

	private long count[] = new long[32];
	private long totalCount = 0;
	private double minValue = Double.MAX_VALUE;
	private double maxValue = Double.MIN_VALUE;
	private boolean constrainedRange = false;

	/**
	 * Creates a new histogram object.
	 */
	public FixedBinNumericHistogram() {
		totalCount = 0;
	}

	/**
	 * Creates a new histogram object.
	 */
	public FixedBinNumericHistogram(
			final int size ) {
		count = new long[size];
	}

	public FixedBinNumericHistogram(
			final int bins,
			final double minValue,
			final double maxValue ) {
		count = new long[bins];
		this.minValue = minValue;
		this.maxValue = maxValue;
		constrainedRange = true;
	}

	public double[] quantile(
			final int bins ) {
		final double[] result = new double[bins];
		final double binSize = 1.0 / bins;
		for (int bin = 0; bin < bins; bin++) {
			result[bin] = quantile(binSize * (bin + 1));
		}
		return result;
	}

	public double cdf(
			final double val ) {
		return sum(
				val,
				false) / totalCount;
	}

	/**
	 * Estimate number of values consumed up to provided value.
	 * 
	 * @param val
	 * @return the number of estimated points
	 */
	public double sum(
			final double val,
			boolean inclusive ) {
		if (val < this.minValue) {
			return 0.0;
		}
		final double range = maxValue - minValue;
		if ((range <= 0.0) || (totalCount == 0)) {
			return totalCount;
		}

		final int bin = Math.min(
				(int) Math.floor((((val - minValue) / range) * count.length)),
				count.length - 1);

		double c = 0;
		final double perBinSize = binSize();
		for (int i = 0; i < bin; i++) {
			c += count[i];
		}
		final double percentageOfLastBin = Math.min(
				1.0,
				(val - ((perBinSize * (bin)) + minValue)) / perBinSize);
		c += (percentageOfLastBin * count[bin]);
		return c > 0 ? c : (inclusive ? 1.0 : c);

	}

	private double binSize() {
		final double v = (maxValue - minValue) / count.length;
		return (FloatCompareUtils.checkDoublesEqual(
				v,
				0.0)) ? 1.0 : v;
	}

	public double quantile(
			final double percentage ) {
		final double fractionOfTotal = percentage * totalCount;
		double countThisFar = 0;
		int bin = 0;

		for (; (bin < count.length) && (countThisFar < fractionOfTotal); bin++) {
			countThisFar += count[bin];
		}
		if (bin == 0) {
			return minValue;
		}
		final double perBinSize = binSize();
		final double countUptoLastBin = countThisFar - count[bin - 1];
		return minValue + ((perBinSize * bin) + (perBinSize * ((fractionOfTotal - countUptoLastBin) / count[bin - 1])));
	}

	public double percentPopulationOverRange(
			final double start,
			final double stop ) {
		return cdf(stop) - cdf(start);
	}

	public long totalSampleSize() {
		return totalCount;
	}

	public long[] count(
			final int bins ) {
		final long[] result = new long[bins];
		double start = minValue;
		double range = maxValue - minValue;
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

	public void merge(
			final NumericHistogram mergeable ) {

		FixedBinNumericHistogram myTypeOfHist = (FixedBinNumericHistogram) mergeable;
		final double newMinValue = Math.min(
				minValue,
				myTypeOfHist.minValue);
		final double newMaxValue = Math.max(
				maxValue,
				myTypeOfHist.maxValue);
		this.redistribute(
				newMinValue,
				newMaxValue);
		myTypeOfHist.redistribute(
				newMinValue,
				newMaxValue);
		for (int i = 0; i < count.length; i++) {
			count[i] += myTypeOfHist.count[i];
		}

		maxValue = newMaxValue;
		minValue = newMinValue;
		totalCount += myTypeOfHist.totalCount;
	}

	public int bufferSize() {
		return 28 + (8 * count.length);
	}

	public void toBinary(
			ByteBuffer buffer ) {
		buffer.putLong(totalCount);
		buffer.putDouble(minValue);
		buffer.putDouble(maxValue);
		buffer.putInt(count.length);
		for (int i = 0; i < count.length; i++) {
			buffer.putLong(count[i]);
		}
	}

	public void fromBinary(
			ByteBuffer buffer ) {
		totalCount = buffer.getLong();
		minValue = buffer.getDouble();
		maxValue = buffer.getDouble();
		final int s = buffer.getInt();
		count = new long[s];
		for (int i = 0; i < s; i++) {
			count[i] = buffer.getLong();
		}
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
		return count.length;
	}

	public void add(
			final double num ) {
		add(
				1L,
				num);
	}

	public void add(
			final long amount,
			final double num ) {
		if (constrainedRange && ((num < minValue) || (num > maxValue))) {
			return;
		}
		// entry of the the same value or first entry
		if ((totalCount == 0L) || FloatCompareUtils.checkDoublesEqual(
				minValue,
				num)) {
			count[0] += amount;
			minValue = num;
			maxValue = Math.max(
					num,
					maxValue);
		} // else if entry has a different value
		else if (FloatCompareUtils.checkDoublesEqual(
				maxValue,
				minValue)) { // &&
								// num
								// is
			// neither
			if (num < minValue) {
				count[count.length - 1] = count[0];
				count[0] = amount;
				minValue = num;

			}
			else if (num > maxValue) {
				count[count.length - 1] = amount;
				// count[0] is unchanged
				maxValue = num;
			}
		}
		else {
			if (num < minValue) {
				redistribute(
						num,
						maxValue);
				minValue = num;

			}
			else if (num > maxValue) {
				redistribute(
						minValue,
						num);
				maxValue = num;
			}
			final double range = maxValue - minValue;
			final double b = (((num - minValue) / range) * count.length);
			final int bin = Math.min(
					(int) Math.floor(b),
					count.length - 1);
			count[bin] += amount;
		}

		totalCount += amount;
	}

	private void redistribute(
			final double newMinValue,
			final double newMaxValue ) {
		redistribute(
				new long[count.length],
				newMinValue,
				newMaxValue);
	}

	private void redistribute(
			final long[] newCount,
			final double newMinValue,
			final double newMaxValue ) {
		final double perBinSize = binSize();
		final double newRange = (newMaxValue - newMinValue);
		final double newPerBinsSize = newRange / count.length;
		double currentWindowStart = minValue;
		double currentWindowStop = minValue + perBinSize;
		for (int bin = 0; bin < count.length; bin++) {
			long distributionCount = 0;
			int destinationBin = Math.min(
					(int) Math.floor((((currentWindowStart - newMinValue) / newRange) * count.length)),
					count.length - 1);
			double destinationWindowStart = newMinValue + (destinationBin * newPerBinsSize);
			double destinationWindowStop = destinationWindowStart + newPerBinsSize;
			while (count[bin] > 0) {
				if (currentWindowStart < destinationWindowStart) {
					// take whatever is left over
					distributionCount = count[bin];
				}
				else {
					final double diff = Math.min(
							Math.max(
									currentWindowStop - destinationWindowStop,
									0.0),
							perBinSize);
					distributionCount = Math.round(count[bin] * (1.0 - (diff / perBinSize)));
				}
				newCount[destinationBin] += distributionCount;
				count[bin] -= distributionCount;

				if (destinationWindowStop < currentWindowStop) {
					destinationWindowStart = destinationWindowStop;
					destinationWindowStop += newPerBinsSize;
					destinationBin += 1;
					if ((destinationBin == count.length) && (count[bin] > 0)) {
						newCount[bin] += count[bin];
						count[bin] = 0;
					}
				}
			}

			currentWindowStart = currentWindowStop;
			currentWindowStop += perBinSize;

		}
		count = newCount;
	}

	public double getMaxValue() {
		return maxValue;
	};

	public double getMinValue() {
		return minValue;
	};

	public static class FixedBinNumericHistogramFactory implements
			NumericHistogramFactory
	{

		@Override
		public NumericHistogram create(
				int bins ) {
			return new FixedBinNumericHistogram(
					bins);
		}

		@Override
		public NumericHistogram create(
				int bins,
				double minValue,
				double maxValue ) {
			return new FixedBinNumericHistogram(
					bins,
					minValue,
					maxValue);
		}

	}
}
