/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.core.store.adapter.statistics;

import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.store.adapter.statistics.histogram.FixedBinNumericHistogram;

/**
 *
 * Fixed number of bins for a histogram. Unless configured, the range will
 * expand dynamically, redistributing the data as necessary into the wider bins.
 *
 * The advantage of constraining the range of the statistic is to ignore values
 * outside the range, such as erroneous values. Erroneous values force extremes
 * in the histogram. For example, if the expected range of values falls between
 * 0 and 1 and a value of 10000 occurs, then a single bin contains the entire
 * population between 0 and 1, a single bin represents the single value of
 * 10000. If there are extremes in the data, then use
 * {@link FeatureNumericHistogramStatistics} instead.
 *
 *
 * The default number of bins is 32.
 *
 */
public abstract class FixedBinNumericStatistics<T> extends
		AbstractDataStatistics<T, FixedBinNumericHistogram, FieldStatisticsQueryBuilder<FixedBinNumericHistogram>>
{
	protected FixedBinNumericHistogram histogram;

	protected FixedBinNumericStatistics() {
		super();
		histogram = new FixedBinNumericHistogram(
				1024);
	}

	public FixedBinNumericStatistics(
			final Short internalDataAdapterId,
			final StatisticsType<FixedBinNumericHistogram, FieldStatisticsQueryBuilder<FixedBinNumericHistogram>> type,
			final String fieldName ) {
		this(
				internalDataAdapterId,
				type,
				fieldName,
				1024);
	}

	public FixedBinNumericStatistics(
			final Short internalDataAdapterId,
			final StatisticsType<FixedBinNumericHistogram, FieldStatisticsQueryBuilder<FixedBinNumericHistogram>> type,
			final String fieldName,
			final int bins ) {
		super(
				internalDataAdapterId,
				type,
				fieldName);
		histogram = new FixedBinNumericHistogram(
				bins);
	}

	public FixedBinNumericStatistics(
			final Short internalDataAdapterId,
			final StatisticsType<FixedBinNumericHistogram, FieldStatisticsQueryBuilder<FixedBinNumericHistogram>> type,
			final String fieldName,
			final int bins,
			final double minValue,
			final double maxValue ) {
		super(
				internalDataAdapterId,
				type,
				fieldName);
		histogram = new FixedBinNumericHistogram(
				bins,
				minValue,
				maxValue);

	}

	@Override
	public FixedBinNumericHistogram getResult() {
		return histogram;
	}

	public double[] quantile(
			final int bins ) {
		return histogram.quantile(bins);
	}

	public double cdf(
			final double val ) {
		return histogram.cdf(val);
	}

	public double quantile(
			final double percentage ) {
		return histogram.quantile(percentage);
	}

	public double percentPopulationOverRange(
			final double start,
			final double stop ) {
		return cdf(stop) - cdf(start);
	}

	public long totalSampleSize() {
		return histogram.getTotalCount();
	}

	public long[] count(
			final int binSize ) {
		return histogram.count(binSize);
	}

	@Override
	public void merge(
			final Mergeable mergeable ) {
		if (mergeable instanceof FixedBinNumericStatistics) {
			final FixedBinNumericStatistics tobeMerged = (FixedBinNumericStatistics) mergeable;
			histogram.merge(tobeMerged.histogram);
		}
	}

	@Override
	public byte[] toBinary() {
		final ByteBuffer buffer = super.binaryBuffer(histogram.bufferSize());
		histogram.toBinary(buffer);
		return buffer.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buffer = super.binaryBuffer(bytes);
		histogram.fromBinary(buffer);
	}

	protected void add(
			final long amount,
			final double num ) {
		this.histogram.add(
				amount,
				num);
	}

	public abstract String getFieldIdentifier();

	@Override
	public String toString() {
		final StringBuffer buffer = new StringBuffer();
		buffer.append(
				"histogram[adapterId=").append(
				super.getAdapterId());
		buffer.append(
				", identifier=").append(
				getFieldIdentifier());
		final MessageFormat mf = new MessageFormat(
				"{0,number,#.######}");
		buffer.append(", range={");
		buffer.append(
				mf.format(new Object[] {
					Double.valueOf(histogram.getMinValue())
				})).append(
				' ');
		buffer.append(mf.format(new Object[] {
			Double.valueOf(histogram.getMaxValue())
		}));
		buffer.append("}, bins={");
		for (final double v : this.quantile(10)) {
			buffer.append(
					mf.format(new Object[] {
						Double.valueOf(v)
					})).append(
					' ');
		}
		buffer.deleteCharAt(buffer.length() - 1);
		buffer.append("}, counts={");
		for (final long v : count(10)) {
			buffer.append(
					mf.format(new Object[] {
						Long.valueOf(v)
					})).append(
					' ');
		}
		buffer.deleteCharAt(buffer.length() - 1);
		buffer.append("}]");
		return buffer.toString();
	}

	@Override
	protected String resultsName() {
		return "histogram";
	}

	@Override
	protected Object resultsValue() {
		final Map<String, Object> value = new HashMap<>();

		value.put(
				"range_min",
				histogram.getMinValue());
		value.put(
				"range_max",
				histogram.getMaxValue());
		final Collection<Double> binsArray = new ArrayList<>();
		for (final double v : this.quantile(10)) {
			binsArray.add(v);
		}
		value.put(
				"bins",
				binsArray);

		final Collection<Long> countsArray = new ArrayList<>();
		for (final long v : count(10)) {
			countsArray.add(v);
		}
		value.put(
				"counts",
				countsArray);
		return value;
	}

}
