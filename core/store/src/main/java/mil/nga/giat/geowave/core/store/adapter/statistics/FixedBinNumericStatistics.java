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
package mil.nga.giat.geowave.core.store.adapter.statistics;

import java.nio.ByteBuffer;
import java.text.MessageFormat;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.store.adapter.InternalAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.histogram.FixedBinNumericHistogram.FixedBinNumericHistogramFactory;
import mil.nga.giat.geowave.core.store.adapter.statistics.histogram.NumericHistogram;
import mil.nga.giat.geowave.core.store.adapter.statistics.histogram.NumericHistogramFactory;
import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

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
		AbstractDataStatistics<T>
{
	public static final ByteArrayId STATS_TYPE = new ByteArrayId(
			"FIXED_BIN_NUMERIC_HISTOGRAM");
	// private static final NumericHistogramFactory HistFactory = new
	// MinimalBinDistanceHistogramFactory();
	private static final NumericHistogramFactory HistFactory = new FixedBinNumericHistogramFactory();
	NumericHistogram histogram = HistFactory.create(1024);

	protected FixedBinNumericStatistics() {
		super();
	}

	public FixedBinNumericStatistics(
			final Short internalDataAdapterId,
			final ByteArrayId statisticsId ) {
		super(
				internalDataAdapterId,
				statisticsId);
	}

	public FixedBinNumericStatistics(
			final Short internalDataAdapterId,
			final ByteArrayId statisticsId,
			final int bins ) {
		super(
				internalDataAdapterId,
				statisticsId);
		histogram = HistFactory.create(bins);
	}

	public FixedBinNumericStatistics(
			final Short internalDataAdapterId,
			final ByteArrayId statisticsId,
			final int bins,
			final double minValue,
			final double maxValue ) {
		super(
				internalDataAdapterId,
				statisticsId);
		histogram = HistFactory.create(
				bins,
				minValue,
				maxValue);

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
		final byte result[] = new byte[buffer.position()];
		buffer.rewind();
		buffer.get(result);
		return result;
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
				"histogram[internalDataAdapterId=").append(
				super.getInternalDataAdapterId());
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

	/**
	 * Convert Fixed Bin Numeric statistics to a JSON object
	 */

	@Override
	public JSONObject toJSONObject(
			final InternalAdapterStore store )
			throws JSONException {
		final JSONObject jo = new JSONObject();
		jo.put(
				"type",
				STATS_TYPE.getString());
		jo.put(
				"dataAdapterID",
				store.getAdapterId(internalDataAdapterId));

		jo.put(
				"field_identifier",
				getFieldIdentifier());

		jo.put(
				"range_min",
				histogram.getMinValue());
		jo.put(
				"range_max",
				histogram.getMaxValue());
		final JSONArray binsArray = new JSONArray();
		for (final double v : this.quantile(10)) {
			binsArray.add(v);
		}
		jo.put(
				"bins",
				binsArray);

		final JSONArray countsArray = new JSONArray();
		for (final long v : count(10)) {
			countsArray.add(v);
		}
		jo.put(
				"counts",
				countsArray);

		return jo;
	}

}
