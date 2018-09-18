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
package org.locationtech.geowave.adapter.vector.stats;

import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.DataFormatException;

import org.HdrHistogram.AbstractHistogram;
import org.HdrHistogram.DoubleHistogram;
import org.HdrHistogram.Histogram;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.geotime.store.statistics.FieldNameStatistic;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.store.adapter.statistics.AbstractDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.FieldStatisticsQueryBuilder;
import org.locationtech.geowave.core.store.adapter.statistics.FieldStatisticsType;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.opengis.feature.simple.SimpleFeature;

/**
 * Dynamic histogram provide very high accuracy for CDF and quantiles over the a
 * numeric attribute.
 *
 */
public class FeatureNumericHistogramStatistics extends
		AbstractDataStatistics<SimpleFeature, Pair<DoubleHistogram, DoubleHistogram>, FieldStatisticsQueryBuilder<Pair<DoubleHistogram, DoubleHistogram>>> implements
		FieldNameStatistic
{
	public static final FieldStatisticsType<Pair<DoubleHistogram, DoubleHistogram>> STATS_TYPE = new FieldStatisticsType<>(
			"ATT_HISTOGRAM");
	private DoubleHistogram positiveHistogram = new LocalDoubleHistogram();
	private DoubleHistogram negativeHistogram = null;

	// Max value is determined by the level of accuracy required, using a
	// formula provided
	// HdrHistogram
	private final double maxValue = (Math.pow(
			2,
			63) / Math.pow(
			2,
			14)) - 1;
	private final double minValue = -(maxValue);

	public FeatureNumericHistogramStatistics() {
		super();
	}

	public FeatureNumericHistogramStatistics(
			final String fieldName ) {
		this(
				null,
				fieldName);
	}

	public FeatureNumericHistogramStatistics(
			final Short adapterId,
			final String fieldName ) {
		super(
				adapterId,
				STATS_TYPE,
				fieldName);
	}

	@Override
	public String getFieldName() {
		return extendedId;
	}

	@Override
	public InternalDataStatistics<SimpleFeature, Pair<DoubleHistogram, DoubleHistogram>, FieldStatisticsQueryBuilder<Pair<DoubleHistogram, DoubleHistogram>>> duplicate() {
		return new FeatureNumericHistogramStatistics(
				adapterId,
				getFieldName());
	}

	private double percentageNegative() {
		final long nc = negativeHistogram == null ? 0 : negativeHistogram.getTotalCount();
		final long tc = positiveHistogram.getTotalCount() + nc;
		return (double) nc / (double) tc;
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
		final double percentageNegative = percentageNegative();
		if ((val < 0) || ((1.0 - percentageNegative) < 0.000000001)) {
			// subtract one from percentage since negative is negated so
			// percentage is inverted
			return (percentageNegative > 0) ? percentageNegative
					* (1.0 - (negativeHistogram.getPercentileAtOrBelowValue(-val) / 100.0)) : 0.0;
		}
		else {
			return percentageNegative
					+ ((1.0 - percentageNegative) * (positiveHistogram.getPercentileAtOrBelowValue(val) / 100.0));
		}

	}

	public double quantile(
			final double percentage ) {
		final double percentageNegative = percentageNegative();
		if (percentage < percentageNegative) {
			// subtract one from percentage since negative is negated so
			// percentage is inverted
			return -negativeHistogram.getValueAtPercentile((1.0 - (percentage / percentageNegative)) * 100.0);
		}
		else {
			return positiveHistogram.getValueAtPercentile((percentage / (1.0 - percentageNegative)) * 100.0);
		}
	}

	public double percentPopulationOverRange(
			final double start,
			final double stop ) {
		return cdf(stop) - cdf(start);
	}

	public long totalSampleSize() {
		return positiveHistogram.getTotalCount() + (negativeHistogram == null ? 0 : negativeHistogram.getTotalCount());
	}

	public long[] count(
			final int bins ) {
		final long[] result = new long[bins];
		final double max = positiveHistogram.getMaxValue();
		final double min = negativeHistogram == null ? positiveHistogram.getMinValue() : -negativeHistogram
				.getMaxValue();
		final double binSize = (max - min) / (bins);
		long last = 0;
		final long tc = totalSampleSize();
		for (int bin = 0; bin < bins; bin++) {
			final double val = cdf(min + ((bin + 1.0) * binSize)) * tc;
			final long next = (long) val - last;
			result[bin] = next;
			last += next;
		}
		return result;
	}

	@Override
	public void merge(
			final Mergeable mergeable ) {
		if (mergeable instanceof FeatureNumericHistogramStatistics) {
			positiveHistogram.add(((FeatureNumericHistogramStatistics) mergeable).positiveHistogram);
			if (((FeatureNumericHistogramStatistics) mergeable).negativeHistogram != null) {
				if (negativeHistogram != null) {
					negativeHistogram.add(((FeatureNumericHistogramStatistics) mergeable).negativeHistogram);
				}
				else {
					negativeHistogram = ((FeatureNumericHistogramStatistics) mergeable).negativeHistogram;
				}
			}
		}
	}

	@Override
	public byte[] toBinary() {
		final int positiveBytes = positiveHistogram.getEstimatedFootprintInBytes();
		final int bytesNeeded = positiveBytes
				+ (negativeHistogram == null ? 0 : negativeHistogram.getEstimatedFootprintInBytes());
		final ByteBuffer buffer = super.binaryBuffer(bytesNeeded + 5);
		final int startPosition = buffer.position();
		buffer.putInt(startPosition); // buffer out an int
		positiveHistogram.encodeIntoCompressedByteBuffer(buffer);
		final int endPosition = buffer.position();
		buffer.position(startPosition);
		buffer.putInt(endPosition);
		buffer.position(endPosition);
		if (negativeHistogram != null) {
			buffer.put((byte) 0x01);
			negativeHistogram.encodeIntoCompressedByteBuffer(buffer);
		}
		else {
			buffer.put((byte) 0x00);
		}
		final byte result[] = new byte[buffer.position() + 1];
		buffer.rewind();
		buffer.get(result);
		return result;
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buffer = super.binaryBuffer(bytes);
		final int endPosition = buffer.getInt();
		try {
			positiveHistogram = DoubleHistogram.decodeFromCompressedByteBuffer(
					buffer,
					LocalInternalHistogram.class,
					0);
			buffer.position(endPosition);
			positiveHistogram.setAutoResize(true);
			if (buffer.get() == (byte) 0x01) {
				negativeHistogram = DoubleHistogram.decodeFromCompressedByteBuffer(
						buffer,
						LocalInternalHistogram.class,
						0);
				negativeHistogram.setAutoResize(true);
			}
		}
		catch (final DataFormatException e) {
			throw new RuntimeException(
					"Cannot decode statistic",
					e);
		}
	}

	@Override
	public void entryIngested(
			final SimpleFeature entry,
			final GeoWaveRow... rows ) {
		final Object o = entry.getAttribute(getFieldName());
		if (o == null) {
			return;
		}
		if (o instanceof Date) {
			add(((Date) o).getTime());
		}
		else if (o instanceof Number) {
			add(((Number) o).doubleValue());
		}
	}

	protected void add(
			final double num ) {
		if ((num < minValue) || (num > maxValue) || Double.isNaN(num)) {
			return;
		}
		if (num >= 0) {
			positiveHistogram.recordValue(num);
		}
		else {
			getNegativeHistogram().recordValue(
					-num);
		}
	}

	@Override
	public String toString() {
		final StringBuffer buffer = new StringBuffer();
		buffer.append(
				"histogram[internalDataAdapterId=").append(
				super.getAdapterId());
		buffer.append(
				", field=").append(
				getFieldName());
		buffer.append(", bins={");
		final MessageFormat mf = new MessageFormat(
				"{0,number,#.######}");
		for (final double v : this.quantile(10)) {
			buffer.append(
					mf.format(new Object[] {
						Double.valueOf(v)
					})).append(
					' ');
		}
		buffer.deleteCharAt(buffer.length() - 1);
		buffer.append(", counts={");
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

	private DoubleHistogram getNegativeHistogram() {
		if (negativeHistogram == null) {
			negativeHistogram = new LocalDoubleHistogram();
		}
		return negativeHistogram;
	}

	public static class LocalDoubleHistogram extends
			DoubleHistogram
	{

		public LocalDoubleHistogram() {
			super(
					2,
					4,
					LocalInternalHistogram.class);
			super.setAutoResize(true);
		}

		/**
		 *
		 */
		private static final long serialVersionUID = 5504684423053828467L;

	}

	@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = {
		"HE_INHERITS_EQUALS_USE_HASHCODE"
	})
	public static class LocalInternalHistogram extends
			Histogram
	{
		/**
		 *
		 */
		private static final long serialVersionUID = 4369054277576423915L;

		public LocalInternalHistogram(
				final AbstractHistogram source ) {
			super(
					source);
			source.setAutoResize(true);
			super.setAutoResize(true);
		}

		public LocalInternalHistogram(
				final int numberOfSignificantValueDigits ) {
			super(
					numberOfSignificantValueDigits);
			super.setAutoResize(true);
		}

		public LocalInternalHistogram(
				final long highestTrackableValue,
				final int numberOfSignificantValueDigits ) {
			super(
					highestTrackableValue,
					numberOfSignificantValueDigits);
			super.setAutoResize(true);
		}

		public LocalInternalHistogram(
				final long lowestDiscernibleValue,
				final long highestTrackableValue,
				final int numberOfSignificantValueDigits ) {
			super(
					lowestDiscernibleValue,
					highestTrackableValue,
					numberOfSignificantValueDigits);
			super.setAutoResize(true);
		}

	}

	public static class FeatureNumericHistogramConfig implements
			StatsConfig<SimpleFeature>
	{
		/**
		 *
		 */
		private static final long serialVersionUID = 6309383518148391565L;

		@Override
		public InternalDataStatistics<SimpleFeature, Pair<DoubleHistogram, DoubleHistogram>, FieldStatisticsQueryBuilder<Pair<DoubleHistogram, DoubleHistogram>>> create(
				final Short internalDataAdapterId,
				final String fieldName ) {
			return new FeatureNumericHistogramStatistics(
					internalDataAdapterId,
					fieldName);
		}

		@Override
		public byte[] toBinary() {
			return new byte[0];
		}

		@Override
		public void fromBinary(
				final byte[] bytes ) {}
	}

	@Override
	public Pair<DoubleHistogram, DoubleHistogram> getResult() {
		return Pair.of(
				negativeHistogram,
				positiveHistogram);
	}

	@Override
	protected String resultsName() {
		return "histograms";
	}

	@Override
	protected Object resultsValue() {
		final Map<String, List<Object>> results = new HashMap<>();
		final List<Object> quantilesList = new ArrayList<>();
		for (final double v : this.quantile(10)) {
			quantilesList.add(v);
		}
		results.put(
				"quantiles",
				quantilesList);
		final List<Object> countsList = new ArrayList<>();
		for (final long v : count(10)) {
			countsList.add(v);
		}
		results.put(
				"counts",
				countsList);
		return results;
	}
}
