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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.store.adapter.statistics.histogram.ByteUtils;
import org.locationtech.geowave.core.store.adapter.statistics.histogram.NumericHistogram;
import org.locationtech.geowave.core.store.adapter.statistics.histogram.TDigestNumericHistogram;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;

/**
 * Dynamic histogram provide very high accuracy for CDF and quantiles over the a
 * numeric attribute.
 *
 */
public class RowRangeHistogramStatistics<T> extends
		AbstractDataStatistics<T, NumericHistogram, PartitionStatisticsQueryBuilder<NumericHistogram>>
{
	public static final PartitionStatisticsType<NumericHistogram> STATS_TYPE = new PartitionStatisticsType<>(
			"ROW_RANGE_HISTOGRAM");
	private NumericHistogram histogram;

	public RowRangeHistogramStatistics() {
		super();
	}

	public RowRangeHistogramStatistics(
			final String indexName,
			final ByteArray partitionKey ) {
		this(
				null,
				indexName,
				partitionKey);
	}

	public RowRangeHistogramStatistics(
			final Short internalDataAdapterId,
			final String indexName,
			final ByteArray partitionKey ) {
		super(
				internalDataAdapterId,
				STATS_TYPE,
				PartitionStatisticsQueryBuilder.composeId(
						indexName,
						partitionKey));
		histogram = createHistogram();
	}

	private static NumericHistogram createHistogram() {
		return new TDigestNumericHistogram();
	}

	@Override
	public InternalDataStatistics<T, NumericHistogram, PartitionStatisticsQueryBuilder<NumericHistogram>> duplicate() {
		final Pair<String, ByteArray> pair = PartitionStatisticsQueryBuilder
				.decomposeIndexAndPartitionFromId(extendedId);
		return new RowRangeHistogramStatistics<>(
				adapterId,
				pair.getLeft(), // indexName
				pair.getRight());
	}

	public double cardinality(
			final byte[] start,
			final byte[] end ) {
		return (end == null ? histogram.getTotalCount() : (histogram.sum(
				ByteUtils.toDouble(end),
				true))// should be inclusive
				- (start == null ? 0 : histogram.sum(
						ByteUtils.toDouble(start),
						false))); // should be exclusive
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
			final byte[] id ) {
		return histogram.cdf(ByteUtils.toDouble(id));
	}

	public double quantile(
			final double percentage ) {
		return histogram.quantile((percentage));
	}

	public double percentPopulationOverRange(
			final byte[] start,
			final byte[] stop ) {
		return cdf(stop) - cdf(start);
	}

	public long getTotalCount() {
		return histogram.getTotalCount();
	}

	@Override
	public void merge(
			final Mergeable mergeable ) {
		if (mergeable instanceof RowRangeHistogramStatistics) {
			final NumericHistogram otherHistogram = ((RowRangeHistogramStatistics) mergeable).histogram;
			if (histogram == null) {
				histogram = otherHistogram;
			}
			else if (otherHistogram != null) {
				histogram.merge(otherHistogram);
			}
		}
	}

	@Override
	public byte[] toBinary() {
		final int bufferSize = histogram != null ? histogram.bufferSize() : 0;
		final ByteBuffer buffer = super.binaryBuffer(bufferSize);
		if (histogram != null) {
			histogram.toBinary(buffer);
		}
		return buffer.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buffer = super.binaryBuffer(bytes);
		final NumericHistogram histogram = createHistogram();
		if (buffer.hasRemaining()) {
			histogram.fromBinary(buffer);
		}
		this.histogram = histogram;
	}

	@Override
	public void entryIngested(
			final T entry,
			final GeoWaveRow... kvs ) {
		for (final GeoWaveRow kv : kvs) {
			final byte[] idBytes = kv.getSortKey();
			add(ByteUtils.toDouble(idBytes));
		}
	}

	protected void add(
			final double num ) {
		histogram.add(num);
	}

	@Override
	public String toString() {
		final StringBuffer buffer = new StringBuffer();
		final Pair<String, ByteArray> indexAndPartition = PartitionStatisticsQueryBuilder
				.decomposeIndexAndPartitionFromId(extendedId);
		buffer.append(
				"histogram[index=").append(
				indexAndPartition.getLeft());
		if ((indexAndPartition.getRight() != null) && (indexAndPartition.getRight().getBytes() != null)
				&& (indexAndPartition.getRight().getBytes().length > 0)) {
			buffer.append(
					", partitionAsHex=").append(
					indexAndPartition.getRight().getHexString());
		}
		if (histogram != null) {
			buffer.append(", quantiles={");
			for (int i = 1; i < 10; i++) {

				buffer.append((i * 10) + "%: " + histogram.quantile(i * 0.1));
				buffer.append(' ');
			}
			buffer.deleteCharAt(buffer.length() - 1);
			buffer.append("}]");
		}
		buffer.append("]");
		return buffer.toString();
	}

	@Override
	public NumericHistogram getResult() {
		return histogram;
	}

	@Override
	protected String resultsName() {
		return "histogram";
	}

	@Override
	protected Object resultsValue() {
		final Pair<String, ByteArray> indexAndPartition = PartitionStatisticsQueryBuilder
				.decomposeIndexAndPartitionFromId(extendedId);
		final Map<String, Object> retVal = new HashMap<>();
		retVal.put(
				"index",
				indexAndPartition.getLeft());
		retVal.put(
				"partitionAsHex",
				indexAndPartition.getRight().getHexString());
		if (histogram != null) {
			final Map<String, Object> histogramMap = new HashMap<>();
			histogramMap.put(
					"min",
					Double.toString(histogram.getMinValue()));
			histogramMap.put(
					"max",
					Double.toString(histogram.getMaxValue()));
			final Collection<String> quantilesArray = new ArrayList<>();
			for (int i = 1; i < 10; i++) {
				quantilesArray.add((i * 10) + "%: " + histogram.quantile(i * 0.1));
			}
			histogramMap.put(
					"quantiles",
					quantilesArray);
			retVal.put(
					"histogramValues",
					histogramMap);
		}
		else {
			retVal.put(
					"histogram",
					"empty");
		}
		return retVal;
	}
}
