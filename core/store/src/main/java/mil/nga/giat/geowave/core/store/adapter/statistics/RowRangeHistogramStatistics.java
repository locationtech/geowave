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

import org.apache.commons.lang3.tuple.Pair;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.store.adapter.statistics.histogram.ByteUtils;
import mil.nga.giat.geowave.core.store.adapter.statistics.histogram.MinimalBinDistanceHistogram.MinimalBinDistanceHistogramFactory;
import mil.nga.giat.geowave.core.store.adapter.statistics.histogram.NumericHistogram;
import mil.nga.giat.geowave.core.store.adapter.statistics.histogram.NumericHistogramFactory;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

/**
 * Dynamic histogram provide very high accuracy for CDF and quantiles over the a
 * numeric attribute.
 *
 */
public class RowRangeHistogramStatistics<T> extends
		AbstractDataStatistics<T>
{
	public static final ByteArrayId STATS_TYPE = new ByteArrayId(
			"ROW_RANGE_HISTOGRAM");
	private static final NumericHistogramFactory HistFactory = new MinimalBinDistanceHistogramFactory();
	private NumericHistogram histogram;

	public RowRangeHistogramStatistics() {
		super();
	}

	public RowRangeHistogramStatistics(
			final ByteArrayId dataAdapterId,
			final ByteArrayId indexId,
			final ByteArrayId partitionKey ) {
		super(
				dataAdapterId,
				composeId(
						indexId,
						partitionKey));
		histogram = createHistogram();
	}

	private static NumericHistogram createHistogram() {
		return HistFactory.create(1024);
	}

	public static ByteArrayId composeId(
			final ByteArrayId indexId,
			final ByteArrayId partitionKey ) {
		if ((partitionKey == null) || (partitionKey.getBytes() == null) || (partitionKey.getBytes().length == 0)) {
			return new ByteArrayId(
					STATS_TYPE.getString() + STATS_SEPARATOR.getString() + indexId.getString());
		}
		return AbstractDataStatistics.composeId(
				STATS_TYPE.getString() + STATS_SEPARATOR.getString() + indexId.getString(),
				ByteArrayUtils.byteArrayToString(partitionKey.getBytes()));
	}

	@Override
	public DataStatistics<T> duplicate() {
		final Pair<ByteArrayId, ByteArrayId> pair = decomposeIndexAndPartitionFromId(statisticsId);
		return new RowRangeHistogramStatistics<T>(
				dataAdapterId,
				pair.getLeft(), // indexId
				pair.getRight());
	}

	public static Pair<ByteArrayId, ByteArrayId> decomposeIndexAndPartitionFromId(
			final ByteArrayId id ) {
		// Need to account for length of type and of the separator
		final int lengthOfNonId = STATS_TYPE.getBytes().length + STATS_SEPARATOR.getString().length();
		final int idLength = id.getBytes().length - lengthOfNonId;
		final byte[] idBytes = new byte[idLength];
		System.arraycopy(
				id.getBytes(),
				lengthOfNonId,
				idBytes,
				0,
				idLength);
		final String idString = id.getString();
		final int pos = idString.lastIndexOf(STATS_ID_SEPARATOR);
		if (pos < 0) {
			return Pair.of(
					new ByteArrayId(
							idString),
					null);
		}
		return Pair.of(
				new ByteArrayId(
						idString.substring(
								0,
								pos)),
				new ByteArrayId(
						ByteArrayUtils.byteArrayFromString(idString.substring(pos + 1))));
	}

	public double cardinality(
			final byte[] start,
			final byte[] end ) {
		return ((end == null ? histogram.getTotalCount() : histogram.sum(
				ByteUtils.toDouble(end),
				true)) - (start == null ? 0 : histogram.sum(
				ByteUtils.toDouble(start),
				false)));
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

	public long[] count(
			final int bins ) {
		return histogram.count(bins);
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

	public long getLeftMostCount(
			final ByteArrayId partition ) {
		return (long) Math.ceil(histogram.sum(
				histogram.getMinValue(),
				true));
	}

	public long totalSampleSize() {
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
		histogram.add(
				1,
				num);
	}

	@Override
	public String toString() {
		final StringBuffer buffer = new StringBuffer();
		final Pair<ByteArrayId, ByteArrayId> indexAndPartition = decomposeIndexAndPartitionFromId(statisticsId);
		buffer.append(
				"histogram[index=").append(
				indexAndPartition.getLeft().getString());
		if (indexAndPartition.getRight() != null && indexAndPartition.getRight().getBytes() != null
				&& indexAndPartition.getRight().getBytes().length > 0) {
			buffer.append(
					", partitionAsHex=").append(
					indexAndPartition.getRight().getHexString());
		}
		if (histogram != null) {
			buffer.append(", bins={");
			for (final double v : histogram.quantile(10)) {
				buffer.append(v);
				buffer.append(' ');
			}
			buffer.deleteCharAt(buffer.length() - 1);
			buffer.append("}, counts={");
			for (final long v : histogram.count(10)) {
				buffer.append(
						v).append(
						' ');
			}

			buffer.deleteCharAt(buffer.length() - 1);
			buffer.append("}]");
		}
		buffer.append("]");
		return buffer.toString();
	}

	/**
	 * Convert Row Range Numeric statistics to a JSON object
	 */

	@Override
	public JSONObject toJSONObject()
			throws JSONException {
		final JSONObject jo = new JSONObject();
		jo.put(
				"type",
				STATS_TYPE.getString());
		final Pair<ByteArrayId, ByteArrayId> indexAndPartition = decomposeIndexAndPartitionFromId(statisticsId);
		jo.put(
				"index",
				indexAndPartition.getLeft().getString());
		jo.put(
				"partitionAsHex",
				indexAndPartition.getRight().getHexString());
		if (histogram != null) {
			final JSONObject histogramJson = new JSONObject();
			histogramJson.put(
					"range_min",
					histogram.getMinValue());
			histogramJson.put(
					"range_min",
					histogram.getMinValue());
			histogramJson.put(
					"range_max",
					histogram.getMaxValue());
			histogramJson.put(
					"totalCount",
					histogram.getTotalCount());
			final JSONArray binsArray = new JSONArray();
			for (final double v : histogram.quantile(10)) {
				binsArray.add(v);
			}
			histogramJson.put(
					"bins",
					binsArray);

			final JSONArray countsArray = new JSONArray();
			for (final long v : histogram.count(10)) {
				countsArray.add(v);
			}
			histogramJson.put(
					"counts",
					countsArray);
			jo.put(
					"histogram",
					histogramJson);
		}
		else {
			jo.put(
					"histogram",
					"empty");
		}
		return jo;
	}
}
