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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.store.adapter.statistics.histogram.NumericHistogram;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;

/**
 *
 * This class really just needs to get ingest callbacks and collect individual
 * RowRangeHistogramStatistics per partition. It should never be persisted as
 * is, and is only a DataStatistic to match the current interfaces for the
 * lowest impact mechanism to store histograms per partition instead of all
 * together
 *
 * @param <T>
 *            The type of the row to keep statistics on
 */
public class RowRangeHistogramStatisticsSet<T> extends
		AbstractDataStatistics<T, Map<ByteArrayId, RowRangeHistogramStatistics<T>>, IndexStatisticsQueryBuilder<Map<ByteArrayId, RowRangeHistogramStatistics<T>>>> implements
		DataStatisticsSet<T, Map<ByteArrayId, RowRangeHistogramStatistics<T>>, NumericHistogram, PartitionStatisticsQueryBuilder<NumericHistogram>, IndexStatisticsQueryBuilder<Map<ByteArrayId, RowRangeHistogramStatistics<T>>>>
{
	public static final IndexStatisticsType<Map<ByteArrayId, RowRangeHistogramStatistics<?>>> STATS_TYPE = new IndexStatisticsType<>(
			RowRangeHistogramStatistics.STATS_TYPE.getString());
	private final Map<ByteArrayId, RowRangeHistogramStatistics<T>> histogramPerPartition = new HashMap<>();

	public RowRangeHistogramStatisticsSet() {
		super();
	}

	public RowRangeHistogramStatisticsSet(
			final Short adapterId,
			final String indexName ) {
		super(
				adapterId,
				(StatisticsType) STATS_TYPE,
				indexName);
	}

	private synchronized RowRangeHistogramStatistics<T> getPartitionStatistic(
			final ByteArrayId partitionKey ) {
		RowRangeHistogramStatistics<T> histogram = histogramPerPartition.get(partitionKey);
		if (histogram == null) {
			histogram = new RowRangeHistogramStatistics<>(
					adapterId,
					extendedId,
					partitionKey);
			histogramPerPartition.put(
					partitionKey,
					histogram);
		}
		return histogram;
	}

	@Override
	public void merge(
			final Mergeable merge ) {
		throw new UnsupportedOperationException(
				"Merge should never be called");
	}

	@Override
	public byte[] toBinary() {
		throw new UnsupportedOperationException(
				"toBinary should never be called");
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		throw new UnsupportedOperationException(
				"fromBinary should never be called");
	}

	@Override
	public void entryIngested(
			final T entry,
			final GeoWaveRow... rows ) {
		if (rows != null) {
			// call entry ingested once per row
			for (final GeoWaveRow row : rows) {
				getPartitionStatistic(
						getPartitionKey(row.getPartitionKey())).entryIngested(
						entry,
						row);
			}
		}
	}

	@Override
	public InternalDataStatistics<T, NumericHistogram, PartitionStatisticsQueryBuilder<NumericHistogram>>[] getStatisticsSet() {
		return histogramPerPartition.values().toArray(
				new InternalDataStatistics[histogramPerPartition.size()]);
	}

	protected static ByteArrayId getPartitionKey(
			final byte[] partitionBytes ) {
		return ((partitionBytes == null) || (partitionBytes.length == 0)) ? null : new ByteArrayId(
				partitionBytes);
	}

	@Override
	public Map<ByteArrayId, RowRangeHistogramStatistics<T>> getResult() {
		return histogramPerPartition;
	}

	@Override
	protected String resultsName() {
		return "histogramSet";
	}

	@Override
	protected Object resultsValue() {
		final Collection<Object> values = new ArrayList<>();
		for (final RowRangeHistogramStatistics<?> h : histogramPerPartition.values()) {
			values.add(h.resultsValue());
		}
		return values;
	}

}
