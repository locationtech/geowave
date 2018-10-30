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

import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;

public class MaxDuplicatesStatistics<T> extends
		AbstractDataStatistics<T, Integer, IndexStatisticsQueryBuilder<Integer>>
{
	public static final IndexStatisticsType<Integer> STATS_TYPE = new IndexStatisticsType<>(
			"MAX_DUPLICATES");
	private int maxDuplicates = 0;

	public MaxDuplicatesStatistics() {
		super();
	}

	public int getEntriesWithDifferingFieldVisibilities() {
		return maxDuplicates;
	}

	private MaxDuplicatesStatistics(
			final short internalDataAdapterId,
			final String indexName,
			final int maxDuplicates ) {
		super(
				internalDataAdapterId,
				STATS_TYPE,
				indexName);
		this.maxDuplicates = maxDuplicates;
	}

	public MaxDuplicatesStatistics(
			final short internalDataAdapterId,
			final String indexName ) {
		super(
				internalDataAdapterId,
				STATS_TYPE,
				indexName);
	}

	@Override
	public InternalDataStatistics<T, Integer, IndexStatisticsQueryBuilder<Integer>> duplicate() {
		return (InternalDataStatistics) new MaxDuplicatesStatistics<>(
				adapterId,
				extendedId,
				maxDuplicates);
	}

	@Override
	public byte[] toBinary() {
		final ByteBuffer buf = super.binaryBuffer(8);
		buf.putInt(maxDuplicates);
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = super.binaryBuffer(bytes);
		maxDuplicates = buf.getInt();
	}

	@Override
	public void entryIngested(
			final T entry,
			final GeoWaveRow... kvs ) {
		for (final GeoWaveRow kv : kvs) {
			maxDuplicates = Math.max(
					maxDuplicates,
					kv.getNumberOfDuplicates());
		}
	}

	@Override
	public void merge(
			final Mergeable merge ) {
		if ((merge != null) && (merge instanceof MaxDuplicatesStatistics)) {
			maxDuplicates = Math.max(
					maxDuplicates,
					((MaxDuplicatesStatistics) merge).maxDuplicates);
		}
	}

	@Override
	public Integer getResult() {
		return maxDuplicates;
	}

	@Override
	protected String resultsName() {
		return "maxDuplicates";
	}

	@Override
	protected String resultsValue() {
		return Integer.toString(maxDuplicates);
	}
}
