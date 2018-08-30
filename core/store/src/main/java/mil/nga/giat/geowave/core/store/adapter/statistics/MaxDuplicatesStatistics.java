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

import org.apache.commons.lang3.ArrayUtils;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;

public class MaxDuplicatesStatistics<T> extends
		AbstractDataStatistics<T>
{
	public static final ByteArrayId STATS_TYPE = new ByteArrayId(
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
			final ByteArrayId statsId,
			final int maxDuplicates ) {
		super(
				internalDataAdapterId,
				statsId);
		this.maxDuplicates = maxDuplicates;
	}

	public MaxDuplicatesStatistics(
			final short internalDataAdapterId,
			final ByteArrayId indexId ) {
		super(
				internalDataAdapterId,
				composeId(indexId));
	}

	public static ByteArrayId composeId(
			final ByteArrayId indexId ) {
		return new ByteArrayId(
				ArrayUtils.addAll(
						ArrayUtils.addAll(
								STATS_TYPE.getBytes(),
								STATS_SEPARATOR.getBytes()),
						indexId.getBytes()));
	}

	@Override
	public DataStatistics<T> duplicate() {
		return new MaxDuplicatesStatistics<>(
				internalDataAdapterId,
				statisticsId,
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
}
