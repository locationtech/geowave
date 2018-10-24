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
import java.util.HashSet;
import java.util.List;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.callback.DeleteCallback;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;

public class DuplicateEntryCount<T> extends
		AbstractDataStatistics<T, Long, IndexStatisticsQueryBuilder<Long>> implements
		DeleteCallback<T, GeoWaveRow>
{
	public static final IndexStatisticsType<Long> STATS_TYPE = new IndexStatisticsType<>(
			"DUPLICATE_ENTRY_COUNT");
	private long entriesWithDuplicates = 0;

	public DuplicateEntryCount() {
		super();
	}

	public long getEntriesWithDuplicatesCount() {
		return entriesWithDuplicates;
	}

	public boolean isAnyEntryHaveDuplicates() {
		return entriesWithDuplicates > 0;
	}

	private DuplicateEntryCount(
			final Short internalDataAdapterId,
			final String indexName,
			final long entriesWithDuplicates ) {
		super(
				internalDataAdapterId,
				STATS_TYPE,
				indexName);
		this.entriesWithDuplicates = entriesWithDuplicates;
	}

	public DuplicateEntryCount(
			final Short internalDataAdapterId,
			final String indexName ) {
		super(
				internalDataAdapterId,
				STATS_TYPE,
				indexName);
	}

	@Override
	public InternalDataStatistics<T, Long, IndexStatisticsQueryBuilder<Long>> duplicate() {
		return new DuplicateEntryCount<>(
				adapterId,
				extendedId,
				entriesWithDuplicates);
	}

	@Override
	public byte[] toBinary() {
		final ByteBuffer buf = super.binaryBuffer(8);
		buf.putLong(entriesWithDuplicates);
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = super.binaryBuffer(bytes);
		entriesWithDuplicates = buf.getLong();
	}

	@Override
	public void entryIngested(
			final T entry,
			final GeoWaveRow... kvs ) {
		if (kvs.length > 0) {
			if (entryHasDuplicates(kvs[0])) {
				entriesWithDuplicates++;
			}
		}
	}

	/**
	 * This is expensive, but necessary since there may be duplicates
	 */
	// TODO entryDeleted should only be called once with all duplicates
	private transient HashSet<ByteArray> ids = new HashSet<>();

	@Override
	public void entryDeleted(
			final T entry,
			final GeoWaveRow... kvs ) {
		if (kvs.length > 0) {
			if (entryHasDuplicates(kvs[0])) {
				if (ids.add(new ByteArray(
						kvs[0].getDataId()))) {
					entriesWithDuplicates--;
				}
			}
		}
	}

	@Override
	public void merge(
			final Mergeable merge ) {
		if ((merge != null) && (merge instanceof DuplicateEntryCount)) {
			entriesWithDuplicates += ((DuplicateEntryCount) merge).entriesWithDuplicates;
		}
	}

	private static boolean entryHasDuplicates(
			final GeoWaveRow kv ) {
		return kv.getNumberOfDuplicates() > 0;
	}

	public static DuplicateEntryCount getDuplicateCounts(
			final Index index,
			final List<Short> adapterIdsToQuery,
			final DataStatisticsStore statisticsStore,
			final String... authorizations ) {
		DuplicateEntryCount combinedDuplicateCount = null;
		for (final short adapterId : adapterIdsToQuery) {
			try (final CloseableIterator<InternalDataStatistics<?, ?, ?>> adapterVisibilityCountIt = statisticsStore
					.getDataStatistics(
							adapterId,
							index.getName(),
							STATS_TYPE,
							authorizations)) {
				if (adapterVisibilityCountIt.hasNext()) {
					final DuplicateEntryCount adapterVisibilityCount = (DuplicateEntryCount) adapterVisibilityCountIt
							.next();
					if (combinedDuplicateCount == null) {
						combinedDuplicateCount = adapterVisibilityCount;
					}
					else {
						combinedDuplicateCount.merge(adapterVisibilityCount);
					}
				}
			}
		}
		return combinedDuplicateCount;
	}

	@Override
	public Long getResult() {
		return entriesWithDuplicates;
	}

	@Override
	protected String resultsName() {
		return "entriesWithDuplicates";
	}

	@Override
	protected Object resultsValue() {
		return Long.toString(entriesWithDuplicates);
	}
}
