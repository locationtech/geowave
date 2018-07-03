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
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.store.adapter.InternalAdapterStore;
import mil.nga.giat.geowave.core.store.callback.DeleteCallback;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

public class DuplicateEntryCount<T> extends
		AbstractDataStatistics<T> implements
		DeleteCallback<T, GeoWaveRow>
{
	public static final ByteArrayId STATS_TYPE = new ByteArrayId(
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
			final ByteArrayId statsId,
			final long entriesWithDuplicates ) {
		super(
				internalDataAdapterId,
				statsId);
		this.entriesWithDuplicates = entriesWithDuplicates;
	}

	public DuplicateEntryCount(
			final ByteArrayId indexId ) {
		this(
				null,
				indexId);
	}

	public DuplicateEntryCount(
			final Short internalDataAdapterId,
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
		return new DuplicateEntryCount<>(
				internalDataAdapterId,
				statisticsId,
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

	@Override
	public void entryDeleted(
			final T entry,
			final GeoWaveRow... kvs ) {
		if (kvs.length > 0) {
			if (entryHasDuplicates(kvs[0])) {
				entriesWithDuplicates--;
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
			final PrimaryIndex index,
			final List<Short> adapterIdsToQuery,
			final DataStatisticsStore statisticsStore,
			final String... authorizations ) {
		DuplicateEntryCount combinedDuplicateCount = null;
		for (final short adapterId : adapterIdsToQuery) {
			final DuplicateEntryCount adapterVisibilityCount = (DuplicateEntryCount) statisticsStore.getDataStatistics(
					adapterId,
					DuplicateEntryCount.composeId(index.getId()),
					authorizations);
			if (combinedDuplicateCount == null) {
				combinedDuplicateCount = adapterVisibilityCount;
			}
			else {
				combinedDuplicateCount.merge(adapterVisibilityCount);
			}
		}
		return combinedDuplicateCount;
	}

	/**
	 * Convert Duplicate Count statistics to a JSON object
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
				"statisticsID",
				statisticsId.getString());
		jo.put(
				"dataAdapterID",
				store.getAdapterId(internalDataAdapterId));
		jo.put(
				"count",
				entriesWithDuplicates);

		return jo;
	}
}
