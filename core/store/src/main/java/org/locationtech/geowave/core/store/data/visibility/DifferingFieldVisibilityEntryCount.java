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
package org.locationtech.geowave.core.store.data.visibility;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.statistics.AbstractDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.callback.DeleteCallback;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.index.PrimaryIndex;

import net.sf.json.JSONException;
import net.sf.json.JSONObject;

public class DifferingFieldVisibilityEntryCount<T> extends
		AbstractDataStatistics<T> implements
		DeleteCallback<T, GeoWaveRow>
{
	public static final ByteArrayId STATS_TYPE = new ByteArrayId(
			"DIFFERING_VISIBILITY_COUNT");

	private long entriesWithDifferingFieldVisibilities = 0;

	public DifferingFieldVisibilityEntryCount() {
		super();
	}

	public long getEntriesWithDifferingFieldVisibilities() {
		return entriesWithDifferingFieldVisibilities;
	}

	public boolean isAnyEntryDifferingFieldVisiblity() {
		return entriesWithDifferingFieldVisibilities > 0;
	}

	private DifferingFieldVisibilityEntryCount(
			final short internalDataAdapterId,
			final ByteArrayId statisticsId,
			final long entriesWithDifferingFieldVisibilities ) {
		super(
				internalDataAdapterId,
				composeId(statisticsId));
		this.entriesWithDifferingFieldVisibilities = entriesWithDifferingFieldVisibilities;
	}

	public DifferingFieldVisibilityEntryCount(
			final short internalDataAdapterId,
			final ByteArrayId statisticsId ) {
		super(
				internalDataAdapterId,
				composeId(statisticsId));
	}

	public static ByteArrayId composeId(
			final ByteArrayId statisticsId ) {
		return composeId(
				STATS_TYPE.getString(),
				statisticsId.getString());
	}

	@Override
	public DataStatistics<T> duplicate() {
		return new DifferingFieldVisibilityEntryCount<>(
				internalDataAdapterId,
				statisticsId,
				entriesWithDifferingFieldVisibilities);
	}

	@Override
	public byte[] toBinary() {
		final ByteBuffer buf = super.binaryBuffer(8);
		buf.putLong(entriesWithDifferingFieldVisibilities);
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = super.binaryBuffer(bytes);
		entriesWithDifferingFieldVisibilities = buf.getLong();
	}

	@Override
	public void entryIngested(
			final T entry,
			final GeoWaveRow... kvs ) {
		for (final GeoWaveRow kv : kvs) {
			if (entryHasDifferentVisibilities(kv)) {
				if (ids.add(new ByteArrayId(
						kvs[0].getDataId()))) {
					entriesWithDifferingFieldVisibilities++;
				}
			}
		}
	}

	/**
	 * This is expensive, but necessary since there may be duplicates
	 */
	// TODO entryDeleted should only be called once with all duplicates
	private transient HashSet<ByteArrayId> ids = new HashSet<ByteArrayId>();

	@Override
	public void entryDeleted(
			final T entry,
			final GeoWaveRow... kvs ) {
		for (final GeoWaveRow kv : kvs) {
			if (entryHasDifferentVisibilities(kv)) {
				entriesWithDifferingFieldVisibilities--;
			}
		}
	}

	@Override
	public void merge(
			final Mergeable merge ) {
		if ((merge != null) && (merge instanceof DifferingFieldVisibilityEntryCount)) {
			entriesWithDifferingFieldVisibilities += ((DifferingFieldVisibilityEntryCount) merge).entriesWithDifferingFieldVisibilities;
		}
	}

	private static boolean entryHasDifferentVisibilities(
			final GeoWaveRow geowaveRow ) {
		if ((geowaveRow.getFieldValues() != null) && (geowaveRow.getFieldValues().length > 1)) {
			// if there is 0 or 1 field, there won't be differing visibilities
			return true;
		}
		return false;

	}

	public static DifferingFieldVisibilityEntryCount getVisibilityCounts(
			final PrimaryIndex index,
			final Collection<Short> adapterIdsToQuery,
			final DataStatisticsStore statisticsStore,
			final String... authorizations ) {
		DifferingFieldVisibilityEntryCount combinedVisibilityCount = null;
		for (final short adapterId : adapterIdsToQuery) {
			final DifferingFieldVisibilityEntryCount adapterVisibilityCount = (DifferingFieldVisibilityEntryCount) statisticsStore
					.getDataStatistics(
							adapterId,
							DifferingFieldVisibilityEntryCount.composeId(index.getId()),
							authorizations);
			if (combinedVisibilityCount == null) {
				combinedVisibilityCount = adapterVisibilityCount;
			}
			else {
				combinedVisibilityCount.merge(adapterVisibilityCount);
			}
		}
		return combinedVisibilityCount;
	}

	/**
	 * Convert Differing Visibility statistics to a JSON object
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
				"statisticsID",
				statisticsId.getString());
		jo.put(
				"count",
				entriesWithDifferingFieldVisibilities);
		return jo;
	}
}
