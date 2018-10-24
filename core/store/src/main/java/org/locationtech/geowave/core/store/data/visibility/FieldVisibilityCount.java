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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.statistics.AbstractDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.adapter.statistics.IndexStatisticsQueryBuilder;
import org.locationtech.geowave.core.store.adapter.statistics.IndexStatisticsType;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.callback.DeleteCallback;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.util.VisibilityExpression;

import com.google.common.collect.Sets;

public class FieldVisibilityCount<T> extends
		AbstractDataStatistics<T, Map<ByteArray, Long>, IndexStatisticsQueryBuilder<Map<ByteArray, Long>>> implements
		DeleteCallback<T, GeoWaveRow>
{
	public static final IndexStatisticsType<Map<ByteArray, Long>> STATS_TYPE = new IndexStatisticsType<>(
			"FIELD_VISIBILITY_COUNT");
	private final Map<ByteArray, Long> countsPerVisibility;

	public FieldVisibilityCount() {
		super();
		countsPerVisibility = new HashMap<>();
	}

	private FieldVisibilityCount(
			final short internalDataAdapterId,
			final String indexName,
			final Map<ByteArray, Long> countsPerVisibility ) {
		super(
				internalDataAdapterId,
				STATS_TYPE,
				indexName);
		this.countsPerVisibility = countsPerVisibility;
	}

	public FieldVisibilityCount(
			final short internalDataAdapterId,
			final String indexName ) {
		this(
				internalDataAdapterId,
				indexName,
				new HashMap<>());
	}

	@Override
	public byte[] toBinary() {
		int bufferSize = 4;
		final List<byte[]> serializedCounts = new ArrayList<>();
		for (final Entry<ByteArray, Long> entry : countsPerVisibility.entrySet()) {
			if (entry.getValue() != 0) {
				final byte[] key = entry.getKey().getBytes();
				final ByteBuffer buf = ByteBuffer.allocate(key.length + 12);
				buf.putInt(key.length);
				buf.put(key);
				buf.putLong(entry.getValue());
				final byte[] serializedEntry = buf.array();
				serializedCounts.add(serializedEntry);
				bufferSize += serializedEntry.length;
			}
		}
		final ByteBuffer buf = super.binaryBuffer(bufferSize);
		buf.putInt(serializedCounts.size());
		for (final byte[] count : serializedCounts) {
			buf.put(count);
		}
		return buf.array();
	}

	@Override
	public InternalDataStatistics<T, Map<ByteArray, Long>, IndexStatisticsQueryBuilder<Map<ByteArray, Long>>> duplicate() {
		return new FieldVisibilityCount<>(
				adapterId,
				extendedId,
				this.countsPerVisibility);
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = super.binaryBuffer(bytes);
		final int size = buf.getInt();
		countsPerVisibility.clear();
		for (int i = 0; i < size; i++) {
			final int idCount = buf.getInt();
			final byte[] id = new byte[idCount];
			buf.get(id);
			final long count = buf.getLong();
			if (count != 0) {
				countsPerVisibility.put(
						new ByteArray(
								id),
						count);
			}
		}
	}

	@Override
	public void entryIngested(
			final T entry,
			final GeoWaveRow... kvs ) {
		updateEntry(
				1,
				kvs);
	}

	private void updateEntry(
			final int incrementValue,
			final GeoWaveRow... kvs ) {
		for (final GeoWaveRow row : kvs) {
			final GeoWaveValue[] values = row.getFieldValues();
			for (final GeoWaveValue v : values) {
				ByteArray visibility = new ByteArray(
						new byte[] {});
				if (v.getVisibility() != null) {
					visibility = new ByteArray(
							v.getVisibility());
				}
				Long count = countsPerVisibility.get(visibility);
				if (count == null) {
					count = 0L;
				}
				countsPerVisibility.put(
						visibility,
						count + incrementValue);
			}
		}
	}

	@Override
	public void entryDeleted(
			final T entry,
			final GeoWaveRow... kvs ) {
		updateEntry(
				-1,
				kvs);
	}

	public boolean isAuthorizationsLimiting(
			final String... authorizations ) {
		final Set<String> set = Sets.newHashSet(authorizations);
		for (final Entry<ByteArray, Long> vis : countsPerVisibility.entrySet()) {
			if ((vis.getValue() > 0) && (vis.getKey() != null) && (vis.getKey().getBytes().length > 0)
					&& !VisibilityExpression.evaluate(
							vis.getKey().getString(),
							set)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public void merge(
			final Mergeable merge ) {
		if ((merge != null) && (merge instanceof FieldVisibilityCount)) {
			final Map<ByteArray, Long> otherCounts = ((FieldVisibilityCount) merge).countsPerVisibility;
			for (final Entry<ByteArray, Long> entry : otherCounts.entrySet()) {
				Long count = countsPerVisibility.get(entry.getKey());
				if (count == null) {
					count = 0L;
				}
				countsPerVisibility.put(
						entry.getKey(),
						count + entry.getValue());
			}
		}
	}

	public static FieldVisibilityCount getVisibilityCounts(
			final Index index,
			final Collection<Short> adapterIdsToQuery,
			final DataStatisticsStore statisticsStore,
			final String... authorizations ) {
		FieldVisibilityCount combinedVisibilityCount = null;
		for (final short adapterId : adapterIdsToQuery) {
			try (final CloseableIterator<InternalDataStatistics<?, ?, ?>> adapterVisibilityCountIt = statisticsStore
					.getDataStatistics(
							adapterId,
							index.getName(),
							STATS_TYPE,
							authorizations)) {
				if (adapterVisibilityCountIt.hasNext()) {
					final FieldVisibilityCount adapterVisibilityCount = (FieldVisibilityCount) adapterVisibilityCountIt
							.next();
					if (combinedVisibilityCount == null) {
						combinedVisibilityCount = adapterVisibilityCount;
					}
					else {
						combinedVisibilityCount.merge(adapterVisibilityCount);
					}
				}
			}
		}
		return combinedVisibilityCount;
	}

	@Override
	public Map<ByteArray, Long> getResult() {
		return countsPerVisibility;
	}

	@Override
	protected String resultsName() {
		return "countsPerVisibility";
	}

	@Override
	protected Object resultsValue() {
		final Map<String, Object> retVal = new HashMap<>();
		for (final Entry<ByteArray, Long> entry : countsPerVisibility.entrySet()) {
			retVal.put(
					entry.getKey().getString(),
					entry.getValue());
		}
		return retVal;
	}
}
