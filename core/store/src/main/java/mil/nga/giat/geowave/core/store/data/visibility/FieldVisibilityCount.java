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
package mil.nga.giat.geowave.core.store.data.visibility;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.store.adapter.statistics.AbstractDataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.callback.DeleteCallback;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.entities.GeoWaveValue;

public class FieldVisibilityCount<T> extends
		AbstractDataStatistics<T> implements
		DeleteCallback<T, GeoWaveRow>
{
	public static final ByteArrayId STATS_TYPE = new ByteArrayId(
			"FIELD_VISIBILITY_COUNT");
	private final Map<ByteArrayId, Long> countsPerVisibility;

	public FieldVisibilityCount() {
		super();
		countsPerVisibility = new HashMap<ByteArrayId, Long>();
	}

	private FieldVisibilityCount(
			final short internalDataAdapterId,
			final ByteArrayId statisticsId,
			final Map<ByteArrayId, Long> countsPerVisibility ) {
		super(
				internalDataAdapterId,
				composeId(statisticsId));
		this.countsPerVisibility = countsPerVisibility;
	}

	public FieldVisibilityCount(
			final short internalDataAdapterId,
			final ByteArrayId statisticsId ) {
		super(
				internalDataAdapterId,
				composeId(statisticsId));
		countsPerVisibility = new HashMap<ByteArrayId, Long>();
	}

	public static ByteArrayId composeId(
			final ByteArrayId statisticsId ) {
		return composeId(
				STATS_TYPE.getString(),
				statisticsId.getString());
	}

	@Override
	public byte[] toBinary() {
		int bufferSize = 4;
		final List<byte[]> serializedCounts = new ArrayList<byte[]>();
		for (final Entry<ByteArrayId, Long> entry : countsPerVisibility.entrySet()) {
			final byte[] key = entry.getKey().getBytes();
			final ByteBuffer buf = ByteBuffer.allocate(key.length + 12);
			buf.putInt(key.length);
			buf.put(key);
			buf.putLong(entry.getValue());
			final byte[] serializedEntry = buf.array();
			serializedCounts.add(serializedEntry);
			bufferSize += serializedEntry.length;
		}
		final ByteBuffer buf = super.binaryBuffer(bufferSize);
		buf.putInt(serializedCounts.size());
		for (final byte[] count : serializedCounts) {
			buf.put(count);
		}
		return buf.array();
	}

	@Override
	public DataStatistics<T> duplicate() {
		return new FieldVisibilityCount<T>(
				internalDataAdapterId,
				statisticsId,
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
			countsPerVisibility.put(
					new ByteArrayId(
							id),
					count);
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
				ByteArrayId visibility = new ByteArrayId(
						new byte[] {});
				if (v.getVisibility() != null) {
					visibility = new ByteArrayId(
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

	@Override
	public void merge(
			final Mergeable merge ) {
		if ((merge != null) && (merge instanceof FieldVisibilityCount)) {
			final Map<ByteArrayId, Long> otherCounts = ((FieldVisibilityCount) merge).countsPerVisibility;
			for (final Entry<ByteArrayId, Long> entry : otherCounts.entrySet()) {
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
}
