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
package mil.nga.giat.geowave.datastore.accumulo.util;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang.ArrayUtils;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloRowId;
import mil.nga.giat.geowave.mapreduce.HadoopWritableSerializationTool;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;

/**
 * This is used internally to translate Accumulo rows into native objects (using
 * the appropriate data adapter). It also performs any client-side filtering. It
 * will peek at the next entry in the accumulo iterator to always maintain a
 * reference to the next value. It maintains the adapter ID, data ID, and
 * original accumulo key in the GeoWaveInputKey for use by the
 * GeoWaveInputFormat.
 *
 * @param <T>
 *            The type for the entry
 */
public class InputFormatIteratorWrapper<T> implements
		Iterator<Entry<GeoWaveInputKey, T>>
{
	private final PrimaryIndex index;
	private final Iterator<Entry<Key, Value>> scannerIt;
	private final QueryFilter clientFilter;
	private final HadoopWritableSerializationTool serializationTool;
	private final boolean isOutputWritable;
	private Entry<GeoWaveInputKey, T> nextValue;
	private final boolean wholeRowEncoding;

	public InputFormatIteratorWrapper(
			final boolean wholeRowEncoding,
			final AdapterStore adapterStore,
			final PrimaryIndex index,
			final Iterator<Entry<Key, Value>> scannerIt,
			final boolean isOutputWritable,
			final QueryFilter clientFilter ) {
		this.wholeRowEncoding = wholeRowEncoding;
		this.serializationTool = new HadoopWritableSerializationTool(
				adapterStore);
		this.index = index;
		this.scannerIt = scannerIt;
		this.clientFilter = clientFilter;
		this.isOutputWritable = isOutputWritable;
	}

	private void findNext() {
		while ((nextValue == null) && scannerIt.hasNext()) {
			final Entry<Key, Value> row = scannerIt.next();
			final Entry<GeoWaveInputKey, T> decodedValue = decodeRow(
					wholeRowEncoding,
					row,
					clientFilter,
					index);
			if (decodedValue != null) {
				nextValue = decodedValue;
				return;
			}
		}
	}

	@SuppressWarnings("unchecked")
	private Entry<GeoWaveInputKey, T> decodeRow(
			final boolean wholeRowEncoding,
			final Entry<Key, Value> row,
			final QueryFilter clientFilter,
			final PrimaryIndex index ) {
		final AccumuloRowId rowId = new AccumuloRowId(
				row.getKey());
		final Object value = AccumuloUtils.decodeRow(
				row.getKey(),
				row.getValue(),
				wholeRowEncoding,
				rowId,
				serializationTool.getAdapterStore(),
				clientFilter,
				index);
		if (value == null) {
			return null;
		}
		final ByteArrayId adapterId = new ByteArrayId(
				rowId.getAdapterId());
		final T result = (T) (isOutputWritable ? serializationTool.getHadoopWritableSerializerForAdapter(
				adapterId).toWritable(
				value) : value);
		final GeoWaveInputKey key = new GeoWaveInputKey(
				adapterId,
				new ByteArrayId(
						// if deduplication is disabled, prefix the actual data
						// ID with the index ID concatenated with the insertion
						// ID to gaurantee uniqueness and effectively disable
						// aggregating by only the data ID
						rowId.isDeduplicationEnabled() ? rowId.getDataId() : ArrayUtils.addAll(
								ArrayUtils.addAll(
										index.getId().getBytes(),
										rowId.getInsertionId()),
								rowId.getDataId())));
		key.setInsertionId(new ByteArrayId(
				rowId.getInsertionId()));
		return new GeoWaveInputFormatEntry(
				key,
				result);
	}

	@Override
	public boolean hasNext() {
		findNext();
		return nextValue != null;
	}

	@Override
	public Entry<GeoWaveInputKey, T> next()
			throws NoSuchElementException {
		final Entry<GeoWaveInputKey, T> previousNext = nextValue;
		if (nextValue == null) {
			throw new NoSuchElementException();
		}
		nextValue = null;
		return previousNext;
	}

	@Override
	public void remove() {
		scannerIt.remove();
	}

	private final class GeoWaveInputFormatEntry implements
			Map.Entry<GeoWaveInputKey, T>
	{
		private final GeoWaveInputKey key;
		private T value;

		public GeoWaveInputFormatEntry(
				final GeoWaveInputKey key,
				final T value ) {
			this.key = key;
			this.value = value;
		}

		@Override
		public GeoWaveInputKey getKey() {
			return key;
		}

		@Override
		public T getValue() {
			return value;
		}

		@Override
		public T setValue(
				final T value ) {
			final T old = this.value;
			this.value = value;
			return old;
		}
	}
}
