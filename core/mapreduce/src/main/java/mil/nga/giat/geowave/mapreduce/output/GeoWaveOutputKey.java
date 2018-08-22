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
package mil.nga.giat.geowave.mapreduce.output;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.InternalDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.TransientAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.mapreduce.GeoWaveKey;

/**
 * This class encapsulates the unique identifier for GeoWave to ingest data
 * using a map-reduce GeoWave output format. The record writer must have bother
 * the adapter and the index for the data element to ingest.
 */
public class GeoWaveOutputKey<T> implements
		WritableComparable<GeoWaveOutputKey>,
		java.io.Serializable
{
	private final static Logger LOGGER = LoggerFactory.getLogger(GeoWaveOutputKey.class);
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	protected ByteArrayId adapterId;
	private Collection<ByteArrayId> indexIds;
	transient private WritableDataAdapter<T> adapter;

	protected GeoWaveOutputKey() {
		super();
	}

	public GeoWaveOutputKey(
			final ByteArrayId adapterId,
			final ByteArrayId indexId ) {
		this.adapterId = adapterId;
		indexIds = Arrays.asList(indexId);
	}

	public GeoWaveOutputKey(
			final ByteArrayId adapterId,
			final Collection<ByteArrayId> indexIds ) {
		this.adapterId = adapterId;
		this.indexIds = indexIds;
	}

	public GeoWaveOutputKey(
			final WritableDataAdapter<T> adapter,
			final Collection<ByteArrayId> indexIds ) {
		this.adapter = adapter;
		this.indexIds = indexIds;
		adapterId = adapter.getAdapterId();
	}

	public ByteArrayId getAdapterId() {
		return adapterId;
	}

	public void setAdapterId(
			final ByteArrayId adapterId ) {
		this.adapterId = adapterId;
	}

	public Collection<ByteArrayId> getIndexIds() {
		return indexIds;
	}

	public WritableDataAdapter<T> getAdapter(
			final TransientAdapterStore adapterCache ) {
		if (adapter != null) {
			return adapter;
		}
		final DataAdapter<?> adapter = adapterCache.getAdapter(adapterId);
		if (adapter instanceof WritableDataAdapter) {
			return (WritableDataAdapter<T>) adapter;
		}
		LOGGER.warn("Adapter is not writable");
		return null;
	}

	@Override
	public int compareTo(
			final GeoWaveOutputKey o ) {
		final int adapterCompare = WritableComparator.compareBytes(
				adapterId.getBytes(),
				0,
				adapterId.getBytes().length,
				o.adapterId.getBytes(),
				0,
				o.adapterId.getBytes().length);
		if (adapterCompare != 0) {
			return adapterCompare;
		}
		final GeoWaveOutputKey other = (GeoWaveOutputKey) o;
		final byte[] thisIndex = getConcatenatedIndexId();
		final byte[] otherIndex = other.getConcatenatedIndexId();
		return WritableComparator.compareBytes(
				thisIndex,
				0,
				thisIndex.length,
				otherIndex,
				0,
				otherIndex.length);
	}

	private byte[] getConcatenatedIndexId() {
		final Iterator<ByteArrayId> iterator = indexIds.iterator();
		byte[] bytes = iterator.next().getBytes();
		if (indexIds.size() > 1) {
			while (iterator.hasNext()) {
				bytes = ArrayUtils.addAll(
						bytes,
						iterator.next().getBytes());
			}
		}
		return bytes;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((adapterId == null) ? 0 : adapterId.hashCode());
		result = (prime * result) + ((indexIds == null) ? 0 : indexIds.hashCode());
		return result;
	}

	@Override
	public boolean equals(
			final Object obj ) {
		if (this == obj) {
			return true;
		}
		if (!super.equals(obj)) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final GeoWaveOutputKey other = (GeoWaveOutputKey) obj;
		if (adapterId == null) {
			if (other.adapterId != null) return false;
		}
		else if (!adapterId.equals(other.adapterId)) return false;
		if (indexIds == null) {
			if (other.indexIds != null) {
				return false;
			}
		}
		else if (!indexIds.equals(other.indexIds)) {
			return false;
		}
		return true;
	}

	@Override
	public void readFields(
			final DataInput input )
			throws IOException {
		final int adapterIdLength = input.readInt();
		final byte[] adapterIdBinary = new byte[adapterIdLength];
		input.readFully(adapterIdBinary);
		adapterId = new ByteArrayId(
				adapterIdBinary);
		final byte indexIdCount = input.readByte();
		indexIds = new ArrayList<ByteArrayId>();
		for (int i = 0; i < indexIdCount; i++) {
			final int indexIdLength = input.readInt();
			final byte[] indexIdBytes = new byte[indexIdLength];
			input.readFully(indexIdBytes);
			indexIds.add(new ByteArrayId(
					indexIdBytes));
		}
	}

	@Override
	public void write(
			final DataOutput output )
			throws IOException {
		final byte[] adapterIdBinary = adapterId.getBytes();
		output.writeInt(adapterIdBinary.length);
		output.write(adapterIdBinary);
		output.writeByte(indexIds.size());
		for (final ByteArrayId indexId : indexIds) {
			output.writeInt(indexId.getBytes().length);
			output.write(indexId.getBytes());
		}
	}
}
