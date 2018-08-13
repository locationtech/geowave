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
package mil.nga.giat.geowave.mapreduce.input;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.mapreduce.GeoWaveKey;

/**
 * This class encapsulates the unique identifier for GeoWave input data using a
 * map-reduce GeoWave input format. The combination of the the adapter ID and
 * the data ID should be unique.
 */
public class GeoWaveInputKey implements
		WritableComparable<GeoWaveInputKey>,
		java.io.Serializable
{
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	protected Short internalAdapterId;
	private ByteArrayId dataId;
	private transient mil.nga.giat.geowave.core.store.entities.GeoWaveKey key;

	public GeoWaveInputKey() {

	}

	public GeoWaveInputKey(
			final mil.nga.giat.geowave.core.store.entities.GeoWaveKey key,
			final ByteArrayId indexId ) {
		this(
				key.getInternalAdapterId(),
				key,
				indexId);
	}

	public GeoWaveInputKey(
			final short internalAdapterId,
			final ByteArrayId dataId ) {
		this.internalAdapterId = internalAdapterId;
		this.dataId = dataId;
	}

	public GeoWaveInputKey(
			final short internalAdapterId,
			final mil.nga.giat.geowave.core.store.entities.GeoWaveKey key,
			final ByteArrayId indexId ) {
		this.internalAdapterId = internalAdapterId;
		if (key.getNumberOfDuplicates() > 0) {
			dataId = new ByteArrayId(
					key.getDataId());
		}
		else {
			// if deduplication should be disabled, prefix the actual data
			// ID with the index ID concatenated with the insertion
			// ID to gaurantee uniqueness and effectively disable
			// aggregating by only the data ID
			byte[] idBytes = key.getDataId();
			if (key.getSortKey() != null) {
				idBytes = ArrayUtils.addAll(
						key.getSortKey(),
						idBytes);
			}
			if (key.getPartitionKey() != null) {
				idBytes = ArrayUtils.addAll(
						key.getPartitionKey(),
						idBytes);
			}
			if (indexId != null) {
				idBytes = ArrayUtils.addAll(
						indexId.getBytes(),
						idBytes);
			}
			dataId = new ByteArrayId(
					idBytes);
		}
		this.key = key;
	}

	public Pair<byte[], byte[]> getPartitionAndSortKey(
			final PrimaryIndex index ) {
		final int partitionKeyLength = index.getIndexStrategy().getPartitionKeyLength();
		final int indexIdLength = index.getId().getBytes().length;
		if (dataId.getBytes().length < (indexIdLength + partitionKeyLength)) {
			return null;
		}
		else {
			final byte[] partitionKey = Arrays.copyOfRange(
					dataId.getBytes(),
					indexIdLength,
					indexIdLength + partitionKeyLength);
			final byte[] sortKey = Arrays.copyOfRange(
					dataId.getBytes(),
					indexIdLength + partitionKeyLength,
					dataId.getBytes().length);
			return ImmutablePair.of(
					partitionKey,
					sortKey);
		}
	}

	public mil.nga.giat.geowave.core.store.entities.GeoWaveKey getGeoWaveKey() {
		return key;
	}

	public void setGeoWaveKey(
			final mil.nga.giat.geowave.core.store.entities.GeoWaveKey key ) {
		this.key = key;
	}

	public short getInternalAdapterId() {
		return internalAdapterId;
	}

	public void setInternalAdapterId(
			final short internalAdapterId ) {
		this.internalAdapterId = internalAdapterId;
	}

	public void setDataId(
			final ByteArrayId dataId ) {
		this.dataId = dataId;
	}

	public ByteArrayId getDataId() {
		return dataId;
	}

	@Override
	public int compareTo(
			final GeoWaveInputKey o ) {
		byte[] internalAdapterIdBytes = ByteArrayUtils.shortToByteArray(internalAdapterId);
		final int adapterCompare = WritableComparator.compareBytes(
				internalAdapterIdBytes,
				0,
				internalAdapterIdBytes.length,
				ByteArrayUtils.shortToByteArray(o.internalAdapterId),
				0,
				ByteArrayUtils.shortToByteArray(o.internalAdapterId).length);

		if (adapterCompare != 0) {
			return adapterCompare;
		}
		final GeoWaveInputKey other = (GeoWaveInputKey) o;
		return WritableComparator.compareBytes(
				dataId.getBytes(),
				0,
				dataId.getBytes().length,
				other.dataId.getBytes(),
				0,
				other.dataId.getBytes().length);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((dataId == null) ? 0 : dataId.hashCode());
		result = prime * result + ((internalAdapterId == null) ? 0 : internalAdapterId.hashCode());
		return result;
	}

	@Override
	public boolean equals(
			Object obj ) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		GeoWaveInputKey other = (GeoWaveInputKey) obj;
		if (dataId == null) {
			if (other.dataId != null) return false;
		}
		else if (!dataId.equals(other.dataId)) return false;
		if (internalAdapterId == null) {
			if (other.internalAdapterId != null) return false;
		}
		else if (!internalAdapterId.equals(other.internalAdapterId)) return false;
		return true;
	}

	@Override
	public void readFields(
			final DataInput input )
			throws IOException {
		internalAdapterId = input.readShort();
		final int dataIdLength = input.readInt();
		final byte[] dataIdBytes = new byte[dataIdLength];
		input.readFully(dataIdBytes);
		dataId = new ByteArrayId(
				dataIdBytes);
	}

	@Override
	public void write(
			final DataOutput output )
			throws IOException {
		output.writeShort(internalAdapterId);
		output.writeInt(dataId.getBytes().length);
		output.write(dataId.getBytes());
	}
}
