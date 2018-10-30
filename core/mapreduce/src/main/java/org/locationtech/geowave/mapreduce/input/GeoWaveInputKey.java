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
package org.locationtech.geowave.mapreduce.input;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.api.Index;

import com.google.common.primitives.Bytes;

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
	private ByteArray dataId;
	private transient org.locationtech.geowave.core.store.entities.GeoWaveKey key;

	public GeoWaveInputKey() {

	}

	public GeoWaveInputKey(
			final org.locationtech.geowave.core.store.entities.GeoWaveKey key,
			final String indexName ) {
		this(
				key.getAdapterId(),
				key,
				indexName);
	}

	public GeoWaveInputKey(
			final short internalAdapterId,
			final ByteArray dataId ) {
		this.internalAdapterId = internalAdapterId;
		this.dataId = dataId;
	}

	public GeoWaveInputKey(
			final short internalAdapterId,
			final org.locationtech.geowave.core.store.entities.GeoWaveKey key,
			final String indexName ) {
		this.internalAdapterId = internalAdapterId;
		if (key.getNumberOfDuplicates() > 0) {
			dataId = new ByteArray(
					key.getDataId());
		}
		else {
			// if deduplication should be disabled, prefix the actual data
			// ID with the index ID concatenated with the insertion
			// ID to gaurantee uniqueness and effectively disable
			// aggregating by only the data ID
			dataId = new ByteArray(
					Bytes.concat(
							indexName == null ? new byte[0] : StringUtils.stringToBinary(indexName),
							key.getPartitionKey() == null ? new byte[0] : key.getPartitionKey(),
							key.getSortKey() == null ? new byte[0] : key.getSortKey(),
							key.getDataId()));
		}
		this.key = key;
	}

	public Pair<byte[], byte[]> getPartitionAndSortKey(
			final Index index ) {
		final int partitionKeyLength = index.getIndexStrategy().getPartitionKeyLength();
		final int indexIdLength = StringUtils.stringToBinary(index.getName()).length;
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

	public org.locationtech.geowave.core.store.entities.GeoWaveKey getGeoWaveKey() {
		return key;
	}

	public void setGeoWaveKey(
			final org.locationtech.geowave.core.store.entities.GeoWaveKey key ) {
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
			final ByteArray dataId ) {
		this.dataId = dataId;
	}

	public ByteArray getDataId() {
		return dataId;
	}

	@Override
	public int compareTo(
			final GeoWaveInputKey o ) {
		final byte[] internalAdapterIdBytes = ByteArrayUtils.shortToByteArray(internalAdapterId);
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
		final GeoWaveInputKey other = o;
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
		result = (prime * result) + ((dataId == null) ? 0 : dataId.hashCode());
		result = (prime * result) + ((internalAdapterId == null) ? 0 : internalAdapterId.hashCode());
		return result;
	}

	@Override
	public boolean equals(
			final Object obj ) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final GeoWaveInputKey other = (GeoWaveInputKey) obj;
		if (dataId == null) {
			if (other.dataId != null) {
				return false;
			}
		}
		else if (!dataId.equals(other.dataId)) {
			return false;
		}
		if (internalAdapterId == null) {
			if (other.internalAdapterId != null) {
				return false;
			}
		}
		else if (!internalAdapterId.equals(other.internalAdapterId)) {
			return false;
		}
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
		dataId = new ByteArray(
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
