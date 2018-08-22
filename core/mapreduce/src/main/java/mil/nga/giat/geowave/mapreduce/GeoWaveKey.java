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
package mil.nga.giat.geowave.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import mil.nga.giat.geowave.core.index.ByteArrayUtils;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * This is the base class for both GeoWaveInputKey and GeoWaveOutputKey and is
 * responsible for persisting the adapter ID
 */
public abstract class GeoWaveKey implements
		WritableComparable<GeoWaveKey>,
		java.io.Serializable
{
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	protected Short adapterId;

	protected GeoWaveKey() {}

	public GeoWaveKey(
			final short adapterId ) {
		this.adapterId = adapterId;
	}

	public short getadapterId() {
		return adapterId;
	}

	public void setAdapterId(
			final short adapterId ) {
		this.adapterId = adapterId;
	}

	@Override
	public int compareTo(
			final GeoWaveKey o ) {
		byte[] internalAdapterIdBytes = ByteArrayUtils.shortToByteArray(adapterId);
		return WritableComparator.compareBytes(
				internalAdapterIdBytes,
				0,
				internalAdapterIdBytes.length,
				ByteArrayUtils.shortToByteArray(o.adapterId),
				0,
				ByteArrayUtils.shortToByteArray(o.adapterId).length);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((adapterId == null) ? 0 : adapterId.hashCode());
		return result;
	}

	@Override
	public boolean equals(
			Object obj ) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		GeoWaveKey other = (GeoWaveKey) obj;
		if (adapterId == null) {
			if (other.adapterId != null) return false;
		}
		else if (!adapterId.equals(other.adapterId)) return false;
		return true;
	}

	@Override
	public void readFields(
			final DataInput input )
			throws IOException {
		// final int adapterIdLength = input.readInt();
		// final byte[] adapterIdBinary = new byte[adapterIdLength];
		// input.readFully(adapterIdBinary);
		// adapterId = new ByteArrayId(adapterIdBinary);
		adapterId = input.readShort();
	}

	@Override
	public void write(
			final DataOutput output )
			throws IOException {
		// final byte[] adapterIdBinary = adapterId.getBytes();
		// output.writeInt(adapterIdBinary.length);
		// output.write(adapterIdBinary);
		output.writeShort(adapterId);
	}

}
