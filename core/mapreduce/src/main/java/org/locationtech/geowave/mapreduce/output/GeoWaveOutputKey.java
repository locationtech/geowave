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
package org.locationtech.geowave.mapreduce.output;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.WritableComparable;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.adapter.TransientAdapterStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.ingest.GeoWaveData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	protected String typeName;
	private String[] indexNames;
	transient private DataTypeAdapter<T> adapter;

	protected GeoWaveOutputKey() {
		super();
	}

	public GeoWaveOutputKey(
			final String typeName,
			final String indexName ) {
		this.typeName = typeName;
		indexNames = new String[] {
			indexName
		};
	}

	public GeoWaveOutputKey(
			final String typeName,
			final String[] indexNames ) {
		this.typeName = typeName;
		this.indexNames = indexNames;
	}

	public GeoWaveOutputKey(
			final DataTypeAdapter<T> adapter,
			final String[] indexNames ) {
		this.adapter = adapter;
		this.indexNames = indexNames;
		typeName = adapter.getTypeName();
	}

	public GeoWaveOutputKey(
			final GeoWaveData<T> data ) {
		this.adapter = data.getAdapter();
		this.indexNames = data.getIndexNames();
		this.typeName = data.getTypeName();
	}

	public String getTypeName() {
		return typeName;
	}

	public void setTypeName(
			final String typeName ) {
		this.typeName = typeName;
	}

	public String[] getIndexNames() {
		return indexNames;
	}

	public DataTypeAdapter<T> getAdapter(
			final TransientAdapterStore adapterCache ) {
		if (adapter != null) {
			return adapter;
		}
		return (DataTypeAdapter<T>) adapterCache.getAdapter(typeName);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(indexNames);
		result = prime * result + ((typeName == null) ? 0 : typeName.hashCode());
		return result;
	}

	@Override
	public boolean equals(
			Object obj ) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		GeoWaveOutputKey other = (GeoWaveOutputKey) obj;
		if (!Arrays.equals(
				indexNames,
				other.indexNames)) return false;
		if (typeName == null) {
			if (other.typeName != null) return false;
		}
		else if (!typeName.equals(other.typeName)) return false;
		return true;
	}

	@Override
	public int compareTo(
			final GeoWaveOutputKey o ) {
		final int adapterCompare = typeName.compareTo(o.typeName);
		if (adapterCompare != 0) {
			return adapterCompare;
		}
		final int lengthCompare = Integer.compare(
				indexNames.length,
				o.indexNames.length);
		if (lengthCompare != 0) {
			return lengthCompare;
		}
		for (int i = 0; i < indexNames.length; i++) {
			final int indexNameCompare = indexNames[i].compareTo(o.indexNames[i]);
			if (indexNameCompare != 0) {
				return indexNameCompare;
			}
		}
		return 0;
	}

	@Override
	public void readFields(
			final DataInput input )
			throws IOException {
		final int typeNameLength = input.readInt();
		final byte[] typeNameBinary = new byte[typeNameLength];
		input.readFully(typeNameBinary);
		typeName = StringUtils.stringFromBinary(typeNameBinary);
		final byte indexNameCount = input.readByte();
		indexNames = new String[indexNameCount];
		for (int i = 0; i < indexNameCount; i++) {
			final int indexNameLength = input.readInt();
			final byte[] indexNameBytes = new byte[indexNameLength];
			input.readFully(indexNameBytes);
			indexNames[i] = StringUtils.stringFromBinary(indexNameBytes);
		}
	}

	@Override
	public void write(
			final DataOutput output )
			throws IOException {
		final byte[] typeNameBinary = StringUtils.stringToBinary(typeName);
		output.writeInt(typeNameBinary.length);
		output.write(typeNameBinary);
		output.writeByte(indexNames.length);
		for (final String indexName : indexNames) {
			final byte[] indexNameBytes = StringUtils.stringToBinary(indexName);
			output.writeInt(indexNameBytes.length);
			output.write(indexNameBytes);
		}
	}
}
