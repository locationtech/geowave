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
package mil.nga.giat.geowave.core.store;

import java.nio.ByteBuffer;
import java.util.Arrays;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.persist.Persistable;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

/**
 * Meta-data for retaining Adapter to Index association
 * 
 * 
 * 
 */
public class AdapterToIndexMapping implements
		Persistable
{
	private ByteArrayId adapterId;
	private ByteArrayId[] indexIds;

	public AdapterToIndexMapping() {

	}

	public AdapterToIndexMapping(
			ByteArrayId adapterId,
			PrimaryIndex[] indices ) {
		super();
		this.adapterId = adapterId;
		this.indexIds = new ByteArrayId[indices.length];
		for (int i = 0; i < indices.length; i++)
			indexIds[i] = indices[i].getId();
	}

	public AdapterToIndexMapping(
			ByteArrayId adapterId,
			ByteArrayId[] indexIds ) {
		super();
		this.adapterId = adapterId;
		this.indexIds = indexIds;
	}

	public ByteArrayId getAdapterId() {
		return adapterId;
	}

	public ByteArrayId[] getIndexIds() {
		return indexIds;
	}

	public PrimaryIndex[] getIndices(
			IndexStore indexStore ) {
		final PrimaryIndex[] indices = new PrimaryIndex[indexIds.length];
		for (int i = 0; i < this.indexIds.length; i++) {
			indices[i] = (PrimaryIndex) indexStore.getIndex(this.indexIds[i]);
		}
		return indices;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((adapterId == null) ? 0 : adapterId.hashCode());
		result = prime * result + Arrays.hashCode(indexIds);
		return result;
	}

	@Override
	public boolean equals(
			Object obj ) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		AdapterToIndexMapping other = (AdapterToIndexMapping) obj;
		if (adapterId == null) {
			if (other.adapterId != null) return false;
		}
		else if (!adapterId.equals(other.adapterId)) return false;
		if (!Arrays.equals(
				indexIds,
				other.indexIds)) return false;
		return true;
	}

	public boolean contains(
			ByteArrayId indexId ) {
		for (final ByteArrayId id : this.indexIds)
			if (id.equals(indexId)) return true;
		return false;
	}

	public boolean isNotEmpty() {
		return this.indexIds.length > 0;
	}

	@Override
	public byte[] toBinary() {
		final byte[] adapterIdBytes = this.adapterId.getBytes();
		final byte[] indexIdBytes = ByteArrayId.toBytes(this.indexIds);
		final ByteBuffer buf = ByteBuffer.allocate(adapterIdBytes.length + 4 + indexIdBytes.length);
		buf.putInt(adapterIdBytes.length);
		buf.put(adapterIdBytes);
		buf.put(indexIdBytes);
		return buf.array();
	}

	@Override
	public void fromBinary(
			byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		int l = buf.getInt();
		final byte[] adapterIdBytes = new byte[l];
		buf.get(adapterIdBytes);
		this.adapterId = new ByteArrayId(
				adapterIdBytes);
		final byte[] indexIdBytes = new byte[bytes.length - 4 - adapterIdBytes.length];
		buf.get(indexIdBytes);
		this.indexIds = ByteArrayId.fromBytes(indexIdBytes);
	}
}
