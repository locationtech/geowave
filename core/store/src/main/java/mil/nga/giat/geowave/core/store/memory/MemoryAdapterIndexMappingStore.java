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
package mil.nga.giat.geowave.core.store.memory;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.persist.PersistenceUtils;
import mil.nga.giat.geowave.core.store.AdapterToIndexMapping;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;

/**
 * This is a simple HashMap based in-memory implementation of the
 * AdapterIndexMappingStore and can be useful if it is undesirable to persist
 * and query objects within another storage mechanism such as an Accumulo table.
 */
public class MemoryAdapterIndexMappingStore implements
		AdapterIndexMappingStore,
		Serializable
{
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	private Map<ByteArrayId, AdapterToIndexMapping> toIndexMapping;

	public MemoryAdapterIndexMappingStore() {
		toIndexMapping = new HashMap<ByteArrayId, AdapterToIndexMapping>();
	}

	private void writeObject(
			final java.io.ObjectOutputStream out )
			throws IOException {
		final int count = toIndexMapping.size();
		out.writeInt(count);
		for (final Map.Entry<ByteArrayId, AdapterToIndexMapping> entry : toIndexMapping.entrySet()) {
			out.writeObject(entry.getKey());
			final byte[] val = PersistenceUtils.toBinary(entry.getValue());
			out.writeObject(val);
		}
	}

	private void readObject(
			final java.io.ObjectInputStream in )
			throws IOException,
			ClassNotFoundException {
		final int count = in.readInt();
		toIndexMapping = new HashMap<ByteArrayId, AdapterToIndexMapping>();
		for (int i = 0; i < count; i++) {
			final ByteArrayId id = (ByteArrayId) in.readObject();
			final byte[] data = (byte[]) in.readObject();
			toIndexMapping.put(
					id,
					(AdapterToIndexMapping) PersistenceUtils.fromBinary(data));
		}
	}

	@Override
	public AdapterToIndexMapping getIndicesForAdapter(
			ByteArrayId adapterId ) {
		if (toIndexMapping.containsKey(adapterId)) return toIndexMapping.get(adapterId);
		return new AdapterToIndexMapping(
				adapterId,
				new ByteArrayId[0]);
	}

	@Override
	public void addAdapterIndexMapping(
			AdapterToIndexMapping mapping )
			throws MismatchedIndexToAdapterMapping {
		final AdapterToIndexMapping oldMapping = toIndexMapping.get(mapping.getAdapterId());
		if (oldMapping != null && !oldMapping.equals(mapping)) throw new MismatchedIndexToAdapterMapping(
				oldMapping);
		toIndexMapping.put(
				mapping.getAdapterId(),
				mapping);
	}

	@Override
	public void remove(
			ByteArrayId adapterId ) {
		toIndexMapping.remove(adapterId);
	}

	@Override
	public void removeAll() {
		toIndexMapping.clear();
	}

}
