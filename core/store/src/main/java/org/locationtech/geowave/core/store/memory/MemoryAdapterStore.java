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
package org.locationtech.geowave.core.store.memory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.TransientAdapterStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;

/**
 * This is a simple HashMap based in-memory implementation of the AdapterStore
 * and can be useful if it is undesirable to persist and query objects within
 * another storage mechanism such as an Accumulo table.
 */
public class MemoryAdapterStore implements
		TransientAdapterStore,
		Serializable
{
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	private Map<String, DataTypeAdapter<?>> adapterMap;

	public MemoryAdapterStore() {
		adapterMap = Collections.synchronizedMap(new HashMap<String, DataTypeAdapter<?>>());
	}

	public MemoryAdapterStore(
			final DataTypeAdapter<?>[] adapters ) {
		adapterMap = Collections.synchronizedMap(new HashMap<String, DataTypeAdapter<?>>());
		for (final DataTypeAdapter<?> adapter : adapters) {
			adapterMap.put(
					adapter.getTypeName(),
					adapter);
		}
	}

	@Override
	public void addAdapter(
			final DataTypeAdapter<?> adapter ) {
		adapterMap.put(
				adapter.getTypeName(),
				adapter);
	}

	@Override
	public DataTypeAdapter<?> getAdapter(
			final String typeName ) {
		return adapterMap.get(typeName);
	}

	@Override
	public boolean adapterExists(
			final String typeName ) {
		return adapterMap.containsKey(typeName);
	}

	@Override
	public CloseableIterator<DataTypeAdapter<?>> getAdapters() {
		return new CloseableIterator.Wrapper<DataTypeAdapter<?>>(
				new ArrayList<DataTypeAdapter<?>>(
						adapterMap.values()).iterator());
	}

	@Override
	public void removeAll() {
		adapterMap.clear();
	}

	private void writeObject(
			final java.io.ObjectOutputStream out )
			throws IOException {
		final int count = adapterMap.size();
		out.writeInt(count);
		for (final Map.Entry<String, DataTypeAdapter<?>> entry : adapterMap.entrySet()) {
			out.writeUTF(entry.getKey());
			final byte[] val = PersistenceUtils.toBinary(entry.getValue());
			out.writeObject(val);
		}
	}

	private void readObject(
			final java.io.ObjectInputStream in )
			throws IOException,
			ClassNotFoundException {
		final int count = in.readInt();
		adapterMap = Collections.synchronizedMap(new HashMap<String, DataTypeAdapter<?>>());
		for (int i = 0; i < count; i++) {
			final String id = in.readUTF();
			final byte[] data = (byte[]) in.readObject();
			adapterMap.put(
					id,
					(DataTypeAdapter<?>) PersistenceUtils.fromBinary(data));
		}
	}

	public void removeAdapter(
			String typeName ) {
		adapterMap.remove(typeName);
	}
}
