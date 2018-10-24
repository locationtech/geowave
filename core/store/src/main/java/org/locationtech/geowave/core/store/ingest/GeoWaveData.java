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
package org.locationtech.geowave.core.store.ingest;

import java.util.Collection;

import org.locationtech.geowave.core.store.adapter.TransientAdapterStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;

/**
 * This models any information that is necessary to ingest an entry into
 * GeoWave: the adapter and index you wish to use as well as the actual data
 *
 * @param <T>
 *            The java type for the actual data being ingested
 */
public class GeoWaveData<T>
{
	protected String typeName;
	private final String[] indexNames;
	private final T data;
	transient private DataTypeAdapter<T> adapter;

	public GeoWaveData(
			final String typeName,
			final String[] indexNames,
			final T data ) {
		this.typeName = typeName;
		this.indexNames = indexNames;
		this.data = data;
	}

	public GeoWaveData(
			final DataTypeAdapter<T> adapter,
			final String[] indexNames,
			final T data ) {
		this.adapter = adapter;
		this.indexNames = indexNames;
		this.data = data;
	}

	public String[] getIndexNames() {
		return indexNames;
	}

	public T getValue() {
		return data;
	}

	public DataTypeAdapter<T> getAdapter() {
		return adapter;
	}

	public DataTypeAdapter<T> getAdapter(
			final TransientAdapterStore adapterCache ) {
		if (adapter != null) {
			return adapter;
		}
		return (DataTypeAdapter<T>) adapterCache.getAdapter(typeName);
	}

	public String getTypeName() {
		return typeName;
	}
}
