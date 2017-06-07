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
package mil.nga.giat.geowave.core.ingest;

import java.util.Collection;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.mapreduce.output.GeoWaveOutputKey;

/**
 * This models any information that is necessary to ingest an entry into
 * GeoWave: the adapter and index you wish to use as well as the actual data
 * 
 * @param <T>
 *            The java type for the actual data being ingested
 */
public class GeoWaveData<T>
{
	private final GeoWaveOutputKey<T> outputKey;
	private final T data;

	public GeoWaveData(
			final ByteArrayId adapterId,
			final Collection<ByteArrayId> indexIds,
			final T data ) {
		this.outputKey = new GeoWaveOutputKey<T>(
				adapterId,
				indexIds);
		this.data = data;
	}

	public GeoWaveData(
			final WritableDataAdapter<T> adapter,
			final Collection<ByteArrayId> indexIds,
			final T data ) {
		this.outputKey = new GeoWaveOutputKey<T>(
				adapter,
				indexIds);
		this.data = data;
	}

	public WritableDataAdapter<T> getAdapter(
			final AdapterStore adapterCache ) {
		return outputKey.getAdapter(adapterCache);
	}

	public Collection<ByteArrayId> getIndexIds() {
		return outputKey.getIndexIds();
	}

	public T getValue() {
		return data;
	}

	public ByteArrayId getAdapterId() {
		return outputKey.getAdapterId();
	}

	public GeoWaveOutputKey<T> getOutputKey() {
		return outputKey;
	}
}
