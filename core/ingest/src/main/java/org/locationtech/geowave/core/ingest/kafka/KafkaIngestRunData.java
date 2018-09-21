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
package org.locationtech.geowave.core.ingest.kafka;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.ingest.GeoWaveData;
import org.locationtech.geowave.core.store.adapter.AdapterStore;
import org.locationtech.geowave.core.store.adapter.TransientAdapterStore;
import org.locationtech.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.IndexWriter;
import org.locationtech.geowave.core.store.memory.MemoryAdapterStore;

/**
 * A class to hold intermediate run data that must be used throughout the life
 * of an ingest process.
 */
public class KafkaIngestRunData implements
		Closeable
{
	private final Map<ByteArrayId, IndexWriter> adapterIdToWriterCache = new HashMap<ByteArrayId, IndexWriter>();
	private final TransientAdapterStore adapterCache;
	private final DataStore dataStore;

	public KafkaIngestRunData(
			final List<DataTypeAdapter<?>> adapters,
			final DataStore dataStore ) {
		this.dataStore = dataStore;
		adapterCache = new MemoryAdapterStore(
				adapters.toArray(new DataTypeAdapter[adapters.size()]));
	}

	public DataTypeAdapter<?> getDataAdapter(
			final GeoWaveData<?> data ) {
		return data.getAdapter(adapterCache);
	}

	public synchronized IndexWriter getIndexWriter(
			final DataTypeAdapter<?> adapter,
			final Index... requiredIndices )
			throws MismatchedIndexToAdapterMapping {
		IndexWriter indexWriter = adapterIdToWriterCache.get(adapter.getAdapterId());
		if (indexWriter == null) {
			indexWriter = dataStore.createWriter(
					adapter,
					requiredIndices);
			adapterIdToWriterCache.put(
					adapter.getAdapterId(),
					indexWriter);
		}
		return indexWriter;
	}

	@Override
	public void close()
			throws IOException {
		synchronized (this) {
			for (final IndexWriter indexWriter : adapterIdToWriterCache.values()) {
				indexWriter.close();
			}
			adapterIdToWriterCache.clear();
		}
	}

	public void flush() {
		synchronized (this) {
			for (final IndexWriter indexWriter : adapterIdToWriterCache.values()) {
				indexWriter.flush();
			}
		}
	}

}
