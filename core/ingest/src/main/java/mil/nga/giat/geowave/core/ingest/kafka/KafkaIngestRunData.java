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
package mil.nga.giat.geowave.core.ingest.kafka;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.ingest.GeoWaveData;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.memory.MemoryAdapterStore;

/**
 * A class to hold intermediate run data that must be used throughout the life
 * of an ingest process.
 */
public class KafkaIngestRunData implements
		Closeable
{
	private final Map<ByteArrayId, IndexWriter> adapterIdToWriterCache = new HashMap<ByteArrayId, IndexWriter>();
	private final AdapterStore adapterCache;
	private final DataStore dataStore;

	public KafkaIngestRunData(
			final List<WritableDataAdapter<?>> adapters,
			final DataStore dataStore ) {
		this.dataStore = dataStore;
		adapterCache = new MemoryAdapterStore(
				adapters.toArray(new WritableDataAdapter[adapters.size()]));
	}

	public WritableDataAdapter<?> getDataAdapter(
			final GeoWaveData<?> data ) {
		return data.getAdapter(adapterCache);
	}

	public synchronized IndexWriter getIndexWriter(
			final DataAdapter<?> adapter,
			final PrimaryIndex... requiredIndices )
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
