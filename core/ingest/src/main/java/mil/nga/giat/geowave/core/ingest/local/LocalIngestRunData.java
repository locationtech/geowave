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
package mil.nga.giat.geowave.core.ingest.local;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.KeyedObjectPool;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;

import mil.nga.giat.geowave.core.ingest.GeoWaveData;
import mil.nga.giat.geowave.core.store.AdapterToIndexMapping;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.memory.MemoryAdapterStore;
import mil.nga.giat.geowave.core.store.memory.MemoryIndexStore;

/**
 * This class maintains a pool of index writers keyed by the primary index. In
 * addition, it contains a static method to help create the blocking queue
 * needed by threads to execute ingest of individual GeoWaveData items.
 * 
 */
public class LocalIngestRunData implements
		Closeable
{

	private final KeyedObjectPool<AdapterToIndexMapping, IndexWriter> indexWriterPool;
	private final AdapterStore adapterCache;
	private final IndexStore indexCache;
	private final DataStore dataStore;

	public LocalIngestRunData(
			final List<WritableDataAdapter<?>> adapters,
			final DataStore dataStore ) {
		this.dataStore = dataStore;
		// NOTE: This should be thread-safe because the adapterCache is never
		// added to after this point. It's a static list.
		adapterCache = new MemoryAdapterStore(
				adapters.toArray(new WritableDataAdapter[adapters.size()]));
		indexWriterPool = new GenericKeyedObjectPool<>(
				new IndexWriterFactory());
		this.indexCache = new MemoryIndexStore();
	}

	public WritableDataAdapter<?> getDataAdapter(
			final GeoWaveData<?> data ) {
		return data.getAdapter(adapterCache);
	}

	public void addAdapter(
			DataAdapter<?> adapter ) {
		adapterCache.addAdapter(adapter);
	}

	public void addIndices(
			final List<PrimaryIndex> indices ) {
		for (PrimaryIndex index : indices) {
			if (!indexCache.indexExists(index.getId())) indexCache.addIndex(index);
		}
	}

	/**
	 * Return an index writer from the pool. The pool will create a new one The
	 * pool will not be cleaned up until the end. (No items will be cleaned up
	 * until the end)
	 * 
	 * @param index
	 * @return
	 * @throws Exception
	 */
	public IndexWriter getIndexWriter(
			final AdapterToIndexMapping mapping )
			throws Exception {
		return indexWriterPool.borrowObject(mapping);
	}

	/**
	 * Return the index writer to the pool
	 * 
	 * @param index
	 *            - the primary index used to create the writer
	 * @param writer
	 * @throws Exception
	 */
	public void releaseIndexWriter(
			final AdapterToIndexMapping mapping,
			final IndexWriter writer )
			throws Exception {
		indexWriterPool.returnObject(
				mapping,
				writer);
	}

	@Override
	public void close()
			throws IOException {
		indexWriterPool.close();
	}

	/**
	 * A factory implementing the default Apache Commons Pool interface to
	 * return new instances of an index writer for a given primary index.
	 */
	public class IndexWriterFactory extends
			BaseKeyedPooledObjectFactory<AdapterToIndexMapping, IndexWriter>
	{

		@Override
		public IndexWriter<?> create(
				AdapterToIndexMapping mapping )
				throws Exception {
			return dataStore.createWriter(
					adapterCache.getAdapter(mapping.getAdapterId()),
					mapping.getIndices(indexCache));
		}

		@Override
		public void destroyObject(
				AdapterToIndexMapping key,
				PooledObject<IndexWriter> p )
				throws Exception {
			super.destroyObject(
					key,
					p);
			p.getObject().close();
		}

		@Override
		public PooledObject<IndexWriter> wrap(
				IndexWriter writer ) {
			return new DefaultPooledObject<IndexWriter>(
					writer);
		}
	}
}
