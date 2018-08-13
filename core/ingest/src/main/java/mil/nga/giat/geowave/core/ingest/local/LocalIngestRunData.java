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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.KeyedObjectPool;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.ingest.GeoWaveData;
import mil.nga.giat.geowave.core.store.AdapterToIndexMapping;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.TransientAdapterStore;
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
	private static class AdapterIdKeyWithIndices
	{
		private ByteArrayId adapterId;
		private PrimaryIndex[] indices;

		public AdapterIdKeyWithIndices(
				ByteArrayId adapterId,
				PrimaryIndex[] indices ) {
			super();
			this.adapterId = adapterId;
			this.indices = indices;
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
			AdapterIdKeyWithIndices other = (AdapterIdKeyWithIndices) obj;
			if (adapterId == null) {
				if (other.adapterId != null) return false;
			}
			else if (!adapterId.equals(other.adapterId)) return false;
			return true;
		}
	}

	private final KeyedObjectPool<AdapterIdKeyWithIndices, IndexWriter> indexWriterPool;

	private final TransientAdapterStore adapterStore;
	private final DataStore dataStore;

	public LocalIngestRunData(
			final List<WritableDataAdapter<?>> adapters,
			final DataStore dataStore ) {
		this.dataStore = dataStore;
		indexWriterPool = new GenericKeyedObjectPool<>(
				new IndexWriterFactory());
		adapterStore = new MemoryAdapterStore(
				adapters.toArray(new WritableDataAdapter[0]));
	}

	public WritableDataAdapter<?> getDataAdapter(
			final GeoWaveData<?> data ) {
		return data.getAdapter(adapterStore);
	}

	public void addAdapter(
			final WritableDataAdapter<?> adapter ) {
		adapterStore.addAdapter(adapter);
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
			final ByteArrayId adapterId,
			List<PrimaryIndex> indices )
			throws Exception {
		return indexWriterPool.borrowObject(new AdapterIdKeyWithIndices(
				adapterId,
				indices.toArray(new PrimaryIndex[0])));
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
			final ByteArrayId adapterId,
			final IndexWriter writer )
			throws Exception {
		indexWriterPool.returnObject(
				new AdapterIdKeyWithIndices(
						adapterId,
						new PrimaryIndex[0]),
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
			BaseKeyedPooledObjectFactory<AdapterIdKeyWithIndices, IndexWriter>
	{

		@Override
		public IndexWriter<?> create(
				final AdapterIdKeyWithIndices adapterWithIndices )
				throws Exception {
			return dataStore.createWriter(
					(WritableDataAdapter<?>) adapterStore.getAdapter(adapterWithIndices.adapterId),
					adapterWithIndices.indices);
		}

		@Override
		public void destroyObject(
				final AdapterIdKeyWithIndices key,
				final PooledObject<IndexWriter> p )
				throws Exception {
			super.destroyObject(
					key,
					p);
			p.getObject().close();
		}

		@Override
		public PooledObject<IndexWriter> wrap(
				final IndexWriter writer ) {
			return new DefaultPooledObject<IndexWriter>(
					writer);
		}
	}
}
