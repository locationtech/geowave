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
package org.locationtech.geowave.core.ingest.local;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.KeyedObjectPool;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.locationtech.geowave.core.store.adapter.TransientAdapterStore;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.ingest.GeoWaveData;
import org.locationtech.geowave.core.store.memory.MemoryAdapterStore;

/**
 * This class maintains a pool of index writers keyed by the primary index. In
 * addition, it contains a static method to help create the blocking queue
 * needed by threads to execute ingest of individual GeoWaveData items.
 *
 */
public class LocalIngestRunData implements
		Closeable
{
	private static class TypeNameKeyWithIndices
	{
		private final String typeName;
		private final Index[] indices;

		public TypeNameKeyWithIndices(
				final String typeName,
				final Index[] indices ) {
			super();
			this.typeName = typeName;
			this.indices = indices;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = (prime * result) + ((typeName == null) ? 0 : typeName.hashCode());
			return result;
		}

		@Override
		public boolean equals(
				final Object obj ) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			final TypeNameKeyWithIndices other = (TypeNameKeyWithIndices) obj;
			if (typeName == null) {
				if (other.typeName != null) {
					return false;
				}
			}
			else if (!typeName.equals(other.typeName)) {
				return false;
			}
			return true;
		}
	}

	private final KeyedObjectPool<TypeNameKeyWithIndices, Writer> indexWriterPool;

	private final TransientAdapterStore adapterStore;
	private final DataStore dataStore;

	public LocalIngestRunData(
			final List<DataTypeAdapter<?>> adapters,
			final DataStore dataStore ) {
		this.dataStore = dataStore;
		indexWriterPool = new GenericKeyedObjectPool<>(
				new IndexWriterFactory());
		adapterStore = new MemoryAdapterStore(
				adapters.toArray(new DataTypeAdapter[0]));
	}

	public DataTypeAdapter<?> getDataAdapter(
			final GeoWaveData<?> data ) {
		return data.getAdapter(adapterStore);
	}

	public void addAdapter(
			final DataTypeAdapter<?> adapter ) {
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
	public Writer getIndexWriter(
			final String typeName,
			final List<Index> indices )
			throws Exception {
		return indexWriterPool.borrowObject(new TypeNameKeyWithIndices(
				typeName,
				indices.toArray(new Index[0])));
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
			final String typeName,
			final Writer writer )
			throws Exception {
		indexWriterPool.returnObject(
				new TypeNameKeyWithIndices(
						typeName,
						new Index[0]),
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
			BaseKeyedPooledObjectFactory<TypeNameKeyWithIndices, Writer>
	{

		@Override
		public synchronized Writer<?> create(
				final TypeNameKeyWithIndices adapterWithIndices )
				throws Exception {
			dataStore.addType(
					adapterStore.getAdapter(adapterWithIndices.typeName),
					adapterWithIndices.indices);
			return dataStore.createWriter(adapterWithIndices.typeName);
		}

		@Override
		public void destroyObject(
				final TypeNameKeyWithIndices key,
				final PooledObject<Writer> p )
				throws Exception {
			super.destroyObject(
					key,
					p);
			p.getObject().close();
		}

		@Override
		public PooledObject<Writer> wrap(
				final Writer writer ) {
			return new DefaultPooledObject<>(
					writer);
		}
	}
}
