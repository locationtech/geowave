/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.ingest;

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
import org.locationtech.geowave.core.store.api.VisibilityHandler;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.memory.MemoryAdapterStore;
import com.clearspring.analytics.util.Lists;

/**
 * This class maintains a pool of index writers keyed by the primary index. In addition, it contains
 * a static method to help create the blocking queue needed by threads to execute ingest of
 * individual GeoWaveData items.
 */
public class LocalIngestRunData implements Closeable {
  private static class TypeNameKeyWithIndices {
    private final String typeName;
    private final Index[] indices;

    public TypeNameKeyWithIndices(final String typeName, final Index[] indices) {
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
    public boolean equals(final Object obj) {
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
      } else if (!typeName.equals(other.typeName)) {
        return false;
      }
      return true;
    }
  }

  private final KeyedObjectPool<TypeNameKeyWithIndices, Writer> indexWriterPool;

  private final TransientAdapterStore adapterStore;
  private final DataStore dataStore;
  private final VisibilityHandler visibilityHandler;

  public LocalIngestRunData(
      final List<DataTypeAdapter<?>> adapters,
      final DataStore dataStore,
      final VisibilityHandler visibilityHandler) {
    this.dataStore = dataStore;
    this.visibilityHandler = visibilityHandler;
    indexWriterPool = new GenericKeyedObjectPool<>(new IndexWriterFactory());
    adapterStore = new MemoryAdapterStore(adapters.toArray(new DataTypeAdapter[0]));
  }

  public DataTypeAdapter<?> getDataAdapter(final GeoWaveData<?> data) {
    return data.getAdapter(adapterStore);
  }

  public void addAdapter(final DataTypeAdapter<?> adapter) {
    adapterStore.addAdapter(adapter);
  }

  /**
   * Return an index writer from the pool. The pool will create a new one if it doesn't exist. The
   * pool will not be cleaned up until the end.
   *
   * @param typeName the type being written
   * @param indices the indices to write to
   * @return the index writer
   * @throws Exception
   */
  public Writer getIndexWriter(final String typeName, final List<Index> indices) throws Exception {
    return indexWriterPool.borrowObject(
        new TypeNameKeyWithIndices(typeName, indices.toArray(new Index[0])));
  }

  /**
   * Return an index writer to the pool.
   *
   * @param typeName the type for the writer
   * @param writer the writer to return
   * @throws Exception
   */
  public void releaseIndexWriter(final String typeName, final Writer writer) throws Exception {
    indexWriterPool.returnObject(new TypeNameKeyWithIndices(typeName, new Index[0]), writer);
  }

  @Override
  public void close() throws IOException {
    indexWriterPool.close();
  }

  /**
   * A factory implementing the default Apache Commons Pool interface to return new instances of an
   * index writer for a given primary index.
   */
  public class IndexWriterFactory extends
      BaseKeyedPooledObjectFactory<TypeNameKeyWithIndices, Writer> {

    @Override
    public synchronized Writer<?> create(final TypeNameKeyWithIndices adapterWithIndices)
        throws Exception {
      dataStore.addType(
          adapterStore.getAdapter(adapterWithIndices.typeName),
          visibilityHandler,
          Lists.newArrayList(),
          adapterWithIndices.indices);
      return dataStore.createWriter(adapterWithIndices.typeName, visibilityHandler);
    }

    @Override
    public void destroyObject(final TypeNameKeyWithIndices key, final PooledObject<Writer> p)
        throws Exception {
      super.destroyObject(key, p);
      p.getObject().close();
    }

    @Override
    public PooledObject<Writer> wrap(final Writer writer) {
      return new DefaultPooledObject<>(writer);
    }
  }
}
