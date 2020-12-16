/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.constraints;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import org.locationtech.geowave.core.index.CustomIndexStrategy;
import org.locationtech.geowave.core.index.CustomIndexStrategy.PersistableBiPredicate;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.adapter.AbstractAdapterPersistenceEncoding;
import org.locationtech.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.data.IndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.data.MultiFieldPersistentDataset;
import org.locationtech.geowave.core.store.data.PersistentDataset;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.index.IndexImpl;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;
import org.locationtech.geowave.core.store.util.GenericTypeResolver;
import com.google.common.primitives.Bytes;

public class CustomQueryConstraintsWithFilter<T, C extends Persistable> extends
    CustomQueryConstraints<C> {
  private DataTypeAdapter<T> adapter;

  public CustomQueryConstraintsWithFilter() {
    super();
  }

  public CustomQueryConstraintsWithFilter(
      final C customConstraints,
      final DataTypeAdapter<T> adapter) {
    super(customConstraints);
    this.adapter = adapter;
  }

  @Override
  public byte[] toBinary() {
    final byte[] adapterBinary = PersistenceUtils.toBinary(adapter);
    return Bytes.concat(
        VarintUtils.writeUnsignedInt(adapterBinary.length),
        adapterBinary,
        super.toBinary());
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    final byte[] adapterBinary = new byte[VarintUtils.readUnsignedInt(buf)];
    buf.get(adapterBinary);
    adapter = (DataTypeAdapter<T>) PersistenceUtils.fromBinary(adapterBinary);
    final byte[] superBinary = new byte[buf.remaining()];
    buf.get(superBinary);
    super.fromBinary(superBinary);
  }

  @Override
  public List<QueryFilter> createFilters(final Index index) {
    if (index instanceof CustomIndexStrategy) {
      final Class<?>[] genericClasses =
          GenericTypeResolver.resolveTypeArguments(index.getClass(), CustomIndexStrategy.class);
      if ((genericClasses != null)
          && (genericClasses.length == 2)
          && genericClasses[1].isInstance(getCustomConstraints())) {
        return Collections.singletonList(
            new InternalCustomQueryFilter(
                getCustomConstraints(),
                adapter,
                ((CustomIndexStrategy) index).getFilter(getCustomConstraints())));
      }
    }
    return Collections.emptyList();
  }

  public static class InternalCustomQueryFilter<T, C extends Persistable> implements QueryFilter {
    private C customConstraints;
    private DataTypeAdapter<T> adapter;
    private PersistableBiPredicate<T, C> predicate;

    public InternalCustomQueryFilter() {}

    public InternalCustomQueryFilter(
        final C customConstraints,
        final DataTypeAdapter<T> adapter,
        final PersistableBiPredicate<T, C> predicate) {
      super();
      this.customConstraints = customConstraints;
      this.adapter = adapter;
      this.predicate = predicate;
    }

    public C getCustomConstraints() {
      return customConstraints;
    }

    @Override
    public byte[] toBinary() {
      final byte[] adapterBytes = PersistenceUtils.toBinary(adapter);
      final byte[] predicateBytes = PersistenceUtils.toBinary(predicate);
      return Bytes.concat(
          VarintUtils.writeUnsignedInt(adapterBytes.length),
          adapterBytes,
          VarintUtils.writeUnsignedInt(predicateBytes.length),
          predicateBytes,
          PersistenceUtils.toBinary(customConstraints));
    }

    @Override
    public void fromBinary(final byte[] bytes) {
      final ByteBuffer buf = ByteBuffer.wrap(bytes);
      final byte[] adapterBytes = new byte[VarintUtils.readUnsignedInt(buf)];
      buf.get(adapterBytes);
      adapter = (DataTypeAdapter<T>) PersistenceUtils.fromBinary(adapterBytes);
      final byte[] predicateBytes = new byte[VarintUtils.readUnsignedInt(buf)];
      buf.get(predicateBytes);
      predicate = (PersistableBiPredicate<T, C>) PersistenceUtils.fromBinary(predicateBytes);
      final byte[] constraintsBytes = new byte[buf.remaining()];
      buf.get(constraintsBytes);
      customConstraints = (C) PersistenceUtils.fromBinary(constraintsBytes);
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = (prime * result) + ((adapter == null) ? 0 : adapter.hashCode());
      result = (prime * result) + ((customConstraints == null) ? 0 : customConstraints.hashCode());
      result = (prime * result) + ((predicate == null) ? 0 : predicate.hashCode());
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
      final InternalCustomQueryFilter other = (InternalCustomQueryFilter) obj;
      if (adapter == null) {
        if (other.adapter != null) {
          return false;
        }
      } else if (!adapter.equals(other.adapter)) {
        return false;
      }
      if (customConstraints == null) {
        if (other.customConstraints != null) {
          return false;
        }
      } else if (!customConstraints.equals(other.customConstraints)) {
        return false;
      }
      if (predicate == null) {
        if (other.predicate != null) {
          return false;
        }
      } else if (!predicate.equals(other.predicate)) {
        return false;
      }
      return true;
    }

    @Override
    public boolean accept(
        final CommonIndexModel indexModel,
        final IndexedPersistenceEncoding<?> persistenceEncoding) {
      if ((predicate != null) && (indexModel != null) && (adapter != null)) {
        final PersistentDataset<Object> adapterExtendedValues = new MultiFieldPersistentDataset<>();
        if (persistenceEncoding instanceof AbstractAdapterPersistenceEncoding) {
          ((AbstractAdapterPersistenceEncoding) persistenceEncoding).convertUnknownValues(
              adapter,
              indexModel);
          final PersistentDataset<Object> existingExtValues =
              ((AbstractAdapterPersistenceEncoding) persistenceEncoding).getAdapterExtendedData();

          if (persistenceEncoding.isAsync()) {
            return false;
          }
          if (existingExtValues != null) {
            adapterExtendedValues.addValues(existingExtValues.getValues());
          }
        }
        final IndexedAdapterPersistenceEncoding encoding =
            new IndexedAdapterPersistenceEncoding(
                persistenceEncoding.getInternalAdapterId(),
                persistenceEncoding.getDataId(),
                persistenceEncoding.getInsertionPartitionKey(),
                persistenceEncoding.getInsertionSortKey(),
                persistenceEncoding.getDuplicateCount(),
                (PersistentDataset) persistenceEncoding.getCommonData(),
                new MultiFieldPersistentDataset<byte[]>(),
                adapterExtendedValues);

        final T entry =
            adapter.decode(
                encoding,
                new IndexImpl(
                    null,
                    // we have to assume this adapter doesn't use the numeric index strategy
                    // and only the common index model to decode the entry,
                    // we pass along a null strategy to eliminate the necessity to send a
                    // serialization of the strategy in the options of this iterator
                    indexModel));
        if (entry == null) {
          return false;
        }
        return predicate.test(entry, customConstraints);
      }
      return false;
    }
  }

  @Override
  public QueryConstraints createQueryConstraints(
      final DataTypeAdapter<?> adapter,
      final Index index) {
    return this;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = (prime * result) + ((adapter == null) ? 0 : adapter.hashCode());
    return result;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (!super.equals(obj)) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final CustomQueryConstraintsWithFilter other = (CustomQueryConstraintsWithFilter) obj;
    if (adapter == null) {
      if (other.adapter != null) {
        return false;
      }
    } else if (!adapter.equals(other.adapter)) {
      return false;
    }
    return true;
  }
}
