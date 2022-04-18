/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.index;

import java.nio.ByteBuffer;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.CustomIndexStrategy;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.index.QueryRanges;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;

/**
 *
 * This is a basic wrapper around a custom index strategy
 *
 * @param <E> The entry type (such as SimpleFeature, GridCoverage, or whatever type the adapter
 *        uses)
 * @param <C> The custom constraints type can be any arbitrary type, although should be persistable
 *        so that it can work outside of just client code (such as server-side filtering,
 *        map-reduce, or spark)
 */
public class CustomIndex<E, C extends Persistable> extends NullIndex implements
    CustomIndexStrategy<E, C> {
  private CustomIndexStrategy<E, C> indexStrategy;

  public CustomIndex() {
    super();
  }

  public CustomIndex(final CustomIndexStrategy<E, C> indexStrategy, final String id) {
    super(id);
    this.indexStrategy = indexStrategy;
  }

  public CustomIndexStrategy<E, C> getCustomIndexStrategy() {
    return indexStrategy;
  }

  @Override
  public InsertionIds getInsertionIds(final E entry) {
    return indexStrategy.getInsertionIds(entry);
  }

  @Override
  public QueryRanges getQueryRanges(final C constraints) {
    return indexStrategy.getQueryRanges(constraints);
  }

  @Override
  public PersistableBiPredicate<E, C> getFilter(final C constraints) {
    return indexStrategy.getFilter(constraints);
  }

  @Override
  public int hashCode() {
    return getName().hashCode();
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
    final IndexImpl other = (IndexImpl) obj;
    return getName().equals(other.getName());
  }

  @Override
  public byte[] toBinary() {
    final byte[] baseBinary = super.toBinary();
    final byte[] additionalBinary = PersistenceUtils.toBinary(indexStrategy);
    final ByteBuffer buf =
        ByteBuffer.allocate(
            VarintUtils.unsignedIntByteLength(baseBinary.length)
                + baseBinary.length
                + additionalBinary.length);
    VarintUtils.writeUnsignedInt(baseBinary.length, buf);
    buf.put(baseBinary);
    buf.put(additionalBinary);
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    final byte[] baseBinary = ByteArrayUtils.safeRead(buf, VarintUtils.readUnsignedInt(buf));
    super.fromBinary(baseBinary);
    final byte[] additionalBinary = ByteArrayUtils.safeRead(buf, buf.remaining());
    indexStrategy = (CustomIndexStrategy<E, C>) PersistenceUtils.fromBinary(additionalBinary);
  }

  @Override
  public Class<C> getConstraintsClass() {
    return indexStrategy.getConstraintsClass();
  }

}
