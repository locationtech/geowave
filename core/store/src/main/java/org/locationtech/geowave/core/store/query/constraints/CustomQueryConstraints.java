/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
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
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.numeric.BasicNumericDataset;
import org.locationtech.geowave.core.index.numeric.MultiDimensionalNumericData;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;
import com.clearspring.analytics.util.Lists;

public class CustomQueryConstraints<C extends Persistable> implements
    AdapterAndIndexBasedQueryConstraints {
  private C customConstraints;
  private List<QueryFilter> filters;

  public CustomQueryConstraints() {
    super();
  }

  public CustomQueryConstraints(final C customConstraints) {
    this(customConstraints, Lists.newArrayList());
  }

  public CustomQueryConstraints(final C customConstraints, final List<QueryFilter> filters) {
    this.customConstraints = customConstraints;
    this.filters = filters;
  }

  public C getCustomConstraints() {
    return customConstraints;
  }

  @Override
  public byte[] toBinary() {
    final byte[] constraintBytes = PersistenceUtils.toBinary(customConstraints);
    final byte[] filterBytes = PersistenceUtils.toBinary(filters);
    final ByteBuffer buffer =
        ByteBuffer.allocate(
            VarintUtils.unsignedIntByteLength(constraintBytes.length)
                + VarintUtils.unsignedIntByteLength(filterBytes.length)
                + constraintBytes.length
                + filterBytes.length);
    VarintUtils.writeUnsignedInt(constraintBytes.length, buffer);
    buffer.put(constraintBytes);
    VarintUtils.writeUnsignedInt(filterBytes.length, buffer);
    buffer.put(filterBytes);
    return buffer.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buffer = ByteBuffer.wrap(bytes);
    final byte[] constraintBytes = new byte[VarintUtils.readUnsignedInt(buffer)];
    buffer.get(constraintBytes);
    customConstraints = (C) PersistenceUtils.fromBinary(constraintBytes);
    final byte[] filterBytes = new byte[VarintUtils.readUnsignedInt(buffer)];
    buffer.get(filterBytes);
    filters = (List) PersistenceUtils.fromBinaryAsList(filterBytes);
  }

  @Override
  public List<QueryFilter> createFilters(final Index index) {
    return filters;
  }

  @Override
  public List<MultiDimensionalNumericData> getIndexConstraints(final Index index) {
    if (index instanceof CustomIndexStrategy) {
      if (((CustomIndexStrategy) index).getConstraintsClass().isInstance(customConstraints)) {
        return Collections.singletonList(new InternalCustomConstraints(customConstraints));
      }
    }
    return Collections.emptyList();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = (prime * result) + ((customConstraints == null) ? 0 : customConstraints.hashCode());
    result = (prime * result) + filters.hashCode();
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
    final CustomQueryConstraints other = (CustomQueryConstraints) obj;
    if (customConstraints == null) {
      if (other.customConstraints != null) {
        return false;
      }
    } else if (!customConstraints.equals(other.customConstraints)) {
      return false;
    }
    if (!filters.equals(other.filters)) {
      return false;
    }
    return true;
  }

  public static class InternalCustomConstraints<C extends Persistable> extends BasicNumericDataset {
    private C customConstraints;

    public InternalCustomConstraints() {}

    public InternalCustomConstraints(final C customConstraints) {
      super();
      this.customConstraints = customConstraints;
    }

    public C getCustomConstraints() {
      return customConstraints;
    }

    @Override
    public byte[] toBinary() {
      return PersistenceUtils.toBinary(customConstraints);
    }

    @Override
    public void fromBinary(final byte[] bytes) {
      customConstraints = (C) PersistenceUtils.fromBinary(bytes);
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = super.hashCode();
      result = (prime * result) + ((customConstraints == null) ? 0 : customConstraints.hashCode());
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
      final InternalCustomConstraints other = (InternalCustomConstraints) obj;
      if (customConstraints == null) {
        if (other.customConstraints != null) {
          return false;
        }
      } else if (!customConstraints.equals(other.customConstraints)) {
        return false;
      }
      return true;
    }
  }

  @Override
  public QueryConstraints createQueryConstraints(
      final InternalDataAdapter<?> adapter,
      final Index index,
      final AdapterToIndexMapping indexMapping) {
    if ((index instanceof CustomIndexStrategy)
        && (((CustomIndexStrategy) index).getFilter(getCustomConstraints()) != null)) {
      return new CustomQueryConstraintsWithFilter(
          getCustomConstraints(),
          adapter,
          new AdapterToIndexMapping[] {indexMapping});
    }
    return this;
  }
}
