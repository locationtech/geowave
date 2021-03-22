/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.constraints;

import java.util.Collections;
import java.util.List;
import org.locationtech.geowave.core.index.CustomIndexStrategy;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.index.sfc.data.BasicNumericDataset;
import org.locationtech.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;

public class CustomQueryConstraints<C extends Persistable> implements
    AdapterAndIndexBasedQueryConstraints {
  private C customConstraints;

  public CustomQueryConstraints() {
    super();
  }

  public CustomQueryConstraints(final C customConstraints) {
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
  public List<QueryFilter> createFilters(final Index index) {
    return Collections.emptyList();
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
