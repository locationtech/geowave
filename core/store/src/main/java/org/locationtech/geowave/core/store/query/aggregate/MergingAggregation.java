/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.aggregate;

import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;

public class MergingAggregation<T extends Mergeable> implements Aggregation<Persistable, T, T> {
  private T result = null;

  @Override
  public Persistable getParameters() {
    return null;
  }

  @Override
  public void setParameters(final Persistable parameters) {}

  @Override
  public T getResult() {
    return result;
  }

  @Override
  public byte[] resultToBinary(final T result) {
    if (result == null) {
      return new byte[0];
    }
    return PersistenceUtils.toBinary(result);
  }

  @Override
  public T resultFromBinary(final byte[] binary) {
    if (binary.length > 0) {
      return (T) PersistenceUtils.fromBinary(binary);
    }
    return null;
  }

  @Override
  public void clearResult() {
    result = null;
  }

  @Override
  public void aggregate(final DataTypeAdapter<T> adapter, final T entry) {
    if (result == null) {
      result = entry;
    } else {
      result.merge(entry);
    }
  }

}
