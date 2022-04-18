/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics.visibility;

import org.locationtech.geowave.core.store.EntryVisibilityHandler;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.flatten.BitmaskUtils;

/**
 * This assigns the visibility of the key-value with the most-significant field bitmask (the first
 * fields in the bitmask are the indexed fields, and all indexed fields should be the default
 * visibility which should be the minimal set of visibility constraints of any field)
 */
public class DefaultStatisticVisibility<T> implements EntryVisibilityHandler<T> {

  @Override
  public byte[] getVisibility(final T entry, final GeoWaveRow... kvs) {
    if (kvs.length == 1 && kvs[0].getFieldValues().length == 1) {
      return kvs[0].getFieldValues()[0].getVisibility();
    }
    int lowestOrdinal = Integer.MAX_VALUE;
    byte[] lowestOrdinalVisibility = null;
    for (final GeoWaveRow kv : kvs) {
      for (final GeoWaveValue v : kv.getFieldValues()) {
        final int pos = BitmaskUtils.getLowestFieldPosition(v.getFieldMask());
        if (pos == 0) {
          return v.getVisibility();
        }
        if (pos <= lowestOrdinal) {
          lowestOrdinal = pos;
          lowestOrdinalVisibility = v.getVisibility();
        }
      }
    }
    return lowestOrdinalVisibility;
  }
}
