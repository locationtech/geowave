/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics;

import java.util.Arrays;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;

/**
 * Reads GeoWaveMetadata as statistic values, and merges any values that share a primary id.
 */
public class StatisticValueReader<V extends StatisticValue<R>, R> implements CloseableIterator<V> {

  private final CloseableIterator<GeoWaveMetadata> metadataIter;
  private final Statistic<V> statistic;

  private V next = null;
  private byte[] nextPrimaryId = null;

  public StatisticValueReader(
      final CloseableIterator<GeoWaveMetadata> metadataIter,
      final Statistic<V> statistic) {
    this.metadataIter = metadataIter;
    this.statistic = statistic;
  }

  @Override
  public boolean hasNext() {
    return (next != null) || metadataIter.hasNext();
  }

  @Override
  public V next() {
    V currentValue = next;
    byte[] currentPrimaryId = nextPrimaryId;
    next = null;
    nextPrimaryId = null;
    while (metadataIter.hasNext()) {
      final GeoWaveMetadata row = metadataIter.next();

      final V entry = statistic.createEmpty();
      entry.fromBinary(PersistenceUtils.stripClassId(row.getValue()));
      if (currentValue == null) {
        currentValue = entry;
        currentPrimaryId = row.getPrimaryId();
      } else {
        if (Arrays.equals(currentPrimaryId, row.getPrimaryId())) {
          currentValue.merge(entry);
        } else {
          next = entry;
          nextPrimaryId = row.getPrimaryId();
          break;
        }
      }
    }
    if (currentValue != null && statistic.getBinningStrategy() != null) {
      currentValue.setBin(getBinFromValueId(statistic.getId(), currentPrimaryId));
    }
    return currentValue;
  }

  @Override
  public void close() {
    metadataIter.close();
  }

  public static ByteArray getBinFromValueId(
      final StatisticId<?> statisticId,
      final byte[] valueId) {
    int binIndex =
        statisticId.getUniqueId().getBytes().length + StatisticId.UNIQUE_ID_SEPARATOR.length;
    byte[] binBytes = new byte[valueId.length - binIndex];
    for (int i = 0; i < binBytes.length; i++) {
      binBytes[i] = valueId[i + binIndex];
    }
    return new ByteArray(binBytes);
  }

}
