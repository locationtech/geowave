/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics;

import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.operations.MetadataWriter;

/**
 * Writes statistic values to the data store using a given metadata writer.
 */
public class StatisticValueWriter<V extends StatisticValue<?>> implements AutoCloseable {
  private final MetadataWriter writer;
  private final Statistic<V> statistic;

  public StatisticValueWriter(final MetadataWriter writer, final Statistic<V> statistic) {
    this.writer = writer;
    this.statistic = statistic;
  }

  @Override
  public void close() throws Exception {
    writer.close();
  }

  public void writeStatisticValue(final byte[] bin, final byte[] visibility, V value) {
    byte[] primaryId;
    if (statistic.getBinningStrategy() != null) {
      primaryId = StatisticValue.getValueId(statistic.getId(), bin);
    } else {
      primaryId = statistic.getId().getUniqueId().getBytes();
    }
    writer.write(
        new GeoWaveMetadata(
            primaryId,
            statistic.getId().getGroupId().getBytes(),
            visibility,
            PersistenceUtils.toBinary(value)));
  }

}
