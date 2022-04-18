/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.util;

import org.locationtech.geowave.core.geotime.store.statistics.BoundingBoxStatistic;
import org.locationtech.geowave.core.geotime.store.statistics.BoundingBoxStatistic.BoundingBoxValue;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;
import org.locationtech.jts.geom.Envelope;

public class FeatureGeometryUtils {

  public static Envelope getGeoBounds(
      final DataStorePluginOptions dataStorePlugin,
      final String typeName,
      final String geomField) {
    final DataStatisticsStore statisticsStore = dataStorePlugin.createDataStatisticsStore();
    final InternalAdapterStore internalAdapterStore = dataStorePlugin.createInternalAdapterStore();
    final PersistentAdapterStore adapterStore = dataStorePlugin.createAdapterStore();
    final short adapterId = internalAdapterStore.getAdapterId(typeName);
    final DataTypeAdapter<?> adapter = adapterStore.getAdapter(adapterId);

    try (CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> statIter =
        statisticsStore.getFieldStatistics(
            adapter,
            BoundingBoxStatistic.STATS_TYPE,
            geomField,
            null)) {
      if (statIter.hasNext()) {
        BoundingBoxStatistic statistic = (BoundingBoxStatistic) statIter.next();
        if (statistic != null) {
          BoundingBoxValue value = statisticsStore.getStatisticValue(statistic);
          if (value != null) {
            return value.getValue();
          }
        }
      }
    }
    return null;
  }
}
