/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.util;

import java.text.SimpleDateFormat;
import java.util.Date;
import org.locationtech.geowave.core.geotime.store.query.TemporalRange;
import org.locationtech.geowave.core.geotime.store.statistics.TimeRangeStatistic;
import org.locationtech.geowave.core.geotime.store.statistics.TimeRangeStatistic.TimeRangeValue;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;

public class DateUtilities {

  public static Date parseISO(String input) throws java.text.ParseException {

    // NOTE: SimpleDateFormat uses GMT[-+]hh:mm for the TZ which breaks
    // things a bit. Before we go on we have to repair this.
    final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssz");

    // this is zero time so we need to add that TZ indicator for
    if (input.endsWith("Z")) {
      input = input.substring(0, input.length() - 1) + "GMT-00:00";
    } else {
      final int inset = 6;

      final String s0 = input.substring(0, input.length() - inset);
      final String s1 = input.substring(input.length() - inset, input.length());

      input = s0 + "GMT" + s1;
    }

    return df.parse(input);
  }

  public static TemporalRange getTemporalRange(
      final DataStorePluginOptions dataStorePlugin,
      final String typeName,
      final String timeField) {
    final DataStatisticsStore statisticsStore = dataStorePlugin.createDataStatisticsStore();
    final InternalAdapterStore internalAdapterStore = dataStorePlugin.createInternalAdapterStore();
    final PersistentAdapterStore adapterStore = dataStorePlugin.createAdapterStore();
    final short adapterId = internalAdapterStore.getAdapterId(typeName);
    final DataTypeAdapter<?> adapter = adapterStore.getAdapter(adapterId);
    // if this is a ranged schema, we have to get complete bounds
    if (timeField.contains("|")) {
      final int pipeIndex = timeField.indexOf("|");
      final String startField = timeField.substring(0, pipeIndex);
      final String endField = timeField.substring(pipeIndex + 1);

      Date start = null;
      Date end = null;

      try (CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> statIter =
          statisticsStore.getFieldStatistics(
              adapter,
              TimeRangeStatistic.STATS_TYPE,
              startField,
              null)) {
        if (statIter.hasNext()) {
          TimeRangeStatistic statistic = (TimeRangeStatistic) statIter.next();
          if (statistic != null) {
            TimeRangeValue value = statisticsStore.getStatisticValue(statistic);
            if (value != null) {
              start = value.getMinTime();
            }
          }
        }
      }

      try (CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> statIter =
          statisticsStore.getFieldStatistics(
              adapter,
              TimeRangeStatistic.STATS_TYPE,
              endField,
              null)) {
        if (statIter.hasNext()) {
          TimeRangeStatistic statistic = (TimeRangeStatistic) statIter.next();
          if (statistic != null) {
            TimeRangeValue value = statisticsStore.getStatisticValue(statistic);
            if (value != null) {
              end = value.getMaxTime();
            }
          }
        }
      }

      if ((start != null) && (end != null)) {
        return new TemporalRange(start, end);
      }
    } else {
      // Look up the time range stat for this adapter
      try (CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> statIter =
          statisticsStore.getFieldStatistics(
              adapter,
              TimeRangeStatistic.STATS_TYPE,
              timeField,
              null)) {
        if (statIter.hasNext()) {
          TimeRangeStatistic statistic = (TimeRangeStatistic) statIter.next();
          if (statistic != null) {
            TimeRangeValue value = statisticsStore.getStatisticValue(statistic);
            if (value != null) {
              return value.asTemporalRange();
            }
          }
        }
      }
    }

    return null;
  }
}
