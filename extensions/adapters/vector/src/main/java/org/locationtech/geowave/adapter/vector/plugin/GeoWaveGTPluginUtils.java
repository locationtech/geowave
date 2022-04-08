/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.plugin;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import org.geotools.feature.visitor.MaxVisitor;
import org.geotools.feature.visitor.MinVisitor;
import org.locationtech.geowave.core.geotime.store.statistics.TimeRangeStatistic;
import org.locationtech.geowave.core.geotime.store.statistics.TimeRangeStatistic.TimeRangeValue;
import org.locationtech.geowave.core.geotime.util.ExtractAttributesFilter;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.FieldStatistic;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.statistics.field.NumericRangeStatistic;
import org.locationtech.geowave.core.store.statistics.field.NumericRangeStatistic.NumericRangeValue;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import com.beust.jcommander.internal.Lists;
import com.beust.jcommander.internal.Maps;

class GeoWaveGTPluginUtils {

  protected static Map<String, List<FieldStatistic<?>>> getFieldStats(
      final DataStatisticsStore statisticsStore,
      final DataTypeAdapter<?> adapter) {
    final Map<String, List<FieldStatistic<?>>> adapterFieldStatistics = Maps.newHashMap();
    try (CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> statistics =
        statisticsStore.getFieldStatistics(adapter, null, null, null)) {
      while (statistics.hasNext()) {
        final FieldStatistic<?> next = (FieldStatistic<?>) statistics.next();
        List<FieldStatistic<?>> fieldStats = adapterFieldStatistics.get(next.getFieldName());
        if (fieldStats == null) {
          fieldStats = Lists.newArrayList();
          adapterFieldStatistics.put(next.getFieldName(), fieldStats);
        }
        fieldStats.add(next);
      }
    }
    return adapterFieldStatistics;
  }

  protected static boolean accepts(
      final DataStatisticsStore statisticsStore,
      final DataTypeAdapter<?> adapter,
      final org.opengis.feature.FeatureVisitor visitor,
      final org.opengis.util.ProgressListener progress,
      final SimpleFeatureType featureType) throws IOException {
    if ((visitor instanceof MinVisitor)) {
      final ExtractAttributesFilter filter = new ExtractAttributesFilter();

      final MinVisitor minVisitor = (MinVisitor) visitor;
      final Collection<String> attrs =
          (Collection<String>) minVisitor.getExpression().accept(filter, null);
      int acceptedCount = 0;
      final Map<String, List<FieldStatistic<?>>> adapterFieldStatistics =
          getFieldStats(statisticsStore, adapter);
      for (final String attr : attrs) {
        if (!adapterFieldStatistics.containsKey(attr)) {
          continue;
        }
        for (final FieldStatistic<?> stat : adapterFieldStatistics.get(attr)) {
          if ((stat instanceof TimeRangeStatistic) && (stat.getBinningStrategy() == null)) {
            final TimeRangeValue statValue =
                statisticsStore.getStatisticValue((TimeRangeStatistic) stat);
            if (statValue != null) {
              minVisitor.setValue(convertToType(attr, new Date(statValue.getMin()), featureType));
              acceptedCount++;
            }
          } else if (stat instanceof NumericRangeStatistic) {
            try (CloseableIterator<NumericRangeValue> values =
                statisticsStore.getStatisticValues((NumericRangeStatistic) stat)) {
              NumericRangeValue statValue = ((NumericRangeStatistic) stat).createEmpty();
              while (values.hasNext()) {
                statValue.merge(values.next());
              }
              if (statValue.isSet()) {
                minVisitor.setValue(convertToType(attr, statValue.getMin(), featureType));
                acceptedCount++;
              }
            }
          }
        }
      }

      if (acceptedCount > 0) {
        if (progress != null) {
          progress.complete();
        }
        return true;
      }
    } else if ((visitor instanceof MaxVisitor)) {
      final ExtractAttributesFilter filter = new ExtractAttributesFilter();

      final MaxVisitor maxVisitor = (MaxVisitor) visitor;
      final Collection<String> attrs =
          (Collection<String>) maxVisitor.getExpression().accept(filter, null);
      int acceptedCount = 0;
      final Map<String, List<FieldStatistic<?>>> adapterFieldStatistics =
          getFieldStats(statisticsStore, adapter);
      for (final String attr : attrs) {
        for (final FieldStatistic<?> stat : adapterFieldStatistics.get(attr)) {
          if ((stat instanceof TimeRangeStatistic) && (stat.getBinningStrategy() == null)) {
            final TimeRangeValue statValue =
                statisticsStore.getStatisticValue((TimeRangeStatistic) stat);
            if (statValue != null) {
              maxVisitor.setValue(convertToType(attr, new Date(statValue.getMax()), featureType));
              acceptedCount++;
            }
          } else if (stat instanceof NumericRangeStatistic) {
            try (CloseableIterator<NumericRangeValue> values =
                statisticsStore.getStatisticValues((NumericRangeStatistic) stat)) {
              NumericRangeValue statValue = ((NumericRangeStatistic) stat).createEmpty();
              while (values.hasNext()) {
                statValue.merge(values.next());
              }
              if (statValue.isSet()) {
                maxVisitor.setValue(convertToType(attr, statValue.getMax(), featureType));
                acceptedCount++;
              }
            }
          }
        }
      }

      if (acceptedCount > 0) {
        if (progress != null) {
          progress.complete();
        }
        return true;
      }
    }
    return false;
  }

  protected static Object convertToType(
      final String attrName,
      final Object value,
      final SimpleFeatureType featureType) {
    final AttributeDescriptor descriptor = featureType.getDescriptor(attrName);
    if (descriptor == null) {
      return value;
    }
    final Class<?> attrClass = descriptor.getType().getBinding();
    if (attrClass.isInstance(value)) {
      return value;
    }
    if (Number.class.isAssignableFrom(attrClass) && Number.class.isInstance(value)) {
      if (Double.class.isAssignableFrom(attrClass)) {
        return ((Number) value).doubleValue();
      }
      if (Float.class.isAssignableFrom(attrClass)) {
        return ((Number) value).floatValue();
      }
      if (Long.class.isAssignableFrom(attrClass)) {
        return ((Number) value).longValue();
      }
      if (Integer.class.isAssignableFrom(attrClass)) {
        return ((Number) value).intValue();
      }
      if (Short.class.isAssignableFrom(attrClass)) {
        return ((Number) value).shortValue();
      }
      if (Byte.class.isAssignableFrom(attrClass)) {
        return ((Number) value).byteValue();
      }
      if (BigInteger.class.isAssignableFrom(attrClass)) {
        return BigInteger.valueOf(((Number) value).longValue());
      }
      if (BigDecimal.class.isAssignableFrom(attrClass)) {
        return BigDecimal.valueOf(((Number) value).doubleValue());
      }
    }
    if (Calendar.class.isAssignableFrom(attrClass)) {
      if (Date.class.isInstance(value)) {
        final Calendar c = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        c.setTime((Date) value);
        return c;
      }
    }
    if (Timestamp.class.isAssignableFrom(attrClass)) {
      if (Date.class.isInstance(value)) {
        final Timestamp ts = new Timestamp(((Date) value).getTime());
        return ts;
      }
    }
    return value;
  }
}
