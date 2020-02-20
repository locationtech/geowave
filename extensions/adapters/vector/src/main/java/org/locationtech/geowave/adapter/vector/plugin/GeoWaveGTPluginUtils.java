/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import org.geotools.feature.visitor.MaxVisitor;
import org.geotools.feature.visitor.MinVisitor;
import org.locationtech.geowave.core.geotime.store.statistics.FieldNameStatistic;
import org.locationtech.geowave.core.geotime.store.statistics.TimeRangeDataStatistics;
import org.locationtech.geowave.core.geotime.util.ExtractAttributesFilter;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.NumericRangeDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.StatisticsId;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

class GeoWaveGTPluginUtils {

  protected static List<InternalDataStatistics<SimpleFeature, ?, ?>> getStatsFor(
      final String name,
      final Map<StatisticsId, InternalDataStatistics<SimpleFeature, ?, ?>> statsMap) {
    final List<InternalDataStatistics<SimpleFeature, ?, ?>> stats = new LinkedList<>();
    for (final Map.Entry<StatisticsId, InternalDataStatistics<SimpleFeature, ?, ?>> stat : statsMap.entrySet()) {
      if ((stat.getValue() instanceof FieldNameStatistic)
          && ((FieldNameStatistic) stat.getValue()).getFieldName().endsWith(name)) {
        stats.add(stat.getValue());
      }
    }
    return stats;
  }

  protected static boolean accepts(
      final org.opengis.feature.FeatureVisitor visitor,
      final org.opengis.util.ProgressListener progress,
      final SimpleFeatureType featureType,
      final Map<StatisticsId, InternalDataStatistics<SimpleFeature, ?, ?>> statsMap)
      throws IOException {
    if ((visitor instanceof MinVisitor)) {
      final ExtractAttributesFilter filter = new ExtractAttributesFilter();

      final MinVisitor minVisitor = (MinVisitor) visitor;
      final Collection<String> attrs =
          (Collection<String>) minVisitor.getExpression().accept(filter, null);
      int acceptedCount = 0;
      for (final String attr : attrs) {
        for (final InternalDataStatistics<SimpleFeature, ?, ?> stat : getStatsFor(attr, statsMap)) {
          if (stat instanceof TimeRangeDataStatistics) {
            minVisitor.setValue(
                convertToType(
                    attr,
                    new Date(((TimeRangeDataStatistics) stat).getMin()),
                    featureType));
            acceptedCount++;
          } else if (stat instanceof NumericRangeDataStatistics) {
            minVisitor.setValue(
                convertToType(attr, ((NumericRangeDataStatistics) stat).getMin(), featureType));
            acceptedCount++;
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
      for (final String attr : attrs) {
        for (final InternalDataStatistics<SimpleFeature, ?, ?> stat : getStatsFor(attr, statsMap)) {
          if (stat instanceof TimeRangeDataStatistics) {
            maxVisitor.setValue(
                convertToType(
                    attr,
                    new Date(((TimeRangeDataStatistics) stat).getMax()),
                    featureType));
            acceptedCount++;
          } else if (stat instanceof NumericRangeDataStatistics) {
            maxVisitor.setValue(
                convertToType(attr, ((NumericRangeDataStatistics) stat).getMax(), featureType));
            acceptedCount++;
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
