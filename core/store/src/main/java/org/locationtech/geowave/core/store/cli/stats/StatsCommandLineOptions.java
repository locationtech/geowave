/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.cli.stats;

import java.util.List;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.FieldDescriptor;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.DataTypeStatistic;
import org.locationtech.geowave.core.store.api.FieldStatistic;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.IndexStatistic;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.statistics.StatisticType;
import org.locationtech.geowave.core.store.statistics.StatisticsRegistry;
import org.locationtech.geowave.core.store.statistics.adapter.DataTypeStatisticType;
import org.locationtech.geowave.core.store.statistics.field.FieldStatisticType;
import org.locationtech.geowave.core.store.statistics.index.IndexStatisticType;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.clearspring.analytics.util.Lists;

public class StatsCommandLineOptions {

  @Parameter(names = {"-t", "--type"}, description = "The type of the statistic.")
  private String statType;

  @Parameter(names = "--indexName", description = "The name of the index, for index statistics.")
  private String indexName;

  @Parameter(
      names = "--typeName",
      description = "The name of the data type adapter, for field and type statistics.")
  private String typeName;

  @Parameter(names = "--fieldName", description = "The name of the field, for field statistics.")
  private String fieldName;

  @Parameter(names = "--tag", description = "The tag of the statistic.")
  private String tag;

  @Parameter(names = "--auth", description = "The authorizations used when querying statistics.")
  private String authorizations;

  public StatsCommandLineOptions() {}

  public String getAuthorizations() {
    return authorizations;
  }

  public void setAuthorizations(final String authorizations) {
    this.authorizations = authorizations;
  }

  public void setIndexName(final String indexName) {
    this.indexName = indexName;
  }

  public String getIndexName() {
    return indexName;
  }

  public void setTypeName(final String typeName) {
    this.typeName = typeName;
  }

  public String getTypeName() {
    return typeName;
  }

  public void setFieldName(final String fieldName) {
    this.fieldName = fieldName;
  }

  public String getFieldName() {
    return fieldName;
  }

  public void setTag(final String tag) {
    this.tag = tag;
  }

  public String getTag() {
    return tag;
  }

  public void setStatType(final String statType) {
    this.statType = statType;
  }

  public String getStatType() {
    return statType;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public List<Statistic<? extends StatisticValue<?>>> resolveMatchingStatistics(
      final DataStore dataStore,
      final DataStatisticsStore statsStore,
      final IndexStore indexStore) {
    final List<Statistic<? extends StatisticValue<?>>> matching = Lists.newArrayList();
    if ((indexName != null) && ((typeName != null) || (fieldName != null))) {
      throw new ParameterException(
          "Unable to process index statistics for a single type. Specify either an index name or a type name.");
    }
    StatisticType statisticType = null;
    if (statType != null) {
      statisticType = StatisticsRegistry.instance().getStatisticType(statType);

      if (statisticType == null) {
        throw new ParameterException("Unrecognized statistic type: " + statType);
      }
    }
    if (statisticType != null) {
      if (statisticType instanceof IndexStatisticType) {
        if (indexName == null) {
          throw new ParameterException(
              "An index name must be supplied when specifying an index statistic type.");
        }
        final Index index = indexStore.getIndex(indexName);
        if (index == null) {
          throw new ParameterException("Unable to find an index named: " + indexName);
        }
        try (CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> stats =
            statsStore.getIndexStatistics(index, statisticType, tag)) {
          stats.forEachRemaining(stat -> matching.add(stat));
        }
      } else if (statisticType instanceof DataTypeStatisticType) {
        if (typeName == null) {
          throw new ParameterException(
              "A type name must be supplied when specifying a data type statistic type.");
        }
        final DataTypeAdapter<?> adapter = dataStore.getType(typeName);
        if (adapter == null) {
          throw new ParameterException("Unable to find an type named: " + typeName);
        }
        try (CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> stats =
            statsStore.getDataTypeStatistics(adapter, statisticType, tag)) {
          stats.forEachRemaining(stat -> matching.add(stat));
        }
      } else if (statisticType instanceof FieldStatisticType) {
        if (typeName == null) {
          throw new ParameterException(
              "A type name must be supplied when specifying a field statistic type.");
        }
        final DataTypeAdapter<?> adapter = dataStore.getType(typeName);
        if (adapter == null) {
          throw new ParameterException("Unable to find an type named: " + typeName);
        }
        if (fieldName == null) {
          throw new ParameterException(
              "A field name must be supplied when specifying a field statistic type.");
        }
        boolean fieldFound = false;
        final FieldDescriptor[] fields = adapter.getFieldDescriptors();
        for (int i = 0; i < fields.length; i++) {
          if (fields[i].fieldName().equals(fieldName)) {
            fieldFound = true;
            break;
          }
        }
        if (!fieldFound) {
          throw new ParameterException(
              "Unable to find a field named '" + fieldName + "' on type '" + typeName + "'.");
        }
        try (CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> stats =
            statsStore.getFieldStatistics(adapter, statisticType, fieldName, tag)) {
          stats.forEachRemaining(stat -> matching.add(stat));
        }
      }
    } else {
      try (CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> stats =
          statsStore.getAllStatistics(null)) {
        stats.forEachRemaining(stat -> {
          // This could all be optimized to one giant check, but it's split for readability
          if ((tag != null) && !tag.equals(stat.getTag())) {
            return;
          }
          if ((indexName != null)
              && (!(stat instanceof IndexStatistic)
                  || !indexName.equals(((IndexStatistic) stat).getIndexName()))) {
            return;
          }
          if (typeName != null) {
            if (stat instanceof IndexStatistic) {
              return;
            }
            if ((stat instanceof DataTypeStatistic)
                && !typeName.equals(((DataTypeStatistic) stat).getTypeName())) {
              return;
            }
            if ((stat instanceof FieldStatistic)
                && !typeName.equals(((FieldStatistic) stat).getTypeName())) {
              return;
            }
          }
          if ((fieldName != null)
              && (!(stat instanceof FieldStatistic)
                  || !fieldName.equals(((FieldStatistic) stat).getFieldName()))) {
            return;
          }
          matching.add(stat);
        });
      }
    }
    return matching;
  }

}
