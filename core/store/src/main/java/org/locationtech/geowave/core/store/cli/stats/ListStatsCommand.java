/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.cli.stats;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.Command;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.exceptions.TargetNotFoundException;
import org.locationtech.geowave.core.cli.utils.ConsoleTablePrinter;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.DataTypeStatistic;
import org.locationtech.geowave.core.store.api.FieldStatistic;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.IndexStatistic;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.statistics.StatisticType;
import org.locationtech.geowave.core.store.statistics.StatisticsRegistry;
import org.locationtech.geowave.core.store.statistics.StatisticsValueIterator;
import org.locationtech.geowave.core.store.statistics.adapter.DataTypeStatisticType;
import org.locationtech.geowave.core.store.statistics.binning.CompositeBinningStrategy;
import org.locationtech.geowave.core.store.statistics.binning.DataTypeBinningStrategy;
import org.locationtech.geowave.core.store.statistics.field.FieldStatisticType;
import org.locationtech.geowave.core.store.statistics.index.IndexStatisticType;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.internal.Console;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

@GeowaveOperation(name = "list", parentOperation = StatsSection.class)
@Parameters(commandDescription = "Print statistics of a data store to standard output")
public class ListStatsCommand extends AbstractStatsCommand<String> implements Command {

  @Parameter(description = "<store name>")
  private List<String> parameters = new ArrayList<>();

  @Parameter(
      names = "--limit",
      description = "Limit the number or rows returned.  By default, all results will be displayed.")
  private Integer limit = null;

  @Parameter(names = "--csv", description = "Output statistics in CSV format.")
  private boolean csv = false;

  private String retValue = "";

  @Override
  public void execute(final OperationParams params) throws TargetNotFoundException {
    computeResults(params);
  }

  @Override
  protected boolean performStatsCommand(
      final DataStorePluginOptions storeOptions,
      final StatsCommandLineOptions statsOptions,
      final Console console) throws IOException {

    final DataStatisticsStore statsStore = storeOptions.createDataStatisticsStore();
    final IndexStore indexStore = storeOptions.createIndexStore();

    final String[] authorizations = getAuthorizations(statsOptions.getAuthorizations());

    DataTypeAdapter<?> adapter = null;

    if (statsOptions.getTypeName() != null) {
      adapter = storeOptions.createDataStore().getType(statsOptions.getTypeName());
      if (adapter == null) {
        throw new ParameterException(
            "A type called " + statsOptions.getTypeName() + " was not found.");
      }
    }

    StatisticType<StatisticValue<Object>> statisticType = null;
    if (statsOptions.getStatType() != null) {
      statisticType = StatisticsRegistry.instance().getStatisticType(statsOptions.getStatType());

      if (statisticType == null) {
        throw new ParameterException("Unrecognized statistic type: " + statsOptions.getStatType());
      }
    }

    List<String> headers = null;
    List<Statistic<?>> statsToList = Lists.newLinkedList();
    ValueTransformer transformer = null;
    Predicate<StatisticValue<?>> filter;
    if (statsOptions.getIndexName() != null) {
      if (statisticType != null && !(statisticType instanceof IndexStatisticType)) {
        throw new ParameterException(
            "Only index statistic types can be specified when listing statistics for a specific index.");
      }
      Index index = indexStore.getIndex(statsOptions.getIndexName());
      if (index == null) {
        throw new ParameterException(
            "An index called " + statsOptions.getIndexName() + " was not found.");
      }
      headers = Lists.newArrayList("Statistic", "Tag", "Bin", "Value");
      transformer = new ValueToRow();
      try (CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> stats =
          statsStore.getIndexStatistics(index, statisticType, statsOptions.getTag())) {
        if (adapter != null) {
          stats.forEachRemaining(stat -> {
            if (stat.getBinningStrategy() instanceof DataTypeBinningStrategy
                || (stat.getBinningStrategy() instanceof CompositeBinningStrategy
                    && ((CompositeBinningStrategy) stat.getBinningStrategy()).usesStrategy(
                        DataTypeBinningStrategy.class))) {
              statsToList.add(stat);
            }
          });
          filter = new IndexAdapterFilter(adapter.getTypeName());
        } else {
          stats.forEachRemaining(statsToList::add);
          filter = null;
        }
      }
    } else if (statsOptions.getTypeName() != null) {
      filter = null;
      if (statsOptions.getFieldName() != null) {
        if (statisticType != null && !(statisticType instanceof FieldStatisticType)) {
          throw new ParameterException(
              "Only field statistic types can be specified when listing statistics for a specific field.");
        }
        headers = Lists.newArrayList("Statistic", "Tag", "Bin", "Value");
        transformer = new ValueToRow();
        try (CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> stats =
            statsStore.getFieldStatistics(
                adapter,
                statisticType,
                statsOptions.getFieldName(),
                statsOptions.getTag())) {
          stats.forEachRemaining(statsToList::add);
        }
      } else {
        if (statisticType != null && statisticType instanceof IndexStatisticType) {
          throw new ParameterException(
              "Only data type and field statistic types can be specified when listing statistics for a specific data type.");
        }
        headers = Lists.newArrayList("Statistic", "Tag", "Field", "Bin", "Value");
        transformer = new ValueToFieldRow();
        if (statisticType == null || statisticType instanceof DataTypeStatisticType) {
          try (CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> stats =
              statsStore.getDataTypeStatistics(adapter, statisticType, statsOptions.getTag())) {
            stats.forEachRemaining(statsToList::add);
          }
        }
        if (statisticType == null || statisticType instanceof FieldStatisticType) {
          try (CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> stats =
              statsStore.getFieldStatistics(adapter, statisticType, null, statsOptions.getTag())) {
            stats.forEachRemaining(statsToList::add);
          }
        }
      }
    } else if (statsOptions.getFieldName() != null) {
      throw new ParameterException("A type name must be supplied with a field name.");
    } else {
      filter = null;
      headers = Lists.newArrayList("Index/Adapter", "Statistic", "Tag", "Field", "Bin", "Value");
      transformer = new ValueToAllRow();
      try (CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> stats =
          statsStore.getAllStatistics(statisticType)) {
        stats.forEachRemaining(stat -> {
          if (statsOptions.getTag() == null || stat.getTag().equals(statsOptions.getTag())) {
            statsToList.add(stat);
          }
        });
      }
    }
    Collections.sort(statsToList, new StatComparator());
    try (StatisticsValueIterator values =
        new StatisticsValueIterator(statsStore, statsToList.iterator(), null, authorizations)) {
      Iterator<List<Object>> rows =
          Iterators.transform(
              filter == null ? values : Iterators.filter(values, v -> filter.test(v)),
              transformer::transform);
      if (limit != null) {
        rows = Iterators.limit(rows, limit);
      }
      if (rows.hasNext()) {
        if (csv) {
          StringBuilder sb = new StringBuilder();
          sb.append(Arrays.toString(headers.toArray()));
          rows.forEachRemaining(row -> sb.append(Arrays.toString(row.toArray())));
          retValue = sb.toString();
          console.println(retValue);
        } else {
          console.println("Matching statistics:");
          ConsoleTablePrinter printer =
              new ConsoleTablePrinter(0, limit != null ? limit : 30, console);
          printer.print(headers, rows);
        }
      } else {
        console.println("No matching statistics were found.");
      }
    }

    return true;
  }

  public List<String> getParameters() {
    return parameters;
  }

  public void setParameters(final String storeName) {
    parameters = new ArrayList<>();
    parameters.add(storeName);
  }

  @Override
  public String computeResults(final OperationParams params) throws TargetNotFoundException {
    // Ensure we have all the required arguments
    if (parameters.size() < 1) {
      throw new ParameterException("Requires arguments: <store name>");
    }
    super.run(params, parameters);
    if (!retValue.equals("")) {
      return retValue;
    } else {
      return "No Data Found";
    }
  }

  private static class IndexAdapterFilter implements Predicate<StatisticValue<?>> {

    private final ByteArray adapterBin;

    public IndexAdapterFilter(final String typeName) {
      this.adapterBin = DataTypeBinningStrategy.getBin(typeName);
    }

    @Override
    public boolean test(StatisticValue<?> value) {
      Statistic<?> statistic = value.getStatistic();
      if (statistic.getBinningStrategy() instanceof DataTypeBinningStrategy) {
        return Arrays.equals(value.getBin().getBytes(), adapterBin.getBytes());
      } else if (statistic.getBinningStrategy() instanceof CompositeBinningStrategy
          && ((CompositeBinningStrategy) statistic.getBinningStrategy()).usesStrategy(
              DataTypeBinningStrategy.class)) {
        CompositeBinningStrategy binningStrategy =
            (CompositeBinningStrategy) statistic.getBinningStrategy();
        if (binningStrategy.binMatches(DataTypeBinningStrategy.class, value.getBin(), adapterBin)) {
          return true;
        }
      }
      return false;
    }

  }

  private static class StatComparator implements Comparator<Statistic<?>>, Serializable {

    private static final long serialVersionUID = 7635824822932295378L;

    @Override
    public int compare(Statistic<?> o1, Statistic<?> o2) {
      int compare = 0;
      if ((o1 instanceof IndexStatistic && o2 instanceof DataTypeStatistic)
          || (o1 instanceof IndexStatistic && o2 instanceof FieldStatistic)
          || (o1 instanceof DataTypeStatistic && o2 instanceof FieldStatistic)) {
        compare = -1;
      } else if ((o2 instanceof IndexStatistic && o1 instanceof DataTypeStatistic)
          || (o2 instanceof IndexStatistic && o1 instanceof FieldStatistic)
          || (o2 instanceof DataTypeStatistic && o1 instanceof FieldStatistic)) {
        compare = 1;
      }
      if (compare == 0) {
        compare =
            o1.getId().getGroupId().getString().compareTo(o2.getId().getGroupId().getString());
      }
      if (compare == 0) {
        compare = o1.getStatisticType().getString().compareTo(o2.getStatisticType().getString());
      }
      if (compare == 0) {
        compare = o1.getTag().compareTo(o2.getTag());
      }
      return compare;
    }

  }

  private static interface ValueTransformer {
    List<Object> transform(StatisticValue<?> value);
  }

  private static class ValueToRow implements ValueTransformer {
    @Override
    public List<Object> transform(StatisticValue<?> value) {
      return Lists.newArrayList(
          value.getStatistic().getStatisticType(),
          value.getStatistic().getTag(),
          value.getStatistic().getBinningStrategy() != null
              ? value.getStatistic().getBinningStrategy().binToString(value.getBin())
              : "N/A",
          value);
    }
  }

  private static class ValueToFieldRow implements ValueTransformer {
    @Override
    public List<Object> transform(StatisticValue<?> value) {
      String fieldName =
          value.getStatistic() instanceof FieldStatistic
              ? ((FieldStatistic<?>) value.getStatistic()).getFieldName()
              : "N/A";
      return Lists.newArrayList(
          value.getStatistic().getStatisticType(),
          value.getStatistic().getTag(),
          fieldName,
          value.getStatistic().getBinningStrategy() != null
              ? value.getStatistic().getBinningStrategy().binToString(value.getBin())
              : "N/A",
          value);
    }
  }

  private static class ValueToAllRow implements ValueTransformer {
    @Override
    public List<Object> transform(StatisticValue<?> value) {
      Statistic<?> statistic = value.getStatistic();
      String indexOrAdapter = null;
      String field = "N/A";
      String bin = "N/A";
      if (statistic instanceof IndexStatistic) {
        indexOrAdapter = ((IndexStatistic<?>) statistic).getIndexName();
      } else if (statistic instanceof DataTypeStatistic) {
        indexOrAdapter = ((DataTypeStatistic<?>) statistic).getTypeName();
      } else if (statistic instanceof FieldStatistic) {
        indexOrAdapter = ((FieldStatistic<?>) statistic).getTypeName();
        field = ((FieldStatistic<?>) statistic).getFieldName();
      }
      if (statistic.getBinningStrategy() != null) {
        bin = statistic.getBinningStrategy().binToString(value.getBin());
      }
      return Lists.newArrayList(
          indexOrAdapter,
          statistic.getStatisticType(),
          statistic.getTag(),
          field,
          bin,
          value);
    }
  }
}
