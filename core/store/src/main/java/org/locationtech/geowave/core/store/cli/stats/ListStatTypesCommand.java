/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.cli.stats;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.locationtech.geowave.core.cli.annotations.GeowaveOperation;
import org.locationtech.geowave.core.cli.api.OperationParams;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.locationtech.geowave.core.cli.utils.ConsoleTablePrinter;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.DataTypeStatistic;
import org.locationtech.geowave.core.store.api.FieldStatistic;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.IndexStatistic;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticBinningStrategy;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.cli.CLIUtils;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.statistics.StatisticsRegistry;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.internal.Console;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

@GeowaveOperation(name = "listtypes", parentOperation = StatsSection.class)
@Parameters(
    commandDescription = "List statistic types that are compatible with the given data store, "
        + "if no data store is provided, all registered statistics will be listed.")
public class ListStatTypesCommand extends ServiceEnabledCommand<Void> {

  @Parameter(description = "<store name>")
  private final List<String> parameters = new ArrayList<>();

  @Parameter(
      names = {"--indexName"},
      description = "If specified, only statistics that are compatible with this index will be listed.")
  private String indexName = null;

  @Parameter(
      names = {"--typeName"},
      description = "If specified, only statistics that are compatible with this type will be listed.")
  private String typeName = null;

  @Parameter(
      names = {"--fieldName"},
      description = "If specified, only statistics that are compatible with this field will be displayed.")
  private String fieldName = null;

  @Parameter(
      names = {"-b", "--binningStrategies"},
      description = "If specified, a list of registered binning strategies will be displayed.")
  private boolean binningStrategies = false;


  @Override
  public void execute(final OperationParams params) {
    computeResults(params);
  }

  @Override
  public Void computeResults(final OperationParams params) {
    if (parameters.isEmpty()) {
      listAllRegisteredStatistics(params.getConsole());
      return null;
    }

    final String storeName = parameters.get(0);

    // Attempt to load store.
    final DataStorePluginOptions storeOptions =
        CLIUtils.loadStore(storeName, getGeoWaveConfigFile(params), params.getConsole());

    final DataStore dataStore = storeOptions.createDataStore();

    if ((indexName != null) && (typeName != null)) {
      throw new ParameterException("Specify either index name or type name, not both.");
    }

    final Index index = indexName != null ? dataStore.getIndex(indexName) : null;
    if ((indexName != null) && (index == null)) {
      throw new ParameterException("Unable to find index: " + indexName);
    }

    final DataTypeAdapter<?> adapter = typeName != null ? dataStore.getType(typeName) : null;
    if ((typeName != null) && (adapter == null)) {
      throw new ParameterException("Unrecognized type name: " + typeName);
    }

    final Map<String, List<? extends Statistic<? extends StatisticValue<?>>>> indexStats =
        Maps.newHashMap();
    final Map<String, List<? extends Statistic<? extends StatisticValue<?>>>> adapterStats =
        Maps.newHashMap();
    final Map<String, Map<String, List<? extends Statistic<? extends StatisticValue<?>>>>> fieldStats =
        Maps.newHashMap();
    boolean hasAdapters = false;
    if (adapter == null) {
      if (index != null) {
        indexStats.put(
            index.getName(),
            StatisticsRegistry.instance().getRegisteredIndexStatistics(index.getClass()));
      } else {
        final DataTypeAdapter<?>[] adapters = dataStore.getTypes();
        for (final DataTypeAdapter<?> dataAdapter : adapters) {
          hasAdapters = true;
          adapterStats.put(
              dataAdapter.getTypeName(),
              StatisticsRegistry.instance().getRegisteredDataTypeStatistics(
                  dataAdapter.getDataClass()));
          fieldStats.put(
              dataAdapter.getTypeName(),
              StatisticsRegistry.instance().getRegisteredFieldStatistics(dataAdapter, fieldName));
        }

        final Index[] indices = dataStore.getIndices();
        for (final Index idx : indices) {
          indexStats.put(
              idx.getName(),
              StatisticsRegistry.instance().getRegisteredIndexStatistics(idx.getClass()));
        }
      }
    } else {
      hasAdapters = true;
      adapterStats.put(
          adapter.getTypeName(),
          StatisticsRegistry.instance().getRegisteredDataTypeStatistics(adapter.getDataClass()));
      fieldStats.put(
          adapter.getTypeName(),
          StatisticsRegistry.instance().getRegisteredFieldStatistics(adapter, fieldName));
    }

    final ConsoleTablePrinter printer =
        new ConsoleTablePrinter(0, Integer.MAX_VALUE, params.getConsole());
    if (hasAdapters) {
      displayIndexStats(printer, indexStats);
      displayAdapterStats(printer, adapterStats);
      displayFieldStats(printer, fieldStats);
      displayBinningStrategies(printer);
    } else {
      params.getConsole().println("There are no types in the data store.");
    }
    return null;
  }

  private void listAllRegisteredStatistics(final Console console) {
    final List<Statistic<?>> indexStats = Lists.newLinkedList();
    final List<Statistic<?>> adapterStats = Lists.newLinkedList();
    final List<Statistic<?>> fieldStats = Lists.newLinkedList();
    final List<? extends Statistic<? extends StatisticValue<?>>> allStats =
        StatisticsRegistry.instance().getAllRegisteredStatistics();
    Collections.sort(
        allStats,
        (s1, s2) -> s1.getStatisticType().getString().compareTo(s2.getStatisticType().getString()));
    for (final Statistic<?> statistic : allStats) {
      if (statistic instanceof IndexStatistic) {
        indexStats.add(statistic);
      } else if (statistic instanceof DataTypeStatistic) {
        adapterStats.add(statistic);
      } else if (statistic instanceof FieldStatistic) {
        fieldStats.add(statistic);
      }
    }
    final ConsoleTablePrinter printer = new ConsoleTablePrinter(0, Integer.MAX_VALUE, console);
    displayStatList(printer, indexStats, "Registered Index Statistics");
    displayStatList(printer, adapterStats, "Registered Adapter Statistics");
    displayStatList(printer, fieldStats, "Registered Field Statistics");
    displayBinningStrategies(printer);
  }

  private void displayBinningStrategies(final ConsoleTablePrinter printer) {
    if (!binningStrategies) {
      return;
    }
    printer.println("Registered Binning Strategies: ");
    final List<StatisticBinningStrategy> binningStrategies =
        StatisticsRegistry.instance().getAllRegisteredBinningStrategies();
    final List<List<Object>> rows = Lists.newArrayListWithCapacity(binningStrategies.size());
    for (final StatisticBinningStrategy binningStrategy : binningStrategies) {
      rows.add(Arrays.asList(binningStrategy.getStrategyName(), binningStrategy.getDescription()));
    }
    printer.print(Arrays.asList("Strategy", "Description"), rows);
  }

  private void displayStatList(
      final ConsoleTablePrinter printer,
      final List<? extends Statistic<? extends StatisticValue<?>>> stats,
      final String title) {
    printer.println(title + ": ");
    final List<List<Object>> rows = Lists.newArrayListWithCapacity(stats.size());

    for (final Statistic<?> o : stats) {
      rows.add(Arrays.asList(o.getStatisticType(), o.getDescription()));
    }
    printer.print(Arrays.asList("Statistic", "Description"), rows);
  }

  private void displayIndexStats(
      final ConsoleTablePrinter printer,
      final Map<String, List<? extends Statistic<? extends StatisticValue<?>>>> stats) {
    if (stats.size() == 0) {
      return;
    }
    printer.println("Compatible index statistics: ");
    final List<List<Object>> rows = Lists.newArrayListWithCapacity(stats.size());
    for (final Entry<String, List<? extends Statistic<? extends StatisticValue<?>>>> indexStats : stats.entrySet()) {
      boolean first = true;
      for (final Statistic<?> o : indexStats.getValue()) {
        rows.add(
            Arrays.asList(
                first ? indexStats.getKey() : "",
                o.getStatisticType(),
                o.getDescription()));
        first = false;
      }
    }
    printer.print(Arrays.asList("Index", "Statistic", "Description"), rows);
  }

  private void displayAdapterStats(
      final ConsoleTablePrinter printer,
      final Map<String, List<? extends Statistic<? extends StatisticValue<?>>>> stats) {
    if (stats.size() == 0) {
      return;
    }
    printer.println("Compatible data type statistics: ");
    final List<List<Object>> rows = Lists.newArrayListWithCapacity(stats.size());
    for (final Entry<String, List<? extends Statistic<? extends StatisticValue<?>>>> adapterStats : stats.entrySet()) {
      boolean first = true;
      for (final Statistic<?> o : adapterStats.getValue()) {
        rows.add(
            Arrays.asList(
                first ? adapterStats.getKey() : "",
                o.getStatisticType(),
                o.getDescription()));
        first = false;
      }
    }
    printer.print(Arrays.asList("Type", "Statistic", "Description"), rows);
  }

  private void displayFieldStats(
      final ConsoleTablePrinter printer,
      final Map<String, Map<String, List<? extends Statistic<? extends StatisticValue<?>>>>> stats) {
    if (stats.size() == 0) {
      return;
    }
    printer.println("Compatible field statistics: ");
    final List<List<Object>> rows = Lists.newArrayListWithCapacity(stats.size());
    for (final Entry<String, Map<String, List<? extends Statistic<? extends StatisticValue<?>>>>> adapterStats : stats.entrySet()) {
      boolean firstAdapter = true;
      for (final Entry<String, List<? extends Statistic<? extends StatisticValue<?>>>> fieldStats : adapterStats.getValue().entrySet()) {
        boolean firstField = true;
        for (final Statistic<?> o : fieldStats.getValue()) {
          rows.add(
              Arrays.asList(
                  firstAdapter ? adapterStats.getKey() : "",
                  firstField ? fieldStats.getKey() : "",
                  o.getStatisticType(),
                  o.getDescription()));
          firstAdapter = false;
          firstField = false;
        }
      }
    }
    printer.print(Arrays.asList("Type", "Field", "Statistic", "Description"), rows);
  }
}
