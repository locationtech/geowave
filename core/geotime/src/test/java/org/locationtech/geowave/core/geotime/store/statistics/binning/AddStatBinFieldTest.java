/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.statistics.binning;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.geowave.core.cli.api.Command;
import org.locationtech.geowave.core.cli.api.Operation;
import org.locationtech.geowave.core.cli.operations.GeoWaveTopLevelSection;
import org.locationtech.geowave.core.cli.parser.CommandLineOperationParams;
import org.locationtech.geowave.core.cli.parser.OperationParser;
import org.locationtech.geowave.core.store.GeoWaveStoreFinder;
import org.locationtech.geowave.core.store.StoreFactoryFamilySpi;
import org.locationtech.geowave.core.store.adapter.BasicDataTypeAdapter;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataStoreFactory;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.index.AttributeDimensionalityTypeProvider;
import org.locationtech.geowave.core.store.index.AttributeIndexOptions;
import org.locationtech.geowave.core.store.memory.MemoryRequiredOptions;
import org.locationtech.geowave.core.store.memory.MemoryStoreFactoryFamily;
import com.beust.jcommander.ParameterException;

/** Runs `geowave stat add` the way GeoWaveMain does, against a memory store from a config file. */
public class AddStatBinFieldTest {
  private static final String NAMESPACE = "add_stat_bin_field";

  public static class Rec {
    private String id;
    private Date time;
    private Long cell;

    public Rec() {}

    public String getId() {
      return id;
    }

    public void setId(final String id) {
      this.id = id;
    }

    public Date getTime() {
      return time;
    }

    public void setTime(final Date time) {
      this.time = time;
    }

    public Long getCell() {
      return cell;
    }

    public void setCell(final Long cell) {
      this.cell = cell;
    }
  }

  private static final StoreFactoryFamilySpi MEMORY = new MemoryStoreFactoryFamily();
  private File config;
  private DataStore dataStore;

  // the command line finds a store type by its SPI registration, which the memory store lacks
  @BeforeClass
  public static void registerMemoryStore() {
    GeoWaveStoreFinder.getRegisteredStoreFactoryFamilies().put(MEMORY.getType(), MEMORY);
  }

  @AfterClass
  public static void unregisterMemoryStore() {
    GeoWaveStoreFinder.getRegisteredStoreFactoryFamilies().remove(MEMORY.getType());
  }

  @Before
  public void createStoreAndType() throws IOException {
    config = File.createTempFile("geowave-config", ".properties");
    assertTrue(config.delete());
    assertNull(
        geowave(
            "-cf",
            config.getAbsolutePath(),
            "store",
            "add",
            "s",
            "-t",
            "memory",
            "--gwNamespace",
            NAMESPACE));
    dataStore = DataStoreFactory.createDataStore(memoryOptions());
    dataStore.addType(BasicDataTypeAdapter.newAdapter("Rec", Rec.class, "id"));
    dataStore.addIndex(
        "Rec",
        AttributeDimensionalityTypeProvider.createIndexFromOptions(
            dataStore,
            new AttributeIndexOptions("Rec", "id")));
  }

  @After
  public void deleteStore() {
    dataStore.deleteAll();
    config.delete();
  }

  private static MemoryRequiredOptions memoryOptions() {
    final MemoryRequiredOptions options = new MemoryRequiredOptions();
    options.setGeoWaveNamespace(NAMESPACE);
    return options;
  }

  // the parse and execute steps of GeoWaveMain, which exits non-zero on either exception
  private static Throwable geowave(final String... args) {
    final CommandLineOperationParams params =
        new OperationParser().parse(GeoWaveTopLevelSection.class, args);
    if (params.getSuccessCode() != 0) {
      return params.getSuccessException();
    }
    for (final Operation operation : params.getOperationMap().values()) {
      if (operation instanceof Command) {
        try {
          ((Command) operation).execute(params);
        } catch (final Exception e) {
          return e;
        }
        break;
      }
    }
    return null;
  }

  private Throwable statAdd(final boolean skipCalculation, final String... binningArgs) {
    final List<String> args =
        new ArrayList<>(
            Arrays.asList(
                "-cf",
                config.getAbsolutePath(),
                "stat",
                "add",
                "s",
                "-t",
                "COUNT",
                "--typeName",
                "Rec"));
    args.addAll(Arrays.asList(binningArgs));
    if (skipCalculation) {
      args.add("--skipCalculation");
    }
    return geowave(args.toArray(new String[0]));
  }

  // read through a new data store, since the one that added a statistic caches it
  private static List<String> persistedBinnedStatistics() {
    final List<String> binned = new ArrayList<>();
    for (final Statistic<?> statistic : DataStoreFactory.createDataStore(
        memoryOptions()).getDataTypeStatistics("Rec")) {
      if (statistic.getBinningStrategy() != null) {
        binned.add(statistic.getBinningStrategy().getStrategyName() + " " + statistic.getTag());
      }
    }
    Collections.sort(binned);
    return binned;
  }

  @Test
  public void twoBinFieldsAreRejectedBeforeAnythingIsPersisted() {
    for (final String[] binning : new String[][] {
        {"-b", "H3_CELL", "--binField", "cell", "--binField", "time"},
        {"-b", "EPOCH_INTERVAL", "--binField", "time", "--binField", "cell"}}) {
      for (final boolean skipCalculation : new boolean[] {false, true}) {
        final Throwable e = statAdd(skipCalculation, binning);
        assertTrue(String.valueOf(e), e instanceof ParameterException);
        assertEquals(
            binning[1] + " bins by exactly one field, but --binField was given 2 times",
            e.getMessage());
        assertEquals(Collections.emptyList(), persistedBinnedStatistics());
      }
    }
  }

  @Test
  public void oneBinFieldIsPersisted() {
    assertNull(statAdd(false, "-b", "H3_CELL", "--binField", "cell"));
    assertNull(
        statAdd(true, "-b", "EPOCH_INTERVAL", "--binField", "time", "--binInterval", "HOUR"));
    assertEquals(
        Arrays.asList("EPOCH_INTERVAL time-HOUR", "H3_CELL cell-H3(8)"),
        persistedBinnedStatistics());
  }
}
