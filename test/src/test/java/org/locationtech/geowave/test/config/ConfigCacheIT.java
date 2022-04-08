/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.test.config;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.locationtech.geowave.core.cli.operations.config.SetCommand;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.cli.parser.ManualOperationParams;
import org.locationtech.geowave.core.store.GeoWaveStoreFinder;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.core.store.cli.store.AddStoreCommand;
import org.locationtech.geowave.core.store.cli.store.CopyConfigStoreCommand;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.cli.store.RemoveStoreCommand;
import org.locationtech.geowave.core.store.memory.MemoryRequiredOptions;
import org.locationtech.geowave.core.store.memory.MemoryStoreFactoryFamily;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigCacheIT {

  public File configFile = null;
  public ManualOperationParams operationParams = null;

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigCacheIT.class);
  private static long startMillis;

  @BeforeClass
  public static void startTimer() {
    startMillis = System.currentTimeMillis();
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*         RUNNING ConfigCacheIT         *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");
  }

  @AfterClass
  public static void reportTest() {
    LOGGER.warn("-----------------------------------------");
    LOGGER.warn("*                                       *");
    LOGGER.warn("*      FINISHED ConfigCacheIT           *");
    LOGGER.warn(
        "*         "
            + ((System.currentTimeMillis() - startMillis) / 1000)
            + "s elapsed.                 *");
    LOGGER.warn("*                                       *");
    LOGGER.warn("-----------------------------------------");
  }

  @Before
  public void before() throws IOException {
    configFile = File.createTempFile("test_config", null);
    operationParams = new ManualOperationParams();
    operationParams.getContext().put(ConfigOptions.PROPERTIES_FILE_CONTEXT, configFile);
    GeoWaveStoreFinder.getRegisteredStoreFactoryFamilies().put(
        "memory",
        new MemoryStoreFactoryFamily());
  }

  @After
  public void after() {
    if (configFile.exists()) {
      configFile.delete();
    }
    GeoWaveStoreFinder.getRegisteredStoreFactoryFamilies().remove("memory");
  }

  @Test
  public void addStore() {
    final String storeName = new MemoryStoreFactoryFamily().getType();

    final AddStoreCommand command = new AddStoreCommand();
    command.setParameters("abc");
    command.setMakeDefault(true);
    command.setStoreType(storeName);

    // This will load the params via SPI.
    command.prepare(operationParams);

    final DataStorePluginOptions options = command.getPluginOptions();

    final MemoryRequiredOptions opts = (MemoryRequiredOptions) options.getFactoryOptions();
    opts.setGeoWaveNamespace("namespace");

    command.execute(operationParams);

    final Properties props = ConfigOptions.loadProperties(configFile);

    Assert.assertEquals(
        "namespace",
        props.getProperty("store.abc.opts." + StoreFactoryOptions.GEOWAVE_NAMESPACE_OPTION));
    Assert.assertEquals(
        "abc",
        props.getProperty(DataStorePluginOptions.DEFAULT_PROPERTY_NAMESPACE));
  }

  @Test
  public void addStoreFromDefault() {
    addStore();

    // Now make from default
    final AddStoreCommand command = new AddStoreCommand();
    command.setParameters("abc2");
    command.setMakeDefault(false);

    // This will load the params via SPI.
    command.prepare(operationParams);

    final DataStorePluginOptions options = command.getPluginOptions();

    final MemoryRequiredOptions opts = (MemoryRequiredOptions) options.getFactoryOptions();
    opts.setGeoWaveNamespace("namespace2");

    command.execute(operationParams);

    final Properties props = ConfigOptions.loadProperties(configFile);

    Assert.assertEquals(
        "namespace2",
        props.getProperty("store.abc2.opts." + StoreFactoryOptions.GEOWAVE_NAMESPACE_OPTION));
  }

  @Test
  public void copyStore() {
    addStore();

    // Now make from default
    final CopyConfigStoreCommand command = new CopyConfigStoreCommand();
    command.setParameters("abc", "abc2");

    // This will load the params via SPI.
    command.prepare(operationParams);
    command.execute(operationParams);

    final Properties props = ConfigOptions.loadProperties(configFile);

    Assert.assertEquals(
        "namespace",
        props.getProperty("store.abc2.opts." + StoreFactoryOptions.GEOWAVE_NAMESPACE_OPTION));
  }

  @Test
  public void removeStore() throws Exception {
    addStore();

    final RemoveStoreCommand command = new RemoveStoreCommand();
    command.setEntryName("abc");

    command.prepare(operationParams);
    command.execute(operationParams);

    final Properties props = ConfigOptions.loadProperties(configFile);

    Assert.assertEquals(1, props.size());
  }

  @Test
  public void set() {
    final SetCommand command = new SetCommand();
    command.setParameters("lala", "5");
    command.prepare(operationParams);
    command.execute(operationParams);

    final Properties props = ConfigOptions.loadProperties(configFile);

    Assert.assertEquals(1, props.size());
    Assert.assertEquals("5", props.getProperty("lala"));
  }
}
