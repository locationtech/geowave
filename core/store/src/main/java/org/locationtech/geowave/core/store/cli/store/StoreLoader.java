/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.cli.store;

import java.io.File;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Properties;
import org.locationtech.geowave.core.cli.api.DefaultPluginOptions;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.cli.operations.config.security.utils.SecurityUtils;
import org.locationtech.geowave.core.cli.utils.JCommanderParameterUtils;
import org.locationtech.geowave.core.store.StoreFactoryFamilySpi;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.internal.Console;

/**
 * This is a convenience class which sets up some obvious values in the OperationParams based on the
 * parsed 'store name' from the main parameter. The other parameters are saved in case they need to
 * be used.
 */
public class StoreLoader {
  private static final Logger LOGGER = LoggerFactory.getLogger(StoreLoader.class);

  private final String storeName;

  private DataStorePluginOptions dataStorePlugin = null;

  /** Constructor */
  public StoreLoader(final String store) {
    storeName = store;
  }

  /**
   * Attempt to load the data store configuration from the config file.
   *
   * @param configFile
   * @return {@code true} if the configuration was successfully loaded
   */
  public boolean loadFromConfig(final File configFile) {
    return loadFromConfig(configFile, new JCommander().getConsole());
  }


  /**
   * Attempt to load the data store configuration from the config file.
   *
   * @param console the console to print output to
   * @param configFile
   * @return {@code true} if the configuration was successfully loaded
   */
  public boolean loadFromConfig(final File configFile, final Console console) {

    final String namespace = DataStorePluginOptions.getStoreNamespace(storeName);

    return loadFromConfig(
        ConfigOptions.loadProperties(configFile, "^" + namespace),
        namespace,
        configFile,
        console);
  }

  /**
   * Attempt to load the data store configuration from the config file.
   *
   * @param configFile
   * @return {@code true} if the configuration was successfully loaded
   */
  public boolean loadFromConfig(
      final Properties props,
      final String namespace,
      final File configFile,
      final Console console) {

    dataStorePlugin = new DataStorePluginOptions();

    // load all plugin options and initialize dataStorePlugin with type and
    // options
    if (!dataStorePlugin.load(props, namespace)) {
      return false;
    }

    // knowing the datastore plugin options and class type, get all fields
    // and parameters in order to detect which are password fields
    if ((configFile != null) && (dataStorePlugin.getFactoryOptions() != null)) {
      File tokenFile = SecurityUtils.getFormattedTokenKeyFileForConfig(configFile);
      final Field[] fields = dataStorePlugin.getFactoryOptions().getClass().getDeclaredFields();
      for (final Field field : fields) {
        for (final Annotation annotation : field.getAnnotations()) {
          if (annotation.annotationType() == Parameter.class) {
            final Parameter parameter = (Parameter) annotation;
            if (JCommanderParameterUtils.isPassword(parameter)) {
              final String storeFieldName =
                  ((namespace != null) && !"".equals(namespace.trim()))
                      ? namespace + "." + DefaultPluginOptions.OPTS + "." + field.getName()
                      : field.getName();
              if (props.containsKey(storeFieldName)) {
                final String value = props.getProperty(storeFieldName);
                String decryptedValue = value;
                try {
                  decryptedValue =
                      SecurityUtils.decryptHexEncodedValue(
                          value,
                          tokenFile.getAbsolutePath(),
                          console);
                } catch (final Exception e) {
                  LOGGER.error(
                      "An error occurred encrypting specified password value: "
                          + e.getLocalizedMessage(),
                      e);
                }
                props.setProperty(storeFieldName, decryptedValue);
              }
            }
          }
        }
      }
      tokenFile = null;
    }
    // reload datastore plugin with new password-encrypted properties
    if (!dataStorePlugin.load(props, namespace)) {
      return false;
    }

    return true;
  }

  public DataStorePluginOptions getDataStorePlugin() {
    return dataStorePlugin;
  }

  public void setDataStorePlugin(final DataStorePluginOptions dataStorePlugin) {
    this.dataStorePlugin = dataStorePlugin;
  }

  public String getStoreName() {
    return storeName;
  }

  public StoreFactoryFamilySpi getFactoryFamily() {
    return dataStorePlugin.getFactoryFamily();
  }

  public StoreFactoryOptions getFactoryOptions() {
    return dataStorePlugin.getFactoryOptions();
  }

  public DataStore createDataStore() {
    return dataStorePlugin.createDataStore();
  }

  public PersistentAdapterStore createAdapterStore() {
    return dataStorePlugin.createAdapterStore();
  }

  public InternalAdapterStore createInternalAdapterStore() {
    return dataStorePlugin.createInternalAdapterStore();
  }

  public IndexStore createIndexStore() {
    return dataStorePlugin.createIndexStore();
  }

  public DataStatisticsStore createDataStatisticsStore() {
    return dataStorePlugin.createDataStatisticsStore();
  }

  public AdapterIndexMappingStore createAdapterIndexMappingStore() {
    return dataStorePlugin.createAdapterIndexMappingStore();
  }
}
