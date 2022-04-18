/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.locationtech.geowave.core.geotime.util.SimpleFeatureUserDataConfiguration;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * Represents a set of configurations maintained within the user data of a simple feature type and
 * is tracked by the type name.
 */
public class SimpleFeatureUserDataConfigurationSet implements java.io.Serializable, Persistable {
  private static final long serialVersionUID = -1266366263353595379L;
  private static Logger LOGGER =
      LoggerFactory.getLogger(SimpleFeatureUserDataConfigurationSet.class);
  public static final String SIMPLE_FEATURE_CONFIG_FILE_PROP = "SIMPLE_FEATURE_CONFIG_FILE";

  /**
   * Name string accessed Map of SimpleFeatureUserDataConfiguration in this object. The name is the
   * SimpleFeatureType name that will have a configuration set.
   */
  private Map<String, List<SimpleFeatureUserDataConfiguration>> configurations = new HashMap<>();

  /**
   * Default Constructor<br>
   */
  public SimpleFeatureUserDataConfigurationSet() {}

  /**
   * Constructor<br> Creates a new SimpleFeatureUserDataConfigurationSet configured using the passed
   * in SimpleFeature type. Will be accessed using the type name.
   *
   * @param type - SFT to be configured
   */
  public SimpleFeatureUserDataConfigurationSet(final SimpleFeatureType type) {
    final List<SimpleFeatureUserDataConfiguration> sfudc =
        getConfigurationsForType(type.getTypeName());

    for (final SimpleFeatureUserDataConfiguration configuration : sfudc) {
      configuration.configureFromType(type);
    }
  }

  /**
   * Constructor<br> Creates a new SimpleFeatureUserDataConfigurationSet configured using the passed
   * in SimpleFeature type and adding the passed in configurations. Will be accessed using the type
   * name.
   *
   * @param type
   * @param configurations
   */
  public SimpleFeatureUserDataConfigurationSet(
      final SimpleFeatureType type,
      final List<SimpleFeatureUserDataConfiguration> configurations) {
    super();
    getConfigurationsForType(type.getTypeName()).addAll(configurations);
    configureFromType(type);
  }

  /** @return a Map of all the SimpleFeatureUserDataConfiguration's by name */
  public Map<String, List<SimpleFeatureUserDataConfiguration>> getConfigurations() {
    return configurations;
  }

  /**
   * Gets a List of all the SimpleFeatureUserDataConfigurations for the SFT specified by the
   * 'typeName' string
   *
   * @param typeName - SFT configuration desired
   * @return - List<SimpleFeatureUserDataConfigurations>
   */
  public synchronized List<SimpleFeatureUserDataConfiguration> getConfigurationsForType(
      final String typeName) {
    List<SimpleFeatureUserDataConfiguration> configList = configurations.get(typeName);

    if (configList == null) {
      configList = new ArrayList<>();
      configurations.put(typeName, configList);
    }

    return configList;
  }

  /**
   * Add the passed in configuration to the list of configurations for the specified type name
   *
   * @param typeName - name of type which will get an added configuration
   * @param config - configuration to be added
   */
  public void addConfigurations(
      final String typeName,
      final SimpleFeatureUserDataConfiguration config) {
    getConfigurationsForType(typeName).add(config);
  }

  /**
   * Updates the entire list of SimpleFeatureUserDataConfiguration(s) with information from the
   * passed in SF type
   *
   * @param type - SF type to be updated
   */
  public void configureFromType(final SimpleFeatureType type) {
    final List<SimpleFeatureUserDataConfiguration> sfudc =
        getConfigurationsForType(type.getTypeName());

    // Go through list of SFUD configurations and update each one with
    // information from the
    // passed in SF type

    for (final SimpleFeatureUserDataConfiguration configuration : sfudc) {
      configuration.configureFromType(type);
    }
  }

  /**
   * Updates the SFT with the entire list of SimpleFeatureUserDataConfiguration(s)
   *
   * @param type - SF type to be updated
   */
  public void updateType(final SimpleFeatureType type) {
    final List<SimpleFeatureUserDataConfiguration> sfudc =
        getConfigurationsForType(type.getTypeName());

    // Go through list of SFUD configurations and update each one in the
    // passed in SF type

    for (final SimpleFeatureUserDataConfiguration configuration : sfudc) {
      configuration.updateType(type);
    }
  }

  /**
   * Method that reads user data configuration information from
   * {@value #SIMPLE_FEATURE_CONFIG_FILE_PROP} and updates the passed in SFT.
   *
   * @param type - SFT to be updated
   * @return the SFT passed in as a parameter
   */
  @SuppressWarnings("deprecation")
  public static SimpleFeatureType configureType(final SimpleFeatureType type) {
    // HP Fortify "Path Manipulation" false positive
    // What Fortify considers "user input" comes only
    // from users with OS-level access anyway
    final String configFileName = System.getProperty(SIMPLE_FEATURE_CONFIG_FILE_PROP);
    if (configFileName != null) {
      final File configFile = new File(configFileName);
      if (configFile.exists() && configFile.canRead()) {
        try (FileInputStream input = new FileInputStream(configFile);
            Reader reader = new InputStreamReader(input, "UTF-8")) {
          final ObjectMapper mapper =
              new ObjectMapper().disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
          final SimpleFeatureUserDataConfigurationSet instance =
              mapper.readValue(reader, SimpleFeatureUserDataConfigurationSet.class);
          instance.updateType(type);
        } catch (final IOException e) {
          // HP Fortify "Log Forging" false positive
          // What Fortify considers "user input" comes only
          // from users with OS-level access anyway
          LOGGER.error("Cannot parse JSON congiguration file " + configFileName, e);
        }
      }
    }
    return type;
  }

  @Override
  public byte[] toBinary() {
    int size = 0;
    final List<byte[]> entries = new ArrayList<>(configurations.size());
    for (final Entry<String, List<SimpleFeatureUserDataConfiguration>> e : configurations.entrySet()) {
      final byte[] keyBytes = StringUtils.stringToBinary(e.getKey());
      final List<byte[]> configs = new ArrayList<>(e.getValue().size());
      int entrySize =
          VarintUtils.unsignedIntByteLength(keyBytes.length)
              + keyBytes.length
              + VarintUtils.unsignedIntByteLength(configs.size());
      for (final SimpleFeatureUserDataConfiguration config : e.getValue()) {
        final byte[] confBytes = PersistenceUtils.toBinary(config);
        entrySize += VarintUtils.unsignedIntByteLength(confBytes.length);
        entrySize += confBytes.length;
        configs.add(confBytes);
      }
      size += entrySize;
      final ByteBuffer buf = ByteBuffer.allocate(entrySize);
      VarintUtils.writeUnsignedInt(keyBytes.length, buf);
      buf.put(keyBytes);
      VarintUtils.writeUnsignedInt(configs.size(), buf);
      for (final byte[] confBytes : configs) {
        VarintUtils.writeUnsignedInt(confBytes.length, buf);
        buf.put(confBytes);
      }
      entries.add(buf.array());
    }
    size += VarintUtils.unsignedIntByteLength(configurations.size());
    final ByteBuffer buf = ByteBuffer.allocate(size);
    VarintUtils.writeUnsignedInt(configurations.size(), buf);
    for (final byte[] e : entries) {
      buf.put(e);
    }
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    final int entrySize = VarintUtils.readUnsignedInt(buf);
    final Map<String, List<SimpleFeatureUserDataConfiguration>> internalConfigurations =
        new HashMap<>(entrySize);
    for (int i = 0; i < entrySize; i++) {
      final int keySize = VarintUtils.readUnsignedInt(buf);
      final byte[] keyBytes = ByteArrayUtils.safeRead(buf, keySize);
      final String key = StringUtils.stringFromBinary(keyBytes);
      final int numConfigs = VarintUtils.readUnsignedInt(buf);
      final List<SimpleFeatureUserDataConfiguration> confList = new ArrayList<>(numConfigs);
      for (int c = 0; c < numConfigs; c++) {
        final byte[] entryBytes = ByteArrayUtils.safeRead(buf, VarintUtils.readUnsignedInt(buf));
        confList.add((SimpleFeatureUserDataConfiguration) PersistenceUtils.fromBinary(entryBytes));
      }
      internalConfigurations.put(key, confList);
    }
    configurations = internalConfigurations;
  }
}
