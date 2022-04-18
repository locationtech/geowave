/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.param;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.locationtech.geowave.analytic.PropertyManagement;
import org.locationtech.geowave.analytic.store.PersistableStore;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InputStoreParameterHelper implements ParameterHelper<PersistableStore> {
  /** */
  private static final long serialVersionUID = 1L;

  static final Logger LOGGER = LoggerFactory.getLogger(InputStoreParameterHelper.class);

  @Override
  public Class<PersistableStore> getBaseClass() {
    return PersistableStore.class;
  }

  @Override
  public void setValue(
      final Configuration config,
      final Class<?> scope,
      final PersistableStore value) {
    final DataStorePluginOptions options = value.getDataStoreOptions();
    GeoWaveInputFormat.setStoreOptions(config, options);
  }

  @Override
  public PersistableStore getValue(
      final JobContext context,
      final Class<?> scope,
      final PersistableStore defaultValue) {
    final DataStorePluginOptions pluginOptions = GeoWaveInputFormat.getStoreOptions(context);
    if (pluginOptions != null) {
      return new PersistableStore(pluginOptions);
    } else {
      return defaultValue;
    }
  }

  @Override
  public PersistableStore getValue(final PropertyManagement propertyManagement) {
    try {
      return (PersistableStore) propertyManagement.getProperty(
          StoreParameters.StoreParam.INPUT_STORE);
    } catch (final Exception e) {
      LOGGER.error("Unable to deserialize data store", e);
      return null;
    }
  }

  @Override
  public void setValue(final PropertyManagement propertyManagement, final PersistableStore value) {
    propertyManagement.store(StoreParameters.StoreParam.INPUT_STORE, value);
  }
}
