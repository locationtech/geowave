/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.spi;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.locationtech.geowave.core.index.SPIServiceRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** These are the plugin index types that can be registered and used within Geowave. */
public class DimensionalityTypeRegistry {

  private static final Logger LOGGER = LoggerFactory.getLogger(DimensionalityTypeRegistry.class);

  private static Map<String, DimensionalityTypeProviderSpi> registeredDimensionalityTypes = null;

  private static synchronized void initDimensionalityTypeRegistry() {
    registeredDimensionalityTypes = new HashMap<>();
    final Iterator<DimensionalityTypeProviderSpi> dimensionalityTypesProviders =
        new SPIServiceRegistry(DimensionalityTypeRegistry.class).load(
            DimensionalityTypeProviderSpi.class);
    while (dimensionalityTypesProviders.hasNext()) {
      final DimensionalityTypeProviderSpi dimensionalityTypeProvider =
          dimensionalityTypesProviders.next();
      if (registeredDimensionalityTypes.containsKey(
          dimensionalityTypeProvider.getDimensionalityTypeName())) {
        LOGGER.warn(
            "Dimensionality type '"
                + dimensionalityTypeProvider.getDimensionalityTypeName()
                + "' already registered.  Unable to register type provided by "
                + dimensionalityTypeProvider.getClass().getName());
      } else {
        registeredDimensionalityTypes.put(
            dimensionalityTypeProvider.getDimensionalityTypeName(),
            dimensionalityTypeProvider);
      }
    }
  }

  public static Map<String, DimensionalityTypeProviderSpi> getRegisteredDimensionalityTypes() {
    if (registeredDimensionalityTypes == null) {
      initDimensionalityTypeRegistry();
    }
    return Collections.unmodifiableMap(registeredDimensionalityTypes);
  }

  public static DimensionalityTypeProviderSpi getSelectedDimensionalityProvider(
      final String dimensionalityType) {
    if (registeredDimensionalityTypes == null) {
      initDimensionalityTypeRegistry();
    }

    return registeredDimensionalityTypes.get(dimensionalityType);
  }
}
