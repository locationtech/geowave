/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.memory;

import org.locationtech.geowave.core.store.BaseDataStoreFamily;
import org.locationtech.geowave.core.store.StoreFactoryFamilySpi;

public class MemoryStoreFactoryFamily extends BaseDataStoreFamily implements StoreFactoryFamilySpi {
  private static final String TYPE = "memory";
  private static final String DESCRIPTION =
      "A GeoWave store that is in memory typically only used for test purposes";

  public MemoryStoreFactoryFamily() {
    super(TYPE, DESCRIPTION, new MemoryFactoryHelper());
  }
}
