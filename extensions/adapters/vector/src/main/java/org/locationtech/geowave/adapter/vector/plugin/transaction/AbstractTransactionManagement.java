/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.plugin.transaction;

import org.locationtech.geowave.adapter.vector.plugin.GeoWaveDataStoreComponents;

public abstract class AbstractTransactionManagement implements GeoWaveTransaction {

  protected final GeoWaveDataStoreComponents components;

  public AbstractTransactionManagement(final GeoWaveDataStoreComponents components) {
    super();
    this.components = components;
  }

  @Override
  public StatisticsCache getDataStatistics() {
    return new StatisticsCache(
        components.getStatsStore(),
        components.getAdapter(),
        composeAuthorizations());
  }
}
