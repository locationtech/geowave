/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.examples.stats;

import org.locationtech.geowave.core.store.statistics.StatisticsRegistrySPI;
import org.locationtech.geowave.examples.stats.WordCountStatistic.WordCountValue;

/**
 * This class allows GeoWave to discover new statistics and binning strategies on the classpath.
 * This allows developers to create statistics that fit their use cases in the simplest way possible
 * without having to worry about the inner workings of the statistics system.
 * 
 * When adding new statistics via a statistics registry, the registry class needs to be added to
 * `src/main/resources/META-INF/services/org.locationtech.geowave.core.store.statistics.StatisticsRegistrySPI`.
 */
public class ExampleRegisteredStatistics implements StatisticsRegistrySPI {

  @Override
  public RegisteredStatistic[] getRegisteredStatistics() {
    // Register the example word count statistic with some persistable IDs that aren't being used by
    // GeoWave.
    return new RegisteredStatistic[] {
        new RegisteredStatistic(
            WordCountStatistic.STATS_TYPE,
            WordCountStatistic::new,
            WordCountValue::new,
            (short) 20100,
            (short) 20101),};
  }

  @Override
  public RegisteredBinningStrategy[] getRegisteredBinningStrategies() {
    // New binning strategies can also be registered using this interface
    return new RegisteredBinningStrategy[] {};
  }
}
