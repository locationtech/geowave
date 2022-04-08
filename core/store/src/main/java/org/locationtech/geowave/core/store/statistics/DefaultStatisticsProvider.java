/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics;

import java.util.List;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticValue;

/**
 * This interface can be used with data type adapters and indices so that default statistics will be
 * added to the data store when the adapter/index is added.
 */
public interface DefaultStatisticsProvider {
  /**
   * Get all default statistics for this adapter/index.
   * 
   * @return the default statistics
   */
  public List<Statistic<? extends StatisticValue<?>>> getDefaultStatistics();
}
