/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.api;

import org.locationtech.geowave.core.store.statistics.StatisticType;

/**
 * Base interface for statistic queries.
 *
 * @param <V> the statistic value type
 * @param <R> the return type of the statistic value
 */
public interface StatisticQuery<V extends StatisticValue<R>, R> {
  /**
   * @return the statistic type for the query
   */
  public StatisticType<V> statisticType();

  /**
   * @return the tag filter
   */
  public String tag();

  /**
   * @return the bin filter
   */
  public BinConstraints binConstraints();

  /**
   * @return the authorizations for the query
   */
  public String[] authorizations();
}
