/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics.adapter;

import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.statistics.StatisticType;

/**
 * Statistic type for data type statistics. Generally used for type checking.
 */
public class DataTypeStatisticType<V extends StatisticValue<?>> extends StatisticType<V> {
  private static final long serialVersionUID = 1L;

  public DataTypeStatisticType(final String id) {
    super(id);
  }
}
