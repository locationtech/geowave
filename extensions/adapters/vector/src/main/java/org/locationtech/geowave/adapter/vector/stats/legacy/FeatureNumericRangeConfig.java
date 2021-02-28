/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.stats.legacy;

import org.locationtech.geowave.adapter.vector.stats.StatsConfig;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.statistics.field.NumericRangeStatistic;
import org.opengis.feature.simple.SimpleFeature;

/** Convert Feature Numeric Range statistics to a JSON object */
public class FeatureNumericRangeConfig implements StatsConfig<SimpleFeature> {
  /** */
  private static final long serialVersionUID = 6309383518148391565L;

  @Override
  public byte[] toBinary() {
    return new byte[0];
  }

  @Override
  public void fromBinary(final byte[] bytes) {}

  @Override
  public Statistic<?> create(String typeName, String fieldName) {
    return new NumericRangeStatistic(typeName, fieldName);
  }
}
