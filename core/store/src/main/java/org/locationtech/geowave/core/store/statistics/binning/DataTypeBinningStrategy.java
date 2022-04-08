/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics.binning;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.StatisticBinningStrategy;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;

/**
 * Statistic binning strategy that bins statistic values by data type name. This is generally only
 * used for index statistics because data type and field statistics would all go under the same bin.
 */
public class DataTypeBinningStrategy implements StatisticBinningStrategy {
  public static final String NAME = "DATA_TYPE";

  @Override
  public String getStrategyName() {
    return NAME;
  }

  @Override
  public String getDescription() {
    return "Bin the statistic by data type.  Only used for index statistics.";
  }

  @Override
  public <T> ByteArray[] getBins(
      final DataTypeAdapter<T> adapter,
      final T entry,
      final GeoWaveRow... rows) {
    return new ByteArray[] {getBin(adapter)};
  }

  @Override
  public String getDefaultTag() {
    return "dataType";
  }

  public static ByteArray getBin(final DataTypeAdapter<?> adapter) {
    if (adapter == null) {
      return new ByteArray();
    }
    return new ByteArray(adapter.getTypeName());
  }

  public static ByteArray getBin(final String typeName) {
    if (typeName == null) {
      return new ByteArray();
    }
    return new ByteArray(typeName);
  }

  @Override
  public byte[] toBinary() {
    return new byte[0];
  }

  @Override
  public void fromBinary(final byte[] bytes) {}

  @Override
  public String binToString(final ByteArray bin) {
    return bin.getString();
  }
}
