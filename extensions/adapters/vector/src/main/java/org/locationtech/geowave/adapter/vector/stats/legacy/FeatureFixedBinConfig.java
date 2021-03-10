/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.stats.legacy;

import java.nio.ByteBuffer;
import org.locationtech.geowave.adapter.vector.stats.StatsConfig;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.statistics.field.FixedBinNumericHistogramStatistic;
import org.opengis.feature.simple.SimpleFeature;

public class FeatureFixedBinConfig implements StatsConfig<SimpleFeature> {
  /** */
  private static final long serialVersionUID = 6309383518148391565L;

  private double minValue = Double.MAX_VALUE;
  private double maxValue = Double.MIN_VALUE;
  private int bins;

  public FeatureFixedBinConfig() {}

  public FeatureFixedBinConfig(final double minValue, final double maxValue, final int bins) {
    super();
    this.minValue = minValue;
    this.maxValue = maxValue;
    this.bins = bins;
  }

  public double getMinValue() {
    return minValue;
  }

  public void setMinValue(final double minValue) {
    this.minValue = minValue;
  }

  public double getMaxValue() {
    return maxValue;
  }

  public void setMaxValue(final double maxValue) {
    this.maxValue = maxValue;
  }

  public int getBins() {
    return bins;
  }

  public void setBins(final int bins) {
    this.bins = bins;
  }

  @Override
  public Statistic<?> create(final String typeName, final String fieldName) {
    return new FixedBinNumericHistogramStatistic(typeName, fieldName, bins, minValue, maxValue);
  }

  @Override
  public byte[] toBinary() {
    final ByteBuffer buf = ByteBuffer.allocate(16);
    buf.putDouble(minValue);
    buf.putDouble(maxValue);
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    minValue = buf.getDouble();
    maxValue = buf.getDouble();
  }
}
