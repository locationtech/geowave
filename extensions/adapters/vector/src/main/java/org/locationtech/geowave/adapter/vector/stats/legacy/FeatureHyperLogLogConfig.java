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
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.statistics.field.HyperLogLogStatistic;
import org.opengis.feature.simple.SimpleFeature;

public class FeatureHyperLogLogConfig implements StatsConfig<SimpleFeature> {
  /** */
  private static final long serialVersionUID = 6309383518148391565L;

  private int precision = 16;

  public FeatureHyperLogLogConfig() {}

  public FeatureHyperLogLogConfig(final int precision) {
    super();
    this.precision = precision;
  }

  public int getPrecision() {
    return precision;
  }

  public void setPrecision(final int precision) {
    this.precision = precision;
  }

  @Override
  public byte[] toBinary() {
    final ByteBuffer buffer = ByteBuffer.allocate(VarintUtils.unsignedIntByteLength(precision));
    VarintUtils.writeUnsignedInt(precision, buffer);
    return buffer.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    precision = VarintUtils.readUnsignedInt(ByteBuffer.wrap(bytes));
  }

  @Override
  public Statistic<?> create(String typeName, String fieldName) {
    return new HyperLogLogStatistic(typeName, fieldName, precision);
  }
}
