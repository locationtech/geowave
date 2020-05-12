/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.aggregate;

import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import java.nio.ByteBuffer;

/**
 * The configuration parameters of a {@link BinningAggregation}
 *
 * @param <P> The Persistable that the sub-aggregation uses for configuration.
 * @param <T> The type that is being sent to the sub-aggregations for binning.
 */
public class BinningAggregationOptions<P extends Persistable, T> implements Persistable {

  /**
   * The baseBytes should contain primarily the classId of the Aggregation. This is used in
   * conjunction with the baseParams to create a fully-functional aggregation.
   *
   * When a new bin is created, these bytes are deserialized into a new {@code Aggregation<P,R,T>}
   * object.
   *
   * This is used to create the helperAggregation if it doesn't exist, and is used to create the
   * aggregation for new bins, when a new bin is created.
   */
  byte[] baseBytes;

  /**
   * The baseBytes should contain all the parameters needed to finish instantiating the base
   * aggregation that constitutes this meta-aggregation.
   */
  P baseParams;

  /**
   * The strategy that we use to bin entries with.
   */
  AggregationBinningStrategy<T> binningStrategy;

  /**
   * The maximum bins that the binning aggregation can support.
   */
  int maxBins;

  public BinningAggregationOptions() {}

  public BinningAggregationOptions(
      byte[] baseBytes,
      P baseParams,
      AggregationBinningStrategy<T> binningStrategy,
      int maxBins) {
    this.baseBytes = baseBytes;
    this.baseParams = baseParams;
    this.binningStrategy = binningStrategy;
    this.maxBins = maxBins;
  }

  @Override
  public byte[] toBinary() {
    byte[] paramsBytes = PersistenceUtils.toBinary(this.baseParams);
    byte[] strategyBytes = PersistenceUtils.toBinary(this.binningStrategy);
    return ByteBuffer.allocate(
        16 + this.baseBytes.length + paramsBytes.length + strategyBytes.length).putInt(
            this.baseBytes.length).put(this.baseBytes).putInt(paramsBytes.length).put(
                paramsBytes).putInt(strategyBytes.length).put(strategyBytes).putInt(
                    maxBins).array();
  }

  @Override
  public void fromBinary(byte[] bytes) {
    ByteBuffer bb = ByteBuffer.wrap(bytes);

    int baseBytesLen = bb.getInt();
    byte[] baseBytes = new byte[baseBytesLen];
    bb.get(baseBytes);
    this.baseBytes = baseBytes;

    int paramsBytesLen = bb.getInt();
    byte[] paramsBytes = new byte[paramsBytesLen];
    bb.get(paramsBytes);
    this.baseParams = (P) PersistenceUtils.fromBinary(paramsBytes);

    int strategyBytesLen = bb.getInt();
    byte[] strategyBytes = new byte[strategyBytesLen];
    bb.get(strategyBytes);
    this.binningStrategy =
        (AggregationBinningStrategy<T>) PersistenceUtils.fromBinary(strategyBytes);

    this.maxBins = bb.getInt();
  }
}
