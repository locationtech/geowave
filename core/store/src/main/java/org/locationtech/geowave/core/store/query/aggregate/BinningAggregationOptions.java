/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.aggregate;

import java.nio.ByteBuffer;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.api.BinningStrategy;

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
  byte[] baseParamBytes;

  /**
   * The strategy that we use to bin entries with.
   */
  BinningStrategy binningStrategy;

  /**
   * The maximum bins that the binning aggregation can support.
   */
  int maxBins;

  public BinningAggregationOptions() {}

  public BinningAggregationOptions(
      final byte[] baseBytes,
      final byte[] baseParamBytes,
      final BinningStrategy binningStrategy,
      final int maxBins) {
    this.baseBytes = baseBytes;
    this.baseParamBytes = baseParamBytes;
    this.binningStrategy = binningStrategy;
    this.maxBins = maxBins;
  }

  @Override
  public byte[] toBinary() {
    final byte[] strategyBytes = PersistenceUtils.toBinary(this.binningStrategy);
    final byte[] baseParams = baseParamBytes == null ? new byte[0] : baseParamBytes;
    return ByteBuffer.allocate(
        16 + this.baseBytes.length + baseParams.length + strategyBytes.length).putInt(
            this.baseBytes.length).put(this.baseBytes).putInt(baseParams.length).put(
                baseParams).putInt(strategyBytes.length).put(strategyBytes).putInt(maxBins).array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer bb = ByteBuffer.wrap(bytes);

    final int baseBytesLen = bb.getInt();
    final byte[] baseBytes = new byte[baseBytesLen];
    bb.get(baseBytes);
    this.baseBytes = baseBytes;

    final int paramsBytesLen = bb.getInt();
    final byte[] paramsBytes = new byte[paramsBytesLen];
    if (paramsBytes.length > 0) {
      bb.get(paramsBytes);
      this.baseParamBytes = paramsBytes;
    } else {
      this.baseParamBytes = null;
    }

    final int strategyBytesLen = bb.getInt();
    final byte[] strategyBytes = new byte[strategyBytesLen];
    bb.get(strategyBytes);
    this.binningStrategy = (BinningStrategy) PersistenceUtils.fromBinary(strategyBytes);

    this.maxBins = bb.getInt();
  }
}
