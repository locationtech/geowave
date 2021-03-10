/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics;

import java.nio.ByteBuffer;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticBinningStrategy;
import org.locationtech.geowave.core.store.api.StatisticValue;
import com.beust.jcommander.Parameter;

public abstract class BaseStatistic<V extends StatisticValue<?>> implements Statistic<V> {
  @Parameter(
      names = "--tag",
      description = "A tag for the statistic.  If one is not provided, a default will be set.")
  private String tag = null;

  private final StatisticType<V> statisticType;

  private StatisticBinningStrategy binningStrategy = null;

  protected StatisticId<V> cachedStatisticId = null;

  public BaseStatistic(final StatisticType<V> statisticType) {
    this.statisticType = statisticType;
  }

  public void setTag(final String tag) {
    this.tag = tag;
  }

  public void setInternal() {
    this.tag = INTERNAL_TAG;
  }

  @Override
  public final String getTag() {
    if (tag == null) {
      return binningStrategy != null ? binningStrategy.getDefaultTag() : DEFAULT_TAG;
    }
    return tag;
  }

  public void setBinningStrategy(final StatisticBinningStrategy binningStrategy) {
    this.binningStrategy = binningStrategy;
  }

  @Override
  public StatisticBinningStrategy getBinningStrategy() {
    return binningStrategy;
  }

  @Override
  public final StatisticType<V> getStatisticType() {
    return statisticType;
  }

  private byte[] binningStrategyBytesCache = null;

  protected int byteLength() {
    binningStrategyBytesCache = PersistenceUtils.toBinary(binningStrategy);
    final String resolvedTag = getTag();
    return VarintUtils.unsignedShortByteLength((short) binningStrategyBytesCache.length)
        + binningStrategyBytesCache.length
        + VarintUtils.unsignedShortByteLength((short) resolvedTag.length())
        + resolvedTag.length();
  }

  protected void writeBytes(final ByteBuffer buffer) {
    if (binningStrategyBytesCache == null) {
      binningStrategyBytesCache = PersistenceUtils.toBinary(binningStrategy);
    }
    VarintUtils.writeUnsignedShort((short) binningStrategyBytesCache.length, buffer);
    buffer.put(binningStrategyBytesCache);
    binningStrategyBytesCache = null;
    final byte[] stringBytes = StringUtils.stringToBinary(getTag());
    VarintUtils.writeUnsignedShort((short) stringBytes.length, buffer);
    buffer.put(stringBytes);
  }

  protected void readBytes(final ByteBuffer buffer) {
    short length = VarintUtils.readUnsignedShort(buffer);
    binningStrategyBytesCache = new byte[length];
    buffer.get(binningStrategyBytesCache);
    binningStrategy =
        (StatisticBinningStrategy) PersistenceUtils.fromBinary(binningStrategyBytesCache);
    binningStrategyBytesCache = null;
    length = VarintUtils.readUnsignedShort(buffer);
    final byte[] tagBytes = new byte[length];
    buffer.get(tagBytes);
    tag = StringUtils.stringFromBinary(tagBytes);
  }

  @Override
  public final byte[] toBinary() {
    final ByteBuffer buffer = ByteBuffer.allocate(byteLength());
    writeBytes(buffer);
    return buffer.array();
  }

  @Override
  public final void fromBinary(final byte[] bytes) {
    final ByteBuffer buffer = ByteBuffer.wrap(bytes);
    readBytes(buffer);
  }
}
