/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.api;

import java.nio.ByteBuffer;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.statistics.StatisticId;
import org.locationtech.geowave.core.store.statistics.adapter.DataTypeStatisticType;
import com.beust.jcommander.Parameter;

/**
 * Base class for data type statistics. These statistics are generally updated without looking at
 * individual fields on the data type.
 */
public abstract class DataTypeStatistic<V extends StatisticValue<?>> extends Statistic<V> {

  @Parameter(
      names = "--typeName",
      required = true,
      description = "The data type for the statistic.")
  private String typeName = null;

  public DataTypeStatistic(final DataTypeStatisticType<V> statisticsType) {
    super(statisticsType);
  }

  public DataTypeStatistic(final DataTypeStatisticType<V> statisticsType, final String typeName) {
    super(statisticsType);
    this.typeName = typeName;
  }

  public void setTypeName(final String name) {
    this.typeName = name;
  }

  public final String getTypeName() {
    return typeName;
  }

  @Override
  public boolean isCompatibleWith(final Class<?> adapterClass) {
    return true;
  }

  @Override
  public final StatisticId<V> getId() {
    if (cachedStatisticId == null) {
      cachedStatisticId =
          generateStatisticId(typeName, (DataTypeStatisticType<V>) getStatisticType(), getTag());
    }
    return cachedStatisticId;
  }

  @Override
  protected int byteLength() {
    return super.byteLength()
        + VarintUtils.unsignedShortByteLength((short) typeName.length())
        + typeName.length();
  }

  @Override
  protected void writeBytes(final ByteBuffer buffer) {
    super.writeBytes(buffer);
    VarintUtils.writeUnsignedShort((short) typeName.length(), buffer);
    buffer.put(StringUtils.stringToBinary(typeName));
  }

  @Override
  protected void readBytes(final ByteBuffer buffer) {
    super.readBytes(buffer);
    final byte[] nameBytes = new byte[VarintUtils.readUnsignedShort(buffer)];
    buffer.get(nameBytes);
    typeName = StringUtils.stringFromBinary(nameBytes);
  }

  @Override
  public String toString() {
    final StringBuffer buffer = new StringBuffer();
    buffer.append(getStatisticType().getString()).append("[type=").append(typeName).append("]");
    return buffer.toString();
  }

  public static <V extends StatisticValue<?>> StatisticId<V> generateStatisticId(
      final String typeName,
      final DataTypeStatisticType<V> statisticType,
      final String tag) {
    return new StatisticId<>(generateGroupId(typeName), statisticType, tag);
  }

  public static ByteArray generateGroupId(final String typeName) {
    return new ByteArray("A" + typeName);
  }

}
