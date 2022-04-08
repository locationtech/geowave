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
import org.locationtech.geowave.core.store.statistics.index.IndexStatisticType;
import com.beust.jcommander.Parameter;

/**
 * Base class for index statistics. These statistics are generally updated without using specific
 * details of the entry or the data type.
 */
public abstract class IndexStatistic<V extends StatisticValue<?>> extends Statistic<V> {

  @Parameter(names = "--indexName", required = true, description = "The index for the statistic.")
  private String indexName = null;

  public IndexStatistic(final IndexStatisticType<V> statisticsType) {
    this(statisticsType, null);
  }

  public IndexStatistic(final IndexStatisticType<V> statisticsType, final String indexName) {
    super(statisticsType);
    this.indexName = indexName;
  }

  public void setIndexName(final String name) {
    this.indexName = name;
  }

  public String getIndexName() {
    return indexName;
  }

  @Override
  public boolean isCompatibleWith(final Class<?> indexClass) {
    return true;
  }

  @Override
  public final StatisticId<V> getId() {
    if (cachedStatisticId == null) {
      cachedStatisticId =
          generateStatisticId(indexName, (IndexStatisticType<V>) getStatisticType(), getTag());
    }
    return cachedStatisticId;
  }

  @Override
  protected int byteLength() {
    return super.byteLength()
        + VarintUtils.unsignedShortByteLength((short) indexName.length())
        + indexName.length();
  }

  @Override
  protected void writeBytes(final ByteBuffer buffer) {
    super.writeBytes(buffer);
    VarintUtils.writeUnsignedShort((short) indexName.length(), buffer);
    buffer.put(StringUtils.stringToBinary(indexName));
  }

  @Override
  protected void readBytes(final ByteBuffer buffer) {
    super.readBytes(buffer);
    final byte[] nameBytes = new byte[VarintUtils.readUnsignedShort(buffer)];
    buffer.get(nameBytes);
    indexName = StringUtils.stringFromBinary(nameBytes);
  }


  @Override
  public String toString() {
    final StringBuffer buffer = new StringBuffer();
    buffer.append(getStatisticType().getString()).append("[index=").append(indexName).append("]");
    return buffer.toString();
  }


  public static <V extends StatisticValue<?>> StatisticId<V> generateStatisticId(
      final String indexName,
      final IndexStatisticType<V> statisticType,
      final String tag) {
    return new StatisticId<>(generateGroupId(indexName), statisticType, tag);
  }

  public static ByteArray generateGroupId(final String indexName) {
    return new ByteArray("I" + indexName);
  }

}
