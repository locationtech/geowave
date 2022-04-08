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
import org.locationtech.geowave.core.store.statistics.field.FieldStatisticId;
import org.locationtech.geowave.core.store.statistics.field.FieldStatisticType;
import com.beust.jcommander.Parameter;


/**
 * Base class for field statistics. These statistics are generally updated by using a specific field
 * on a data type.
 */
public abstract class FieldStatistic<V extends StatisticValue<?>> extends Statistic<V> {

  @Parameter(
      names = "--typeName",
      required = true,
      description = "The data type that contains the field for the statistic.")
  private String typeName = null;

  @Parameter(
      names = "--fieldName",
      required = true,
      description = "The field name to use for statistics.")
  private String fieldName = null;

  public FieldStatistic(final FieldStatisticType<V> statisticsType) {
    this(statisticsType, null, null);
  }

  public FieldStatistic(
      final FieldStatisticType<V> statisticsType,
      final String typeName,
      final String fieldName) {
    super(statisticsType);
    this.typeName = typeName;
    this.fieldName = fieldName;
  }

  public void setTypeName(final String name) {
    this.typeName = name;
  }

  public final String getTypeName() {
    return typeName;
  }

  public void setFieldName(final String fieldName) {
    this.fieldName = fieldName;
  }

  public String getFieldName() {
    return this.fieldName;
  }

  @Override
  public abstract boolean isCompatibleWith(Class<?> fieldClass);

  @Override
  public final StatisticId<V> getId() {
    if (cachedStatisticId == null) {
      cachedStatisticId =
          generateStatisticId(
              typeName,
              (FieldStatisticType<V>) getStatisticType(),
              fieldName,
              getTag());
    }
    return cachedStatisticId;
  }

  @Override
  protected int byteLength() {
    return super.byteLength()
        + VarintUtils.unsignedShortByteLength((short) typeName.length())
        + VarintUtils.unsignedShortByteLength((short) fieldName.length())
        + typeName.length()
        + fieldName.length();
  }

  @Override
  protected void writeBytes(final ByteBuffer buffer) {
    super.writeBytes(buffer);
    VarintUtils.writeUnsignedShort((short) typeName.length(), buffer);
    buffer.put(StringUtils.stringToBinary(typeName));
    VarintUtils.writeUnsignedShort((short) fieldName.length(), buffer);
    buffer.put(StringUtils.stringToBinary(fieldName));
  }

  @Override
  protected void readBytes(final ByteBuffer buffer) {
    super.readBytes(buffer);
    final byte[] typeBytes = new byte[VarintUtils.readUnsignedShort(buffer)];
    buffer.get(typeBytes);
    final byte[] nameBytes = new byte[VarintUtils.readUnsignedShort(buffer)];
    buffer.get(nameBytes);
    typeName = StringUtils.stringFromBinary(typeBytes);
    fieldName = StringUtils.stringFromBinary(nameBytes);
  }

  @Override
  public String toString() {
    final StringBuffer buffer = new StringBuffer();
    buffer.append(getStatisticType().getString()).append("[type=").append(typeName).append(
        ", field=").append(fieldName).append("]");
    return buffer.toString();
  }


  public static <V extends StatisticValue<?>> StatisticId<V> generateStatisticId(
      final String typeName,
      final FieldStatisticType<V> statisticType,
      final String fieldName,
      final String tag) {
    return new FieldStatisticId<>(generateGroupId(typeName), statisticType, fieldName, tag);
  }

  public static ByteArray generateGroupId(final String typeName) {
    return new ByteArray("F" + typeName);
  }

}
