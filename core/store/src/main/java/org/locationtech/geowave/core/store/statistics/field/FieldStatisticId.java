/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics.field;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.statistics.StatisticId;
import org.locationtech.geowave.core.store.statistics.StatisticType;
import com.google.common.primitives.Bytes;

/**
 * An extension of Statistic ID that allows the same statistic and tag to be added to multiple
 * fields without conflicting.
 */
public class FieldStatisticId<V extends StatisticValue<?>> extends StatisticId<V> {

  private final String fieldName;

  /**
   * Create a new statistic id with the given group, statistic type, field name and tag.
   * 
   * @param groupId the group id
   * @param statisticType the statistic type
   * @param fieldName the field name
   * @param tag the tag
   */
  public FieldStatisticId(
      final ByteArray groupId,
      final StatisticType<V> statisticType,
      final String fieldName,
      final String tag) {
    super(groupId, statisticType, tag);
    this.fieldName = fieldName;
  }

  /**
   * Get the field name of the underlying statistic.
   * 
   * @return the field name
   */
  public String getFieldName() {
    return fieldName;
  }

  /**
   * Get the unique id of the identifier. The unique id is guaranteed to be unique among all
   * statistics within the same group. Multiple statistics of the same type in the same group can be
   * added by using different tags.
   * 
   * @return the unique id
   */
  @Override
  public ByteArray getUniqueId() {
    if (cachedBytes == null) {
      cachedBytes = generateUniqueId(statisticType, fieldName, tag);
    }
    return cachedBytes;
  }

  /**
   * Generate a unique id with the given statistic type, field name, and tag.
   * 
   * @param statisticType the statistic type
   * @param fieldName the field name
   * @param tag the tag
   * @return the unique id
   */
  public static ByteArray generateUniqueId(
      final StatisticType<?> statisticType,
      final String fieldName,
      final String tag) {
    if (tag == null) {
      return new ByteArray(
          Bytes.concat(
              statisticType.getBytes(),
              StatisticId.UNIQUE_ID_SEPARATOR,
              StringUtils.stringToBinary(fieldName)));
    } else {
      return new ByteArray(
          Bytes.concat(
              statisticType.getBytes(),
              StatisticId.UNIQUE_ID_SEPARATOR,
              StringUtils.stringToBinary(fieldName),
              StatisticId.UNIQUE_ID_SEPARATOR,
              StringUtils.stringToBinary(tag)));
    }
  }
}
