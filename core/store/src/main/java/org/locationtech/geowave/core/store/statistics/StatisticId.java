/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.api.StatisticValue;
import com.google.common.primitives.Bytes;

/**
 * A unique identifier for a statistic. The group id of the identifier correlates to the statistic
 * group that it belongs to, for example, all index statistics for a single index belong to the same
 * group, while all field statistics for a given type also belong to the same group. The unique id
 * of the identifier is guaranteed to be unique among all statistics. Multiple statistics of the
 * same type in the same group can be added by using different tags.
 *
 * @param <V> the statistic value type
 */
public class StatisticId<V extends StatisticValue<?>> {

  public static final byte[] UNIQUE_ID_SEPARATOR = new byte[] {'|'};

  protected final ByteArray groupId;

  protected final StatisticType<V> statisticType;

  protected final String tag;

  protected ByteArray cachedBytes = null;

  /**
   * Create a new statistic id with the given group, statistic type, and tag.
   * 
   * @param groupId the group id
   * @param statisticType the statistic type
   * @param tag the tag
   */
  public StatisticId(
      final ByteArray groupId,
      final StatisticType<V> statisticType,
      final String tag) {
    this.groupId = groupId;
    this.statisticType = statisticType;
    this.tag = tag;
  }

  /**
   * Get the statistic type of the statistic represented by this id.
   * 
   * @return the statistic type
   */
  public StatisticType<V> getStatisticType() {
    return statisticType;
  }

  /**
   * Get the tag of the statistic represented by this id.
   * 
   * @return the tag
   */
  public String getTag() {
    return tag;
  }

  /**
   * Get the group id of the identifier. The group id correlates to the statistic group that it
   * belongs to, for example, all index statistics for a single index belong to the same group,
   * while all field statistics for a given type also belong to the same group.
   * 
   * @return the group id
   */
  public ByteArray getGroupId() {
    return groupId;
  }

  /**
   * Get the unique id of the identifier. The unique id is guaranteed to be unique among all
   * statistics. Multiple statistics of the same type in the same group can be added by using
   * different tags.
   * 
   * @return the unique id
   */
  public ByteArray getUniqueId() {
    if (cachedBytes == null) {
      cachedBytes = generateUniqueId(statisticType, tag);
    }
    return cachedBytes;
  }

  /**
   * Generate a unique id with the given statistic type and tag.
   * 
   * @param statisticType the statistic type
   * @param tag the tag
   * @return the unique id
   */
  public static ByteArray generateUniqueId(final StatisticType<?> statisticType, final String tag) {
    if (tag == null) {
      return statisticType;
    } else {
      return new ByteArray(
          Bytes.concat(
              statisticType.getBytes(),
              UNIQUE_ID_SEPARATOR,
              StringUtils.stringToBinary(tag)));
    }
  }

}
