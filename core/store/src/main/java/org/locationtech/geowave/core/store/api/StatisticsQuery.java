/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p>See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.api;

import org.locationtech.geowave.core.store.adapter.statistics.StatisticsId;
import org.locationtech.geowave.core.store.adapter.statistics.StatisticsType;

/**
 * A StatisticsQuery represent the method for querying and aggregating statistics. Use
 * StatisticsQueryBuilder or its extensions to construct this object.
 *
 * @param <R> the result type
 */
public class StatisticsQuery<R> {
  private final String typeName;
  private final StatisticsType<R, ?> statsType;
  private final String extendedId;
  private final String[] authorizations;

  /**
   * Use StatisticsQueryBuilder or its extensions instead to construct this object.
   *
   * @param typeName the data type name
   * @param statsType the type of the statistic
   * @param extendedId an extended ID to ensure uniqueness
   * @param authorizations the authorizations
   */
  public StatisticsQuery(
      final String typeName,
      final StatisticsType<R, ?> statsType,
      final String extendedId,
      final String[] authorizations) {
    super();
    this.typeName = typeName;
    this.statsType = statsType;
    this.extendedId = extendedId;
    this.authorizations = authorizations;
  }

  public String getTypeName() {
    return typeName;
  }

  public StatisticsType<R, ?> getStatsType() {
    return statsType;
  }

  public String getExtendedId() {
    return extendedId;
  }

  public String[] getAuthorizations() {
    return authorizations;
  }

  public StatisticsId getId() {
    return new StatisticsId(statsType, extendedId);
  }
}
