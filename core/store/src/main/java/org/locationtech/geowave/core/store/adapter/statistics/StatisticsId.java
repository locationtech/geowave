/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.adapter.statistics;

public class StatisticsId {
  private final StatisticsType<?, ?> type;
  private final String extendedId;

  public StatisticsId(final StatisticsType<?, ?> type, final String extendedId) {
    super();
    this.type = type;
    this.extendedId = extendedId;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = (prime * result) + ((extendedId == null) ? 0 : extendedId.hashCode());
    result = (prime * result) + ((type == null) ? 0 : type.hashCode());
    return result;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final StatisticsId other = (StatisticsId) obj;
    if (extendedId == null) {
      if (other.extendedId != null) {
        return false;
      }
    } else if (!extendedId.equals(other.extendedId)) {
      return false;
    }
    if (type == null) {
      if (other.type != null) {
        return false;
      }
    } else if (!type.equals(other.type)) {
      return false;
    }
    return true;
  }

  public StatisticsType<?, ?> getType() {
    return type;
  }

  public String getExtendedId() {
    return extendedId;
  }
}
