/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics;

import java.util.Arrays;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.api.StatisticValue;

/**
 * This class serves as the base implementation for a statistic type, based on {@link ByteArray}.
 *
 * @param <V> the statistic value type
 */
public abstract class StatisticType<V extends StatisticValue<?>> extends ByteArray {
  private static final long serialVersionUID = 1L;

  public StatisticType(final String id) {
    super(id);
  }

  @Override
  public boolean equals(final Object obj) {
    // If all we know is the name of the stat type,
    // but not the class we need to override equals on
    // the base statistics type so that the
    // class does not need to match
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof StatisticType)) {
      return false;
    }
    final StatisticType<?> other = (StatisticType<?>) obj;
    return Arrays.equals(bytes, other.getBytes());
  }

  @Override
  public String toString() {
    return getString();
  }
}
