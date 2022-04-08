/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.api;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.store.statistics.StatisticId;
import com.google.common.primitives.Bytes;

/**
 * Base class for values of a statistic. This class is responsible for the updates, serialization,
 * and merging of statistic values.
 *
 * @param <R> the return type of the statistic value
 */
public abstract class StatisticValue<R> implements Mergeable {
  public static final ByteArray NO_BIN = new ByteArray();
  protected final Statistic<?> statistic;

  protected ByteArray bin = NO_BIN;

  /**
   * Construct a new value with the given parent statistic.
   * 
   * @param statistic the parent statistic
   */
  public StatisticValue(final Statistic<?> statistic) {
    this.statistic = statistic;
  }

  /**
   * Get the parent statistic. Note, this may be null in cases of server-side statistic merging.
   * 
   * @return the parent statistic
   */
  public Statistic<?> getStatistic() {
    return statistic;
  }

  /**
   * Sets the bin for this value. Only used if the underlying statistic uses a binning strategy.
   * 
   * @param bin the bin for this value
   */
  public void setBin(final ByteArray bin) {
    this.bin = bin;
  }

  /**
   * Gets the bin for this value. If the underlying statistic does not use a binning strategy, an
   * empty byte array will be returned.
   * 
   * @return the bin for this value
   */
  public ByteArray getBin() {
    return bin;
  }

  /**
   * Merge another statistic value into this one.
   * 
   * IMPORTANT: This function cannot guarantee that the Statistic will be available. Any variables
   * needed from the statistic for merging must be serialized with the value.
   */
  @Override
  public abstract void merge(Mergeable merge);

  /**
   * Get the raw value of the statistic value.
   * 
   * @return the raw value
   */
  public abstract R getValue();

  @Override
  public String toString() {
    return getValue().toString();
  }


  /**
   * Get a unique identifier for a value given a statistic id and bin.
   * 
   * @param statisticId the statistic id
   * @param bin the bin
   * @return a unique identifier for the value
   */
  public static byte[] getValueId(StatisticId<?> statisticId, ByteArray bin) {
    return getValueId(statisticId, bin == null ? null : bin.getBytes());
  }

  /**
   * Get a unique identifier for a value given a statistic id and bin.
   * 
   * @param statisticId the statistic id
   * @param bin the bin
   * @return a unique identifier for the value
   */
  public static byte[] getValueId(StatisticId<?> statisticId, byte[] bin) {
    if (bin != null) {
      return Bytes.concat(
          statisticId.getUniqueId().getBytes(),
          StatisticId.UNIQUE_ID_SEPARATOR,
          bin);
    }
    return statisticId.getUniqueId().getBytes();
  }
}
