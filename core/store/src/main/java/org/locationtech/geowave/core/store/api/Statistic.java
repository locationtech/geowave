/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.api;

import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.EntryVisibilityHandler;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.statistics.StatisticId;
import org.locationtech.geowave.core.store.statistics.StatisticType;
import org.locationtech.geowave.core.store.statistics.visibility.EmptyStatisticVisibility;

/**
 * This is the base interface for all statistics in GeoWave.
 *
 * @param <V> the statistic value type
 */
public interface Statistic<V extends StatisticValue<?>> extends Persistable {

  /**
   * Statistics that are used by internal GeoWave systems use this tag.
   */
  static final String INTERNAL_TAG = "internal";

  /**
   * Statistics that are not explicitly tagged and do not have a binning strategy will use this tag.
   */
  static final String DEFAULT_TAG = "default";

  /**
   * Get the statistic type associated with the statistic.
   *
   * @return the statistic type
   */
  StatisticType<V> getStatisticType();

  /**
   * Get the tag for the statistic.
   *
   * @return the tag
   */
  String getTag();

  /**
   * Get a human-readable description of this statistic.
   *
   * @return a description of the statistic
   */
  String getDescription();

  /**
   * Create a new value for this statistic, initialized to a base state (no entries ingested).
   *
   * @return the new value
   */
  V createEmpty();

  /**
   * @return {@code true} if the statistic is an internal statistic
   */
  default boolean isInternal() {
    return INTERNAL_TAG.equals(getTag());
  }

  /**
   * Get the visibility handler for the statistic.
   *
   * @param indexModel the index model
   * @param type the data tyep
   * @return the visiblity handler
   */
  default <T> EntryVisibilityHandler<T> getVisibilityHandler(
      final CommonIndexModel indexModel,
      final DataTypeAdapter<T> type) {
    return new EmptyStatisticVisibility<>();
  }

  /**
   * Determine if the statistic is compatible with the given class.
   *
   * @param clazz the class to check
   * @return {@code true} if the statistic is compatible
   */
  boolean isCompatibleWith(final Class<?> clazz);

  /**
   * Return the unique identifier for the statistic.
   *
   * @return the statistic id
   */
  StatisticId<V> getId();

  /**
   * Returns the binning strategy used by the statistic.
   *
   * @return the binning strategy, or {@code null} if there is none
   */
  StatisticBinningStrategy getBinningStrategy();
}
