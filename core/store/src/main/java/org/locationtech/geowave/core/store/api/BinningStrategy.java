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
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;

/**
 *
 * A binning strategy is used to bin data in an aggregation query or in a statistic.
 *
 */
public interface BinningStrategy extends Persistable {

  /**
   * Get the bins used by the given entry. Each bin will have a separate value.
   *
   * @param type the data type
   * @param entry the entry
   * @param rows the rows created for the entry
   * @return a set of bins used by the given entry
   *
   * @param <T> The type that will be used to bin on and the weight for a particular bin (if
   *        multiple bins sometimes they can be weighted, a supplier is used to defer evaluation).
   *        This could be anything, but you may see things like {@code SimpleFeature}, or
   *        {@code CommonIndexedPersistenceEncoding} used mostly.
   */
  <T> ByteArray[] getBins(DataTypeAdapter<T> type, T entry, GeoWaveRow... rows);

  /**
   * This computes a weight for the bin of a given entry. This can be useful for binning strategies
   * that produce multiple bins for a single entry to be able to weight/scale statistics by the
   * percent of coverage that the bounds of the bin covers the overall entry. For example, a time
   * range may cover multiple bins and the weight would likely be the percent of coverage that each
   * bin overlaps the ingested time range (and therefore something like a count statistic or any
   * summing statistic could scale the contribution by the weight).
   * 
   * @param <T> The type that will be used to bin on and the weight for a particular bin (if
   *        multiple bins sometimes they can be weighted, a supplier is used to defer evaluation).
   *        This could be anything, but you may see things like {@code SimpleFeature}, or
   *        {@code CommonIndexedPersistenceEncoding} used mostly.
   * @param bin the bin used for the given entry for which to get a weighting factor
   * @param type the data type
   * @param entry the entry
   * @param rows the rows created for the entry
   * @return the weighting factor for this bin
   */
  default <T> double getWeight(
      final ByteArray bin,
      final DataTypeAdapter<T> type,
      final T entry,
      final GeoWaveRow... rows) {
    return 1;
  }
}
