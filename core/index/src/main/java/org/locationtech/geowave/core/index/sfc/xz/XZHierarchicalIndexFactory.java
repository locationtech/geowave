/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index.sfc.xz;

import org.locationtech.geowave.core.index.dimension.NumericDimensionDefinition;
import org.locationtech.geowave.core.index.sfc.SFCFactory.SFCType;
import org.locationtech.geowave.core.index.sfc.tiered.TieredSFCIndexFactory;
import org.locationtech.geowave.core.index.sfc.tiered.TieredSFCIndexStrategy;

/**
 * A factory for creating a Hierachical XZ Index strategy with a TieredSFCIndexStrategy substrategy
 * using various approaches for breaking down the bits of precision per tier
 */
public class XZHierarchicalIndexFactory {

  public static XZHierarchicalIndexStrategy createFullIncrementalTieredStrategy(
      final NumericDimensionDefinition[] baseDefinitions,
      final int[] maxBitsPerDimension,
      final SFCType sfcType) {
    return createFullIncrementalTieredStrategy(baseDefinitions, maxBitsPerDimension, sfcType, null);
  }

  /**
   * @param baseDefinitions an array of Numeric Dimension Definitions
   * @param maxBitsPerDimension the max cardinality for the Index Strategy
   * @param sfcType the type of space filling curve (e.g. Hilbert)
   * @param maxEstimatedDuplicatedIds the max number of duplicate SFC IDs
   * @return an Index Strategy object with a tier for every incremental cardinality between the
   *         lowest max bits of precision and 0
   */
  public static XZHierarchicalIndexStrategy createFullIncrementalTieredStrategy(
      final NumericDimensionDefinition[] baseDefinitions,
      final int[] maxBitsPerDimension,
      final SFCType sfcType,
      final Long maxEstimatedDuplicatedIds) {
    final TieredSFCIndexStrategy rasterStrategy =
        TieredSFCIndexFactory.createFullIncrementalTieredStrategy(
            baseDefinitions,
            maxBitsPerDimension,
            sfcType,
            maxEstimatedDuplicatedIds);

    return new XZHierarchicalIndexStrategy(baseDefinitions, rasterStrategy, maxBitsPerDimension);
  }
}
