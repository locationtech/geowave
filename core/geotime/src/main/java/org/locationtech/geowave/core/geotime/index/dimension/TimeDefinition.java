/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.index.dimension;

import org.locationtech.geowave.core.geotime.index.dimension.TemporalBinningStrategy.Unit;
import org.locationtech.geowave.core.index.dimension.UnboundedDimensionDefinition;
import org.locationtech.geowave.core.index.dimension.bin.IndexBinningStrategy;
import org.locationtech.geowave.core.index.numeric.NumericData;
import org.locationtech.geowave.core.index.numeric.NumericRange;

/**
 * The Time Definition class is a convenience class used to define a dimension which is associated
 * with a time dimension.
 */
public class TimeDefinition extends UnboundedDimensionDefinition {
  public TimeDefinition() {
    super();
  }

  /**
   * Constructor used to create a new Unbounded Binning Strategy based upon a temporal binning
   * strategy of the unit parameter. The unit can be of DAY, MONTH, or YEAR.
   *
   * @param unit an enumeration of temporal units (DAY, MONTH, or YEAR)
   */
  public TimeDefinition(final Unit unit) {
    super(new TemporalBinningStrategy(unit));
  }

  /**
   * Constructor used to create a new Unbounded Binning Strategy based upon a generic binning
   * strategy.
   *
   * @param binningStrategy a object which defines the bins
   */
  public TimeDefinition(final IndexBinningStrategy binningStrategy) {
    super(binningStrategy);
  }

  @Override
  public NumericData getFullRange() {
    return new NumericRange(0, System.currentTimeMillis() + 1);
  }
}
