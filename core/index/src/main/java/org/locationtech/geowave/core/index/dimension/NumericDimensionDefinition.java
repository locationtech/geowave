/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index.dimension;

import org.locationtech.geowave.core.index.dimension.bin.BinRange;
import org.locationtech.geowave.core.index.numeric.NumericData;
import org.locationtech.geowave.core.index.numeric.NumericRange;
import org.locationtech.geowave.core.index.persist.Persistable;

/**
 * The Numeric Dimension Definition interface defines the attributes and methods of a class which
 * forms the Space Filling Curve dimension.
 */
public interface NumericDimensionDefinition extends Persistable {
  double getRange();

  /**
   * Used to normalize a value within the bounds of the range to a percentage of the range between 0
   * and 1
   *
   * @return normalized value
   */
  double normalize(double value);

  /**
   * Used to denormalize the numeric data set from a value between 0 and 1 scaled to fit within its
   * native bounds
   *
   * @return the denormalized value
   */
  double denormalize(double value);

  /**
   * Returns the set of normalized ranges
   *
   * @param range a numeric range of the data set
   * @return an array of BinRange[] objects
   */
  BinRange[] getNormalizedRanges(NumericData range);

  /**
   * Returns a range in the native bounds of the dimension definition, denormalized from a bin and
   * separate range
   *
   * @param range a numeric range of the data set, with a bin
   * @return a NumericRange representing the given bin and range
   */
  NumericRange getDenormalizedRange(BinRange range);

  /**
   * If this numeric dimension definition uses bins, it is given a fixed length for the bin ID
   *
   * @return the fixed length for this dimensions bin ID
   */
  int getFixedBinIdSize();

  /**
   * Returns the native bounds of the dimension definition
   *
   * @return a range representing the minimum value and the maximum value for this dimension
   *         definition
   */
  NumericRange getBounds();

  /**
   * @return the entire allowed range
   */
  NumericData getFullRange();
}
