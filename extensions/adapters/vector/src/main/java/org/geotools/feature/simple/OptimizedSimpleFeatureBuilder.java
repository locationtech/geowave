/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.geotools.feature.simple;

import org.opengis.feature.simple.SimpleFeatureType;

/**
 * Variation of SimpleFeatureBuilder that skips object conversion, since GeoWave handles that
 * already.
 */
public class OptimizedSimpleFeatureBuilder extends SimpleFeatureBuilder {

  public OptimizedSimpleFeatureBuilder(final SimpleFeatureType featureType) {
    super(featureType);
  }

  @Override
  public void set(int index, Object value) {
    if (index >= values.length)
      throw new ArrayIndexOutOfBoundsException(
          "Can handle " + values.length + " attributes only, index is " + index);

    values[index] = value;
  }
}
