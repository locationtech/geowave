/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.extract;

import org.locationtech.jts.geom.Geometry;

public abstract class EmptyDimensionExtractor<T> implements DimensionExtractor<T> {

  /**
   *
   */
  private static final long serialVersionUID = 1L;
  private static final double[] EMPTY_VAL = new double[0];
  private static final String[] EMPTY_NAME = new String[0];

  @Override
  public double[] getDimensions(final T anObject) {
    return EMPTY_VAL;
  }

  @Override
  public String[] getDimensionNames() {
    return EMPTY_NAME;
  }

  @Override
  public abstract Geometry getGeometry(T anObject);

  @Override
  public abstract String getGroupID(T anObject);
}
