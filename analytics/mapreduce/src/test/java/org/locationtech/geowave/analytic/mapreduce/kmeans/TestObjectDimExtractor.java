/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.mapreduce.kmeans;

import org.locationtech.geowave.analytic.extract.DimensionExtractor;
import org.locationtech.geowave.analytic.extract.EmptyDimensionExtractor;
import org.locationtech.jts.geom.Geometry;

public class TestObjectDimExtractor extends EmptyDimensionExtractor<TestObject> implements
    DimensionExtractor<TestObject> {
  /**
   *
   */
  private static final long serialVersionUID = 1L;

  @Override
  public String getGroupID(final TestObject anObject) {
    return anObject.getGroupID();
  }

  @Override
  public Geometry getGeometry(final TestObject anObject) {
    return anObject.geo;
  }
}
