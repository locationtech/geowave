/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.dimension;

public class CustomCRSBoundedSpatialDimensionY extends CustomCRSBoundedSpatialDimension {

  public CustomCRSBoundedSpatialDimensionY() {}

  public CustomCRSBoundedSpatialDimensionY(final double min, final double max) {
    super((byte) 1, min, max);
  }

}
