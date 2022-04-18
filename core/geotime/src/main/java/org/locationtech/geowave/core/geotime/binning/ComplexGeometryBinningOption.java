/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.binning;

public enum ComplexGeometryBinningOption {
  USE_CENTROID_ONLY, USE_FULL_GEOMETRY, USE_FULL_GEOMETRY_SCALE_BY_OVERLAP;

  // is used by python converter
  public static ComplexGeometryBinningOption fromString(final String code) {

    for (final ComplexGeometryBinningOption output : ComplexGeometryBinningOption.values()) {
      if (output.toString().equalsIgnoreCase(code)) {
        return output;
      }
    }

    return null;
  }
}
