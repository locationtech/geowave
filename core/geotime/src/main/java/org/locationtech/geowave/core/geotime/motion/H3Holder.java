/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.motion;

import org.locationtech.geowave.core.geotime.binning.H3CellRanges;
import com.uber.h3core.H3Core;
import com.uber.h3core.LengthUnit;

/** The shared H3 instance and the average hexagon edge length per level. */
final class H3Holder {
  private H3Holder() {}

  static H3Core h3() {
    return Lazy.H3;
  }

  static double averageEdgeMeters(final int level) {
    return Lazy.EDGE_METERS[level];
  }

  private static final class Lazy {
    private static final H3Core H3 = H3CellRanges.h3();
    private static final double[] EDGE_METERS = edges();

    private static double[] edges() {
      final double[] edges = new double[16];
      for (int level = 0; level < edges.length; level++) {
        edges[level] = H3.getHexagonEdgeLengthAvg(level, LengthUnit.m);
      }
      return edges;
    }
  }
}
