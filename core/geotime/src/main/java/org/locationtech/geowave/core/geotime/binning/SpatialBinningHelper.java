/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.binning;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.api.BinConstraints.ByteArrayConstraints;
import org.locationtech.geowave.core.store.statistics.query.BinConstraintsImpl.ExplicitConstraints;
import org.locationtech.jts.geom.Geometry;

interface SpatialBinningHelper {
  ByteArray[] getSpatialBins(final Geometry geometry, int precision);

  default ByteArrayConstraints getGeometryConstraints(final Geometry geom, final int precision) {
    return new ExplicitConstraints(getSpatialBins(geom, precision));
  }

  Geometry getBinGeometry(final ByteArray bin, int precision);

  default String binToString(final byte[] binId) {
    return new ByteArray(binId).getHexString();
  }

  default int getBinByteLength(final int precision) {
    return precision;
  }
}
