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
import org.locationtech.jts.geom.Geometry;

public enum SpatialBinningType implements SpatialBinningHelper {
  H3(new H3BinningHelper()), S2(new S2BinningHelper()), GEOHASH(new GeohashBinningHelper());

  private SpatialBinningHelper helperDelegate;

  private SpatialBinningType(final SpatialBinningHelper helperDelegate) {
    this.helperDelegate = helperDelegate;
  }

  @Override
  public ByteArray[] getSpatialBins(final Geometry geometry, final int precision) {
    // TODO if geometry is not WGS84 we need to transform it
    return helperDelegate.getSpatialBins(geometry, precision);
  }

  @Override
  public ByteArrayConstraints getGeometryConstraints(final Geometry geom, final int precision) {
    // TODO if geometry is not WGS84 we need to transform it
    return helperDelegate.getGeometryConstraints(geom, precision);
  }


  @Override
  public Geometry getBinGeometry(final ByteArray bin, final int precision) {
    return helperDelegate.getBinGeometry(bin, precision);
  }

  @Override
  public String binToString(final byte[] binId) {
    return helperDelegate.binToString(binId);
  }

  @Override
  public int getBinByteLength(final int precision) {
    return helperDelegate.getBinByteLength(precision);
  }

  // is used by python converter
  public static SpatialBinningType fromString(final String code) {

    for (final SpatialBinningType output : SpatialBinningType.values()) {
      if (output.toString().equalsIgnoreCase(code)) {
        return output;
      }
    }

    return null;
  }
}
