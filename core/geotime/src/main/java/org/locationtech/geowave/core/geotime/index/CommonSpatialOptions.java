/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.index;

import javax.annotation.Nullable;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.spi.DimensionalityTypeOptions;
import com.beust.jcommander.Parameter;

public abstract class CommonSpatialOptions implements DimensionalityTypeOptions {
  @Parameter(
      names = {"-c", "--crs"},
      required = false,
      description = "The native Coordinate Reference System used within the index.  All spatial data will be projected into this CRS for appropriate indexing as needed.")
  protected String crs = GeometryUtils.DEFAULT_CRS_STR;

  @Parameter(
      names = {"-gp", "--geometryPrecision"},
      required = false,
      description = "The maximum precision of the geometry when encoding.  Lower precision will save more disk space when encoding. (Between -8 and 7)")
  protected int geometryPrecision = GeometryUtils.MAX_GEOMETRY_PRECISION;

  @Parameter(
      names = {"-fp", "--fullGeometryPrecision"},
      required = false,
      description = "If specified, geometry will be encoded losslessly.  Uses more disk space.")
  protected boolean fullGeometryPrecision = false;

  public void setCrs(final String crs) {
    this.crs = crs;
  }

  public String getCrs() {
    return crs;
  }

  public void setGeometryPrecision(final @Nullable Integer geometryPrecision) {
    if (geometryPrecision == null) {
      fullGeometryPrecision = true;
    } else {
      fullGeometryPrecision = false;
      this.geometryPrecision = geometryPrecision;
    }
  }

  public Integer getGeometryPrecision() {
    if (fullGeometryPrecision) {
      return null;
    } else {
      return geometryPrecision;
    }
  }
}
