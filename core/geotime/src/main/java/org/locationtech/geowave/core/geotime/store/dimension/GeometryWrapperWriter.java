/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.dimension;

import javax.annotation.Nullable;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.data.field.FieldWriter;

public class GeometryWrapperWriter implements FieldWriter<Object, GeometryWrapper> {

  private Integer geometryPrecision = null;

  public GeometryWrapperWriter(@Nullable final Integer geometryPrecision) {
    this.geometryPrecision = geometryPrecision;
  }

  public void setPrecision(@Nullable final Integer geometryPrecision) {
    this.geometryPrecision = geometryPrecision;
  }

  @Override
  public byte[] writeField(final GeometryWrapper geometry) {
    return GeometryUtils.geometryToBinary(geometry.getGeometry(), geometryPrecision);
  }

  @Override
  public byte[] getVisibility(
      final Object rowValue,
      final String fieldName,
      final GeometryWrapper geometry) {
    return geometry.getVisibility();
  }
}
