/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.dimension;

import javax.annotation.Nullable;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.data.field.FieldReader;

public class GeometryWrapperReader implements FieldReader<GeometryWrapper> {

  private Integer geometryPrecision = null;

  public GeometryWrapperReader(@Nullable Integer geometryPrecision) {
    this.geometryPrecision = geometryPrecision;
  }

  public void setPrecision(@Nullable Integer geometryPrecision) {
    this.geometryPrecision = geometryPrecision;
  }

  @Override
  public GeometryWrapper readField(final byte[] fieldData) {
    return new GeometryWrapper(GeometryUtils.geometryFromBinary(fieldData, geometryPrecision));
  }
}
