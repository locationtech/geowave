/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.migration.legacy.core.geotime;

import javax.annotation.Nullable;
import org.locationtech.geowave.core.geotime.store.dimension.CustomCRSSpatialDimension;
import org.locationtech.geowave.core.geotime.store.dimension.CustomCRSSpatialField;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.api.Index;

public class LegacyCustomCRSSpatialField extends LegacySpatialField<CustomCRSSpatialField> {

  public LegacyCustomCRSSpatialField() {}

  public LegacyCustomCRSSpatialField(
      final CustomCRSSpatialDimension baseDefinition,
      final @Nullable Integer geometryPrecision) {
    super(baseDefinition, geometryPrecision);
  }


  @Override
  public CustomCRSSpatialField getUpdatedField(final Index index) {
    return new CustomCRSSpatialField(
        (CustomCRSSpatialDimension) baseDefinition,
        geometryPrecision,
        GeometryUtils.getIndexCrs(index));
  }

}
