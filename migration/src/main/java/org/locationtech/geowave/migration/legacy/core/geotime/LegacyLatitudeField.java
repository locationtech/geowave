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
import org.locationtech.geowave.core.geotime.index.dimension.LatitudeDefinition;
import org.locationtech.geowave.core.geotime.store.dimension.LatitudeField;
import org.locationtech.geowave.core.store.api.Index;

public class LegacyLatitudeField extends LegacySpatialField<LatitudeField> {

  public LegacyLatitudeField() {}

  public LegacyLatitudeField(
      final @Nullable Integer geometryPrecision,
      final boolean useHalfRange) {
    super(new LatitudeDefinition(useHalfRange), geometryPrecision);
  }

  @Override
  public LatitudeField getUpdatedField(final Index index) {
    return new LatitudeField(baseDefinition, geometryPrecision);
  }

}
