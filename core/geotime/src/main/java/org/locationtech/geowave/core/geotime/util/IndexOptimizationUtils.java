/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.util;

import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.InternalGeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.dimension.CustomCRSSpatialDimension;
import org.locationtech.geowave.core.geotime.store.dimension.CustomCRSSpatialField;
import org.locationtech.geowave.core.geotime.store.dimension.LatitudeField;
import org.locationtech.geowave.core.geotime.store.dimension.LongitudeField;
import org.locationtech.geowave.core.geotime.store.dimension.TimeField;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;

public class IndexOptimizationUtils {

  public static InternalGeotoolsFeatureDataAdapter unwrapGeotoolsFeatureDataAdapter(
      final DataTypeAdapter<?> adapter) {
    if (adapter instanceof InternalGeotoolsFeatureDataAdapter) {
      return (InternalGeotoolsFeatureDataAdapter) adapter;
    }
    return null;
  }

  public static boolean hasAtLeastSpatial(final Index index) {
    if ((index == null)
        || (index.getIndexModel() == null)
        || (index.getIndexModel().getDimensions() == null)) {
      return false;
    }
    boolean hasLatitude = false;
    boolean hasLongitude = false;
    for (final NumericDimensionField dimension : index.getIndexModel().getDimensions()) {
      if (dimension instanceof LatitudeField) {
        hasLatitude = true;
      }
      if (dimension instanceof LongitudeField) {
        hasLongitude = true;
      }
      if (dimension instanceof CustomCRSSpatialField) {
        if (((CustomCRSSpatialDimension) dimension.getBaseDefinition()).getAxis() == 0) {
          hasLongitude = true;
        } else {
          hasLatitude = true;
        }
      }
    }
    return hasLatitude && hasLongitude;
  }

  public static boolean hasTime(final Index index, final GeotoolsFeatureDataAdapter adapter) {
    return hasTime(index) && adapter.hasTemporalConstraints();
  }

  public static boolean hasTime(final Index index) {
    if ((index == null)
        || (index.getIndexModel() == null)
        || (index.getIndexModel().getDimensions() == null)) {
      return false;
    }
    for (final NumericDimensionField dimension : index.getIndexModel().getDimensions()) {
      if (dimension instanceof TimeField) {
        return true;
      }
    }
    return false;
  }
}
