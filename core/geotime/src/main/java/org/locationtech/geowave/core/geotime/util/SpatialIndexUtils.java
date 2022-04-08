/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.util;

import org.locationtech.geowave.core.geotime.index.dimension.LatitudeDefinition;
import org.locationtech.geowave.core.geotime.index.dimension.LongitudeDefinition;
import org.locationtech.geowave.core.geotime.store.dimension.CustomCRSBoundedSpatialDimension;
import org.locationtech.geowave.core.geotime.store.dimension.CustomCRSBoundedSpatialDimensionX;
import org.locationtech.geowave.core.geotime.store.dimension.CustomCRSBoundedSpatialDimensionY;
import org.locationtech.geowave.core.geotime.store.dimension.CustomCRSUnboundedSpatialDimensionX;
import org.locationtech.geowave.core.geotime.store.dimension.CustomCRSUnboundedSpatialDimensionY;
import org.locationtech.geowave.core.index.dimension.NumericDimensionDefinition;
import org.locationtech.geowave.core.store.api.Index;

/**
 * Provides helper functions for spatial indices.
 */
public class SpatialIndexUtils {

  /**
   * Determine if the given dimension represents longitude.
   * 
   * @param dimension the dimension to check
   * @return {@code true} if the dimension represents longitude.
   */
  public static boolean isLongitudeDimension(final NumericDimensionDefinition dimension) {
    return (dimension instanceof LongitudeDefinition)
        || (dimension instanceof CustomCRSUnboundedSpatialDimensionX)
        || (dimension instanceof CustomCRSBoundedSpatialDimensionX)
        || (dimension instanceof CustomCRSBoundedSpatialDimension
            && ((CustomCRSBoundedSpatialDimension) dimension).getAxis() == 0x0);
  }

  /**
   * Determine if the given dimension represents latitude.
   * 
   * @param dimension the dimension to check
   * @return {@code true} if the dimension represents latitude.
   */
  public static boolean isLatitudeDimension(final NumericDimensionDefinition dimension) {
    return (dimension instanceof LatitudeDefinition)
        || (dimension instanceof CustomCRSUnboundedSpatialDimensionY)
        || (dimension instanceof CustomCRSBoundedSpatialDimensionY)
        || (dimension instanceof CustomCRSBoundedSpatialDimension
            && ((CustomCRSBoundedSpatialDimension) dimension).getAxis() == 0x1);
  }

  /**
   * Determine if the given index has a latitude and longitude dimension.
   * 
   * @param index the index to check
   * @return {@code true} if the index has spatial dimensions.
   */
  public static boolean hasSpatialDimensions(final Index index) {
    boolean hasLat = false;
    boolean hasLon = false;
    if (index.getIndexStrategy() != null) {
      NumericDimensionDefinition[] indexDimensions =
          index.getIndexStrategy().getOrderedDimensionDefinitions();
      if (indexDimensions != null && indexDimensions.length >= 2) {
        for (int i = 0; i < indexDimensions.length; i++) {
          hasLat = hasLat | isLatitudeDimension(indexDimensions[i]);
          hasLon = hasLon | isLongitudeDimension(indexDimensions[i]);
        }
      }
    }
    return hasLat && hasLon;
  }

}
