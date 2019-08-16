package org.locationtech.geowave.core.geotime.store.dimension;

public class CustomCRSBoundedSpatialDimensionX extends CustomCRSBoundedSpatialDimension {

  public CustomCRSBoundedSpatialDimensionX() {}

  public CustomCRSBoundedSpatialDimensionX(final double min, final double max) {
    super((byte) 0, min, max);
  }

}
