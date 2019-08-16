package org.locationtech.geowave.core.geotime.store.dimension;

public class CustomCRSBoundedSpatialDimensionY extends CustomCRSBoundedSpatialDimension {

  public CustomCRSBoundedSpatialDimensionY() {}

  public CustomCRSBoundedSpatialDimensionY(final double min, final double max) {
    super((byte) 1, min, max);
  }

}
