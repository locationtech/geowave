/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.adapter;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import org.locationtech.geowave.core.geotime.store.dimension.SpatialField;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.store.adapter.FieldDescriptor;
import org.locationtech.geowave.core.store.api.RowBuilder;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * Abstract field mapper for mapping latitude and longitude adapter fields to a singular `Geometry`
 * index field.
 *
 * @param <N> the adapter field type
 */
public abstract class LatLonFieldMapper<N> extends SpatialFieldMapper<N> {
  private static Set<String> suggestedLongitudeFieldNames =
      Sets.newHashSet("longitude", "lon", "x");
  private static Set<String> suggestedLatitudeFieldNames = Sets.newHashSet("latitude", "lat", "y");
  protected boolean xAxisFirst = true;

  @Override
  public void initFromOptions(
      final List<FieldDescriptor<N>> inputFieldDescriptors,
      final IndexFieldOptions options) {
    if (inputFieldDescriptors.size() != 2) {
      throw new RuntimeException("Latitude/Longitude index field mapper expects exactly 2 fields.");
    }
    if (inputFieldDescriptors.get(0).indexHints().contains(SpatialField.LONGITUDE_DIMENSION_HINT)
        && inputFieldDescriptors.get(1).indexHints().contains(
            SpatialField.LONGITUDE_DIMENSION_HINT)) {
      throw new RuntimeException("Two longitude dimension hints were given.");
    } else if (inputFieldDescriptors.get(0).indexHints().contains(
        SpatialField.LATITUDE_DIMENSION_HINT)
        && inputFieldDescriptors.get(1).indexHints().contains(
            SpatialField.LATITUDE_DIMENSION_HINT)) {
      throw new RuntimeException("Two latitude dimension hints were given.");
    }
    xAxisFirst =
        inputFieldDescriptors.get(0).indexHints().contains(SpatialField.LONGITUDE_DIMENSION_HINT)
            || inputFieldDescriptors.get(1).indexHints().contains(
                SpatialField.LATITUDE_DIMENSION_HINT)
            || suggestedLongitudeFieldNames.contains(
                inputFieldDescriptors.get(0).fieldName().toLowerCase());
    super.initFromOptions(inputFieldDescriptors, options);
  }

  @Override
  public String[] getIndexOrderedAdapterFields() {
    if (!xAxisFirst) {
      return new String[] {adapterFields[1], adapterFields[0]};
    }
    return adapterFields;
  }


  @Override
  public void toAdapter(final Geometry indexFieldValue, final RowBuilder<?> rowBuilder) {
    final Point centroid = indexFieldValue.getCentroid();
    if (xAxisFirst) {
      setField(adapterFields[0], centroid.getX(), rowBuilder);
      setField(adapterFields[1], centroid.getY(), rowBuilder);
    }
    setField(adapterFields[0], centroid.getY(), rowBuilder);
    setField(adapterFields[1], centroid.getX(), rowBuilder);
  }

  protected abstract void setField(
      final String fieldName,
      final Double Value,
      final RowBuilder<?> rowBuilder);

  @Override
  protected Geometry getNativeGeometry(List<N> nativeFieldValues) {
    final Coordinate coordinate =
        xAxisFirst ? toCoordinate(nativeFieldValues.get(0), nativeFieldValues.get(1))
            : toCoordinate(nativeFieldValues.get(1), nativeFieldValues.get(0));
    return GeometryUtils.GEOMETRY_FACTORY.createPoint(coordinate);
  }

  protected abstract Coordinate toCoordinate(final N xValue, final N yValue);

  @Override
  public short adapterFieldCount() {
    return 2;
  }

  @Override
  public Set<String> getLowerCaseSuggestedFieldNames() {
    return Sets.newHashSet(
        Iterables.concat(suggestedLongitudeFieldNames, suggestedLatitudeFieldNames));
  }

  @Override
  protected int byteLength() {
    return super.byteLength() + 1;
  }

  protected void writeBytes(final ByteBuffer buffer) {
    super.writeBytes(buffer);
    buffer.put((byte) (xAxisFirst ? 1 : 0));
  }

  protected void readBytes(final ByteBuffer buffer) {
    super.readBytes(buffer);
    xAxisFirst = buffer.get() != 0;
  }

  /**
   * Maps `Double` latitude and longitude adapter fields to a `Geometry` index field.
   */
  public static class DoubleLatLonFieldMapper extends LatLonFieldMapper<Double> {

    @Override
    public Class<Double> adapterFieldType() {
      return Double.class;
    }

    @Override
    protected void setField(
        final String fieldName,
        final Double value,
        final RowBuilder<?> rowBuilder) {
      rowBuilder.setField(fieldName, value);
    }

    @Override
    protected Coordinate toCoordinate(Double xValue, Double yValue) {
      return new Coordinate(xValue, yValue);
    }

  }

  /**
   * Maps `Float` latitude and longitude adapter fields to a `Geometry` index field.
   */
  public static class FloatLatLonFieldMapper extends LatLonFieldMapper<Float> {

    @Override
    public Class<Float> adapterFieldType() {
      return Float.class;
    }

    @Override
    protected void setField(
        final String fieldName,
        final Double value,
        final RowBuilder<?> rowBuilder) {
      rowBuilder.setField(fieldName, value.floatValue());
    }

    @Override
    protected Coordinate toCoordinate(Float xValue, Float yValue) {
      return new Coordinate(xValue, yValue);
    }

  }

}
