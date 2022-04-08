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
import org.apache.commons.lang.ArrayUtils;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.geowave.core.geotime.store.dimension.SpatialField.SpatialIndexFieldOptions;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.adapter.FieldDescriptor;
import org.locationtech.geowave.core.store.api.IndexFieldMapper;
import org.locationtech.jts.geom.Geometry;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maps an adapter spatial field or fields to a geometry index field, transforming the geometry to
 * the appropriate CRS if necessary.
 *
 * @param <N> The class of the adapter spatial field
 */
public abstract class SpatialFieldMapper<N> extends IndexFieldMapper<N, Geometry> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SpatialFieldMapper.class);

  private CoordinateReferenceSystem adapterCRS = null;
  private CoordinateReferenceSystem indexCRS = null;
  private MathTransform transform = null;

  @Override
  public Geometry toIndex(List<N> nativeFieldValues) {
    final Geometry nativeGeometry = getNativeGeometry(nativeFieldValues);
    try {
      if (transform != null) {
        return JTS.transform(nativeGeometry, transform);
      }
    } catch (MismatchedDimensionException | TransformException e) {
      LOGGER.warn(
          "Unable to perform transform to specified CRS of the index, the feature geometry will remain in its original CRS",
          e);
    }
    return nativeGeometry;
  }

  /**
   * Builds a `Geometry` from the native adapter field values.
   * 
   * @param nativeFieldValues the adapter field values
   * @return a `Geometry` that represents the adapter field values
   */
  protected abstract Geometry getNativeGeometry(List<N> nativeFieldValues);

  @Override
  public Class<Geometry> indexFieldType() {
    return Geometry.class;
  }

  @Override
  protected void initFromOptions(
      final List<FieldDescriptor<N>> inputFieldDescriptors,
      final IndexFieldOptions options) {
    indexCRS = GeometryUtils.getDefaultCRS();
    adapterCRS = GeometryUtils.getDefaultCRS();
    if (options instanceof SpatialIndexFieldOptions) {
      indexCRS = ((SpatialIndexFieldOptions) options).crs();
    }
    for (FieldDescriptor<N> field : inputFieldDescriptors) {
      if (field instanceof SpatialFieldDescriptor
          && ((SpatialFieldDescriptor<?>) field).crs() != null) {
        adapterCRS = ((SpatialFieldDescriptor<?>) field).crs();
        break;
      }
    }
    if (!indexCRS.equals(adapterCRS)) {
      try {
        transform = CRS.findMathTransform(adapterCRS, indexCRS, true);
      } catch (FactoryException e) {
        LOGGER.warn("Unable to create coordinate reference system transform", e);
      }
    }
  }

  @Override
  public void transformFieldDescriptors(final FieldDescriptor<?>[] inputFieldDescriptors) {
    if (!indexCRS.equals(adapterCRS)) {
      final String[] mappedFields = getAdapterFields();
      for (int i = 0; i < inputFieldDescriptors.length; i++) {
        final FieldDescriptor<?> field = inputFieldDescriptors[i];
        if (ArrayUtils.contains(mappedFields, field.fieldName())) {
          inputFieldDescriptors[i] =
              new SpatialFieldDescriptorBuilder<>(field.bindingClass()).fieldName(
                  field.fieldName()).crs(indexCRS).build();
        }
      }
    }
  }

  private byte[] indexCRSBytes = null;
  private byte[] adapterCRSBytes = null;

  @Override
  protected int byteLength() {
    indexCRSBytes = StringUtils.stringToBinary(indexCRS.toWKT());
    adapterCRSBytes = StringUtils.stringToBinary(adapterCRS.toWKT());
    return super.byteLength()
        + VarintUtils.unsignedShortByteLength((short) indexCRSBytes.length)
        + VarintUtils.unsignedShortByteLength((short) adapterCRSBytes.length)
        + indexCRSBytes.length
        + adapterCRSBytes.length;
  }

  @Override
  protected void writeBytes(final ByteBuffer buffer) {
    VarintUtils.writeUnsignedShort((short) indexCRSBytes.length, buffer);
    buffer.put(indexCRSBytes);
    VarintUtils.writeUnsignedShort((short) adapterCRSBytes.length, buffer);
    buffer.put(adapterCRSBytes);
    super.writeBytes(buffer);
  }

  @Override
  protected void readBytes(final ByteBuffer buffer) {
    indexCRSBytes = new byte[VarintUtils.readUnsignedShort(buffer)];
    buffer.get(indexCRSBytes);
    adapterCRSBytes = new byte[VarintUtils.readUnsignedShort(buffer)];
    buffer.get(adapterCRSBytes);
    try {
      indexCRS = CRS.parseWKT(StringUtils.stringFromBinary(indexCRSBytes));
      adapterCRS = CRS.parseWKT(StringUtils.stringFromBinary(adapterCRSBytes));
      if (!indexCRS.equals(adapterCRS)) {
        transform = CRS.findMathTransform(adapterCRS, indexCRS, true);
      } else {
        transform = null;
      }
    } catch (FactoryException e) {
      throw new RuntimeException(
          "Unable to decode coordinate reference system for spatial index field mapper.");
    }
    super.readBytes(buffer);
  }

}
