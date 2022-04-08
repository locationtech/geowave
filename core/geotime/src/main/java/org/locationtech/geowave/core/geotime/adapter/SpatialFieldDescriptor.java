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
import java.util.Set;
import org.geotools.referencing.CRS;
import org.locationtech.geowave.core.index.IndexDimensionHint;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.adapter.BaseFieldDescriptor;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

/**
 * An adapter field descriptor that also contains a `CoordinateReferenceSystem`. This is used for
 * determining if the adapter field should be transformed to the CRS of the index when ingesting.
 *
 * @param <T> the adapter field type
 */
public class SpatialFieldDescriptor<T> extends BaseFieldDescriptor<T> {

  private CoordinateReferenceSystem crs;

  public SpatialFieldDescriptor() {}

  public SpatialFieldDescriptor(
      final Class<T> bindingClass,
      final String fieldName,
      final Set<IndexDimensionHint> indexHints,
      final CoordinateReferenceSystem crs) {
    super(bindingClass, fieldName, indexHints);
    this.crs = crs;
  }

  public CoordinateReferenceSystem crs() {
    return this.crs;
  }

  @Override
  public byte[] toBinary() {
    final byte[] parentBytes = super.toBinary();
    final byte[] crsBytes = StringUtils.stringToBinary(crs.toWKT());
    final ByteBuffer buffer =
        ByteBuffer.allocate(
            VarintUtils.unsignedIntByteLength(parentBytes.length)
                + VarintUtils.unsignedIntByteLength(crsBytes.length)
                + parentBytes.length
                + crsBytes.length);
    VarintUtils.writeUnsignedInt(parentBytes.length, buffer);
    buffer.put(parentBytes);
    VarintUtils.writeUnsignedInt(crsBytes.length, buffer);
    buffer.put(crsBytes);
    return buffer.array();
  }

  @Override
  public void fromBinary(byte[] bytes) {
    final ByteBuffer buffer = ByteBuffer.wrap(bytes);
    final byte[] parentBytes = new byte[VarintUtils.readUnsignedInt(buffer)];
    buffer.get(parentBytes);
    super.fromBinary(parentBytes);
    final byte[] crsBytes = new byte[VarintUtils.readUnsignedInt(buffer)];
    buffer.get(crsBytes);
    try {
      crs = CRS.parseWKT(StringUtils.stringFromBinary(crsBytes));
    } catch (FactoryException e) {
      throw new RuntimeException(
          "Unable to decode coordinate reference system for spatial field descriptor.");
    }
  }


}
