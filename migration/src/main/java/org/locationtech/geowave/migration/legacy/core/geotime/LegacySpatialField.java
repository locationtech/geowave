/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.migration.legacy.core.geotime;

import java.nio.ByteBuffer;
import java.util.Set;
import javax.annotation.Nullable;
import org.locationtech.geowave.core.geotime.store.dimension.SpatialField;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.IndexDimensionHint;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.dimension.NumericDimensionDefinition;
import org.locationtech.geowave.core.index.dimension.bin.BinRange;
import org.locationtech.geowave.core.index.numeric.NumericData;
import org.locationtech.geowave.core.index.numeric.NumericRange;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;
import org.locationtech.jts.geom.Geometry;

public abstract class LegacySpatialField<T extends SpatialField> implements
    Persistable,
    NumericDimensionField<Geometry> {

  protected String fieldName;
  protected NumericDimensionDefinition baseDefinition;
  protected Integer geometryPrecision;
  protected T updatedField = null;

  public LegacySpatialField() {}

  public LegacySpatialField(
      final NumericDimensionDefinition baseDefinition,
      final @Nullable Integer geometryPrecision) {
    this.baseDefinition = baseDefinition;
    this.geometryPrecision = geometryPrecision;
    this.fieldName = SpatialField.DEFAULT_GEOMETRY_FIELD_NAME;
  }

  public abstract T getUpdatedField(final Index index);

  @Override
  public byte[] toBinary() {
    final byte[] dimensionBinary = PersistenceUtils.toBinary(baseDefinition);
    final byte[] fieldNameBytes = StringUtils.stringToBinary(fieldName);
    final ByteBuffer buf =
        ByteBuffer.allocate(
            dimensionBinary.length
                + fieldNameBytes.length
                + VarintUtils.unsignedIntByteLength(fieldNameBytes.length)
                + 1);
    VarintUtils.writeUnsignedInt(fieldNameBytes.length, buf);
    buf.put(fieldNameBytes);
    buf.put(dimensionBinary);
    if (geometryPrecision == null) {
      buf.put(Byte.MAX_VALUE);
    } else {
      buf.put((byte) geometryPrecision.intValue());
    }
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    final int fieldNameLength = VarintUtils.readUnsignedInt(buf);
    final byte[] fieldNameBytes = ByteArrayUtils.safeRead(buf, fieldNameLength);
    fieldName = StringUtils.stringFromBinary(fieldNameBytes);
    final byte[] dimensionBinary = new byte[buf.remaining() - 1];
    buf.get(dimensionBinary);
    baseDefinition = (NumericDimensionDefinition) PersistenceUtils.fromBinary(dimensionBinary);
    final byte precision = buf.get();
    if (precision == Byte.MAX_VALUE) {
      geometryPrecision = null;
    } else {
      geometryPrecision = Integer.valueOf(precision);
    }
    updatedField = getUpdatedField(null);
  }

  @Override
  public double getRange() {
    return updatedField.getRange();
  }

  @Override
  public double normalize(final double value) {
    return updatedField.normalize(value);
  }

  @Override
  public double denormalize(final double value) {
    return updatedField.denormalize(value);
  }

  @Override
  public BinRange[] getNormalizedRanges(final NumericData range) {
    return updatedField.getNormalizedRanges(range);
  }

  @Override
  public NumericRange getDenormalizedRange(final BinRange range) {
    return updatedField.getDenormalizedRange(range);
  }

  @Override
  public int getFixedBinIdSize() {
    return updatedField.getFixedBinIdSize();
  }

  @Override
  public NumericRange getBounds() {
    return updatedField.getBounds();
  }

  @Override
  public NumericData getFullRange() {
    return updatedField.getFullRange();
  }

  @Override
  public NumericData getNumericData(final Geometry dataElement) {
    return updatedField.getNumericData(dataElement);
  }

  @Override
  public String getFieldName() {
    return updatedField.getFieldName();
  }

  @Override
  public Set<IndexDimensionHint> getDimensionHints() {
    return updatedField.getDimensionHints();
  }

  @Override
  public FieldWriter<Geometry> getWriter() {
    return updatedField.getWriter();
  }

  @Override
  public FieldReader<Geometry> getReader() {
    return updatedField.getReader();
  }

  @Override
  public NumericDimensionDefinition getBaseDefinition() {
    return updatedField.getBaseDefinition();
  }

  @Override
  public Class<Geometry> getFieldClass() {
    return updatedField.getFieldClass();
  }

}
