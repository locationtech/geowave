/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.dimension;

import java.nio.ByteBuffer;
import javax.annotation.Nullable;
import org.geotools.referencing.CRS;
import org.locationtech.geowave.core.geotime.store.field.GeometrySerializationProvider;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.IndexDimensionHint;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.dimension.NumericDimensionDefinition;
import org.locationtech.geowave.core.index.dimension.bin.BinRange;
import org.locationtech.geowave.core.index.numeric.NumericData;
import org.locationtech.geowave.core.index.numeric.NumericRange;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.api.IndexFieldMapper.IndexFieldOptions;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;
import org.locationtech.jts.geom.Geometry;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A base class for EPSG:4326 latitude/longitude fields that use JTS geometry */
public abstract class SpatialField implements NumericDimensionField<Geometry> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SpatialField.class);
  public static final String DEFAULT_GEOMETRY_FIELD_NAME = "default_geom_dimension";
  public static final IndexDimensionHint LONGITUDE_DIMENSION_HINT =
      new IndexDimensionHint("LONGITUDE");
  public static final IndexDimensionHint LATITUDE_DIMENSION_HINT =
      new IndexDimensionHint("LATITUDE");
  protected NumericDimensionDefinition baseDefinition;
  private FieldReader<Geometry> geometryReader;
  private FieldWriter<Geometry> geometryWriter;
  private Integer geometryPrecision;
  private CoordinateReferenceSystem crs = GeometryUtils.getDefaultCRS();

  protected SpatialField() {
    this(null, null, null);
  }

  protected SpatialField(@Nullable final Integer geometryPrecision) {
    this(null, geometryPrecision, null);
  }

  public SpatialField(
      final NumericDimensionDefinition baseDefinition,
      final @Nullable Integer geometryPrecision) {
    this(baseDefinition, geometryPrecision, null);
  }

  public SpatialField(
      final NumericDimensionDefinition baseDefinition,
      final @Nullable Integer geometryPrecision,
      final @Nullable CoordinateReferenceSystem crs) {
    if (crs != null) {
      this.crs = crs;
    }
    this.baseDefinition = baseDefinition;
    this.geometryPrecision = geometryPrecision;
    final GeometrySerializationProvider serialization =
        new GeometrySerializationProvider(geometryPrecision);
    geometryReader = serialization.getFieldReader();
    geometryWriter = serialization.getFieldWriter();
  }

  public CoordinateReferenceSystem getCRS() {
    return crs;
  }

  public Integer getGeometryPrecision() {
    return geometryPrecision;
  }

  @Override
  public IndexFieldOptions getIndexFieldOptions() {
    return new SpatialIndexFieldOptions(crs);
  }

  @Override
  public Class<Geometry> getFieldClass() {
    return Geometry.class;
  }

  @Override
  public NumericData getFullRange() {
    return baseDefinition.getFullRange();
  }

  @Override
  public NumericRange getDenormalizedRange(final BinRange range) {
    return new NumericRange(range.getNormalizedMin(), range.getNormalizedMax());
  }

  @Override
  public double getRange() {
    return baseDefinition.getRange();
  }

  @Override
  public int getFixedBinIdSize() {
    return 0;
  }

  @Override
  public NumericRange getBounds() {
    return baseDefinition.getBounds();
  }

  @Override
  public double normalize(final double value) {
    return baseDefinition.normalize(value);
  }

  @Override
  public double denormalize(final double value) {
    return baseDefinition.denormalize(value);
  }

  @Override
  public BinRange[] getNormalizedRanges(final NumericData range) {
    return baseDefinition.getNormalizedRanges(range);
  }

  @Override
  public String getFieldName() {
    return DEFAULT_GEOMETRY_FIELD_NAME;
  }

  @Override
  public FieldWriter<Geometry> getWriter() {
    return geometryWriter;
  }

  @Override
  public FieldReader<Geometry> getReader() {
    return geometryReader;
  }

  @Override
  public NumericDimensionDefinition getBaseDefinition() {
    return baseDefinition;
  }

  @Override
  public byte[] toBinary() {
    final byte[] dimensionBinary = PersistenceUtils.toBinary(baseDefinition);
    final byte[] crsBinary = StringUtils.stringToBinary(CRS.toSRS(crs));
    final ByteBuffer buf =
        ByteBuffer.allocate(
            VarintUtils.unsignedShortByteLength((short) dimensionBinary.length)
                + dimensionBinary.length
                + 1
                + crsBinary.length);
    VarintUtils.writeUnsignedShort((short) dimensionBinary.length, buf);
    buf.put(dimensionBinary);
    if (geometryPrecision == null) {
      buf.put(Byte.MAX_VALUE);
    } else {
      buf.put((byte) geometryPrecision.intValue());
    }
    buf.put(crsBinary);
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    final byte[] dimensionBinary = new byte[VarintUtils.readUnsignedShort(buf)];
    buf.get(dimensionBinary);
    baseDefinition = (NumericDimensionDefinition) PersistenceUtils.fromBinary(dimensionBinary);
    final byte precision = buf.get();
    if (precision == Byte.MAX_VALUE) {
      geometryPrecision = null;
    } else {
      geometryPrecision = Integer.valueOf(precision);
    }
    final GeometrySerializationProvider serialization =
        new GeometrySerializationProvider(geometryPrecision);
    geometryReader = serialization.getFieldReader();
    geometryWriter = serialization.getFieldWriter();
    final byte[] crsBinary = new byte[buf.remaining()];
    buf.get(crsBinary);
    try {
      this.crs = CRS.decode(StringUtils.stringFromBinary(crsBinary), true);
    } catch (FactoryException e) {
      LOGGER.warn("Unable to decode index field CRS");
      this.crs = GeometryUtils.getDefaultCRS();
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    final String className = getClass().getName();
    result = (prime * result) + ((className == null) ? 0 : className.hashCode());
    result = (prime * result) + ((baseDefinition == null) ? 0 : baseDefinition.hashCode());
    result = (prime * result) + ((geometryPrecision == null) ? 0 : geometryPrecision.hashCode());
    return result;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final SpatialField other = (SpatialField) obj;
    if (baseDefinition == null) {
      if (other.baseDefinition != null) {
        return false;
      }
    } else if (!baseDefinition.equals(other.baseDefinition)) {
      return false;
    }
    if (geometryPrecision == null) {
      if (other.geometryPrecision != null) {
        return false;
      }
    } else if (!geometryPrecision.equals(other.geometryPrecision)) {
      return false;
    }
    return true;
  }

  public static class SpatialIndexFieldOptions implements IndexFieldOptions {
    private final CoordinateReferenceSystem indexCRS;

    public SpatialIndexFieldOptions(final CoordinateReferenceSystem indexCRS) {
      this.indexCRS = indexCRS;
    }

    public CoordinateReferenceSystem crs() {
      return this.indexCRS;
    }
  }
}
