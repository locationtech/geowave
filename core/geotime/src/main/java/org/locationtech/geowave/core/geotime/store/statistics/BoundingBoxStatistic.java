/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.statistics;

import java.nio.ByteBuffer;
import org.geotools.referencing.CRS;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.FieldStatistic;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.statistics.field.FieldStatisticType;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

public class BoundingBoxStatistic extends FieldStatistic<BoundingBoxStatistic.BoundingBoxValue> {
  public static final FieldStatisticType<BoundingBoxValue> STATS_TYPE =
      new FieldStatisticType<>("BOUNDING_BOX");

  @Parameter(
      names = {"--sourceCrs"},
      description = "CRS of source geometry.",
      converter = CRSConverter.class)
  private CoordinateReferenceSystem sourceCrs = null;

  @Parameter(
      names = {"--crs"},
      description = "CRS of the bounding box statistic.",
      converter = CRSConverter.class)
  private CoordinateReferenceSystem destinationCrs = null;

  private MathTransform crsTransform = null;

  public BoundingBoxStatistic() {
    this(null, null);
  }

  public BoundingBoxStatistic(final String typeName, final String fieldName) {
    this(typeName, fieldName, null, null);
  }

  public BoundingBoxStatistic(
      final String typeName,
      final String fieldName,
      final CoordinateReferenceSystem sourceCrs,
      final CoordinateReferenceSystem destinationCrs) {
    super(STATS_TYPE, typeName, fieldName);
    this.sourceCrs = sourceCrs;
    this.destinationCrs = destinationCrs;
  }

  public MathTransform getTransform() {
    if (sourceCrs != null && destinationCrs != null && crsTransform == null) {
      try {
        crsTransform = CRS.findMathTransform(sourceCrs, destinationCrs, true);
      } catch (FactoryException e) {
        throw new ParameterException(
            "Unable to create CRS transform for bounding box statistic.",
            e);
      }
    }
    return crsTransform;
  }

  public void setSourceCrs(final CoordinateReferenceSystem sourceCrs) {
    this.sourceCrs = sourceCrs;
  }

  public CoordinateReferenceSystem getSourceCrs() {
    return sourceCrs;
  }

  public void setDestinationCrs(final CoordinateReferenceSystem destinationCrs) {
    this.destinationCrs = destinationCrs;
  }

  public CoordinateReferenceSystem getDestinationCrs() {
    return destinationCrs;
  }

  @Override
  public boolean isCompatibleWith(final Class<?> fieldClass) {
    return Geometry.class.isAssignableFrom(fieldClass);
  }

  @Override
  public String getDescription() {
    return "Maintains the bounding box for a geometry field.";
  }

  @Override
  public BoundingBoxValue createEmpty() {
    return new BoundingBoxValue(this);
  }

  private byte[] sourceCrsBytes = null;
  private byte[] destinationCrsBytes = null;

  private void transformToBytes() {
    sourceCrsBytes =
        sourceCrs == null ? new byte[0] : StringUtils.stringToBinary(sourceCrs.toWKT());
    destinationCrsBytes =
        destinationCrs == null ? new byte[0] : StringUtils.stringToBinary(destinationCrs.toWKT());
  }

  @Override
  public int byteLength() {
    if (sourceCrsBytes == null) {
      transformToBytes();
    }
    return super.byteLength()
        + sourceCrsBytes.length
        + VarintUtils.unsignedShortByteLength((short) sourceCrsBytes.length)
        + destinationCrsBytes.length
        + VarintUtils.unsignedShortByteLength((short) destinationCrsBytes.length);
  }

  @Override
  public void writeBytes(final ByteBuffer buffer) {
    super.writeBytes(buffer);
    if (sourceCrsBytes == null) {
      transformToBytes();
    }
    VarintUtils.writeUnsignedShort((short) sourceCrsBytes.length, buffer);
    buffer.put(sourceCrsBytes);
    VarintUtils.writeUnsignedShort((short) destinationCrsBytes.length, buffer);
    buffer.put(destinationCrsBytes);
    sourceCrsBytes = null;
    destinationCrsBytes = null;
  }

  @Override
  public void readBytes(final ByteBuffer buffer) {
    super.readBytes(buffer);
    try {
      short length = VarintUtils.readUnsignedShort(buffer);
      sourceCrsBytes = new byte[length];
      buffer.get(sourceCrsBytes);
      if (length > 0) {
        sourceCrs = CRS.parseWKT(StringUtils.stringFromBinary(sourceCrsBytes));
      }
      length = VarintUtils.readUnsignedShort(buffer);
      destinationCrsBytes = new byte[length];
      buffer.get(destinationCrsBytes);
      if (length > 0) {
        destinationCrs = CRS.parseWKT(StringUtils.stringFromBinary(destinationCrsBytes));
      }
    } catch (FactoryException e) {
      throw new RuntimeException("Unable to parse statistic CRS", e);
    }
    sourceCrsBytes = null;
    destinationCrsBytes = null;
  }

  public static class BoundingBoxValue extends AbstractBoundingBoxValue {

    public BoundingBoxValue() {
      this(null);
    }

    public BoundingBoxValue(final Statistic<?> statistic) {
      super(statistic);
    }

    @Override
    public <T> Envelope getEnvelope(DataTypeAdapter<T> adapter, T entry, GeoWaveRow... rows) {
      BoundingBoxStatistic bboxStatistic = (BoundingBoxStatistic) statistic;
      Object fieldValue = adapter.getFieldValue(entry, bboxStatistic.getFieldName());

      if ((fieldValue != null) && (fieldValue instanceof Geometry)) {
        Geometry geometry = (Geometry) fieldValue;
        if (bboxStatistic.getTransform() != null) {
          geometry = GeometryUtils.crsTransform(geometry, bboxStatistic.getTransform());
        }
        if (geometry != null && !geometry.isEmpty()) {
          return geometry.getEnvelopeInternal();
        }
      }
      return null;
    }

  }

  public static class CRSConverter implements IStringConverter<CoordinateReferenceSystem> {
    @Override
    public CoordinateReferenceSystem convert(final String value) {
      CoordinateReferenceSystem convertedValue;
      try {
        convertedValue = CRS.decode(value);
      } catch (Exception e) {
        throw new ParameterException("Unrecognized CRS: " + value);
      }
      return convertedValue;
    }
  }
}
