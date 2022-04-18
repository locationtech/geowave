/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.statistics.binning;

import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.commons.lang3.ArrayUtils;
import org.locationtech.geowave.core.geotime.binning.ComplexGeometryBinningOption;
import org.locationtech.geowave.core.geotime.binning.SpatialBinningType;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.api.BinConstraints.ByteArrayConstraints;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.statistics.binning.BinningStrategyUtils;
import org.locationtech.geowave.core.store.statistics.binning.FieldValueBinningStrategy;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.primitives.Bytes;

public class SpatialFieldValueBinningStrategy extends FieldValueBinningStrategy {
  public static final String NAME = "SPATIAL";

  @Parameter(
      names = {"--precision", "--resolution", "--length", "--level"},
      description = "The precision (also called resolution, length, or level) of the binning strategy")
  protected int precision = 8;

  @Parameter(
      names = {"--geometry"},
      converter = ComplexGeometryBinningOptionConverter.class,
      description = "Approach for handling complex geometry. Available options are 'USE_CENTROID_ONLY', 'USE_FULL_GEOMETRY', and 'USE_FULL_GEOMETRY_SCALE_BY_OVERLAP'.")
  protected ComplexGeometryBinningOption complexGeometry =
      ComplexGeometryBinningOption.USE_CENTROID_ONLY;

  @Parameter(
      names = {"--type"},
      converter = SpatialBinningTypeConverter.class,
      description = "The type of binning (either h3, s2, or geohash).")
  protected SpatialBinningType type = SpatialBinningType.S2;

  public SpatialFieldValueBinningStrategy() {
    super();
  }

  public SpatialFieldValueBinningStrategy(final String... fields) {
    super(fields);
  }

  public SpatialFieldValueBinningStrategy(
      final SpatialBinningType type,
      final int precision,
      final ComplexGeometryBinningOption complexGeometry,
      final String... fields) {
    super(fields);
    this.type = type;
    this.precision = precision;
    this.complexGeometry = complexGeometry;
  }

  public int getPrecision() {
    return precision;
  }

  public void setPrecision(final int precision) {
    this.precision = precision;
  }

  public ComplexGeometryBinningOption getComplexGeometry() {
    return complexGeometry;
  }

  public void setComplexGeometry(final ComplexGeometryBinningOption complexGeometry) {
    this.complexGeometry = complexGeometry;
  }

  public SpatialBinningType getType() {
    return type;
  }

  public void setType(final SpatialBinningType type) {
    this.type = type;
  }

  @Override
  public String getDefaultTag() {
    // this intentionally doesn't include ComplexGeometryBinningOption, if for some reason a user
    // wants to have multiple on the same fields of the same type with the same precision just
    // different binning options, they'd need to define their own tags
    return super.getDefaultTag() + "-" + type + "(" + precision + ")";
  }

  @Override
  public String getDescription() {
    return "Bin a statistic by a spatial aggregation (such as geohash, H3, or S2) on a specified geometry field.";
  }

  @Override
  public String getStrategyName() {
    return NAME;
  }

  protected ByteArray[] getSpatialBins(final Geometry geometry) {
    return type.getSpatialBins(geometry, precision);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Class<?>[] supportedConstraintClasses() {
    return ArrayUtils.addAll(
        super.supportedConstraintClasses(),
        Envelope.class,
        Envelope[].class,
        Geometry.class,
        Geometry[].class);
  }

  private ByteArray[] getSpatialBinsFromObj(final Object value) {
    if (value instanceof Geometry) {
      if (ComplexGeometryBinningOption.USE_CENTROID_ONLY.equals(complexGeometry)) {
        return getSpatialBins(((Geometry) value).getCentroid());
      }
      return getSpatialBins((Geometry) value);
    }
    return new ByteArray[0];
  }

  @Override
  public <T> ByteArray[] getBins(
      final DataTypeAdapter<T> adapter,
      final T entry,
      final GeoWaveRow... rows) {
    if (fields.isEmpty()) {
      return new ByteArray[0];
    } else if (fields.size() == 1) {
      final Object value = adapter.getFieldValue(entry, fields.get(0));
      return getSpatialBinsFromObj(value);
    }
    final ByteArray[][] fieldValues =
        fields.stream().map(
            field -> getSpatialBinsFromObj(adapter.getFieldValue(entry, field))).toArray(
                ByteArray[][]::new);
    return getAllCombinationsNoSeparator(fieldValues);
  }

  @Override
  public String binToString(final ByteArray bin) {
    final ByteBuffer buffer = ByteBuffer.wrap(bin.getBytes());
    final StringBuffer sb = new StringBuffer();
    while (buffer.remaining() > 0) {
      final byte[] binId = new byte[type.getBinByteLength(precision)];
      buffer.get(binId);
      sb.append(type.binToString(binId));
    }
    if (buffer.remaining() > 0) {
      sb.append('|');
    }
    return sb.toString();
  }

  @Override
  public byte[] toBinary() {
    final byte[] parentBinary = super.toBinary();
    final ByteBuffer buf =
        ByteBuffer.allocate(
            parentBinary.length
                + VarintUtils.unsignedIntByteLength(precision)
                + VarintUtils.unsignedIntByteLength(complexGeometry.ordinal())
                + VarintUtils.unsignedIntByteLength(type.ordinal()));
    VarintUtils.writeUnsignedInt(type.ordinal(), buf);
    VarintUtils.writeUnsignedInt(precision, buf);
    VarintUtils.writeUnsignedInt(complexGeometry.ordinal(), buf);
    buf.put(parentBinary);
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    type = SpatialBinningType.values()[VarintUtils.readUnsignedInt(buf)];
    precision = VarintUtils.readUnsignedInt(buf);
    complexGeometry = ComplexGeometryBinningOption.values()[VarintUtils.readUnsignedInt(buf)];
    final byte[] parentBinary = new byte[buf.remaining()];
    buf.get(parentBinary);
    super.fromBinary(parentBinary);
  }

  @Override
  public <T> double getWeight(
      final ByteArray bin,
      final DataTypeAdapter<T> type,
      final T entry,
      final GeoWaveRow... rows) {
    if (ComplexGeometryBinningOption.USE_FULL_GEOMETRY_SCALE_BY_OVERLAP.equals(complexGeometry)) {
      // only compute if its intended to scale by percent overlap
      final ByteBuffer buffer = ByteBuffer.wrap(bin.getBytes());
      double weight = 1;
      int i = 0;
      while (buffer.remaining() > 0) {
        final byte[] binId = new byte[this.type.getBinByteLength(precision)];
        buffer.get(binId);
        final Geometry binGeom = this.type.getBinGeometry(new ByteArray(binId), precision);

        final Object value = type.getFieldValue(entry, fields.get(i++));
        if (value instanceof Geometry) {
          // This approach could be fairly expensive, but is accurate and general-purpose

          // take the intersection of the field geometry with the bin geometry and take the area
          // weight is the ratio of the intersection area to the entire field geometry area
          final double area = ((Geometry) value).getArea();
          if (area > 0) {
            if (binGeom.intersects((Geometry) value)) {
              final double intersectionArea = binGeom.intersection((Geometry) value).getArea();
              final double fieldWeight = intersectionArea / ((Geometry) value).getArea();
              weight *= fieldWeight;
            }
          } else {
            final double length = ((Geometry) value).getLength();
            if (length > 0) {
              final double intersectionLength = binGeom.intersection((Geometry) value).getLength();
              final double fieldWeight = intersectionLength / ((Geometry) value).getLength();
              weight *= fieldWeight;
            }
            // if it has no area and no length it must be point data and not very applicable for
            // scaling
          }
        }
      }
      return weight;
    }
    return 1;
  }

  @Override
  protected ByteArrayConstraints singleFieldConstraints(final Object constraints) {
    // just convert each into a geometry (or multi-geometry) and let the underlying hashing
    // algorithm handle the rest
    if (constraints instanceof Envelope[]) {
      return type.getGeometryConstraints(
          GeometryUtils.GEOMETRY_FACTORY.createGeometryCollection(
              Arrays.stream((Envelope[]) constraints).map(
                  GeometryUtils.GEOMETRY_FACTORY::toGeometry).toArray(Geometry[]::new)),
          precision);
    } else if (constraints instanceof Envelope) {
      return type.getGeometryConstraints(
          GeometryUtils.GEOMETRY_FACTORY.toGeometry((Envelope) constraints),
          precision);
    } else if (constraints instanceof Geometry) {
      return type.getGeometryConstraints((Geometry) constraints, precision);
    } else if (constraints instanceof Geometry[]) {
      return type.getGeometryConstraints(
          GeometryUtils.GEOMETRY_FACTORY.createGeometryCollection((Geometry[]) constraints),
          precision);
    }
    return super.singleFieldConstraints(constraints);
  }

  private static ByteArray[] getAllCombinationsNoSeparator(final ByteArray[][] perFieldBins) {
    return BinningStrategyUtils.getAllCombinations(
        perFieldBins,
        a -> new ByteArray(
            Bytes.concat(Arrays.stream(a).map(ByteArray::getBytes).toArray(byte[][]::new))));
  }

  public static class ComplexGeometryBinningOptionConverter implements
      IStringConverter<ComplexGeometryBinningOption> {
    @Override
    public ComplexGeometryBinningOption convert(final String value) {
      ComplexGeometryBinningOption convertedValue = null;
      try {
        convertedValue = ComplexGeometryBinningOption.valueOf(value.toUpperCase());
      } catch (final Exception e) {
        // we'll throw the parameter exception instead of printing a stack trace
      }
      if (convertedValue == null) {
        throw new ParameterException(
            "Value "
                + value
                + "can not be converted to ComplexGeometryBinningOption. "
                + "Available values are: "
                + Arrays.toString(ComplexGeometryBinningOption.values()));
      }
      return convertedValue;
    }
  }

  public static class SpatialBinningTypeConverter implements IStringConverter<SpatialBinningType> {

    @Override
    public SpatialBinningType convert(final String value) {
      SpatialBinningType convertedValue = null;
      try {
        convertedValue = SpatialBinningType.valueOf(value.toUpperCase());
      } catch (final Exception e) {
        // we'll throw the parameter exception instead of printing a stack trace
      }
      if (convertedValue == null) {
        throw new ParameterException(
            "Value "
                + value
                + "can not be converted to SpatialBinningType. "
                + "Available values are: "
                + Arrays.toString(SpatialBinningType.values()));
      }
      return convertedValue;
    }

  }
}
