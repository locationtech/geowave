/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query.aggregate;

import java.nio.ByteBuffer;
import org.locationtech.geowave.core.geotime.binning.ComplexGeometryBinningOption;
import org.locationtech.geowave.core.geotime.binning.SpatialBinningType;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.store.api.BinningStrategy;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;

/**
 * This strategy uses a spatial binning type (such as H3, S2, GeoHash) on geometry fields to bin
 * data.
 *
 * @param <T> The type of the entry. The geometry inside of it is queried, and the geohash of that
 *        geometry is used as the bin.
 */
public abstract class SpatialBinningStrategy<T> implements BinningStrategy {

  protected String geometryFieldName;

  /**
   * The precision/resolution/length used by the binning strategy (it usually is equivalent to
   * character length).
   */
  protected int precision;

  protected ComplexGeometryBinningOption complexGeometryBinning;
  protected SpatialBinningType type;

  public SpatialBinningStrategy() {}

  /**
   * Use the given precision to bin objects
   *
   * @param type The type (such as S3, H2, or GeoHash)
   * @param precision The Geohash precision to calculate bins.
   * @param useCentroidOnly for complex geometry such as lines and polygons whether to just
   *        aggregate one hash value based on the centroid or to apply the aggregation to all
   *        overlapping centroids
   * @param geometryFieldName the field name for the geometry to bin by
   */
  public SpatialBinningStrategy(
      final SpatialBinningType type,
      final int precision,
      final boolean useCentroidOnly,
      final String geometryFieldName) {
    this.type = type;
    this.precision = precision;
    // for now scaling by weight isn't wired into aggregations so don't expose that option through
    // the constructor yet, although at some point it would make some sense to add it
    this.complexGeometryBinning =
        useCentroidOnly ? ComplexGeometryBinningOption.USE_CENTROID_ONLY
            : ComplexGeometryBinningOption.USE_FULL_GEOMETRY;
    this.geometryFieldName = geometryFieldName;
  }

  /**
   * Extract the geometry from the entry.
   *
   * @param entry The entry that will be binned using this strategy.
   * @return The geometry object in the entry, or null if no geometry is found.
   */
  abstract Geometry getGeometry(final DataTypeAdapter<T> adapter, T entry);

  /**
   * @return The precision that is used when calculating bins for entries.
   */
  public int getPrecision() {
    return precision;
  }

  /**
   * calculates appropriate bins for a given entry. GeohashBinningStrategy only ever bins into
   * singleton-arrays.
   *
   * @param entry An entry to bin, utilizing its' geohash.
   * @return a length-1 array of the bin that this entry can be placed into. `null` if no Geometry
   *         was found in the entry.
   */
  @Override
  public <I> ByteArray[] getBins(
      final DataTypeAdapter<I> adapter,
      final I entry,
      final GeoWaveRow... rows) {
    final Geometry geometry = getGeometry((DataTypeAdapter<T>) adapter, (T) entry);
    if (geometry == null) {
      return null;
    }
    if (ComplexGeometryBinningOption.USE_CENTROID_ONLY.equals(complexGeometryBinning)) {
      final Point centroid = geometry.getCentroid();
      return type.getSpatialBins(centroid, precision);
    }
    return type.getSpatialBins(geometry, precision);
  }

  @Override
  public byte[] toBinary() {
    final byte[] fieldNameBytes =
        geometryFieldName == null ? new byte[0] : StringUtils.stringToBinary(geometryFieldName);
    final ByteBuffer buf =
        ByteBuffer.allocate(
            fieldNameBytes.length
                + +VarintUtils.unsignedIntByteLength(fieldNameBytes.length)
                + VarintUtils.unsignedIntByteLength(type.ordinal())
                + VarintUtils.unsignedIntByteLength(precision)
                + VarintUtils.unsignedIntByteLength(complexGeometryBinning.ordinal()));
    VarintUtils.writeUnsignedInt(type.ordinal(), buf);
    VarintUtils.writeUnsignedInt(precision, buf);
    VarintUtils.writeUnsignedInt(complexGeometryBinning.ordinal(), buf);
    VarintUtils.writeUnsignedInt(fieldNameBytes.length, buf);
    buf.put(fieldNameBytes);
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    this.type = SpatialBinningType.values()[VarintUtils.readUnsignedInt(buf)];
    this.precision = VarintUtils.readUnsignedInt(buf);
    this.complexGeometryBinning =
        ComplexGeometryBinningOption.values()[VarintUtils.readUnsignedInt(buf)];
    final byte[] fieldNameBytes = new byte[VarintUtils.readUnsignedInt(buf)];
    if (fieldNameBytes.length > 0) {
      buf.get(fieldNameBytes);
      this.geometryFieldName = StringUtils.stringFromBinary(fieldNameBytes);
    } else {
      this.geometryFieldName = null;
    }
  }
}
