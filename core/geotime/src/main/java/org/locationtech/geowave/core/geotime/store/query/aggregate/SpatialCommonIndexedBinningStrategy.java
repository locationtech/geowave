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
import org.locationtech.geowave.core.geotime.binning.SpatialBinningType;
import org.locationtech.geowave.core.geotime.store.dimension.SpatialField;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.data.CommonIndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.data.PersistentDataset;
import org.locationtech.jts.geom.Geometry;

/**
 * A GeohashBinningStrategy that bins CommonIndexedPersistenceEncoding values.
 *
 * @see SpatialBinningStrategy
 */
public class SpatialCommonIndexedBinningStrategy extends
    SpatialBinningStrategy<CommonIndexedPersistenceEncoding> {



  /**
   * Create a binning strategy using a small number of bins. Usage of this method is not
   * recommended, if you are to use this, it should be through serialization.
   */
  public SpatialCommonIndexedBinningStrategy() {
    this(SpatialBinningType.S2, 3, true);
  }

  public SpatialCommonIndexedBinningStrategy(
      final SpatialBinningType type,
      final int precision,
      final boolean useCentroidOnly) {
    this(type, precision, useCentroidOnly, SpatialField.DEFAULT_GEOMETRY_FIELD_NAME);
  }

  /**
   * @param type S2, H3, or GeoHash
   * @param precision the resolution/length of the hash
   * @param useCentroidOnly desired behavior for complex geometry such as lines and polygons whether
   *        to just aggregate one hash value based on the centroid or to apply the aggregation to
   *        all overlapping centroids
   * @param geometryFieldName The field name of the geometry used in a given
   *        CommonIndexedPersistenceEncoding entry. For more documentation on this behavior, see
   *        {@link SpatialBinningStrategy#GeohashBinningStrategy(int) new
   *        GeohashBinningStrategy(int)}.
   */
  public SpatialCommonIndexedBinningStrategy(
      final SpatialBinningType type,
      final int precision,
      final boolean useCentroidOnly,
      final String geometryFieldName) {
    super(type, precision, useCentroidOnly, geometryFieldName);
  }

  @Override
  public Geometry getGeometry(
      final DataTypeAdapter<CommonIndexedPersistenceEncoding> adapter,
      final CommonIndexedPersistenceEncoding entry) {
    final PersistentDataset<Object> data = entry.getCommonData();
    final Object geometryValue = data.getValue(geometryFieldName);
    if (geometryValue instanceof Geometry) {
      return ((Geometry) geometryValue);
    } else {
      return null;
    }
  }

  @Override
  public byte[] toBinary() {
    final byte[] fieldName = geometryFieldName.getBytes(StringUtils.getGeoWaveCharset());
    return ByteBuffer.allocate(4 + 4 + fieldName.length).putInt(getPrecision()).putInt(
        fieldName.length).put(fieldName).array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer bb = ByteBuffer.wrap(bytes);
    precision = bb.getInt();
    final int fieldLen = bb.getInt();
    final byte[] fieldBytes = new byte[fieldLen];
    bb.get(fieldBytes);
    geometryFieldName = new String(fieldBytes, StringUtils.getGeoWaveCharset());
  }
}
