/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query.aggregate;

import org.locationtech.geowave.core.geotime.store.dimension.GeometryWrapper;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.data.CommonIndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.data.PersistentDataset;
import org.locationtech.geowave.core.store.index.CommonIndexValue;
import org.locationtech.jts.geom.Geometry;
import java.nio.ByteBuffer;

/**
 * A GeohashBinningStrategy that bins CommonIndexedPersistenceEncoding values.
 *
 * @see GeohashBinningStrategy
 */
public class GeohashCommonIndexedBinningStrategy extends
    GeohashBinningStrategy<CommonIndexedPersistenceEncoding> {

  private String geometryFieldName;

  /**
   * Create a binning strategy using a small number of bins. Usage of this method is not
   * recommended, if you are to use this, it should be through serialization.
   */
  public GeohashCommonIndexedBinningStrategy() {
    this(2);
  }

  /**
   * @see GeohashBinningStrategy#GeohashBinningStrategy(int)
   * @see GeohashCommonIndexedBinningStrategy#GeohashCommonIndexedBinningStrategy(int, String)
   */
  public GeohashCommonIndexedBinningStrategy(int precision) {
    this(precision, GeometryWrapper.DEFAULT_GEOMETRY_FIELD_NAME);
  }

  /**
   * @param geometryFieldName The field name of the geometry used in a given
   *        CommonIndexedPersistenceEncoding entry. For more documentation on this behavior, see
   *        {@link GeohashBinningStrategy#GeohashBinningStrategy(int) new
   *        GeohashBinningStrategy(int)}.
   */
  public GeohashCommonIndexedBinningStrategy(int precision, String geometryFieldName) {
    super(precision);
    this.geometryFieldName = geometryFieldName;
  }

  @Override
  public Geometry getGeometry(CommonIndexedPersistenceEncoding entry) {
    PersistentDataset<CommonIndexValue> data = entry.getCommonData();
    final CommonIndexValue geometryValue = data.getValue(this.geometryFieldName);
    if (geometryValue instanceof GeometryWrapper) {
      return ((GeometryWrapper) geometryValue).getGeometry();
    } else {
      return null;
    }
  }

  @Override
  public byte[] toBinary() {
    byte[] fieldName = this.geometryFieldName.getBytes(StringUtils.getGeoWaveCharset());
    return ByteBuffer.allocate(4 + 4 + fieldName.length).putInt(this.getPrecision()).putInt(
        fieldName.length).put(fieldName).array();
  }

  @Override
  public void fromBinary(byte[] bytes) {
    ByteBuffer bb = ByteBuffer.wrap(bytes);
    this.precision = bb.getInt();
    int fieldLen = bb.getInt();
    byte[] fieldBytes = new byte[fieldLen];
    bb.get(fieldBytes);
    this.geometryFieldName = new String(fieldBytes, StringUtils.getGeoWaveCharset());
  }
}
