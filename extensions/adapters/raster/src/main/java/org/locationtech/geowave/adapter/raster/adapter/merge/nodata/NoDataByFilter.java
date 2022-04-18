/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.raster.adapter.merge.nodata;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.geotime.util.TWKBReader;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.GeoWaveSerializationException;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NoDataByFilter implements NoDataMetadata {
  private static final Logger LOGGER = LoggerFactory.getLogger(NoDataByFilter.class);
  private Geometry shape;
  private double[][] noDataPerBand;

  public NoDataByFilter() {}

  public NoDataByFilter(final Geometry shape, final double[][] noDataPerBand) {
    this.shape = shape;
    this.noDataPerBand = noDataPerBand;
  }

  public Geometry getShape() {
    return shape;
  }

  public double[][] getNoDataPerBand() {
    return noDataPerBand;
  }

  @Override
  public boolean isNoData(final SampleIndex index, final double value) {
    if ((noDataPerBand != null) && (noDataPerBand.length > index.getBand())) {
      for (final double noDataVal : noDataPerBand[index.getBand()]) {
        // use object equality to capture NaN, and positive and negative
        // infinite equality
        if (new Double(value).equals(new Double(noDataVal))) {
          return true;
        }
      }
    }
    if ((shape != null)
        && !shape.intersects(
            new GeometryFactory().createPoint(new Coordinate(index.getX(), index.getY())))) {
      return true;
    }
    return false;
  }

  @Override
  public byte[] toBinary() {
    final byte[] noDataBinary;
    if ((noDataPerBand != null) && (noDataPerBand.length > 0)) {
      int totalBytes = 0;
      final List<byte[]> noDataValuesBytes = new ArrayList<>(noDataPerBand.length);
      for (final double[] noDataValues : noDataPerBand) {
        final int thisBytes =
            VarintUtils.unsignedIntByteLength(noDataValues.length) + (noDataValues.length * 8);
        totalBytes += thisBytes;
        final ByteBuffer noDataBuf = ByteBuffer.allocate(thisBytes);
        VarintUtils.writeUnsignedInt(noDataValues.length, noDataBuf);
        for (final double noDataValue : noDataValues) {
          noDataBuf.putDouble(noDataValue);
        }
        noDataValuesBytes.add(noDataBuf.array());
      }
      totalBytes += VarintUtils.unsignedIntByteLength(noDataPerBand.length);
      final ByteBuffer noDataBuf = ByteBuffer.allocate(totalBytes);
      VarintUtils.writeUnsignedInt(noDataPerBand.length, noDataBuf);
      for (final byte[] noDataValueBytes : noDataValuesBytes) {
        noDataBuf.put(noDataValueBytes);
      }
      noDataBinary = noDataBuf.array();
    } else {
      noDataBinary = new byte[] {};
    }
    final byte[] geometryBinary;
    if (shape == null) {
      geometryBinary = new byte[0];
    } else {
      geometryBinary = GeometryUtils.geometryToBinary(shape, GeometryUtils.MAX_GEOMETRY_PRECISION);
    }
    final ByteBuffer buf =
        ByteBuffer.allocate(
            geometryBinary.length
                + noDataBinary.length
                + VarintUtils.unsignedIntByteLength(noDataBinary.length));
    VarintUtils.writeUnsignedInt(noDataBinary.length, buf);
    buf.put(noDataBinary);
    buf.put(geometryBinary);
    return buf.array();
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    final int noDataBinaryLength = VarintUtils.readUnsignedInt(buf);
    final int geometryBinaryLength =
        bytes.length - noDataBinaryLength - VarintUtils.unsignedIntByteLength(noDataBinaryLength);
    if (noDataBinaryLength == 0) {
      noDataPerBand = new double[][] {};
    } else {
      final int numBands = VarintUtils.readUnsignedInt(buf);
      ByteArrayUtils.verifyBufferSize(buf, numBands);
      noDataPerBand = new double[numBands][];
      for (int b = 0; b < noDataPerBand.length; b++) {
        final int bandLength = VarintUtils.readUnsignedInt(buf);
        ByteArrayUtils.verifyBufferSize(buf, bandLength);
        noDataPerBand[b] = new double[bandLength];
        for (int i = 0; i < noDataPerBand[b].length; i++) {
          noDataPerBand[b][i] = buf.getDouble();
        }
      }
    }
    if (geometryBinaryLength > 0) {
      try {
        shape = new TWKBReader().read(buf);
      } catch (final ParseException e) {
        throw new GeoWaveSerializationException("Unable to deserialize geometry data", e);
      }
    } else {
      shape = null;
    }
  }

  @Override
  public Set<SampleIndex> getNoDataIndices() {
    return null;
  }
}
