/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.raster.adapter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.geotools.referencing.CRS;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.StringUtils;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.clearspring.analytics.util.Varint;

/**
 * This class is used by GridCoverageDataAdapter to persist GridCoverages. The adapter has
 * information regarding the sample model and color model so all that is necessary to persist is the
 * buffer and the envelope.
 */
public class GridCoverageWritable implements Writable {
  private static final Logger LOGGER = LoggerFactory.getLogger(GridCoverageWritable.class);
  private RasterTile rasterTile;
  private double minX;
  private double maxX;
  private double minY;
  private double maxY;
  private CoordinateReferenceSystem crs;

  public GridCoverageWritable() {}

  public GridCoverageWritable(
      final RasterTile rasterTile,
      final double minX,
      final double maxX,
      final double minY,
      final double maxY,
      final CoordinateReferenceSystem crs) {
    this.rasterTile = rasterTile;
    this.minX = minX;
    this.maxX = maxX;
    this.minY = minY;
    this.maxY = maxY;
    this.crs = crs;
  }

  public CoordinateReferenceSystem getCrs() {
    return crs;
  }

  public RasterTile getRasterTile() {
    return rasterTile;
  }

  public double getMinX() {
    return minX;
  }

  public double getMaxX() {
    return maxX;
  }

  public double getMinY() {
    return minY;
  }

  public double getMaxY() {
    return maxY;
  }

  @Override
  public void readFields(final DataInput input) throws IOException {
    final int rasterTileSize = Varint.readUnsignedVarInt(input);
    final byte[] rasterTileBinary = new byte[rasterTileSize];
    input.readFully(rasterTileBinary);
    rasterTile = new RasterTile();
    rasterTile.fromBinary(rasterTileBinary);
    minX = input.readDouble();
    maxX = input.readDouble();
    minY = input.readDouble();
    maxY = input.readDouble();
    final int crsStrSize = Varint.readUnsignedVarInt(input);

    if (crsStrSize > 0) {
      final byte[] crsStrBytes = new byte[crsStrSize];
      input.readFully(crsStrBytes);
      final String crsStr = StringUtils.stringFromBinary(crsStrBytes);
      try {
        crs = CRS.decode(crsStr);
      } catch (final FactoryException e) {
        LOGGER.error("Unable to decode " + crsStr + " CRS", e);
        throw new RuntimeException("Unable to decode " + crsStr + " CRS", e);
      }
    } else {
      crs = GeometryUtils.getDefaultCRS();
    }
  }

  @Override
  public void write(final DataOutput output) throws IOException {
    final byte[] rasterTileBinary = rasterTile.toBinary();
    Varint.writeUnsignedVarInt(rasterTileBinary.length, output);
    output.write(rasterTileBinary);
    output.writeDouble(minX);
    output.writeDouble(maxX);
    output.writeDouble(minY);
    output.writeDouble(maxY);
    final String crsStr =
        (crs == null) || GeometryUtils.getDefaultCRS().equals(crs) ? "" : CRS.toSRS(crs);
    Varint.writeUnsignedVarInt(crsStr.length(), output);
    output.write(StringUtils.stringToBinary(crsStr));
  }
}
