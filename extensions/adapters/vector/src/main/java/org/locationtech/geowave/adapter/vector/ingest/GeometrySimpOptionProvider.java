/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.ingest;

import java.nio.ByteBuffer;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.simplify.DouglasPeuckerSimplifier;
import com.beust.jcommander.Parameter;

public class GeometrySimpOptionProvider implements Persistable {
  @Parameter(
      names = "--maxVertices",
      description = "Maximum number of vertices to allow for the feature. Features with over this vertice count will be discarded.")
  private int maxVertices = Integer.MAX_VALUE;

  @Parameter(
      names = "--minSimpVertices",
      description = "Minimum vertex count to qualify for geometry simplification.")
  private int simpVertMin = Integer.MAX_VALUE;

  @Parameter(
      names = "--tolerance",
      description = "Maximum error tolerance in geometry simplification. Should range from 0.0 to 1.0 (i.e. .1 = 10%)")
  private double tolerance = 0.02;

  public Geometry simplifyGeometry(final Geometry geom) {
    if (geom.getCoordinates().length > simpVertMin) {
      return DouglasPeuckerSimplifier.simplify(geom, tolerance);
    }
    return geom;
  }

  public boolean filterGeometry(final Geometry geom) {
    return ((geom.getCoordinates().length < maxVertices) && !geom.isEmpty() && geom.isValid());
  }

  @Override
  public byte[] toBinary() {
    final byte[] backingBuffer =
        new byte[VarintUtils.unsignedIntByteLength(maxVertices)
            + VarintUtils.unsignedIntByteLength(simpVertMin)
            + Double.BYTES];
    final ByteBuffer buf = ByteBuffer.wrap(backingBuffer);
    VarintUtils.writeUnsignedInt(maxVertices, buf);
    VarintUtils.writeUnsignedInt(simpVertMin, buf);
    buf.putDouble(tolerance);
    return backingBuffer;
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = ByteBuffer.wrap(bytes);
    maxVertices = VarintUtils.readUnsignedInt(buf);
    simpVertMin = VarintUtils.readUnsignedInt(buf);
    tolerance = buf.getDouble();
  }

  public int getMaxVertices() {
    return maxVertices;
  }

  public int getSimpLimit() {
    return simpVertMin;
  }

  public double getTolerance() {
    return tolerance;
  }
}
