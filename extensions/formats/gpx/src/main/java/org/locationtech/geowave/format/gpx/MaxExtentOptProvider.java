/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.gpx;

import java.nio.ByteBuffer;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.jts.geom.Geometry;
import com.beust.jcommander.Parameter;

public class MaxExtentOptProvider implements Persistable {
  @Parameter(
      names = "--maxLength",
      description = "Maximum extent (in both dimensions) for gpx track in degrees. Used to remove excessively long gpx tracks")
  private double maxExtent = Double.MAX_VALUE;

  @Override
  public byte[] toBinary() {
    final byte[] backingBuffer = new byte[Double.BYTES];
    final ByteBuffer buf = ByteBuffer.wrap(backingBuffer);
    buf.putDouble(maxExtent);
    return backingBuffer;
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    maxExtent = ByteBuffer.wrap(bytes).getDouble();
  }

  public double getMaxExtent() {
    return maxExtent;
  }

  public boolean filterMaxExtent(final Geometry geom) {
    return (geom.getEnvelopeInternal().maxExtent() < maxExtent);
  }
}
