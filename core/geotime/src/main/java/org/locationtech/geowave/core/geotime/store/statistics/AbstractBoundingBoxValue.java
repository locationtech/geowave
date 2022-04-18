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
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.statistics.StatisticsIngestCallback;
import org.locationtech.jts.geom.Envelope;

public abstract class AbstractBoundingBoxValue extends StatisticValue<Envelope> implements
    StatisticsIngestCallback {
  protected double minX = Double.MAX_VALUE;
  protected double minY = Double.MAX_VALUE;
  protected double maxX = -Double.MAX_VALUE;
  protected double maxY = -Double.MAX_VALUE;

  protected AbstractBoundingBoxValue(final Statistic<?> statistic) {
    super(statistic);
  }

  public boolean isSet() {
    if ((minX == Double.MAX_VALUE)
        || (minY == Double.MAX_VALUE)
        || (maxX == -Double.MAX_VALUE)
        || (maxY == -Double.MAX_VALUE)) {
      return false;
    }
    return true;
  }

  public double getMinX() {
    return minX;
  }

  public double getMinY() {
    return minY;
  }

  public double getMaxX() {
    return maxX;
  }

  public double getMaxY() {
    return maxY;
  }

  public double getWidth() {
    return maxX - minX;
  }

  public double getHeight() {
    return maxY - minY;
  }

  @Override
  public void merge(Mergeable merge) {
    if ((merge != null) && (merge instanceof AbstractBoundingBoxValue)) {
      final AbstractBoundingBoxValue bboxStats = (AbstractBoundingBoxValue) merge;
      if (bboxStats.isSet()) {
        minX = Math.min(minX, bboxStats.minX);
        minY = Math.min(minY, bboxStats.minY);
        maxX = Math.max(maxX, bboxStats.maxX);
        maxY = Math.max(maxY, bboxStats.maxY);
      }
    }
  }

  @Override
  public <T> void entryIngested(DataTypeAdapter<T> adapter, T entry, GeoWaveRow... rows) {
    final Envelope env = getEnvelope(adapter, entry);
    if (env != null) {
      minX = Math.min(minX, env.getMinX());
      minY = Math.min(minY, env.getMinY());
      maxX = Math.max(maxX, env.getMaxX());
      maxY = Math.max(maxY, env.getMaxY());
    }
  }

  public abstract <T> Envelope getEnvelope(DataTypeAdapter<T> adapter, T entry, GeoWaveRow... rows);

  @Override
  public Envelope getValue() {
    if (isSet()) {
      return new Envelope(minX, maxX, minY, maxY);
    } else {
      return new Envelope();
    }
  }

  @Override
  public byte[] toBinary() {
    final ByteBuffer buffer = ByteBuffer.allocate(32);
    buffer.putDouble(minX);
    buffer.putDouble(minY);
    buffer.putDouble(maxX);
    buffer.putDouble(maxY);
    return buffer.array();
  }

  @Override
  public void fromBinary(byte[] bytes) {
    final ByteBuffer buffer = ByteBuffer.wrap(bytes);
    minX = buffer.getDouble();
    minY = buffer.getDouble();
    maxX = buffer.getDouble();
    maxY = buffer.getDouble();
  }

}
