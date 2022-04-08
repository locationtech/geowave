/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.raster.stats;

import org.geotools.geometry.GeneralEnvelope;
import org.locationtech.geowave.adapter.raster.FitToIndexGridCoverage;
import org.locationtech.geowave.core.geotime.store.statistics.AbstractBoundingBoxValue;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.DataTypeStatistic;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.statistics.adapter.DataTypeStatisticType;
import org.locationtech.jts.geom.Envelope;
import org.opengis.coverage.grid.GridCoverage;

public class RasterBoundingBoxStatistic extends
    DataTypeStatistic<RasterBoundingBoxStatistic.RasterBoundingBoxValue> {
  public static final DataTypeStatisticType<RasterBoundingBoxValue> STATS_TYPE =
      new DataTypeStatisticType<>("RASTER_BOUNDING_BOX");

  public RasterBoundingBoxStatistic() {
    super(STATS_TYPE);
  }

  public RasterBoundingBoxStatistic(final String typeName) {
    super(STATS_TYPE, typeName);
  }

  @Override
  public String getDescription() {
    return "Maintains a bounding box for a raster data set.";
  }

  @Override
  public boolean isCompatibleWith(final Class<?> adapterClass) {
    return GridCoverage.class.isAssignableFrom(adapterClass);
  }

  @Override
  public RasterBoundingBoxValue createEmpty() {
    return new RasterBoundingBoxValue(this);
  }

  public static class RasterBoundingBoxValue extends AbstractBoundingBoxValue {

    public RasterBoundingBoxValue() {
      this(null);
    }

    public RasterBoundingBoxValue(final Statistic<?> statistic) {
      super(statistic);
    }

    @Override
    public <T> Envelope getEnvelope(
        final DataTypeAdapter<T> adapter,
        final T entry,
        final GeoWaveRow... rows) {
      if (entry instanceof GridCoverage) {
        final org.opengis.geometry.Envelope indexedEnvelope = ((GridCoverage) entry).getEnvelope();
        final org.opengis.geometry.Envelope originalEnvelope;
        if (entry instanceof FitToIndexGridCoverage) {
          originalEnvelope = ((FitToIndexGridCoverage) entry).getOriginalEnvelope();
        } else {
          originalEnvelope = null;
        }
        // we don't want to accumulate the envelope outside of the original if
        // it is fit to the index, so compute the intersection with the original
        // envelope
        final org.opengis.geometry.Envelope resultingEnvelope =
            getIntersection(originalEnvelope, indexedEnvelope);
        if (resultingEnvelope != null) {
          return new Envelope(
              resultingEnvelope.getMinimum(0),
              resultingEnvelope.getMaximum(0),
              resultingEnvelope.getMinimum(1),
              resultingEnvelope.getMaximum(1));
        }
      }
      return null;
    }

  }

  private static org.opengis.geometry.Envelope getIntersection(
      final org.opengis.geometry.Envelope originalEnvelope,
      final org.opengis.geometry.Envelope indexedEnvelope) {
    if (originalEnvelope == null) {
      return indexedEnvelope;
    }
    if (indexedEnvelope == null) {
      return originalEnvelope;
    }
    final int dimensions = originalEnvelope.getDimension();
    final double[] minDP = new double[dimensions];
    final double[] maxDP = new double[dimensions];
    for (int d = 0; d < dimensions; d++) {
      // to perform the intersection of the original envelope and the
      // indexed envelope, use the max of the mins per dimension and the
      // min of the maxes
      minDP[d] = Math.max(originalEnvelope.getMinimum(d), indexedEnvelope.getMinimum(d));
      maxDP[d] = Math.min(originalEnvelope.getMaximum(d), indexedEnvelope.getMaximum(d));
    }
    return new GeneralEnvelope(minDP, maxDP);
  }
}
