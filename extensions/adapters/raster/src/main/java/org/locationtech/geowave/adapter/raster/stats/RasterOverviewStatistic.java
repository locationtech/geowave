/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.raster.stats;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;
import org.apache.commons.lang3.ArrayUtils;
import org.locationtech.geowave.adapter.raster.FitToIndexGridCoverage;
import org.locationtech.geowave.adapter.raster.Resolution;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.DataTypeStatistic;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.statistics.StatisticsIngestCallback;
import org.locationtech.geowave.core.store.statistics.adapter.DataTypeStatisticType;
import org.opengis.coverage.grid.GridCoverage;

public class RasterOverviewStatistic extends
    DataTypeStatistic<RasterOverviewStatistic.RasterOverviewValue> {
  public static final DataTypeStatisticType<RasterOverviewValue> STATS_TYPE =
      new DataTypeStatisticType<>("RASTER_OVERVIEW");


  public RasterOverviewStatistic() {
    super(STATS_TYPE);
  }

  public RasterOverviewStatistic(final String typeName) {
    super(STATS_TYPE, typeName);
  }

  @Override
  public boolean isCompatibleWith(final Class<?> adapterClass) {
    return GridCoverage.class.isAssignableFrom(adapterClass);
  }

  @Override
  public String getDescription() {
    return "Provides an overview of the resolutions of a raster dataset.";
  }

  @Override
  public RasterOverviewValue createEmpty() {
    return new RasterOverviewValue(this);
  }

  public static class RasterOverviewValue extends StatisticValue<Resolution[]> implements
      StatisticsIngestCallback {
    private Resolution[] resolutions = new Resolution[] {};

    public RasterOverviewValue() {
      this(null);
    }

    public RasterOverviewValue(final Statistic<?> statistic) {
      super(statistic);
    }

    public boolean removeResolution(Resolution res) {
      synchronized (this) {
        int index = -1;
        for (int i = 0; i < resolutions.length; i++) {
          if (Arrays.equals(
              resolutions[i].getResolutionPerDimension(),
              res.getResolutionPerDimension())) {
            index = i;
            break;
          }
        }
        if (index >= 0) {
          resolutions = ArrayUtils.remove(resolutions, index);
          return true;
        }
        return false;
      }
    }

    @Override
    public void merge(Mergeable merge) {
      if (merge instanceof RasterOverviewValue) {
        synchronized (this) {
          resolutions =
              incorporateResolutions(resolutions, ((RasterOverviewValue) merge).getValue());
        }
      }
    }

    @Override
    public <T> void entryIngested(DataTypeAdapter<T> adapter, T entry, GeoWaveRow... rows) {
      if (entry instanceof FitToIndexGridCoverage) {
        final FitToIndexGridCoverage fitEntry = (FitToIndexGridCoverage) entry;
        synchronized (this) {
          resolutions =
              incorporateResolutions(resolutions, new Resolution[] {fitEntry.getResolution()});
        }
      }
    }

    @Override
    public Resolution[] getValue() {
      synchronized (this) {
        return resolutions;
      }
    }

    @Override
    public byte[] toBinary() {
      synchronized (this) {
        final List<byte[]> resolutionBinaries = new ArrayList<>(resolutions.length);
        int byteCount = 0; // an int for the list size
        for (final Resolution res : resolutions) {
          final byte[] resBinary = PersistenceUtils.toBinary(res);
          resolutionBinaries.add(resBinary);
          byteCount += (resBinary.length + VarintUtils.unsignedIntByteLength(resBinary.length)); // an
          // int
          // for
          // the
          // binary
          // size
        }
        byteCount += VarintUtils.unsignedIntByteLength(resolutionBinaries.size());

        final ByteBuffer buf = ByteBuffer.allocate(byteCount);
        VarintUtils.writeUnsignedInt(resolutionBinaries.size(), buf);
        for (final byte[] resBinary : resolutionBinaries) {
          VarintUtils.writeUnsignedInt(resBinary.length, buf);
          buf.put(resBinary);
        }
        return buf.array();
      }
    }

    @Override
    public void fromBinary(byte[] bytes) {
      final ByteBuffer buf = ByteBuffer.wrap(bytes);
      final int resLength = VarintUtils.readUnsignedInt(buf);
      synchronized (this) {
        resolutions = new Resolution[resLength];
        for (int i = 0; i < resolutions.length; i++) {
          final byte[] resBytes = ByteArrayUtils.safeRead(buf, VarintUtils.readUnsignedInt(buf));
          resolutions[i] = (Resolution) PersistenceUtils.fromBinary(resBytes);
        }
      }
    }

  }

  private static Resolution[] incorporateResolutions(
      final Resolution[] res1,
      final Resolution[] res2) {
    final TreeSet<Resolution> resolutionSet = new TreeSet<>();
    for (final Resolution res : res1) {
      resolutionSet.add(res);
    }
    for (final Resolution res : res2) {
      resolutionSet.add(res);
    }
    final Resolution[] combinedRes = new Resolution[resolutionSet.size()];
    int i = 0;
    for (final Resolution res : resolutionSet) {
      combinedRes[i++] = res;
    }
    return combinedRes;
  }
}
