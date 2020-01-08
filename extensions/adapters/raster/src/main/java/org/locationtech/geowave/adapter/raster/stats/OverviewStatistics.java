/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import org.apache.commons.lang3.ArrayUtils;
import org.locationtech.geowave.adapter.raster.FitToIndexGridCoverage;
import org.locationtech.geowave.adapter.raster.Resolution;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.adapter.statistics.AbstractDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.BaseStatisticsQueryBuilder;
import org.locationtech.geowave.core.store.adapter.statistics.BaseStatisticsType;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.opengis.coverage.grid.GridCoverage;

public class OverviewStatistics extends
    AbstractDataStatistics<GridCoverage, Resolution[], BaseStatisticsQueryBuilder<Resolution[]>> {
  public static final BaseStatisticsType<Resolution[]> STATS_TYPE =
      new BaseStatisticsType<>("OVERVIEW");

  private Resolution[] resolutions = new Resolution[] {};

  public OverviewStatistics() {
    this(null);
  }

  public OverviewStatistics(final Short adapterId) {
    super(adapterId, STATS_TYPE);
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

      final ByteBuffer buf = super.binaryBuffer(byteCount);
      VarintUtils.writeUnsignedInt(resolutionBinaries.size(), buf);
      for (final byte[] resBinary : resolutionBinaries) {
        VarintUtils.writeUnsignedInt(resBinary.length, buf);
        buf.put(resBinary);
      }
      return buf.array();
    }
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    final ByteBuffer buf = super.binaryBuffer(bytes);
    final int resLength = VarintUtils.readUnsignedInt(buf);
    synchronized (this) {
      resolutions = new Resolution[resLength];
      for (int i = 0; i < resolutions.length; i++) {
        final byte[] resBytes = ByteArrayUtils.safeRead(buf, VarintUtils.readUnsignedInt(buf));
        resolutions[i] = (Resolution) PersistenceUtils.fromBinary(resBytes);
      }
    }
  }

  @Override
  public void entryIngested(final GridCoverage entry, final GeoWaveRow... geoWaveRows) {
    if (entry instanceof FitToIndexGridCoverage) {
      final FitToIndexGridCoverage fitEntry = (FitToIndexGridCoverage) entry;
      synchronized (this) {
        resolutions =
            incorporateResolutions(resolutions, new Resolution[] {fitEntry.getResolution()});
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

  @Override
  public void merge(final Mergeable statistics) {
    if (statistics instanceof OverviewStatistics) {
      synchronized (this) {
        resolutions =
            incorporateResolutions(resolutions, ((OverviewStatistics) statistics).getResolutions());
      }
    }
  }

  public Resolution[] getResolutions() {
    synchronized (this) {
      return resolutions;
    }
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
  public Resolution[] getResult() {
    return getResolutions();
  }

  @Override
  protected String resultsName() {
    return "resolutions";
  }

  @Override
  protected Object resultsValue() {
    final Map<Integer, double[]> map = new HashMap<>();
    synchronized (this) {
      for (int i = 0; i < resolutions.length; i++) {
        map.put(i, resolutions[i].getResolutionPerDimension());
      }
    }
    return map;
  }
}
