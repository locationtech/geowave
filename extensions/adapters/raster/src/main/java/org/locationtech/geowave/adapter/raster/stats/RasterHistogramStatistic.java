/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.raster.stats;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.geotools.coverage.grid.GridCoverage2D;
import org.geotools.coverage.processing.AbstractOperation;
import org.geotools.coverage.processing.BaseStatisticsOperationJAI;
import org.geotools.coverage.processing.CoverageProcessor;
import org.geotools.coverage.processing.operation.Histogram;
import org.locationtech.geowave.adapter.raster.FitToIndexGridCoverage;
import org.locationtech.geowave.adapter.raster.RasterUtils;
import org.locationtech.geowave.adapter.raster.Resolution;
import org.locationtech.geowave.adapter.raster.plugin.GeoWaveGTRasterFormat;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.index.VarintUtils;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.DataTypeStatistic;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.statistics.StatisticsIngestCallback;
import org.locationtech.geowave.core.store.statistics.adapter.DataTypeStatisticType;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.Polygon;
import org.opengis.coverage.grid.GridCoverage;
import org.opengis.parameter.ParameterValueGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RasterHistogramStatistic extends
    DataTypeStatistic<RasterHistogramStatistic.RasterHistogramValue> {
  private static final Logger LOGGER = LoggerFactory.getLogger(RasterHistogramStatistic.class);
  public static final DataTypeStatisticType<RasterHistogramValue> STATS_TYPE =
      new DataTypeStatisticType<>("RASTER_HISTOGRAM");

  private HistogramConfig histogramConfig;

  public RasterHistogramStatistic() {
    super(STATS_TYPE);
  }

  public RasterHistogramStatistic(final String typeName) {
    super(STATS_TYPE, typeName);
  }

  public RasterHistogramStatistic(final String typeName, final HistogramConfig histogramConfig) {
    super(STATS_TYPE, typeName);
    this.histogramConfig = histogramConfig;
  }

  @Override
  public boolean isCompatibleWith(final Class<?> adapterClass) {
    return GridCoverage.class.isAssignableFrom(adapterClass);
  }

  @Override
  public String getDescription() {
    return "Histogram for raster data.";
  }

  @Override
  public RasterHistogramValue createEmpty() {
    return new RasterHistogramValue(this);
  }

  // Cache this so we don't have to serialize the histogram multiple times
  private byte[] histogramConfigBytes = null;

  @Override
  protected int byteLength() {
    if (histogramConfigBytes == null) {
      histogramConfigBytes = PersistenceUtils.toBinary(histogramConfig);
    }
    return super.byteLength()
        + histogramConfigBytes.length
        + VarintUtils.unsignedIntByteLength(histogramConfigBytes.length);
  }

  @Override
  protected void writeBytes(ByteBuffer buffer) {
    super.writeBytes(buffer);
    VarintUtils.writeUnsignedInt(histogramConfigBytes.length, buffer);
    buffer.put(histogramConfigBytes);
    histogramConfigBytes = null;
  }

  @Override
  protected void readBytes(ByteBuffer buffer) {
    super.readBytes(buffer);
    final byte[] configBinary =
        ByteArrayUtils.safeRead(buffer, VarintUtils.readUnsignedInt(buffer));
    histogramConfig = (HistogramConfig) PersistenceUtils.fromBinary(configBinary);
  }

  public static class RasterHistogramValue extends
      StatisticValue<Map<Resolution, javax.media.jai.Histogram>> implements
      StatisticsIngestCallback {
    private final Map<Resolution, javax.media.jai.Histogram> histograms = new HashMap<>();
    private HistogramConfig histogramConfig;

    public RasterHistogramValue() {
      super(null);
      this.histogramConfig = null;
    }

    private RasterHistogramValue(final RasterHistogramStatistic statistic) {
      super(statistic);
      this.histogramConfig = statistic.histogramConfig;
    }

    public Set<Resolution> getResolutions() {
      return histograms.keySet();
    }

    public javax.media.jai.Histogram getHistogram(final Resolution resolution) {
      return histograms.get(resolution);
    }

    @Override
    public void merge(Mergeable merge) {
      if ((merge != null) && (merge instanceof RasterHistogramValue)) {
        final Set<Resolution> resolutions = new HashSet<>(getResolutions());
        resolutions.addAll(((RasterHistogramValue) merge).getResolutions());
        for (final Resolution res : resolutions) {
          final javax.media.jai.Histogram otherHistogram =
              ((RasterHistogramValue) merge).getHistogram(res);
          final javax.media.jai.Histogram thisHistogram = getHistogram(res);
          if (otherHistogram != null) {
            javax.media.jai.Histogram mergedHistogram;
            if (thisHistogram != null) {
              mergedHistogram = mergeHistograms(thisHistogram, otherHistogram);
            } else {
              mergedHistogram = otherHistogram;
            }

            synchronized (this) {
              histograms.put(res, mergedHistogram);
            }
          }
        }
      }
    }

    @Override
    public <T> void entryIngested(DataTypeAdapter<T> adapter, T entry, GeoWaveRow... rows) {
      /*
       * Create the operation for the Histogram with a ROI. No subsampling should be applied.
       */
      final Geometry footprint;
      if (entry instanceof FitToIndexGridCoverage) {
        footprint = ((FitToIndexGridCoverage) entry).getFootprintWorldGeometry();
        if (footprint == null) {
          return;
        }
      } else {
        // this is a condition that isn't going to be exercised typically in
        // any code, but at this point we will assume default CRS
        footprint =
            RasterUtils.getFootprint((GridCoverage) entry, GeoWaveGTRasterFormat.DEFAULT_CRS);
      }

      final GridCoverage originalCoverage;
      Resolution resolution = null;
      if (entry instanceof FitToIndexGridCoverage) {
        originalCoverage = ((FitToIndexGridCoverage) entry).getOriginalCoverage();
        resolution = ((FitToIndexGridCoverage) entry).getResolution();
      } else {
        originalCoverage = (GridCoverage) entry;
      }
      if (footprint instanceof GeometryCollection) {
        final GeometryCollection collection = (GeometryCollection) footprint;
        for (int g = 0; g < collection.getNumGeometries(); g++) {
          final Geometry geom = collection.getGeometryN(g);
          if (geom instanceof Polygon) {
            mergePoly(originalCoverage, (Polygon) geom, resolution);
          }
        }
      } else if (footprint instanceof Polygon) {
        mergePoly(originalCoverage, (Polygon) footprint, resolution);
      }
    }

    @Override
    public Map<Resolution, javax.media.jai.Histogram> getValue() {
      return histograms;
    }

    private void mergePoly(
        final GridCoverage originalCoverage,
        final Polygon poly,
        final Resolution resolution) {
      final CoverageProcessor processor = CoverageProcessor.getInstance();
      final AbstractOperation op = (AbstractOperation) processor.getOperation("Histogram");
      final ParameterValueGroup params = op.getParameters();
      params.parameter("Source").setValue(originalCoverage);
      params.parameter(BaseStatisticsOperationJAI.ROI.getName().getCode()).setValue(poly);
      params.parameter("lowValue").setValue(histogramConfig.getLowValues());
      params.parameter("highValue").setValue(histogramConfig.getHighValues());
      params.parameter("numBins").setValue(histogramConfig.getNumBins());
      try {

        final GridCoverage2D coverage = (GridCoverage2D) op.doOperation(params, null);
        final javax.media.jai.Histogram histogram =
            (javax.media.jai.Histogram) coverage.getProperty(
                Histogram.GT_SYNTHETIC_PROPERTY_HISTOGRAM);

        javax.media.jai.Histogram mergedHistogram;
        final javax.media.jai.Histogram resolutionHistogram = histograms.get(resolution);
        if (resolutionHistogram != null) {
          mergedHistogram = mergeHistograms(resolutionHistogram, histogram);
        } else {
          mergedHistogram = histogram;
        }
        synchronized (this) {
          histograms.put(resolution, mergedHistogram);
        }
      } catch (final Exception e) {
        // this is simply 'info' because there is a known issue in the
        // histogram op when the ROI is so small that the resulting cropped
        // pixel size is 0
        LOGGER.info(
            "This is often a non-issue relating to applying an ROI calculation that results in 0 pixels (the error is in calculating stats).",
            e);
      }
    }

    private static javax.media.jai.Histogram mergeHistograms(
        final javax.media.jai.Histogram histogram1,
        final javax.media.jai.Histogram histogram2) {
      final int numBands = Math.min(histogram1.getNumBands(), histogram2.getNumBands());
      final double[] lowValue1 = histogram1.getLowValue();
      final double[] lowValue2 = histogram2.getLowValue();
      final double[] lowValue = new double[numBands];
      for (int b = 0; b < numBands; b++) {
        lowValue[b] = Math.min(lowValue1[b], lowValue2[b]);
      }
      final double[] highValue1 = histogram1.getHighValue();
      final double[] highValue2 = histogram2.getHighValue();
      final double[] highValue = new double[numBands];
      for (int b = 0; b < numBands; b++) {
        highValue[b] = Math.max(highValue1[b], highValue2[b]);
      }
      final int[][] bins1 = histogram1.getBins();
      final int[][] bins2 = histogram2.getBins();
      final int[] numBins = new int[numBands];
      for (int b = 0; b < numBands; b++) {
        numBins[b] = Math.min(bins1[b].length, bins2[b].length);
      }
      final javax.media.jai.Histogram histogram =
          new javax.media.jai.Histogram(numBins, lowValue, highValue);
      for (int b = 0; b < numBands; b++) {
        // this is a bit of a hack, but the only way to interact with the
        // counts in a mutable way is by getting an array of the bin counts
        // and setting values in the array
        final int[] bins = histogram.getBins(b);
        for (int i = 0; i < bins.length; i++) {
          bins[i] = bins1[b][i] + bins2[b][i];
        }
      }
      return histogram;
    }

    @Override
    public byte[] toBinary() {
      final List<byte[]> perEntryBinary = new ArrayList<>();
      int totalBytes = 0;
      for (final Entry<Resolution, javax.media.jai.Histogram> entry : histograms.entrySet()) {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] keyBytes;
        byte[] valueBytes = new byte[] {};
        if (entry.getKey() != null) {
          keyBytes = PersistenceUtils.toBinary(entry.getKey());
        } else {
          keyBytes = new byte[] {};
        }
        if (entry.getValue() != null) {
          ObjectOutputStream oos;
          try {
            oos = new ObjectOutputStream(baos);
            oos.writeObject(entry.getValue());
            oos.close();
            baos.close();
            valueBytes = baos.toByteArray();
          } catch (final IOException e) {
            LOGGER.warn("Unable to write histogram", e);
          }
        }
        // 8 for key and value lengths as ints

        final int entryBytes =
            VarintUtils.unsignedIntByteLength(keyBytes.length)
                + VarintUtils.unsignedIntByteLength(valueBytes.length)
                + keyBytes.length
                + valueBytes.length;
        final ByteBuffer buf = ByteBuffer.allocate(entryBytes);
        VarintUtils.writeUnsignedInt(keyBytes.length, buf);
        buf.put(keyBytes);
        VarintUtils.writeUnsignedInt(valueBytes.length, buf);
        buf.put(valueBytes);
        perEntryBinary.add(buf.array());
        totalBytes += entryBytes;
      }
      totalBytes += VarintUtils.unsignedIntByteLength(perEntryBinary.size());
      final ByteBuffer buf = ByteBuffer.allocate(totalBytes);
      VarintUtils.writeUnsignedInt(perEntryBinary.size(), buf);
      for (final byte[] entryBinary : perEntryBinary) {
        buf.put(entryBinary);
      }
      return buf.array();
    }

    @Override
    public void fromBinary(byte[] bytes) {
      final ByteBuffer buf = ByteBuffer.wrap(bytes);
      final int numEntries = VarintUtils.readUnsignedInt(buf);
      ByteArrayUtils.verifyBufferSize(buf, numEntries);
      for (int i = 0; i < numEntries; i++) {
        final int keyLength = VarintUtils.readUnsignedInt(buf);
        Resolution key = null;
        if (keyLength > 0) {
          final byte[] keyBytes = ByteArrayUtils.safeRead(buf, keyLength);
          key = (Resolution) PersistenceUtils.fromBinary(keyBytes);
        }
        final int valueLength = VarintUtils.readUnsignedInt(buf);
        javax.media.jai.Histogram histogram = null;
        if (valueLength > 0) {

          final byte[] valueBytes = ByteArrayUtils.safeRead(buf, valueLength);
          ObjectInputStream ois;
          try {
            ois = new ObjectInputStream(new ByteArrayInputStream(valueBytes));
            histogram = (javax.media.jai.Histogram) ois.readObject();
          } catch (IOException | ClassNotFoundException e) {
            LOGGER.warn("Unable to read histogram", e);
          }
        }
        histograms.put(key, histogram);
      }
    }
  }
}
