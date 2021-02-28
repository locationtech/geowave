/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics.field;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.zip.DataFormatException;
import org.HdrHistogram.AbstractHistogram;
import org.HdrHistogram.DoubleHistogram;
import org.HdrHistogram.Histogram;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.statistics.StatisticsIngestCallback;

/**
 * Dynamic histogram provide very high accuracy for CDF and quantiles over the a numeric attribute.
 */
public class NumericHistogramStatistic extends
    FieldStatistic<NumericHistogramStatistic.NumericHistogramValue> {
  public static final FieldStatisticType<NumericHistogramValue> STATS_TYPE =
      new FieldStatisticType<>("NUMERIC_HISTOGRAM");

  public NumericHistogramStatistic() {
    super(STATS_TYPE);
  }

  public NumericHistogramStatistic(final String typeName, final String fieldName) {
    super(STATS_TYPE, typeName, fieldName);
  }

  @Override
  public String getDescription() {
    return "Provides a dynamic histogram for very high accuracy CDF and quantiles over a numeric attribute.";
  }

  @Override
  public NumericHistogramValue createEmpty() {
    return new NumericHistogramValue(this);
  }

  @Override
  public boolean isCompatibleWith(Class<?> fieldClass) {
    return fieldClass.isAssignableFrom(Number.class) || fieldClass.isAssignableFrom(Date.class);
  }

  public static class NumericHistogramValue extends
      StatisticValue<Pair<DoubleHistogram, DoubleHistogram>> implements
      StatisticsIngestCallback {
    // Max value is determined by the level of accuracy required, using a
    // formula provided
    private final double maxValue = (Math.pow(2, 63) / Math.pow(2, 14)) - 1;
    private final double minValue = -(maxValue);

    private DoubleHistogram positiveHistogram = new LocalDoubleHistogram();
    private DoubleHistogram negativeHistogram = null;

    public NumericHistogramValue() {
      this(null);
    }

    private NumericHistogramValue(final Statistic<?> statistic) {
      super(statistic);
    }

    private double percentageNegative() {
      final long nc = negativeHistogram == null ? 0 : negativeHistogram.getTotalCount();
      final long tc = positiveHistogram.getTotalCount() + nc;
      return (double) nc / (double) tc;
    }

    public double[] quantile(final int bins) {
      final double[] result = new double[bins];
      final double binSize = 1.0 / bins;
      for (int bin = 0; bin < bins; bin++) {
        result[bin] = quantile(binSize * (bin + 1));
      }
      return result;
    }

    public double cdf(final double val) {
      final double percentageNegative = percentageNegative();
      if ((val < 0) || ((1.0 - percentageNegative) < 0.000000001)) {
        // subtract one from percentage since negative is negated so
        // percentage is inverted
        return (percentageNegative > 0)
            ? percentageNegative
                * (1.0 - (negativeHistogram.getPercentileAtOrBelowValue(-val) / 100.0))
            : 0.0;
      } else {
        return percentageNegative
            + ((1.0 - percentageNegative)
                * (positiveHistogram.getPercentileAtOrBelowValue(val) / 100.0));
      }
    }

    public double quantile(final double percentage) {
      final double percentageNegative = percentageNegative();
      if (percentage < percentageNegative) {
        // subtract one from percentage since negative is negated so
        // percentage is inverted
        return -negativeHistogram.getValueAtPercentile(
            (1.0 - (percentage / percentageNegative)) * 100.0);
      } else {
        return positiveHistogram.getValueAtPercentile(
            (percentage / (1.0 - percentageNegative)) * 100.0);
      }
    }

    public double percentPopulationOverRange(final double start, final double stop) {
      return cdf(stop) - cdf(start);
    }

    public long totalSampleSize() {
      return positiveHistogram.getTotalCount()
          + (negativeHistogram == null ? 0 : negativeHistogram.getTotalCount());
    }

    public long[] count(final int bins) {
      final long[] result = new long[bins];
      final double max = positiveHistogram.getMaxValue();
      final double min =
          negativeHistogram == null ? positiveHistogram.getMinValue()
              : -negativeHistogram.getMaxValue();
      final double binSize = (max - min) / (bins);
      long last = 0;
      final long tc = totalSampleSize();
      for (int bin = 0; bin < bins; bin++) {
        final double val = cdf(min + ((bin + 1.0) * binSize)) * tc;
        final long next = (long) val - last;
        result[bin] = next;
        last += next;
      }
      return result;
    }

    private DoubleHistogram getNegativeHistogram() {
      if (negativeHistogram == null) {
        negativeHistogram = new LocalDoubleHistogram();
      }
      return negativeHistogram;
    }

    @Override
    public void merge(Mergeable merge) {
      if (merge instanceof NumericHistogramValue) {
        positiveHistogram.add(((NumericHistogramValue) merge).positiveHistogram);
        if (((NumericHistogramValue) merge).negativeHistogram != null) {
          if (negativeHistogram != null) {
            negativeHistogram.add(((NumericHistogramValue) merge).negativeHistogram);
          } else {
            negativeHistogram = ((NumericHistogramValue) merge).negativeHistogram;
          }
        }
      }
    }

    @Override
    public <T> void entryIngested(DataTypeAdapter<T> adapter, T entry, GeoWaveRow... rows) {
      final Object o =
          adapter.getFieldValue(entry, ((NumericHistogramStatistic) statistic).getFieldName());
      if (o == null) {
        return;
      }
      if (o instanceof Date) {
        add(((Date) o).getTime());
      } else if (o instanceof Number) {
        add(((Number) o).doubleValue());
      }
    }

    protected void add(final double num) {
      if ((num < minValue) || (num > maxValue) || Double.isNaN(num)) {
        return;
      }
      if (num >= 0) {
        positiveHistogram.recordValue(num);
      } else {
        getNegativeHistogram().recordValue(-num);
      }
    }

    @Override
    public Pair<DoubleHistogram, DoubleHistogram> getValue() {
      return Pair.of(negativeHistogram, positiveHistogram);
    }

    @Override
    public byte[] toBinary() {
      final int positiveBytes = positiveHistogram.getEstimatedFootprintInBytes();
      final int bytesNeeded =
          positiveBytes
              + (negativeHistogram == null ? 0 : negativeHistogram.getEstimatedFootprintInBytes());
      final ByteBuffer buffer = ByteBuffer.allocate(bytesNeeded + 5);
      final int startPosition = buffer.position();
      buffer.putInt(startPosition); // buffer out an int
      positiveHistogram.encodeIntoCompressedByteBuffer(buffer);
      final int endPosition = buffer.position();
      buffer.position(startPosition);
      buffer.putInt(endPosition);
      buffer.position(endPosition);
      if (negativeHistogram != null) {
        buffer.put((byte) 0x01);
        negativeHistogram.encodeIntoCompressedByteBuffer(buffer);
      } else {
        buffer.put((byte) 0x00);
      }
      final byte result[] = new byte[buffer.position() + 1];
      buffer.rewind();
      buffer.get(result);
      return result;
    }

    @Override
    public void fromBinary(byte[] bytes) {
      final ByteBuffer buffer = ByteBuffer.wrap(bytes);
      final int endPosition = buffer.getInt();
      try {
        positiveHistogram =
            DoubleHistogram.decodeFromCompressedByteBuffer(buffer, LocalInternalHistogram.class, 0);
        buffer.position(endPosition);
        positiveHistogram.setAutoResize(true);
        if (buffer.get() == (byte) 0x01) {
          negativeHistogram =
              DoubleHistogram.decodeFromCompressedByteBuffer(
                  buffer,
                  LocalInternalHistogram.class,
                  0);
          negativeHistogram.setAutoResize(true);
        }
      } catch (final DataFormatException e) {
        throw new RuntimeException("Cannot decode statistic", e);
      }
    }
  }

  public static class LocalDoubleHistogram extends DoubleHistogram {

    public LocalDoubleHistogram() {
      super(2, 4, LocalInternalHistogram.class);
      super.setAutoResize(true);
    }

    /** */
    private static final long serialVersionUID = 5504684423053828467L;
  }

  @edu.umd.cs.findbugs.annotations.SuppressFBWarnings(value = {"HE_INHERITS_EQUALS_USE_HASHCODE"})
  public static class LocalInternalHistogram extends Histogram {
    /** */
    private static final long serialVersionUID = 4369054277576423915L;

    public LocalInternalHistogram(final AbstractHistogram source) {
      super(source);
      source.setAutoResize(true);
      super.setAutoResize(true);
    }

    public LocalInternalHistogram(final int numberOfSignificantValueDigits) {
      super(numberOfSignificantValueDigits);
      super.setAutoResize(true);
    }

    public LocalInternalHistogram(
        final long highestTrackableValue,
        final int numberOfSignificantValueDigits) {
      super(highestTrackableValue, numberOfSignificantValueDigits);
      super.setAutoResize(true);
    }

    public LocalInternalHistogram(
        final long lowestDiscernibleValue,
        final long highestTrackableValue,
        final int numberOfSignificantValueDigits) {
      super(lowestDiscernibleValue, highestTrackableValue, numberOfSignificantValueDigits);
      super.setAutoResize(true);
    }
  }
}
