/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics.field;

import java.nio.ByteBuffer;
import java.util.Date;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.store.adapter.statistics.histogram.NumericHistogram;
import org.locationtech.geowave.core.store.adapter.statistics.histogram.TDigestNumericHistogram;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.FieldStatistic;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.statistics.StatisticsIngestCallback;
import com.beust.jcommander.Parameter;

/**
 * Uses a T-Digest data structure to very efficiently calculate and store the histogram.
 * https://www.sciencedirect.com/science/article/pii/S2665963820300403
 *
 * <p> The default compression is 100.
 */
public class NumericHistogramStatistic extends
    FieldStatistic<NumericHistogramStatistic.NumericHistogramValue> {

  public static final FieldStatisticType<NumericHistogramValue> STATS_TYPE =
      new FieldStatisticType<>("NUMERIC_HISTOGRAM");

  @Parameter(
      names = "--compression",
      description = "The compression parameter. 100 is a common value for normal uses. 1000 is extremely large. The number of centroids retained will be a smallish (usually less than 10) multiple of this number.")
  private double compression = 100;

  public NumericHistogramStatistic() {
    super(STATS_TYPE);
  }

  public NumericHistogramStatistic(final String typeName, final String fieldName) {
    this(typeName, fieldName, 100);
  }

  public NumericHistogramStatistic(
      final String typeName,
      final String fieldName,
      final double compression) {
    super(STATS_TYPE, typeName, fieldName);
    this.compression = compression;
  }

  public void setCompression(final double compression) {
    this.compression = compression;
  }

  public double getCompression() {
    return compression;
  }

  @Override
  public boolean isCompatibleWith(final Class<?> fieldClass) {
    return Number.class.isAssignableFrom(fieldClass) || Date.class.isAssignableFrom(fieldClass);
  }

  @Override
  public String getDescription() {
    return "A numeric histogram using an efficient t-digest data structure.";
  }

  @Override
  public NumericHistogramValue createEmpty() {
    return new NumericHistogramValue(this);
  }

  @Override
  protected int byteLength() {
    return super.byteLength() + Double.BYTES;
  }

  @Override
  protected void writeBytes(ByteBuffer buffer) {
    super.writeBytes(buffer);
    buffer.putDouble(compression);
  }

  @Override
  protected void readBytes(ByteBuffer buffer) {
    super.readBytes(buffer);
    compression = buffer.getDouble();
  }

  public static class NumericHistogramValue extends StatisticValue<NumericHistogram> implements
      StatisticsIngestCallback {

    private TDigestNumericHistogram histogram;

    public NumericHistogramValue() {
      super(null);
      histogram = null;
    }

    public NumericHistogramValue(final NumericHistogramStatistic statistic) {
      super(statistic);
      histogram = new TDigestNumericHistogram(statistic.compression);
    }

    @Override
    public void merge(final Mergeable merge) {
      if ((merge != null) && (merge instanceof NumericHistogramValue)) {
        // here it is important not to use "getValue()" because we want to be able to check for
        // null, and not just get an empty histogram
        final TDigestNumericHistogram other = ((NumericHistogramValue) merge).histogram;
        if ((histogram != null) && (histogram.getTotalCount() > 0)) {
          if ((other != null) && (other.getTotalCount() > 0)) {
            histogram.merge(other);
          }
        } else {
          histogram = other;
        }

      }
    }

    @Override
    public <T> void entryIngested(
        final DataTypeAdapter<T> adapter,
        final T entry,
        final GeoWaveRow... rows) {
      final Object o =
          adapter.getFieldValue(entry, ((NumericHistogramStatistic) getStatistic()).getFieldName());
      if (o == null) {
        return;
      }
      double value;
      if (o instanceof Date) {
        value = ((Date) o).getTime();
      } else if (o instanceof Number) {
        value = ((Number) o).doubleValue();
      } else {
        return;
      }
      if (histogram == null) {
        histogram = new TDigestNumericHistogram();
      }
      histogram.add(value);
    }

    @Override
    public TDigestNumericHistogram getValue() {
      if (histogram == null) {
        histogram = new TDigestNumericHistogram();
      }
      return histogram;
    }

    @Override
    public byte[] toBinary() {
      if (histogram == null) {
        return new byte[0];
      }
      final ByteBuffer buffer = ByteBuffer.allocate(histogram.bufferSize());
      histogram.toBinary(buffer);
      return buffer.array();
    }

    @Override
    public void fromBinary(final byte[] bytes) {
      histogram = new TDigestNumericHistogram();
      if (bytes.length > 0) {
        histogram.fromBinary(ByteBuffer.wrap(bytes));
      }
    }

    public double[] quantile(final int bins) {
      return NumericHistogram.binQuantiles(histogram, bins);
    }

    public double cdf(final double val) {
      return histogram.cdf(val);
    }

    public double quantile(final double percentage) {
      return histogram.quantile(percentage);
    }

    public double percentPopulationOverRange(final double start, final double stop) {
      return cdf(stop) - cdf(start);
    }

    public long totalSampleSize() {
      return histogram.getTotalCount();
    }

    public long[] count(final int binSize) {
      return NumericHistogram.binCounts(histogram, binSize);
    }
  }
}
