/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics.index;

import java.nio.ByteBuffer;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.store.adapter.statistics.histogram.ByteUtils;
import org.locationtech.geowave.core.store.adapter.statistics.histogram.NumericHistogram;
import org.locationtech.geowave.core.store.adapter.statistics.histogram.TDigestNumericHistogram;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.IndexStatistic;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.statistics.StatisticsIngestCallback;

/**
 * Dynamic histogram provide very high accuracy for CDF and quantiles over the a numeric attribute.
 */
public class RowRangeHistogramStatistic extends
    IndexStatistic<RowRangeHistogramStatistic.RowRangeHistogramValue> {
  public static final IndexStatisticType<RowRangeHistogramValue> STATS_TYPE =
      new IndexStatisticType<>("ROW_RANGE_HISTOGRAM");

  public RowRangeHistogramStatistic() {
    super(STATS_TYPE);
  }

  public RowRangeHistogramStatistic(final String indexName) {
    super(STATS_TYPE, indexName);
  }

  @Override
  public String getDescription() {
    return "Provides a histogram of row ranges.";
  }

  @Override
  public RowRangeHistogramValue createEmpty() {
    return new RowRangeHistogramValue(this);
  }

  public static class RowRangeHistogramValue extends StatisticValue<NumericHistogram> implements
      StatisticsIngestCallback {
    private NumericHistogram histogram;

    public RowRangeHistogramValue() {
      this(null);
    }

    public RowRangeHistogramValue(final Statistic<?> statistic) {
      super(statistic);
      histogram = createHistogram();
    }

    public double cardinality(final byte[] start, final byte[] end) {
      final double startSum = start == null ? 0 : histogram.sum(ByteUtils.toDouble(start), true);;
      final double endSum =
          end == null ? histogram.getTotalCount()
              : histogram.sum(ByteUtils.toDoubleAsNextPrefix(end), true);
      return endSum - startSum;
    }

    public double[] quantile(final int bins) {
      final double[] result = new double[bins];
      final double binSize = 1.0 / bins;
      for (int bin = 0; bin < bins; bin++) {
        result[bin] = quantile(binSize * (bin + 1));
      }
      return result;
    }

    public double cdf(final byte[] id) {
      return histogram.cdf(ByteUtils.toDouble(id));
    }

    public double quantile(final double percentage) {
      return histogram.quantile((percentage));
    }

    public double percentPopulationOverRange(final byte[] start, final byte[] stop) {
      return cdf(stop) - cdf(start);
    }

    public long getTotalCount() {
      return histogram.getTotalCount();
    }

    @Override
    public void merge(Mergeable merge) {
      if (merge instanceof RowRangeHistogramValue) {
        final NumericHistogram otherHistogram = ((RowRangeHistogramValue) merge).histogram;
        if (histogram == null) {
          histogram = otherHistogram;
        } else if (otherHistogram != null) {
          histogram.merge(otherHistogram);
        }
      }
    }

    @Override
    public <T> void entryIngested(DataTypeAdapter<T> adapter, T entry, GeoWaveRow... rows) {
      for (final GeoWaveRow kv : rows) {
        final byte[] idBytes = kv.getSortKey();
        histogram.add(ByteUtils.toDouble(idBytes));
      }
    }

    @Override
    public NumericHistogram getValue() {
      return histogram;
    }

    @Override
    public byte[] toBinary() {
      final ByteBuffer buffer = ByteBuffer.allocate(histogram.bufferSize());
      histogram.toBinary(buffer);
      return buffer.array();
    }

    @Override
    public void fromBinary(byte[] bytes) {
      final ByteBuffer buffer = ByteBuffer.wrap(bytes);
      if (buffer.hasRemaining()) {
        histogram.fromBinary(buffer);
      }
    }
  }

  private static NumericHistogram createHistogram() {
    return new TDigestNumericHistogram();
  }
}
