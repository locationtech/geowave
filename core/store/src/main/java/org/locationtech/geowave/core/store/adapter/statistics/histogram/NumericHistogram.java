/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.adapter.statistics.histogram;

import java.nio.ByteBuffer;

public interface NumericHistogram {
  void merge(final NumericHistogram other);

  /** @param v The data point to add to the histogram approximation. */
  void add(final double v);

  /**
   * Gets an approximate quantile value from the current histogram. Some popular quantiles are 0.5
   * (median), 0.95, and 0.98.
   *
   * @param q The requested quantile, must be strictly within the range (0,1).
   * @return The quantile value.
   */
  double quantile(final double q);

  /**
   * Returns the fraction of all points added which are <= x.
   *
   * @return the cumulative distribution function (cdf) result
   */
  double cdf(final double val);

  /**
   * Estimate number of values consumed up to provided value.
   *
   * @param val
   * @return the number of estimated points
   */
  double sum(final double val, boolean inclusive);

  /** @return the amount of byte buffer space to serialize this histogram */
  int bufferSize();

  void toBinary(final ByteBuffer buffer);

  void fromBinary(final ByteBuffer buffer);

  double getMaxValue();

  double getMinValue();

  long getTotalCount();

  static String histogramToString(final NumericHistogram histogram) {
    return "Numeric Histogram[Min: "
        + histogram.getMinValue()
        + ", Max: "
        + histogram.getMaxValue()
        + ", Median: "
        + histogram.quantile(0.5)
        + "]";
  }

  static double[] binQuantiles(final NumericHistogram histogram, final int bins) {
    final double[] result = new double[bins];
    final double binSize = 1.0 / bins;
    for (int bin = 0; bin < bins; bin++) {
      result[bin] = histogram.quantile(binSize * (bin + 1));
    }
    return result;
  }

  static long[] binCounts(final NumericHistogram histogram, final int bins) {
    final long[] result = new long[bins];
    double start = histogram.getMinValue();
    final double range = histogram.getMaxValue() - start;
    final double increment = range / bins;
    start += increment;
    long last = 0;
    for (int bin = 0; bin < bins; bin++, start += increment) {
      final long aggSum = (long) Math.ceil(histogram.sum(start, false));
      result[bin] = aggSum - last;
      last = aggSum;
    }
    return result;
  }
}
