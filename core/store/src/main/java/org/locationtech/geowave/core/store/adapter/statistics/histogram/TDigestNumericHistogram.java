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
import com.tdunning.math.stats.MergingDigest;
import com.tdunning.math.stats.TDigest;

public class TDigestNumericHistogram implements NumericHistogram {
  private static final double DEFAULT_COMPRESSION = 100;
  private TDigest tdigest;

  public TDigestNumericHistogram() {
    this(DEFAULT_COMPRESSION);
  }

  public TDigestNumericHistogram(final double compression) {
    super();
    tdigest = TDigest.createMergingDigest(DEFAULT_COMPRESSION);
  }

  @Override
  public void merge(final NumericHistogram other) {
    if ((other instanceof TDigestNumericHistogram) && (other.getTotalCount() > 0)) {
      tdigest.add(((TDigestNumericHistogram) other).tdigest);
    }
  }

  @Override
  public void add(final double v) {
    tdigest.add(v);
  }

  @Override
  public double quantile(final double q) {
    return tdigest.quantile(q);
  }

  @Override
  public double cdf(final double val) {
    return tdigest.cdf(val);
  }

  @Override
  public int bufferSize() {
    return tdigest.smallByteSize();
  }

  @Override
  public void toBinary(final ByteBuffer buffer) {
    tdigest.asSmallBytes(buffer);
  }

  @Override
  public void fromBinary(final ByteBuffer buffer) {
    tdigest = MergingDigest.fromBytes(buffer);
  }

  @Override
  public double getMaxValue() {
    return tdigest.getMax();
  }

  @Override
  public double getMinValue() {
    return tdigest.getMin();
  }

  @Override
  public long getTotalCount() {
    return tdigest.size();
  }

  @Override
  public double sum(final double val, final boolean inclusive) {
    return tdigest.cdf(val) * tdigest.size();
  }

  @Override
  public String toString() {
    return NumericHistogram.histogramToString(this);
  }

  public TDigest getTdigest() {
    return tdigest;
  }
}
