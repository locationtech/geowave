/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.statistics.query;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.store.api.BinConstraints;
import org.locationtech.geowave.core.store.api.Statistic;

/**
 * The basic implementations for BinConstraints
 */
public class BinConstraintsImpl implements BinConstraints {
  private ByteArrayConstraints constraints;
  private Object object;

  public BinConstraintsImpl() {
    super();
    constraints = new ExplicitConstraints();
  }

  public BinConstraintsImpl(final boolean allBins) {
    super();
    constraints = new ExplicitConstraints(allBins);
  }

  public BinConstraintsImpl(final ByteArray[] bins, final boolean isPrefix) {
    super();
    constraints = new ExplicitConstraints(bins, isPrefix);
  }

  public BinConstraintsImpl(final ByteArrayRange[] binRanges) {
    super();
    constraints = new ExplicitConstraints(binRanges);
  }

  public BinConstraintsImpl(final Object object) {
    super();
    this.object = object;
  }

  @Override
  public ByteArrayConstraints constraints(final Statistic<?> stat) {
    if (constraints != null) {
      return constraints;
    } else if ((stat != null) && (stat.getBinningStrategy() != null) && (object != null)) {
      constraints = stat.getBinningStrategy().constraints(object);
    } else {
      constraints = new ExplicitConstraints();
    }
    return constraints;
  }

  public static class ExplicitConstraints implements ByteArrayConstraints {
    private final ByteArray[] bins;
    private final ByteArrayRange[] binRanges;
    private final boolean isPrefix;
    private final boolean isAllBins;

    public ExplicitConstraints() {
      // empty constraints
      this(false);
    }

    public ExplicitConstraints(final boolean allBins) {
      // empty constraints
      this(new ByteArray[0], false, allBins);
    }

    public ExplicitConstraints(final ByteArray[] bins) {
      this(bins, false);
    }

    public ExplicitConstraints(final ByteArrayRange[] binRanges) {
      this(new ByteArray[0], binRanges, false, false);
    }

    public ExplicitConstraints(final ByteArray[] bins, final boolean isPrefix) {
      this(bins, isPrefix, false);
    }

    public ExplicitConstraints(
        final ByteArray[] bins,
        final boolean isPrefix,
        final boolean isAllBins) {
      this(bins, new ByteArrayRange[0], isPrefix, isAllBins);
    }

    public ExplicitConstraints(
        final ByteArray[] bins,
        final ByteArrayRange[] binRanges,
        final boolean isPrefix,
        final boolean isAllBins) {
      this.bins = bins;
      this.binRanges = binRanges;
      this.isPrefix = isPrefix;
      this.isAllBins = isAllBins;
    }

    @Override
    public ByteArray[] getBins() {
      return bins;
    }

    @Override
    public boolean isPrefix() {
      return isPrefix;
    }

    @Override
    public boolean isAllBins() {
      return isAllBins;
    }

    @Override
    public ByteArrayRange[] getBinRanges() {
      return binRanges;
    }
  }

}
