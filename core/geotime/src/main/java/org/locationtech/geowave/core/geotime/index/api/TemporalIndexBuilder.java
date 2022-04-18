/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.index.api;

import org.locationtech.geowave.core.geotime.index.TemporalDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.index.TemporalOptions;
import org.locationtech.geowave.core.geotime.index.dimension.TemporalBinningStrategy.Unit;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.index.BaseIndexBuilder;

public class TemporalIndexBuilder extends BaseIndexBuilder<TemporalIndexBuilder> {
  private final TemporalOptions options;

  public TemporalIndexBuilder() {
    options = new TemporalOptions();
  }

  public TemporalIndexBuilder setSupportsTimeRanges(final boolean supportsTimeRanges) {
    options.setNoTimeRanges(!supportsTimeRanges);
    return this;
  }

  public TemporalIndexBuilder setPeriodicity(final Unit periodicity) {
    options.setPeriodicity(periodicity);
    return this;
  }

  public TemporalIndexBuilder setMaxDuplicates(final long maxDuplicates) {
    options.setMaxDuplicates(maxDuplicates);
    return this;
  }

  @Override
  public Index createIndex() {
    return createIndex(TemporalDimensionalityTypeProvider.createIndexFromOptions(options));
  }
}
