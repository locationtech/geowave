/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.index.api;

import org.locationtech.geowave.core.geotime.index.SpatialTemporalOptions;
import org.locationtech.geowave.core.geotime.index.SpatialTemporalDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.index.SpatialTemporalDimensionalityTypeProvider.Bias;
import org.locationtech.geowave.core.geotime.index.dimension.TemporalBinningStrategy.Unit;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.index.BaseIndexBuilder;

public class SpatialTemporalIndexBuilder extends BaseIndexBuilder<SpatialTemporalIndexBuilder> {
  private final SpatialTemporalOptions options;

  public SpatialTemporalIndexBuilder() {
    options = new SpatialTemporalOptions();
  }

  public SpatialTemporalIndexBuilder setBias(final Bias bias) {
    options.setBias(bias);
    return this;
  }

  public SpatialTemporalIndexBuilder setPeriodicity(final Unit periodicity) {
    options.setPeriodicity(periodicity);
    return this;
  }

  public SpatialTemporalIndexBuilder setMaxDuplicates(final long maxDuplicates) {
    options.setMaxDuplicates(maxDuplicates);
    return this;
  }

  public SpatialTemporalIndexBuilder setCrs(final String crs) {
    options.setCrs(crs);
    return this;
  }

  @Override
  public Index createIndex() {
    return createIndex(SpatialTemporalDimensionalityTypeProvider.createIndexFromOptions(options));
  }
}
