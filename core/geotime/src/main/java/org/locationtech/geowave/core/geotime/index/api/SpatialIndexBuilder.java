/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.index.api;

import javax.annotation.Nullable;
import org.locationtech.geowave.core.geotime.index.SpatialDimensionalityTypeProvider;
import org.locationtech.geowave.core.geotime.index.SpatialOptions;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.index.BaseIndexBuilder;

public class SpatialIndexBuilder extends BaseIndexBuilder<SpatialIndexBuilder> {
  private final SpatialOptions options;

  public SpatialIndexBuilder() {
    super();
    options = new SpatialOptions();
  }

  public SpatialIndexBuilder setIncludeTimeInCommonIndexModel(final boolean storeTime) {
    options.storeTime(storeTime);
    return this;
  }

  public SpatialIndexBuilder setGeometryPrecision(@Nullable final Integer precision) {
    options.setGeometryPrecision(precision);
    return this;
  }

  public SpatialIndexBuilder setCrs(final String crs) {
    options.setCrs(crs);
    return this;
  }

  @Override
  public Index createIndex() {
    return createIndex(SpatialDimensionalityTypeProvider.createIndexFromOptions(options));
  }
}
