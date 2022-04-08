/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query;

import org.geotools.factory.CommonFactoryFinder;
import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.util.IndexOptimizationUtils;
import org.locationtech.geowave.core.store.api.Index;
import org.opengis.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpatialTemporalQuery extends AbstractVectorConstraints<ExplicitSpatialTemporalQuery> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SpatialTemporalQuery.class);

  public SpatialTemporalQuery() {
    super();
  }

  public SpatialTemporalQuery(final ExplicitSpatialTemporalQuery delegateConstraints) {
    super(delegateConstraints);
  }

  @Override
  protected ExplicitSpatialTemporalQuery newConstraints() {
    return new ExplicitSpatialTemporalQuery();
  }

  @Override
  protected boolean isSupported(final Index index, final GeotoolsFeatureDataAdapter adapter) {
    return IndexOptimizationUtils.hasTime(index, adapter)
        && IndexOptimizationUtils.hasAtLeastSpatial(index);
  }

  @Override
  protected Filter getFilter(final GeotoolsFeatureDataAdapter adapter, final Index index) {
    final Filter spatialFilter = SpatialQuery.getFilter(adapter, index, delegateConstraints);
    if (spatialFilter == null) {
      LOGGER.warn("Spatial filter does not apply to type '" + adapter.getTypeName() + "'");
      return null;
    }
    final Filter temporalFilter = TemporalQuery.getFilter(adapter, delegateConstraints);
    if (temporalFilter == null) {
      LOGGER.warn("Temporal filter does not apply to type '" + adapter.getTypeName() + "'");
      return null;
    }
    return CommonFactoryFinder.getFilterFactory2().and(spatialFilter, temporalFilter);
  }

}
