/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query;

import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.geotime.util.IndexOptimizationUtils;
import org.locationtech.geowave.core.store.api.Index;
import org.opengis.filter.Filter;

public class SpatialQuery extends AbstractVectorConstraints<ExplicitSpatialQuery> {

  public SpatialQuery() {
    super();
  }

  public SpatialQuery(final ExplicitSpatialQuery delegateConstraints) {
    super(delegateConstraints);
  }

  @Override
  protected ExplicitSpatialQuery newConstraints() {
    return new ExplicitSpatialQuery();
  }

  @Override
  protected boolean isSupported(final Index index, final GeotoolsFeatureDataAdapter adapter) {
    return IndexOptimizationUtils.hasAtLeastSpatial(index);
  }

  @Override
  protected Filter getFilter(final GeotoolsFeatureDataAdapter adapter) {
    return getFilter(adapter, delegateConstraints);
  }

  protected static Filter getFilter(
      final GeotoolsFeatureDataAdapter adapter,
      final ExplicitSpatialQuery delegateConstraints) {
    return GeometryUtils.geometryToSpatialOperator(
        delegateConstraints.getQueryGeometry(),
        adapter.getFeatureType().getGeometryDescriptor().getLocalName());
  }
}
