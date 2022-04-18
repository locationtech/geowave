/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.geotools.factory.CommonFactoryFinder;
import org.locationtech.geowave.core.geotime.index.api.TemporalIndexBuilder;
import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.util.IndexOptimizationUtils;
import org.locationtech.geowave.core.geotime.util.TimeUtils;
import org.locationtech.geowave.core.index.numeric.MultiDimensionalNumericData;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.opengis.filter.Filter;

public class TemporalQuery extends AbstractVectorConstraints<ExplicitTemporalQuery> {

  public TemporalQuery() {
    super();
  }

  public TemporalQuery(final ExplicitTemporalQuery delegateConstraints) {
    super(delegateConstraints);
  }

  @Override
  protected ExplicitTemporalQuery newConstraints() {
    return new ExplicitTemporalQuery();
  }

  @Override
  protected boolean isSupported(final Index index, final GeotoolsFeatureDataAdapter adapter) {
    return IndexOptimizationUtils.hasTime(index, adapter);
  }

  @Override
  protected Filter getFilter(final GeotoolsFeatureDataAdapter adapter, final Index index) {
    return getFilter(adapter, delegateConstraints);
  }

  protected static Filter getFilter(
      final GeotoolsFeatureDataAdapter adapter,
      final QueryConstraints delegateConstraints) {
    final List<MultiDimensionalNumericData> constraints =
        delegateConstraints.getIndexConstraints(new TemporalIndexBuilder().createIndex());
    if (adapter.getTimeDescriptors().getTime() != null) {
      return constraintsToFilter(
          constraints,
          data -> TimeUtils.toDuringFilter(
              data.getMinValuesPerDimension()[0].longValue(),
              data.getMaxValuesPerDimension()[0].longValue(),
              adapter.getTimeDescriptors().getTime().getLocalName()));
    } else if ((adapter.getTimeDescriptors().getStartRange() != null)
        && (adapter.getTimeDescriptors().getEndRange() != null)) {
      return constraintsToFilter(
          constraints,
          data -> TimeUtils.toFilter(
              data.getMinValuesPerDimension()[0].longValue(),
              data.getMaxValuesPerDimension()[0].longValue(),
              adapter.getTimeDescriptors().getStartRange().getLocalName(),
              adapter.getTimeDescriptors().getEndRange().getLocalName()));
    }
    return null;
  }

  private static Filter constraintsToFilter(
      final List<MultiDimensionalNumericData> constraints,
      final Function<MultiDimensionalNumericData, Filter> dataToFilter) {
    if (!constraints.isEmpty()) {
      final List<Filter> filters =
          constraints.stream().map(dataToFilter).collect(Collectors.toList());
      if (filters.size() > 1) {
        return CommonFactoryFinder.getFilterFactory2().or(filters);
      } else {
        return filters.get(0);
      }
    } else {
      return null;
    }
  }
}
