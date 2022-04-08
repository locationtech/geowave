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
import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.InternalGeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.util.IndexOptimizationUtils;
import org.locationtech.geowave.core.index.numeric.MultiDimensionalNumericData;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.query.constraints.AdapterAndIndexBasedQueryConstraints;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;
import org.opengis.filter.Filter;

abstract public class AbstractVectorConstraints<T extends QueryConstraints> implements
    AdapterAndIndexBasedQueryConstraints,
    QueryConstraints {
  protected T delegateConstraints;

  protected AbstractVectorConstraints() {}

  public AbstractVectorConstraints(final T delegateConstraints) {
    super();
    this.delegateConstraints = delegateConstraints;
  }

  @Override
  public byte[] toBinary() {
    return delegateConstraints.toBinary();
  }

  @Override
  public List<QueryFilter> createFilters(final Index index) {
    return delegateConstraints.createFilters(index);
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    delegateConstraints = newConstraints();
    delegateConstraints.fromBinary(bytes);
  }

  abstract protected T newConstraints();

  @Override
  public List<MultiDimensionalNumericData> getIndexConstraints(final Index index) {
    return delegateConstraints.getIndexConstraints(index);
  }

  abstract protected boolean isSupported(
      final Index index,
      final GeotoolsFeatureDataAdapter adapter);

  abstract protected Filter getFilter(GeotoolsFeatureDataAdapter adapter, Index index);

  @Override
  public QueryConstraints createQueryConstraints(
      final InternalDataAdapter<?> adapter,
      final Index index,
      final AdapterToIndexMapping indexMapping) {
    final InternalGeotoolsFeatureDataAdapter<?> gtAdapter =
        IndexOptimizationUtils.unwrapGeotoolsFeatureDataAdapter(adapter);
    if (gtAdapter != null) {
      if (!isSupported(index, gtAdapter)) {
        final Filter filter = getFilter(gtAdapter, index);
        if (filter == null) {
          return null;
        }
        return new ExplicitCQLQuery(delegateConstraints, filter, gtAdapter, indexMapping);
      }
    }
    // otherwise just unwrap this
    return delegateConstraints;
  }

}
