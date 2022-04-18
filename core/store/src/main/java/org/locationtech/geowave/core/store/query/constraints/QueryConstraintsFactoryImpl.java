/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.constraints;

import org.locationtech.geowave.core.index.MultiDimensionalCoordinateRangesArray;
import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.api.QueryConstraintsFactory;
import org.locationtech.geowave.core.store.query.constraints.BasicOrderedConstraintQuery.OrderedConstraints;
import org.locationtech.geowave.core.store.query.constraints.BasicQueryByClass.ConstraintsByClass;
import org.locationtech.geowave.core.store.query.filter.BasicQueryFilter.BasicQueryCompareOperation;

public class QueryConstraintsFactoryImpl implements QueryConstraintsFactory {
  public static final QueryConstraintsFactoryImpl SINGLETON_INSTANCE =
      new QueryConstraintsFactoryImpl();

  @Override
  public QueryConstraints dataIds(final byte[]... dataIds) {
    return new DataIdQuery(dataIds);
  }

  @Override
  public QueryConstraints prefix(final byte[] partitionKey, final byte[] sortKeyPrefix) {
    return new PrefixIdQuery(partitionKey, sortKeyPrefix);
  }

  @Override
  public QueryConstraints coordinateRanges(
      final NumericIndexStrategy indexStrategy,
      final MultiDimensionalCoordinateRangesArray[] coordinateRanges) {
    return new CoordinateRangeQuery(indexStrategy, coordinateRanges);
  }

  @Override
  public QueryConstraints customConstraints(final Persistable customConstraints) {
    return new CustomQueryConstraints<>(customConstraints);
  }

  @Override
  public QueryConstraints constraints(final Constraints constraints) {
    if (constraints instanceof ConstraintsByClass) {
      // slightly optimized wrapper for ConstraintsByClass
      return new BasicQueryByClass((ConstraintsByClass) constraints);
    } else if (constraints instanceof OrderedConstraints) {
      // slightly optimized wrapper for OrderedConstraints
      return new BasicOrderedConstraintQuery((OrderedConstraints) constraints);
    }
    return new BasicQuery(constraints);
  }

  @Override
  public QueryConstraints constraints(
      final Constraints constraints,
      final BasicQueryCompareOperation compareOp) {
    if (constraints instanceof ConstraintsByClass) {
      // slightly optimized wrapper for ConstraintsByClass
      return new BasicQueryByClass((ConstraintsByClass) constraints, compareOp);
    } else if (constraints instanceof OrderedConstraints) {
      // slightly optimized wrapper for OrderedConstraints
      return new BasicOrderedConstraintQuery((OrderedConstraints) constraints, compareOp);
    }
    return new BasicQuery(constraints, compareOp);
  }

  @Override
  public QueryConstraints noConstraints() {
    return new EverythingQuery();
  }

  @Override
  public QueryConstraints dataIdsByRange(
      final byte[] startDataIdInclusive,
      final byte[] endDataIdInclusive) {
    return new DataIdRangeQuery(startDataIdInclusive, endDataIdInclusive);
  }

  @Override
  public QueryConstraints dataIdsByRangeReverse(
      final byte[] startDataIdInclusive,
      final byte[] endDataIdInclusive) {
    return new DataIdRangeQuery(startDataIdInclusive, endDataIdInclusive, true);
  }
}
