/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.constraints;

import java.util.List;
import org.locationtech.geowave.core.index.numeric.MultiDimensionalNumericData;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;
import org.locationtech.geowave.core.store.query.filter.BasicQueryFilter;
import org.locationtech.geowave.core.store.query.filter.BasicQueryFilter.BasicQueryCompareOperation;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;

public class BasicQuery implements QueryConstraints {
  protected Constraints constraints;
  // compare OP doesn't need to be serialized because its only used clientside to generate the query
  // filter
  protected transient BasicQueryCompareOperation compareOp = BasicQueryCompareOperation.INTERSECTS;

  public BasicQuery() {}

  public BasicQuery(final Constraints constraints) {
    this(constraints, BasicQueryCompareOperation.INTERSECTS);
  }


  public BasicQuery(final Constraints constraints, final BasicQueryCompareOperation compareOp) {
    super();
    this.constraints = constraints;
    this.compareOp = compareOp;
  }

  @Override
  public List<QueryFilter> createFilters(final Index index) {
    return constraints.createFilters(index, this);
  }

  protected QueryFilter createQueryFilter(
      final MultiDimensionalNumericData constraints,
      final NumericDimensionField<?>[] orderedConstrainedDimensionFields,
      final NumericDimensionField<?>[] unconstrainedDimensionFields,
      final Index index) {
    return new BasicQueryFilter(constraints, orderedConstrainedDimensionFields, compareOp);
  }

  @Override
  public List<MultiDimensionalNumericData> getIndexConstraints(final Index index) {
    return constraints.getIndexConstraints(index);
  }

  @Override
  public byte[] toBinary() {
    return PersistenceUtils.toBinary(constraints);
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    constraints = (Constraints) PersistenceUtils.fromBinary(bytes);
  }
}
