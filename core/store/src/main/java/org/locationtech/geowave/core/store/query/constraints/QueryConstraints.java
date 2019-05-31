/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.constraints;

import java.util.List;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;

/** This interface fully describes a query */
public interface QueryConstraints extends Persistable {
  /**
   * This is a list of filters (either client filters or distributed filters) which will be applied
   * to the result set. QueryFilters of type DistributableQueryFilter will automatically be
   * distributed across nodes, although the class must be on the classpath of each node.
   * Fine-grained filtering and secondary filtering should be applied here as the primary index will
   * only enable coarse-grained filtering.
   *
   * @param indexModel This can be used by the filters to determine the common fields in the index
   * @return A list of the query filters
   */
  public List<QueryFilter> createFilters(Index index);

  /**
   * Return a set of constraints to apply to the primary index based on the indexing strategy used.
   * The ordering of dimensions within the index stategy must match the order of dimensions in the
   * numeric data returned which will represent the constraints applied to the primary index for the
   * query.
   *
   * @param index The index used to generate the constraints for
   * @return A multi-dimensional numeric data set that represents the constraints for the index
   */
  public List<MultiDimensionalNumericData> getIndexConstraints(Index index);

  /**
   * To simplify query constraints, this allows ofr the index to be tightly coupled with the
   * constraints if true.
   *
   * @return A flag indicating that this query is specific to an index that must also be provided
   */
  default boolean indexMustBeSpecified() {
    return false;
  }
}
