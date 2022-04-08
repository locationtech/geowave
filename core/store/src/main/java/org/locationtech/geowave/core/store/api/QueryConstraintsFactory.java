/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.api;

import org.locationtech.geowave.core.index.MultiDimensionalCoordinateRangesArray;
import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.index.CustomIndex;
import org.locationtech.geowave.core.store.query.constraints.Constraints;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.filter.BasicQueryFilter.BasicQueryCompareOperation;

/** This is a simple mechanism to create existing supported query constraints. */
public interface QueryConstraintsFactory {
  /**
   * constrain a query by data IDs
   *
   * @param dataIds the data IDs to constrain to
   * @return the constraints
   */
  QueryConstraints dataIds(final byte[]... dataIds);

  /**
   * constrain a query using a range of data IDs, assuming big endian ordering
   *
   * @param startDataIdInclusive the start of data ID range (inclusive)
   * @param endDataIdInclusive the end of data ID range (inclusive)
   * @return the constraints
   */
  QueryConstraints dataIdsByRange(
      final byte[] startDataIdInclusive,
      final byte[] endDataIdInclusive);

  /**
   * constrain a query using a range of data IDs, assuming big endian ordering
   *
   * RocksDB and HBase are currently the only two that will support this, but allows for reverse
   * iteration from "end" to "start" data ID
   *
   * All other datastores will throw an UnsupportedOperationException and the forward scan should be
   * preferred for those datastores
   *
   * @param startDataIdInclusive the start of data ID range (inclusive)
   * @param endDataIdInclusive the end of data ID range (inclusive)
   * @return the constraints
   */
  QueryConstraints dataIdsByRangeReverse(
      final byte[] startDataIdInclusive,
      final byte[] endDataIdInclusive);

  /**
   * constrain a query by prefix
   *
   * @param partitionKey the prefix
   * @param sortKeyPrefix the sort prefix
   * @return the constraints
   */
  QueryConstraints prefix(final byte[] partitionKey, final byte[] sortKeyPrefix);

  /**
   * constrain by coordinate ranges
   *
   * @param indexStrategy the index strategy
   * @param coordinateRanges the coordinate ranges
   * @return the constraints
   */
  QueryConstraints coordinateRanges(
      final NumericIndexStrategy indexStrategy,
      final MultiDimensionalCoordinateRangesArray[] coordinateRanges);

  /**
   * constrain generally by constraints
   *
   * @param constraints the constraints
   * @return the query constraints
   */
  QueryConstraints constraints(final Constraints constraints);

  /**
   * constrain generally by constraints with a compare operation
   *
   * @param constraints the constraints
   * @param compareOp the relationship to use for comparison
   * @return the query constraints
   */
  QueryConstraints constraints(
      final Constraints constraints,
      final BasicQueryCompareOperation compareOp);

  /**
   * constrain using a custom persistable object NOTE: this only applies to an index that is a
   * {@link CustomIndex} and the instance of these custom constraints must match the generic of the
   * custom index's strategy
   *
   * @param customConstraints the instance of custom constraints
   * @return the query constraints
   */
  QueryConstraints customConstraints(final Persistable customConstraints);

  /**
   * no query constraints, meaning wide open query (this is the default)
   *
   * @return the query constraints
   */
  QueryConstraints noConstraints();
}
