/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.api;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.store.statistics.query.BinConstraintsImpl;

/**
 * This is used by the DataStore to represent constraints on any statistics with binning strategies
 * to only return a certain set of the statistic's bins.
 *
 */
public interface BinConstraints {
  /**
   * Unconstrained, a query will return all of the bins.
   *
   * @return a bin constraint representing all bins
   */
  static BinConstraints allBins() {
    return new BinConstraintsImpl(true);
  }

  /**
   * Sets the bins of the query explicitly. If a queried statistic uses a binning strategy, only
   * values contained in one of the given bins will be return.
   *
   * @param exactMatchBins the bins to match
   * @return a bin constraint representing exact matches of the provided bins
   */
  static BinConstraints of(final ByteArray... exactMatchBins) {
    return new BinConstraintsImpl(exactMatchBins, false);
  }

  /**
   * Sets the bins of the query by prefix. If a queried statistic uses a binning strategy, only
   * values matching the bin prefix will be returned.
   *
   * @param prefixBins the prefixes used to match the bins
   * @return a bin constraint representing the set of bin prefixes
   */
  static BinConstraints ofPrefix(final ByteArray... prefixBins) {
    return new BinConstraintsImpl(prefixBins, true);
  }

  /**
   * Sets the bins of the query by range. If a queried statistic uses a binning strategy, only
   * values matching the range will be returned.
   *
   * @param binRanges the ranges used to match the bins
   * @return a bin constraint representing the set of bin ranges
   */
  static BinConstraints ofRange(final ByteArrayRange... binRanges) {
    return new BinConstraintsImpl(binRanges);
  }

  /**
   * Sets the bins of the query using an object type that is supported by the binning strategy. The
   * result will be constrained to only statistics that use binning strategies that support this
   * type of constraint and the resulting bins will be constrained according to that strategy's
   * usage of this object. For example, spatial binning strategies may use spatial Envelope as
   * constraints, or another example might be a numeric field binning strategy using Range<Double>
   * as constraints. If a queried statistic uses a binning strategy, only values contained in one of
   * the given bins will be return.
   *
   * @param binningStrategyConstraint an object of any type supported by the binning strategy. It
   *        will be interpreted as appropriate by the binning strategy and binning strategies that
   *        do not support this object type will not return any results.
   * @return bin constraints representing the Object
   */
  static BinConstraints ofObject(final Object binningStrategyConstraint) {
    return new BinConstraintsImpl(binningStrategyConstraint);
  }

  /**
   * Used primarily internally to get the explicit bins for this constraint but can be used if there
   * is a need to understand the bins being queried.
   *
   * @param stat the statistic being queried
   * @return the explicit bins being queried
   */
  ByteArrayConstraints constraints(Statistic<?> stat);

  /**
   * Represents more explicit bins than BinConstraints as Objects must be resolved to ByteArrays
   */
  static interface ByteArrayConstraints {
    /**
     * is this a prefix query
     *
     * @return a flag indicating if it is intended to query by bin prefix (otherwise its an exact
     *         match)
     */
    boolean isPrefix();

    /**
     * get the bins to query for
     *
     * @return the bins to query for
     */
    ByteArray[] getBins();

    /**
     * get the bin ranges to query for
     *
     * @return the bin ranges to query for
     */
    ByteArrayRange[] getBinRanges();

    /**
     * is this meant to query all bins
     *
     * @return a flag indiciating if it is meant to query all bins
     */
    boolean isAllBins();
  }
}
