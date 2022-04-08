/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
/** */
package org.locationtech.geowave.core.geotime.util;

import org.locationtech.geowave.core.geotime.store.query.filter.SpatialQueryFilter.CompareOperation;
import org.locationtech.jts.geom.Geometry;

/**
 * @author Ashish Shah <p> This class is used to store results extracted from
 *         ExtractGeometryFilterVisitor class. It simply stores query geometry and its associated
 *         predicate.
 */
public final class ExtractGeometryFilterVisitorResult {
  private final Geometry geometry;
  private final CompareOperation compareOp;

  public ExtractGeometryFilterVisitorResult(
      final Geometry geometry,
      final CompareOperation compareOp) {
    this.geometry = geometry;
    this.compareOp = compareOp;
  }

  /** @return geometry */
  public Geometry getGeometry() {
    return geometry;
  }

  /** @return predicate associated with geometry */
  public CompareOperation getCompareOp() {
    return compareOp;
  }

  /**
   * @param otherResult is ExtractGeometryFilterVisitorResult object
   * @return True if predicates of both ExtractGeometryFilterVisitorResult objects are same
   */
  public boolean matchPredicate(final ExtractGeometryFilterVisitorResult otherResult) {
    return (compareOp == otherResult.getCompareOp());
  }
}
