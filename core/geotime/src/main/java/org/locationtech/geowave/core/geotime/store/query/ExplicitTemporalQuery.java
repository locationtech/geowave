/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query;

import java.util.ArrayList;
import java.util.List;
import org.locationtech.geowave.core.geotime.index.dimension.SimpleTimeDefinition;
import org.locationtech.geowave.core.geotime.index.dimension.TimeDefinition;
import org.locationtech.geowave.core.index.numeric.NumericRange;
import org.locationtech.geowave.core.store.query.constraints.BasicQueryByClass;
import org.threeten.extra.Interval;

/**
 * The Spatial Temporal Query class represents a query in three dimensions. The constraint that is
 * applied represents an intersection operation on the query geometry AND a date range intersection
 * based on startTime and endTime.
 */
public class ExplicitTemporalQuery extends BasicQueryByClass {
  public ExplicitTemporalQuery(final Interval[] intervals) {
    super(createTemporalConstraints(intervals));
  }

  public ExplicitTemporalQuery(final TemporalConstraints contraints) {
    super(createTemporalConstraints(contraints));
  }

  public ExplicitTemporalQuery() {
    super();
  }

  private static ConstraintsByClass createTemporalConstraints(
      final TemporalConstraints temporalConstraints) {
    final List<ConstraintSet> constraints = new ArrayList<>();
    for (final TemporalRange range : temporalConstraints.getRanges()) {
      constraints.add(
          new ConstraintSet(
              new ConstraintData(
                  new NumericRange(range.getStartTime().getTime(), range.getEndTime().getTime()),
                  false),
              TimeDefinition.class,
              SimpleTimeDefinition.class));
    }
    return new ConstraintsByClass(constraints);
  }

  private static ConstraintsByClass createTemporalConstraints(final Interval[] intervals) {
    final List<ConstraintSet> constraints = new ArrayList<>();
    for (final Interval range : intervals) {
      constraints.add(
          new ConstraintSet(
              new ConstraintData(
                  new NumericRange(
                      range.getStart().toEpochMilli(),
                      // intervals are intended to be exclusive on the end so this adjusts for
                      // exclusivity
                      Math.max(range.getEnd().toEpochMilli() - 1, range.getStart().toEpochMilli())),
                  false),
              TimeDefinition.class,
              SimpleTimeDefinition.class));
    }
    return new ConstraintsByClass(constraints);
  }
}
