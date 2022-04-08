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
import java.util.Date;
import java.util.List;
import org.locationtech.geowave.core.geotime.index.dimension.SimpleTimeDefinition;
import org.locationtech.geowave.core.geotime.index.dimension.TimeDefinition;
import org.locationtech.geowave.core.geotime.store.query.filter.SpatialQueryFilter.CompareOperation;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.index.numeric.NumericRange;
import org.locationtech.geowave.core.store.query.filter.BasicQueryFilter.BasicQueryCompareOperation;
import org.locationtech.jts.geom.Geometry;
import org.threeten.extra.Interval;

/**
 * The Spatial Temporal Query class represents a query in three dimensions. The constraint that is
 * applied represents an intersection operation on the query geometry AND a date range intersection
 * based on startTime and endTime.
 */
public class ExplicitSpatialTemporalQuery extends ExplicitSpatialQuery {
  public ExplicitSpatialTemporalQuery() {}

  public ExplicitSpatialTemporalQuery(
      final Date startTime,
      final Date endTime,
      final Geometry queryGeometry) {
    super(createSpatialTemporalConstraints(startTime, endTime, queryGeometry), queryGeometry);
  }

  public ExplicitSpatialTemporalQuery(
      final Date startTime,
      final Date endTime,
      final Geometry queryGeometry,
      final String crsCode) {
    super(
        createSpatialTemporalConstraints(startTime, endTime, queryGeometry),
        queryGeometry,
        crsCode);
  }

  public ExplicitSpatialTemporalQuery(
      final TemporalConstraints constraints,
      final Geometry queryGeometry) {
    super(createSpatialTemporalConstraints(constraints, queryGeometry), queryGeometry);
  }

  public ExplicitSpatialTemporalQuery(
      final TemporalConstraints constraints,
      final Geometry queryGeometry,
      final String crsCode) {
    super(createSpatialTemporalConstraints(constraints, queryGeometry), queryGeometry, crsCode);
  }

  /**
   * If more then on polygon is supplied in the geometry, then the range of time is partnered with
   * each polygon constraint. Note: By default we are using same compareOp for 1D Time filtering as
   * the compareOp of the Spatial query by calling getBaseCompareOp()
   *
   * @param startTime
   * @param endTime
   * @param queryGeometry
   * @param compareOp
   */
  public ExplicitSpatialTemporalQuery(
      final Date startTime,
      final Date endTime,
      final Geometry queryGeometry,
      final CompareOperation compareOp) {
    super(
        createSpatialTemporalConstraints(startTime, endTime, queryGeometry),
        queryGeometry,
        compareOp,
        compareOp.getBaseCompareOp());
  }

  public ExplicitSpatialTemporalQuery(
      final Interval[] intervals,
      final Geometry queryGeometry,
      final String crsCode,
      final CompareOperation compareOp) {
    super(
        createSpatialTemporalConstraints(intervals, queryGeometry),
        queryGeometry,
        crsCode,
        compareOp,
        // it seems like temporal should always use intersection and not
        // inherit from the spatial compare op
        BasicQueryCompareOperation.INTERSECTS);
  }

  /**
   * Applies the set of temporal constraints to the boundaries of the provided polygon. If a
   * multi-polygon is provided, then all matching combinations between temporal ranges and polygons
   * are explored.
   *
   * @param constraints
   * @param queryGeometry
   * @param compareOp
   */
  public ExplicitSpatialTemporalQuery(
      final TemporalConstraints constraints,
      final Geometry queryGeometry,
      final CompareOperation compareOp) {
    super(createSpatialTemporalConstraints(constraints, queryGeometry), queryGeometry, compareOp);
  }

  public static ConstraintSet createConstraints(
      final TemporalRange temporalRange,
      final boolean isDefault) {
    return new ConstraintSet(
        new ConstraintData(
            new NumericRange(
                temporalRange.getStartTime().getTime(),
                temporalRange.getEndTime().getTime()),
            isDefault),
        TimeDefinition.class,
        SimpleTimeDefinition.class);
  }

  public static ConstraintsByClass createConstraints(
      final TemporalConstraints temporalConstraints,
      final boolean isDefault) {
    final List<ConstraintSet> constraints = new ArrayList<>();
    for (final TemporalRange range : temporalConstraints.getRanges()) {
      constraints.add(
          new ConstraintSet(
              new ConstraintData(
                  new NumericRange(range.getStartTime().getTime(), range.getEndTime().getTime()),
                  isDefault),
              TimeDefinition.class,
              SimpleTimeDefinition.class));
    }
    return new ConstraintsByClass(constraints);
  }

  public static ConstraintsByClass createConstraints(
      final Interval[] intervals,
      final boolean isDefault) {
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
                  isDefault),
              TimeDefinition.class,
              SimpleTimeDefinition.class));
    }
    return new ConstraintsByClass(constraints);
  }

  /**
   * Supports multi-polygons and multiple temporal bounds. Creates all matchings between polygon and
   * temporal bounds.
   *
   * @param startTime
   * @param endTime
   * @param queryGeometry
   * @return
   */
  private static ConstraintsByClass createSpatialTemporalConstraints(
      final TemporalConstraints temporalConstraints,
      final Geometry queryGeometry) {
    final ConstraintsByClass geoConstraints =
        GeometryUtils.basicConstraintsFromGeometry(queryGeometry);
    final ConstraintsByClass timeConstraints = createConstraints(temporalConstraints, false);
    return geoConstraints.merge(timeConstraints);
  }

  /**
   * Supports multi-polygons and multiple temporal bounds. Creates all matchings between polygon and
   * temporal bounds.
   *
   * @param startTime
   * @param endTime
   * @param queryGeometry
   * @return
   */
  private static ConstraintsByClass createSpatialTemporalConstraints(
      final Interval[] intervals,
      final Geometry queryGeometry) {
    final ConstraintsByClass geoConstraints =
        GeometryUtils.basicConstraintsFromGeometry(queryGeometry);
    final ConstraintsByClass timeConstraints = createConstraints(intervals, false);
    return geoConstraints.merge(timeConstraints);
  }

  /**
   * Supports multi-polygons. Applies 'temporal bounds' to each geometric constraint.
   *
   * @param startTime
   * @param endTime
   * @param queryGeometry
   * @return
   */
  private static ConstraintsByClass createSpatialTemporalConstraints(
      final Date startTime,
      final Date endTime,
      final Geometry queryGeometry) {
    final ConstraintsByClass geoConstraints =
        GeometryUtils.basicConstraintsFromGeometry(queryGeometry);
    return geoConstraints.merge(
        new ConstraintsByClass(
            new ConstraintSet(
                new ConstraintData(new NumericRange(startTime.getTime(), endTime.getTime()), false),
                TimeDefinition.class,
                SimpleTimeDefinition.class)));
  }
}
