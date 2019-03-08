/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query;

import java.time.Instant;
import java.util.Date;
import org.apache.commons.lang3.ArrayUtils;
import org.locationtech.geowave.core.geotime.store.query.api.SpatialTemporalConstraintsBuilder;
import org.locationtech.geowave.core.geotime.store.query.filter.SpatialQueryFilter.CompareOperation;
import org.locationtech.geowave.core.store.query.constraints.EverythingQuery;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.extra.Interval;

public class SpatialTemporalConstraintsBuilderImpl implements SpatialTemporalConstraintsBuilder {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(SpatialTemporalConstraintsBuilderImpl.class);
  private String crsCode;
  private Geometry geometry;
  private CompareOperation spatialCompareOp;

  private Interval[] timeRanges = new Interval[0];

  @Override
  public SpatialTemporalConstraintsBuilder noSpatialConstraints() {
    geometry = null;
    crsCode = null;
    spatialCompareOp = null;
    return this;
  }

  @Override
  public SpatialTemporalConstraintsBuilder spatialConstraints(final Geometry geometry) {
    this.geometry = geometry;
    return this;
  }

  @Override
  public SpatialTemporalConstraintsBuilder spatialConstraintsCrs(final String crsCode) {
    this.crsCode = crsCode;
    return this;
  }

  @Override
  public SpatialTemporalConstraintsBuilder spatialConstraintsCompareOperation(
      final CompareOperation spatialCompareOp) {
    this.spatialCompareOp = spatialCompareOp;
    return this;
  }

  @Override
  public SpatialTemporalConstraintsBuilder noTemporalConstraints() {
    timeRanges = new Interval[0];
    return this;
  }

  @Override
  public SpatialTemporalConstraintsBuilder addTimeRange(final Date startTime, final Date endTime) {
    return addTimeRange(
        Interval.of(
            Instant.ofEpochMilli(startTime.getTime()),
            Instant.ofEpochMilli(endTime.getTime())));
  }

  @Override
  public SpatialTemporalConstraintsBuilder addTimeRange(final Interval timeRange) {
    timeRanges = ArrayUtils.add(timeRanges, timeRange);
    return this;
  }

  @Override
  public SpatialTemporalConstraintsBuilder setTimeRanges(final Interval[] timeRanges) {
    if (timeRanges == null) {
      this.timeRanges = new Interval[0];
    }
    this.timeRanges = timeRanges;
    return this;
  }

  @Override
  public QueryConstraints build() {
    if ((crsCode != null) && (geometry == null)) {
      LOGGER.warn(
          "CRS code `" + crsCode + "` cannot be applied without a geometry.  Ignoring CRS.");
    }
    if ((spatialCompareOp != null) && (geometry == null)) {
      LOGGER.warn(
          "Spatial compare operator `"
              + spatialCompareOp.name()
              + "` cannot be applied without a geometry.  Ignoring compare operator.");
    }
    if (geometry != null) {
      // its at least spatial
      if (timeRanges.length > 0) {
        // its spatial-temporal
        return new SpatialTemporalQuery(
            new ExplicitSpatialTemporalQuery(timeRanges, geometry, crsCode, spatialCompareOp));
      }
      return new SpatialQuery(new ExplicitSpatialQuery(geometry, crsCode, spatialCompareOp));
    } else if (timeRanges.length > 0) {
      // its temporal only
      return new TemporalQuery(new ExplicitTemporalQuery(timeRanges));
    }
    return new EverythingQuery();
  }
}
