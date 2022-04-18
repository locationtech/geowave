/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.util;

import java.util.HashMap;
import java.util.Map;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.locationtech.geowave.adapter.vector.plugin.transaction.StatisticsCache;
import org.locationtech.geowave.core.geotime.index.dimension.LatitudeDefinition;
import org.locationtech.geowave.core.geotime.index.dimension.LongitudeDefinition;
import org.locationtech.geowave.core.geotime.store.query.ExplicitSpatialTemporalQuery;
import org.locationtech.geowave.core.geotime.store.query.TemporalConstraints;
import org.locationtech.geowave.core.geotime.store.query.TemporalConstraintsSet;
import org.locationtech.geowave.core.geotime.store.query.TemporalRange;
import org.locationtech.geowave.core.geotime.store.statistics.BoundingBoxStatistic;
import org.locationtech.geowave.core.geotime.store.statistics.BoundingBoxStatistic.BoundingBoxValue;
import org.locationtech.geowave.core.geotime.store.statistics.TimeRangeStatistic;
import org.locationtech.geowave.core.geotime.store.statistics.TimeRangeStatistic.TimeRangeValue;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.geotime.util.GeometryUtils.GeoConstraintsWrapper;
import org.locationtech.geowave.core.index.dimension.NumericDimensionDefinition;
import org.locationtech.geowave.core.index.numeric.NumericRange;
import org.locationtech.geowave.core.geotime.util.TimeDescriptors;
import org.locationtech.geowave.core.geotime.util.TimeUtils;
import org.locationtech.geowave.core.store.query.constraints.BasicQueryByClass.ConstraintData;
import org.locationtech.geowave.core.store.query.constraints.BasicQueryByClass.ConstraintSet;
import org.locationtech.geowave.core.store.query.constraints.BasicQueryByClass.ConstraintsByClass;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryIndexHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryIndexHelper.class);

  private static TemporalRange getTimeRange(
      final StatisticsCache statisticsCache,
      final AttributeDescriptor attr) {
    TemporalRange timeRange = null;
    if (attr != null) {
      TimeRangeValue value =
          statisticsCache.getFieldStatistic(TimeRangeStatistic.STATS_TYPE, attr.getLocalName());
      if (value != null) {
        timeRange = value.asTemporalRange();
      }
    }
    return timeRange;
  }

  private static BoundingBoxValue getBounds(
      final StatisticsCache statisticsCache,
      final AttributeDescriptor attr) {
    return statisticsCache.getFieldStatistic(BoundingBoxStatistic.STATS_TYPE, attr.getLocalName());
  }

  /**
   * Clip the provided constraints using the statistics, if available.
   */
  public static TemporalConstraintsSet clipIndexedTemporalConstraints(
      final StatisticsCache statisticsCache,
      final TimeDescriptors timeDescriptors,
      final TemporalConstraintsSet constraintsSet) {
    if ((timeDescriptors.getEndRange() != null) && (timeDescriptors.getStartRange() != null)) {
      final String ename = timeDescriptors.getEndRange().getLocalName();
      final String sname = timeDescriptors.getStartRange().getLocalName();
      if (constraintsSet.hasConstraintsForRange(sname, ename)) {
        final TemporalRange statsStartRange =
            getTimeRange(statisticsCache, timeDescriptors.getStartRange());
        final TemporalRange statsEndRange =
            getTimeRange(statisticsCache, timeDescriptors.getEndRange());
        final TemporalRange fullRange =
            new TemporalRange(statsStartRange.getStartTime(), statsEndRange.getEndTime());

        final TemporalConstraints constraints = constraintsSet.getConstraintsForRange(sname, ename);
        constraints.replaceWithIntersections(
            new TemporalConstraints(fullRange, constraints.getName()));

        constraintsSet.removeAllConstraintsExcept(constraints.getName());
        // this should be fixed to handle interwoven range.
        // specifically look for non-overlapping regions of time
        return constraintsSet;
      }
    } else if ((timeDescriptors.getTime() != null)
        && constraintsSet.hasConstraintsFor(timeDescriptors.getTime().getLocalName())) {
      final String name = timeDescriptors.getTime().getLocalName();
      TemporalRange range = getTimeRange(statisticsCache, timeDescriptors.getTime());
      final TemporalConstraints constraints = constraintsSet.getConstraintsFor(name);
      if (range != null) {
        constraints.replaceWithIntersections(new TemporalConstraints(range, name));
      }
      constraintsSet.removeAllConstraintsExcept(name);
      return constraintsSet;
    }
    return constraintsSet;
  }

  /**
   * Clip the provided bounded box with the statistics for the index
   */
  public static Geometry clipIndexedBBOXConstraints(
      final StatisticsCache statisticsCache,
      final SimpleFeatureType adapterFeatureType,
      final CoordinateReferenceSystem indexCRS,
      final Geometry bbox) {
    final BoundingBoxValue bounds =
        getBounds(statisticsCache, adapterFeatureType.getGeometryDescriptor());
    if ((bounds != null) && bounds.isSet() && (bbox != null)) {
      CoordinateReferenceSystem bboxCRS =
          ((BoundingBoxStatistic) bounds.getStatistic()).getDestinationCrs();
      if (bboxCRS == null) {
        bboxCRS = adapterFeatureType.getCoordinateReferenceSystem();
      }
      try {
        final Geometry geo =
            new GeometryFactory().toGeometry(
                new ReferencedEnvelope(bounds.getValue(), bboxCRS).transform(indexCRS, true));
        return geo.intersection(bbox);
      } catch (MismatchedDimensionException | TransformException | FactoryException e) {
        LOGGER.warn("Unable to transform bounding box statistic to index CRS");
      }
    }
    return bbox;
  }

  public static ConstraintSet getTimeConstraintsFromIndex(
      final StatisticsCache statisticsCache,
      final TimeDescriptors timeDescriptors) {

    if ((timeDescriptors.getEndRange() != null) || (timeDescriptors.getStartRange() != null)) {
      final TemporalRange endRange = getTimeRange(statisticsCache, timeDescriptors.getEndRange());
      final TemporalRange startRange =
          getTimeRange(statisticsCache, timeDescriptors.getStartRange());
      if ((endRange != null) && (startRange != null)) {
        return ExplicitSpatialTemporalQuery.createConstraints(startRange.union(endRange), true);
      } else if (endRange != null) {
        return ExplicitSpatialTemporalQuery.createConstraints(endRange, true);
      } else if (startRange != null) {
        return ExplicitSpatialTemporalQuery.createConstraints(startRange, true);
      }
    } else if (timeDescriptors.getTime() != null) {
      final TemporalRange range = getTimeRange(statisticsCache, timeDescriptors.getTime());
      if (range != null) {
        return ExplicitSpatialTemporalQuery.createConstraints(range, true);
      }
    }
    return new ConstraintSet();
  }

  /**
   * Compose a time constraints. When the provided constraints do not fulfill the indexed
   * dimensions, compose constraints from statistics.
   */
  public static ConstraintsByClass composeTimeConstraints(
      final StatisticsCache statisticsCache,
      final SimpleFeatureType featureType,
      final TimeDescriptors timeDescriptors,
      final TemporalConstraintsSet timeBoundsSet) {

    final TemporalConstraints timeBounds =
        TimeUtils.getTemporalConstraintsForDescriptors(timeDescriptors, timeBoundsSet);
    return (timeBounds != null) && !timeBounds.isEmpty()
        ? ExplicitSpatialTemporalQuery.createConstraints(timeBounds, false)
        : new ConstraintsByClass(getTimeConstraintsFromIndex(statisticsCache, timeDescriptors));
  }

  /**
   * If composed constraints matched statistics constraints, are empty or null, then return empty
   * constraint set.
   */
  public static ConstraintsByClass composeTimeBoundedConstraints(
      final SimpleFeatureType featureType,
      final TimeDescriptors timeDescriptors,
      final TemporalConstraintsSet timeBoundsSet) {

    if ((timeBoundsSet == null) || timeBoundsSet.isEmpty() || !timeDescriptors.hasTime()) {
      return new ConstraintsByClass();
    }

    final TemporalConstraints boundsTemporalConstraints =
        TimeUtils.getTemporalConstraintsForDescriptors(timeDescriptors, timeBoundsSet);

    if (boundsTemporalConstraints.isEmpty()) {
      return new ConstraintsByClass();
    }

    final ConstraintsByClass boundsTimeConstraints =
        ExplicitSpatialTemporalQuery.createConstraints(boundsTemporalConstraints, false);
    return boundsTimeConstraints;
  }

  /**
   * If composed constraints matched statistics constraints, are empty or null, then return empty
   * constraint set
   */
  public static GeoConstraintsWrapper composeGeometricConstraints(
      final SimpleFeatureType featureType,
      final Geometry jtsBounds) {
    if (jtsBounds == null) {
      return new GeoConstraintsWrapper(new ConstraintsByClass(), true, null);
    }
    final GeoConstraintsWrapper geoConstraints =
        GeometryUtils.basicGeoConstraintsWrapperFromGeometry(jtsBounds);
    return geoConstraints;
  }

  /**
   * Compose a query from the set of constraints. When the provided constraints do not fulfill the
   * indexed dimensions, compose constraints from statistics.
   */
  public static ConstraintsByClass composeConstraints(
      final StatisticsCache statisticsCache,
      final SimpleFeatureType featureType,
      final TimeDescriptors timeDescriptors,
      final Geometry jtsBounds,
      final TemporalConstraintsSet timeBoundsSet) {
    final ConstraintsByClass timeConstraints =
        composeTimeConstraints(statisticsCache, featureType, timeDescriptors, timeBoundsSet);
    final GeoConstraintsWrapper geoConstraints =
        composeGeometricConstraints(featureType, jtsBounds);
    return timeConstraints.merge(geoConstraints.getConstraints());
  }
}
