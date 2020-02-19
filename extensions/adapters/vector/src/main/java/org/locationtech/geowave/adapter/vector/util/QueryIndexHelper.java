/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.util;

import java.util.Date;
import java.util.Map;
import org.locationtech.geowave.core.geotime.store.query.ExplicitSpatialTemporalQuery;
import org.locationtech.geowave.core.geotime.store.query.TemporalConstraints;
import org.locationtech.geowave.core.geotime.store.query.TemporalConstraintsSet;
import org.locationtech.geowave.core.geotime.store.query.TemporalRange;
import org.locationtech.geowave.core.geotime.store.query.api.VectorStatisticsQueryBuilder;
import org.locationtech.geowave.core.geotime.store.statistics.BoundingBoxDataStatistics;
import org.locationtech.geowave.core.geotime.store.statistics.TimeRangeDataStatistics;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.geotime.util.GeometryUtils.GeoConstraintsWrapper;
import org.locationtech.geowave.core.geotime.util.TimeDescriptors;
import org.locationtech.geowave.core.geotime.util.TimeUtils;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.StatisticsId;
import org.locationtech.geowave.core.store.query.constraints.BasicQueryByClass.ConstraintSet;
import org.locationtech.geowave.core.store.query.constraints.BasicQueryByClass.ConstraintsByClass;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

public class QueryIndexHelper {

  private static TemporalRange getStatsRange(
      final Map<StatisticsId, InternalDataStatistics<SimpleFeature, ?, ?>> statsMap,
      final AttributeDescriptor attr) {
    final TemporalRange timeRange = new TemporalRange();
    if (attr != null) {
      final TimeRangeDataStatistics stat =
          ((TimeRangeDataStatistics) statsMap.get(
              VectorStatisticsQueryBuilder.newBuilder().factory().timeRange().fieldName(
                  attr.getLocalName()).build().getId()));
      if (stat != null) {
        timeRange.setStartTime(new Date(stat.getMin()));
        timeRange.setEndTime(new Date(stat.getMax()));
      }
    }
    return timeRange;
  }

  /**
   * Clip the provided constraints using the statistics, if available.
   *
   * @param statsMap
   * @param timeDescriptors
   * @param constraintsSet
   * @return
   */
  public static TemporalConstraintsSet clipIndexedTemporalConstraints(
      final Map<StatisticsId, InternalDataStatistics<SimpleFeature, ?, ?>> statsMap,
      final TimeDescriptors timeDescriptors,
      final TemporalConstraintsSet constraintsSet) {
    // TODO: if query range doesn't intersect with the stats, it seems the
    // constraints are removed or empty - does this make sense? It seems
    // this can result in open-ended time when it should find no results.
    if ((timeDescriptors.getEndRange() != null) && (timeDescriptors.getStartRange() != null)) {
      final String ename = timeDescriptors.getEndRange().getLocalName();
      final String sname = timeDescriptors.getStartRange().getLocalName();
      if (constraintsSet.hasConstraintsForRange(sname, ename)) {
        final TemporalRange statsStartRange =
            getStatsRange(statsMap, timeDescriptors.getStartRange());
        final TemporalRange statsEndRange = getStatsRange(statsMap, timeDescriptors.getEndRange());
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
      final TimeRangeDataStatistics stats =
          ((TimeRangeDataStatistics) statsMap.get(
              VectorStatisticsQueryBuilder.newBuilder().factory().timeRange().fieldName(
                  name).build().getId()));

      final TemporalConstraints constraints = constraintsSet.getConstraintsFor(name);
      if (stats != null) {
        constraints.replaceWithIntersections(
            new TemporalConstraints(stats.asTemporalRange(), name));
      }
      constraintsSet.removeAllConstraintsExcept(name);
      return constraintsSet;
    }
    return constraintsSet;
  }

  /**
   * Clip the provided bounded box with the statistics for the index
   *
   * @param featureType
   * @param bbox
   * @param statsMap
   * @return
   */
  public static Geometry clipIndexedBBOXConstraints(
      final SimpleFeatureType featureType,
      final Geometry bbox,
      final Map<StatisticsId, InternalDataStatistics<SimpleFeature, ?, ?>> statsMap) {

    final String geoAttrName = featureType.getGeometryDescriptor().getLocalName();

    final StatisticsId statId =
        VectorStatisticsQueryBuilder.newBuilder().factory().bbox().fieldName(
            geoAttrName).build().getId();
    final BoundingBoxDataStatistics bboxStats = (BoundingBoxDataStatistics) statsMap.get(statId);
    if ((bboxStats != null) && bboxStats.isSet() && (bbox != null)) {
      final Geometry geo = new GeometryFactory().toGeometry(bboxStats.getResult());
      // TODO if the query doesn't intersect the stats this will return an
      // empty geometry, it seems that'd be an opportunity to quickly
      // return no results rather than continuing on and hoping that an
      // empty geometry gives no results and not a full table scan
      return geo.intersection(bbox);
    }
    return bbox;
  }

  public static ConstraintSet getTimeConstraintsFromIndex(
      final TimeDescriptors timeDescriptors,
      final Map<StatisticsId, InternalDataStatistics<SimpleFeature, ?, ?>> stats) {

    if ((timeDescriptors.getEndRange() != null) || (timeDescriptors.getStartRange() != null)) {
      final TimeRangeDataStatistics endRange =
          (timeDescriptors.getEndRange() != null)
              ? ((TimeRangeDataStatistics) stats.get(
                  VectorStatisticsQueryBuilder.newBuilder().factory().timeRange().fieldName(
                      timeDescriptors.getEndRange().getLocalName()).build().getId()))
              : null;
      final TimeRangeDataStatistics startRange =
          (timeDescriptors.getStartRange() != null)
              ? ((TimeRangeDataStatistics) stats.get(
                  VectorStatisticsQueryBuilder.newBuilder().factory().timeRange().fieldName(
                      timeDescriptors.getStartRange().getLocalName()).build().getId()))
              : null;

      if ((endRange != null) && (startRange != null)) {
        return ExplicitSpatialTemporalQuery.createConstraints(
            startRange.asTemporalRange().union(endRange.asTemporalRange()),
            true);
      } else if (endRange != null) {
        return ExplicitSpatialTemporalQuery.createConstraints(endRange.asTemporalRange(), true);
      } else if (startRange != null) {
        return ExplicitSpatialTemporalQuery.createConstraints(startRange.asTemporalRange(), true);
      }
    } else if (timeDescriptors.getTime() != null) {
      final TimeRangeDataStatistics timeStat =
          ((TimeRangeDataStatistics) stats.get(
              VectorStatisticsQueryBuilder.newBuilder().factory().timeRange().fieldName(
                  timeDescriptors.getTime().getLocalName()).build().getId()));
      if (timeStat != null) {
        return ExplicitSpatialTemporalQuery.createConstraints(timeStat.asTemporalRange(), true);
      }
    }
    return new ConstraintSet();
  }

  public static ConstraintSet getBBOXIndexConstraintsFromIndex(
      final SimpleFeatureType featureType,
      final Map<StatisticsId, InternalDataStatistics<SimpleFeature, ?, ?>> statsMap) {
    final String geoAttrName = featureType.getGeometryDescriptor().getLocalName();
    final StatisticsId statId =
        VectorStatisticsQueryBuilder.newBuilder().factory().bbox().fieldName(
            geoAttrName).build().getId();
    final BoundingBoxDataStatistics<SimpleFeature, ?> bboxStats =
        (BoundingBoxDataStatistics<SimpleFeature, ?>) statsMap.get(statId);
    return (bboxStats != null) ? bboxStats.getConstraints() : new ConstraintSet();
  }

  /**
   * Compose a time constraints. When the provided constraints do not fulfill the indexed
   * dimensions, compose constraints from statistics.
   *
   * @param featureType
   * @param timeDescriptors
   * @param statsMap
   * @param timeBoundsSet
   * @return
   */
  public static ConstraintsByClass composeTimeConstraints(
      final SimpleFeatureType featureType,
      final TimeDescriptors timeDescriptors,
      final Map<StatisticsId, InternalDataStatistics<SimpleFeature, ?, ?>> statsMap,
      final TemporalConstraintsSet timeBoundsSet) {

    final TemporalConstraints timeBounds =
        TimeUtils.getTemporalConstraintsForDescriptors(timeDescriptors, timeBoundsSet);
    return (timeBounds != null) && !timeBounds.isEmpty()
        ? ExplicitSpatialTemporalQuery.createConstraints(timeBounds, false)
        : new ConstraintsByClass(getTimeConstraintsFromIndex(timeDescriptors, statsMap));
  }

  /**
   * If composed constraints matched statistics constraints, are empty or null, then return empty
   * constraint set.
   *
   * @param featureType
   * @param timeDescriptors
   * @param statsMap
   * @param timeBoundsSet
   * @return
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
   *
   * @param featureType
   * @param timeDescriptors
   * @param statsMap
   * @param timeBoundsSet
   * @return
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
   *
   * @param featureType
   * @param timeDescriptors
   * @param statsMap
   * @param jtsBounds
   * @param timeBoundsSet
   * @return
   */
  public static ConstraintsByClass composeConstraints(
      final SimpleFeatureType featureType,
      final TimeDescriptors timeDescriptors,
      final Map<StatisticsId, InternalDataStatistics<SimpleFeature, ?, ?>> statsMap,
      final Geometry jtsBounds,
      final TemporalConstraintsSet timeBoundsSet) {
    final ConstraintsByClass timeConstraints =
        composeTimeConstraints(featureType, timeDescriptors, statsMap, timeBoundsSet);
    final GeoConstraintsWrapper geoConstraints =
        composeGeometricConstraints(featureType, jtsBounds);
    return timeConstraints.merge(geoConstraints.getConstraints());
  }
}
