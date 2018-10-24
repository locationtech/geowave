/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.adapter.vector.util;

import java.util.Map;

import org.locationtech.geowave.core.geotime.store.query.SpatialTemporalQuery;
import org.locationtech.geowave.core.geotime.store.query.TemporalConstraints;
import org.locationtech.geowave.core.geotime.store.query.TemporalConstraintsSet;
import org.locationtech.geowave.core.geotime.store.query.TemporalRange;
import org.locationtech.geowave.core.geotime.store.query.api.VectorStatisticsQueryBuilder;
import org.locationtech.geowave.core.geotime.store.statistics.BoundingBoxDataStatistics;
import org.locationtech.geowave.core.geotime.store.statistics.FeatureBoundingBoxStatistics;
import org.locationtech.geowave.core.geotime.store.statistics.FeatureTimeRangeStatistics;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.geotime.util.GeometryUtils.GeoConstraintsWrapper;
import org.locationtech.geowave.core.geotime.util.TimeDescriptors;
import org.locationtech.geowave.core.geotime.util.TimeUtils;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.StatisticsId;
import org.locationtech.geowave.core.store.query.constraints.BasicQuery.ConstraintSet;
import org.locationtech.geowave.core.store.query.constraints.BasicQuery.Constraints;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

import com.vividsolutions.jts.geom.Geometry;

public class QueryIndexHelper
{

	private static TemporalRange getStatsRange(
			final Map<StatisticsId, InternalDataStatistics<SimpleFeature, ?, ?>> statsMap,
			final AttributeDescriptor attr ) {
		final TemporalRange timeRange = new TemporalRange();
		if (attr != null) {
			final FeatureTimeRangeStatistics stat = ((FeatureTimeRangeStatistics) statsMap
					.get(VectorStatisticsQueryBuilder.newBuilder().factory().timeRange().fieldName(
							attr.getLocalName()).build().getId()));
			if (stat != null) {
				timeRange.setStartTime(stat.getMinTime());
				timeRange.setEndTime(stat.getMaxTime());
			}
		}
		return timeRange;
	}

	/**
	 * Clip the provided constraints using the statistics, if available.
	 *
	 *
	 * @param statsMap
	 * @param timeDescriptors
	 * @param constraintsSet
	 * @return
	 */
	public static TemporalConstraintsSet clipIndexedTemporalConstraints(
			final Map<StatisticsId, InternalDataStatistics<SimpleFeature, ?, ?>> statsMap,
			final TimeDescriptors timeDescriptors,
			final TemporalConstraintsSet constraintsSet ) {
		// TODO: if query range doesn't intersect with the stats, it seems the
		// constraints are removed or empty - does this make sense? It seems
		// this can result in open-ended time when it should find no results.
		if ((timeDescriptors.getEndRange() != null) && (timeDescriptors.getStartRange() != null)) {
			final String ename = timeDescriptors.getEndRange().getLocalName();
			final String sname = timeDescriptors.getStartRange().getLocalName();
			if (constraintsSet.hasConstraintsForRange(
					sname,
					ename)) {
				final TemporalRange statsStartRange = getStatsRange(
						statsMap,
						timeDescriptors.getStartRange());
				final TemporalRange statsEndRange = getStatsRange(
						statsMap,
						timeDescriptors.getEndRange());
				final TemporalRange fullRange = new TemporalRange(
						statsStartRange.getStartTime(),
						statsEndRange.getEndTime());

				final TemporalConstraints constraints = constraintsSet.getConstraintsForRange(
						sname,
						ename);
				constraints.replaceWithIntersections(new TemporalConstraints(
						fullRange,
						constraints.getName()));

				constraintsSet.removeAllConstraintsExcept(constraints.getName());
				// this should be fixed to handle interwoven range.
				// specifically look for non-overlapping regions of time
				return constraintsSet;
			}
		}
		else if ((timeDescriptors.getTime() != null)
				&& constraintsSet.hasConstraintsFor(timeDescriptors.getTime().getLocalName())) {
			final String name = timeDescriptors.getTime().getLocalName();
			final FeatureTimeRangeStatistics stats = ((FeatureTimeRangeStatistics) statsMap
					.get(VectorStatisticsQueryBuilder.newBuilder().factory().timeRange().fieldName(
							name).build().getId()));

			final TemporalConstraints constraints = constraintsSet.getConstraintsFor(name);
			if (stats != null) {
				constraints.replaceWithIntersections(new TemporalConstraints(
						stats.asTemporalRange(),
						name));
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
			final Map<StatisticsId, InternalDataStatistics<SimpleFeature, ?, ?>> statsMap ) {

		final String geoAttrName = featureType.getGeometryDescriptor().getLocalName();

		final StatisticsId statId = VectorStatisticsQueryBuilder.newBuilder().factory().bbox().fieldName(
				geoAttrName).build().getId();
		final FeatureBoundingBoxStatistics bboxStats = (FeatureBoundingBoxStatistics) statsMap.get(statId);
		if ((bboxStats != null) && bboxStats.isSet() && (bbox != null)) {
			final Geometry geo = bboxStats.composeGeometry(featureType
					.getGeometryDescriptor()
					.getType()
					.getCoordinateReferenceSystem());
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
			final Map<StatisticsId, InternalDataStatistics<SimpleFeature, ?, ?>> stats ) {

		if ((timeDescriptors.getEndRange() != null) || (timeDescriptors.getStartRange() != null)) {
			final FeatureTimeRangeStatistics endRange = (timeDescriptors.getEndRange() != null) ? ((FeatureTimeRangeStatistics) stats
					.get(VectorStatisticsQueryBuilder.newBuilder().factory().timeRange().fieldName(
							timeDescriptors.getEndRange().getLocalName()).build().getId())) : null;
			final FeatureTimeRangeStatistics startRange = (timeDescriptors.getStartRange() != null) ? ((FeatureTimeRangeStatistics) stats
					.get(VectorStatisticsQueryBuilder.newBuilder().factory().timeRange().fieldName(
							timeDescriptors.getStartRange().getLocalName()).build().getId())) : null;

			if ((endRange != null) && (startRange != null)) {
				return SpatialTemporalQuery.createConstraints(
						startRange.asTemporalRange().union(
								endRange.asTemporalRange()),
						true);
			}
			else if (endRange != null) {
				return SpatialTemporalQuery.createConstraints(
						endRange.asTemporalRange(),
						true);
			}
			else if (startRange != null) {
				return SpatialTemporalQuery.createConstraints(
						startRange.asTemporalRange(),
						true);
			}
		}
		else if (timeDescriptors.getTime() != null) {
			final FeatureTimeRangeStatistics timeStat = ((FeatureTimeRangeStatistics) stats
					.get(VectorStatisticsQueryBuilder.newBuilder().factory().timeRange().fieldName(
							timeDescriptors.getTime().getLocalName()).build().getId()));
			if (timeStat != null) {
				return SpatialTemporalQuery.createConstraints(
						timeStat.asTemporalRange(),
						true);
			}
		}
		return new ConstraintSet();
	}

	public static ConstraintSet getBBOXIndexConstraintsFromIndex(
			final SimpleFeatureType featureType,
			final Map<StatisticsId, InternalDataStatistics<SimpleFeature, ?, ?>> statsMap ) {
		final String geoAttrName = featureType.getGeometryDescriptor().getLocalName();
		final StatisticsId statId = VectorStatisticsQueryBuilder.newBuilder().factory().bbox().fieldName(
				geoAttrName).build().getId();
		final BoundingBoxDataStatistics<SimpleFeature> bboxStats = (BoundingBoxDataStatistics<SimpleFeature>) statsMap
				.get(statId);
		return (bboxStats != null) ? bboxStats.getConstraints() : new ConstraintSet();
	}

	/**
	 * Compose a time constraints. When the provided constraints do not fulfill
	 * the indexed dimensions, compose constraints from statistics.
	 *
	 *
	 * @param featureType
	 * @param timeDescriptors
	 * @param statsMap
	 * @param timeBoundsSet
	 * @return
	 */
	public static Constraints composeTimeConstraints(
			final SimpleFeatureType featureType,
			final TimeDescriptors timeDescriptors,
			final Map<StatisticsId, InternalDataStatistics<SimpleFeature, ?, ?>> statsMap,
			final TemporalConstraintsSet timeBoundsSet ) {

		final TemporalConstraints timeBounds = TimeUtils.getTemporalConstraintsForDescriptors(
				timeDescriptors,
				timeBoundsSet);
		return (timeBounds != null) && !timeBounds.isEmpty() ? SpatialTemporalQuery.createConstraints(
				timeBounds,
				false) : new Constraints(
				getTimeConstraintsFromIndex(
						timeDescriptors,
						statsMap));
	}

	/**
	 * If composed constraints matched statistics constraints, are empty or
	 * null, then return empty constraint set.
	 *
	 *
	 * @param featureType
	 * @param timeDescriptors
	 * @param statsMap
	 * @param timeBoundsSet
	 * @return
	 */
	public static Constraints composeTimeBoundedConstraints(
			final SimpleFeatureType featureType,
			final TimeDescriptors timeDescriptors,
			final Map<StatisticsId, InternalDataStatistics<SimpleFeature, ?, ?>> statsMap,
			final TemporalConstraintsSet timeBoundsSet ) {

		if ((timeBoundsSet == null) || timeBoundsSet.isEmpty() || !timeDescriptors.hasTime()) {
			return new Constraints();
		}

		final TemporalConstraints boundsTemporalConstraints = TimeUtils.getTemporalConstraintsForDescriptors(
				timeDescriptors,
				timeBoundsSet);

		if (boundsTemporalConstraints.isEmpty()) {
			return new Constraints();
		}

		final Constraints indexTimeConstraints = new Constraints(
				QueryIndexHelper.getTimeConstraintsFromIndex(
						timeDescriptors,
						statsMap));

		final Constraints boundsTimeConstraints = SpatialTemporalQuery.createConstraints(
				boundsTemporalConstraints,
				false);
		return (boundsTimeConstraints.matches(indexTimeConstraints)) ? new Constraints() : boundsTimeConstraints;
	}

	/**
	 * If composed constraints matched statistics constraints, are empty or
	 * null, then return empty constraint set
	 *
	 *
	 * @param featureType
	 * @param timeDescriptors
	 * @param statsMap
	 * @param timeBoundsSet
	 * @return
	 */
	public static GeoConstraintsWrapper composeGeometricConstraints(
			final SimpleFeatureType featureType,
			final Map<StatisticsId, InternalDataStatistics<SimpleFeature, ?, ?>> statsMap,
			final Geometry jtsBounds ) {
		if (jtsBounds == null) {
			return new GeoConstraintsWrapper(
					new Constraints(),
					true,
					null);
		}
		final GeoConstraintsWrapper geoConstraints = GeometryUtils.basicGeoConstraintsWrapperFromGeometry(jtsBounds);
		final Constraints statsConstraints = new Constraints(
				getBBOXIndexConstraintsFromIndex(
						featureType,
						statsMap));
		if (geoConstraints.getConstraints().matches(
				statsConstraints)) {
			return new GeoConstraintsWrapper(
					new Constraints(),
					geoConstraints.isConstraintsMatchGeometry(),
					jtsBounds);
		}
		return geoConstraints;
	}

	/**
	 * Compose a query from the set of constraints. When the provided
	 * constraints do not fulfill the indexed dimensions, compose constraints
	 * from statistics.
	 *
	 * @param featureType
	 * @param timeDescriptors
	 * @param statsMap
	 * @param jtsBounds
	 * @param timeBoundsSet
	 * @return
	 */
	public static Constraints composeConstraints(
			final SimpleFeatureType featureType,
			final TimeDescriptors timeDescriptors,
			final Map<StatisticsId, InternalDataStatistics<SimpleFeature, ?, ?>> statsMap,
			final Geometry jtsBounds,
			final TemporalConstraintsSet timeBoundsSet ) {

		final Constraints timeConstraints = composeTimeConstraints(
				featureType,
				timeDescriptors,
				statsMap,
				timeBoundsSet);
		final GeoConstraintsWrapper geoConstraints = composeGeometricConstraints(
				featureType,
				statsMap,
				jtsBounds);
		return timeConstraints.merge(geoConstraints.getConstraints());
	}
}
