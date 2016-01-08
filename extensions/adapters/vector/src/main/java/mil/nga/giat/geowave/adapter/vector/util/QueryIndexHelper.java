package mil.nga.giat.geowave.adapter.vector.util;

import java.util.Date;
import java.util.Map;

import mil.nga.giat.geowave.adapter.vector.stats.FeatureBoundingBoxStatistics;
import mil.nga.giat.geowave.adapter.vector.stats.FeatureTimeRangeStatistics;
import mil.nga.giat.geowave.adapter.vector.utils.TimeDescriptors;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialTemporalQuery;
import mil.nga.giat.geowave.core.geotime.store.query.TemporalConstraints;
import mil.nga.giat.geowave.core.geotime.store.query.TemporalConstraintsSet;
import mil.nga.giat.geowave.core.geotime.store.query.TemporalRange;
import mil.nga.giat.geowave.core.geotime.store.statistics.BoundingBoxDataStatistics;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.query.BasicQuery.ConstraintSet;
import mil.nga.giat.geowave.core.store.query.BasicQuery.Constraints;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

import com.vividsolutions.jts.geom.Geometry;

public class QueryIndexHelper
{

	private static TemporalRange getStatsRange(
			final Map<ByteArrayId, DataStatistics<SimpleFeature>> statsMap,
			final AttributeDescriptor attr ) {
		final TemporalRange timeRange = new TemporalRange();
		if (attr != null) {
			final FeatureTimeRangeStatistics stat = ((FeatureTimeRangeStatistics) statsMap.get(FeatureTimeRangeStatistics.composeId(attr.getLocalName())));
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
			final Map<ByteArrayId, DataStatistics<SimpleFeature>> statsMap,
			final TimeDescriptors timeDescriptors,
			final TemporalConstraintsSet constraintsSet ) {

		if ((timeDescriptors.getEndRange() != null) && (timeDescriptors.getStartRange() != null)) {
			final TemporalRange statsStartRange = getStatsRange(
					statsMap,
					timeDescriptors.getStartRange());
			final TemporalRange statsEndRange = getStatsRange(
					statsMap,
					timeDescriptors.getEndRange());
			final TemporalRange fullRange = new TemporalRange(
					statsStartRange.getStartTime(),
					statsEndRange.getEndTime());

			final String ename = timeDescriptors.getEndRange().getLocalName();
			final String sname = timeDescriptors.getStartRange().getLocalName();
			final String rangeName = sname + "_" + ename;

			final TemporalConstraints sconstraints = constraintsSet.getConstraintsFor(sname);
			final TemporalConstraints econstraints = constraintsSet.getConstraintsFor(ename);

			fullRange.setStartTime(!sconstraints.isEmpty() ? max(
					sconstraints.getMinOr(fullRange.getStartTime()),
					fullRange.getStartTime()) : fullRange.getStartTime());

			fullRange.setEndTime(!econstraints.isEmpty() ? min(
					sconstraints.getMaxOr(fullRange.getEndTime()),
					fullRange.getEndTime()) : fullRange.getEndTime());

			final TemporalConstraints constraints = constraintsSet.getConstraintsFor(rangeName);
			constraints.replaceWithIntersections(new TemporalConstraints(
					fullRange,
					rangeName));

			constraintsSet.removeAllConstraintsExcept(rangeName);
			// this should be fixed to handle interwoven range.
			// specifically look for non-overlapping regions of time
			return constraintsSet;
		}
		else if ((timeDescriptors.getTime() != null) && constraintsSet.hasConstraintsFor(timeDescriptors.getTime().getLocalName())) {
			final String name = timeDescriptors.getTime().getLocalName();
			final FeatureTimeRangeStatistics stats = ((FeatureTimeRangeStatistics) statsMap.get(FeatureTimeRangeStatistics.composeId(name)));

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
	 * Compose temporal constraints given the constraint set and the descriptors
	 * for the index.
	 * 
	 * @param timeDescriptors
	 * @param constraintsSet
	 * @return null if the constraints does not have the fields required by the
	 *         time descriptors
	 */
	public static TemporalConstraints composeRangeTemporalConstraints(
			final TimeDescriptors timeDescriptors,
			final TemporalConstraintsSet constraintsSet ) {

		if ((timeDescriptors.getEndRange() != null) && (timeDescriptors.getStartRange() != null)) {
			final String ename = timeDescriptors.getEndRange().getLocalName();
			final String sname = timeDescriptors.getStartRange().getLocalName();
			final String rangeName = sname + "_" + ename;

			if (constraintsSet.hasConstraintsFor(rangeName)) {
				return constraintsSet.getConstraintsFor(rangeName);
			}

			final TemporalConstraints sconstraints = constraintsSet.getConstraintsFor(sname);
			final TemporalConstraints econstraints = constraintsSet.getConstraintsFor(ename);

			final TemporalRange fullRange = new TemporalRange(
					sconstraints.getStartRange().getStartTime(),
					econstraints.getEndRange().getEndTime());

			return new TemporalConstraints(
					fullRange,
					rangeName);
		}
		else if ((timeDescriptors.getTime() != null) && constraintsSet.hasConstraintsFor(timeDescriptors.getTime().getLocalName())) {
			return constraintsSet.getConstraintsFor(timeDescriptors.getTime().getLocalName());
		}
		return new TemporalConstraints();
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
			final Map<ByteArrayId, DataStatistics<SimpleFeature>> statsMap ) {

		final String geoAttrName = featureType.getGeometryDescriptor().getLocalName();

		final ByteArrayId statId = FeatureBoundingBoxStatistics.composeId(geoAttrName);
		final FeatureBoundingBoxStatistics bboxStats = (FeatureBoundingBoxStatistics) statsMap.get(statId);
		if (bboxStats != null && bbox != null) {
			final Geometry geo = bboxStats.composeGeometry(featureType.getGeometryDescriptor().getType().getCoordinateReferenceSystem());
			return geo.intersection(bbox);
		}
		return bbox;
	}

	public static ConstraintSet getTimeConstraintsFromIndex(
			final TimeDescriptors timeDescriptors,
			final Map<ByteArrayId, DataStatistics<SimpleFeature>> stats ) {

		if ((timeDescriptors.getEndRange() != null) || (timeDescriptors.getStartRange() != null)) {
			final FeatureTimeRangeStatistics endRange = (timeDescriptors.getEndRange() != null) ? ((FeatureTimeRangeStatistics) stats.get(FeatureTimeRangeStatistics.composeId(timeDescriptors.getEndRange().getLocalName()))) : null;
			final FeatureTimeRangeStatistics startRange = (timeDescriptors.getStartRange() != null) ? ((FeatureTimeRangeStatistics) stats.get(FeatureTimeRangeStatistics.composeId(timeDescriptors.getStartRange().getLocalName()))) : null;

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
			final FeatureTimeRangeStatistics timeStat = ((FeatureTimeRangeStatistics) stats.get(FeatureTimeRangeStatistics.composeId(timeDescriptors.getTime().getLocalName())));
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
			final Map<ByteArrayId, DataStatistics<SimpleFeature>> statsMap ) {

		final String geoAttrName = featureType.getGeometryDescriptor().getLocalName();

		final ByteArrayId statId = FeatureBoundingBoxStatistics.composeId(geoAttrName);
		final BoundingBoxDataStatistics<SimpleFeature> bboxStats = (BoundingBoxDataStatistics<SimpleFeature>) statsMap.get(statId);
		return (bboxStats != null) ? bboxStats.getConstraints() : new ConstraintSet();
	}

	public static TemporalConstraints getTemporalConstraintsForDescriptors(
			final TimeDescriptors timeDescriptors,
			final TemporalConstraintsSet timeBoundsSet ) {
		if ((timeBoundsSet == null) || timeBoundsSet.isEmpty()) {
			return new TemporalConstraints();
		}

		if ((timeDescriptors.getStartRange() != null) && (timeDescriptors.getEndRange() != null)) {
			return composeRangeTemporalConstraints(
					timeDescriptors,
					timeBoundsSet);
		}
		else if ((timeDescriptors.getTime() != null) && timeBoundsSet.hasConstraintsFor(timeDescriptors.getTime().getLocalName())) {
			return timeBoundsSet.getConstraintsFor(timeDescriptors.getTime().getLocalName());
		}

		return new TemporalConstraints();
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
			final Map<ByteArrayId, DataStatistics<SimpleFeature>> statsMap,
			final TemporalConstraintsSet timeBoundsSet ) {

		final TemporalConstraints timeBounds = getTemporalConstraintsForDescriptors(
				timeDescriptors,
				timeBoundsSet);
		return timeBounds != null && !timeBounds.isEmpty() ? SpatialTemporalQuery.createConstraints(
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
			final Map<ByteArrayId, DataStatistics<SimpleFeature>> statsMap,
			final TemporalConstraintsSet timeBoundsSet ) {

		if (timeBoundsSet == null || timeBoundsSet.isEmpty() || !timeDescriptors.hasTime()) return new Constraints();

		final TemporalConstraints boundsTemporalConstraints = QueryIndexHelper.getTemporalConstraintsForDescriptors(
				timeDescriptors,
				timeBoundsSet);

		if (boundsTemporalConstraints.isEmpty()) return new Constraints();

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
	public static Constraints composeGeometricConstraints(
			final SimpleFeatureType featureType,
			final Map<ByteArrayId, DataStatistics<SimpleFeature>> statsMap,
			final Geometry jtsBounds ) {
		if (jtsBounds == null) return new Constraints();
		final Constraints geoConstraints = GeometryUtils.basicConstraintsFromGeometry(jtsBounds);
		final Constraints statsConstraints = new Constraints(
				getBBOXIndexConstraintsFromIndex(
						featureType,
						statsMap));
		return (geoConstraints.matches(statsConstraints)) ? new Constraints() : geoConstraints;
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
			final Map<ByteArrayId, DataStatistics<SimpleFeature>> statsMap,
			final Geometry jtsBounds,
			final TemporalConstraintsSet timeBoundsSet ) {

		final Constraints timeConstraints = composeTimeConstraints(
				featureType,
				timeDescriptors,
				statsMap,
				timeBoundsSet);
		final Constraints geoConstraints = composeGeometricConstraints(
				featureType,
				statsMap,
				jtsBounds);
		return timeConstraints.merge(geoConstraints);
	}

	private static Date min(
			final Date d1,
			final Date d2 ) {
		return (d1.before(d2)) ? d1 : d2;
	}

	private static Date max(
			final Date d1,
			final Date d2 ) {
		return (d1.before(d2)) ? d2 : d1;
	}
}
