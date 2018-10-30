package org.locationtech.geowave.core.geotime.store.query;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.locationtech.geowave.core.geotime.store.GeotoolsFeatureDataAdapter;
import org.locationtech.geowave.core.geotime.store.query.filter.SpatialQueryFilter.CompareOperation;
import org.locationtech.geowave.core.geotime.util.ExtractAttributesFilter;
import org.locationtech.geowave.core.geotime.util.ExtractGeometryFilterVisitor;
import org.locationtech.geowave.core.geotime.util.ExtractGeometryFilterVisitorResult;
import org.locationtech.geowave.core.geotime.util.ExtractTimeFilterVisitor;
import org.locationtech.geowave.core.geotime.util.GeometryUtils;
import org.locationtech.geowave.core.geotime.util.GeometryUtils.GeoConstraintsWrapper;
import org.locationtech.geowave.core.geotime.util.IndexOptimizationUtils;
import org.locationtech.geowave.core.geotime.util.TimeDescriptors;
import org.locationtech.geowave.core.geotime.util.TimeUtils;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.index.SecondaryIndexImpl;
import org.locationtech.geowave.core.store.query.constraints.AdapterAndIndexBasedQueryConstraints;
import org.locationtech.geowave.core.store.query.constraints.BasicQuery;
import org.locationtech.geowave.core.store.query.constraints.BasicQuery.Constraints;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Geometry;

public class OptimalCQLQuery implements
		AdapterAndIndexBasedQueryConstraints,
		QueryConstraints
{
	private final static Logger LOGGER = LoggerFactory.getLogger(OptimalCQLQuery.class);

	public static QueryConstraints createOptimalQuery(
			final String cql,
			final GeotoolsFeatureDataAdapter adapter,
			final Index index )
			throws CQLException {
		return createOptimalQuery(
				cql,
				adapter,
				index,
				null);
	}

	public static QueryConstraints createOptimalQuery(
			final String cql,
			final GeotoolsFeatureDataAdapter adapter,
			final Index index,
			final BasicQuery baseQuery )
			throws CQLException {
		return createOptimalQuery(
				cql,
				adapter,
				CompareOperation.INTERSECTS,
				index,
				baseQuery);
	}

	public static QueryConstraints createOptimalQuery(
			final String cql,
			final GeotoolsFeatureDataAdapter adapter,
			final CompareOperation geoCompareOp,
			final Index index,
			final BasicQuery baseQuery )
			throws CQLException {
		final Filter cqlFilter = CQL.toFilter(cql);
		return createOptimalQuery(
				cqlFilter,
				adapter,
				geoCompareOp,
				index,
				baseQuery);
	}

	public static QueryConstraints createOptimalQuery(
			final Filter cqlFilter,
			final GeotoolsFeatureDataAdapter adapter,
			final Index index ) {
		return createOptimalQuery(
				cqlFilter,
				adapter,
				index,
				null);
	}

	public static QueryConstraints createOptimalQuery(
			final Filter cqlFilter,
			final GeotoolsFeatureDataAdapter adapter,
			final Index index,
			final BasicQuery baseQuery ) {
		return createOptimalQuery(
				cqlFilter,
				adapter,
				CompareOperation.INTERSECTS,
				index,
				baseQuery);
	}

	public static QueryConstraints createOptimalQuery(
			final Filter cqlFilter,
			final GeotoolsFeatureDataAdapter adapter,
			final CompareOperation geoCompareOp,
			final Index index,
			BasicQuery baseQuery ) {
		final ExtractAttributesFilter attributesVisitor = new ExtractAttributesFilter();

		final Object obj = cqlFilter.accept(
				attributesVisitor,
				null);

		final Collection<String> attrs;
		if ((obj != null) && (obj instanceof Collection)) {
			attrs = (Collection<String>) obj;
		}
		else {
			attrs = new ArrayList<>();
		}
		// assume the index can't handle spatial or temporal constraints if its
		// null
		final boolean isSpatial = IndexOptimizationUtils.hasAtLeastSpatial(index);
		final boolean isTemporal = IndexOptimizationUtils.hasTime(
				index,
				adapter);
		if (isSpatial) {
			final String geomName = adapter.getFeatureType().getGeometryDescriptor().getLocalName();
			attrs.remove(geomName);
		}
		if (isTemporal) {
			final TimeDescriptors timeDescriptors = adapter.getTimeDescriptors();
			if (timeDescriptors != null) {
				final AttributeDescriptor timeDesc = timeDescriptors.getTime();
				if (timeDesc != null) {
					attrs.remove(timeDesc.getLocalName());
				}
				final AttributeDescriptor startDesc = timeDescriptors.getStartRange();
				if (startDesc != null) {
					attrs.remove(startDesc.getLocalName());
				}
				final AttributeDescriptor endDesc = timeDescriptors.getEndRange();
				if (endDesc != null) {
					attrs.remove(endDesc.getLocalName());
				}
			}
		}
		if (baseQuery == null) {
			// there is only space and time
			final ExtractGeometryFilterVisitorResult geometryAndCompareOp = ExtractGeometryFilterVisitor
					.getConstraints(
							cqlFilter,
							adapter.getFeatureType().getCoordinateReferenceSystem(),
							adapter.getFeatureType().getGeometryDescriptor().getLocalName());
			final TemporalConstraintsSet timeConstraintSet = new ExtractTimeFilterVisitor(
					adapter.getTimeDescriptors()).getConstraints(cqlFilter);

			if (geometryAndCompareOp != null) {
				final Geometry geometry = geometryAndCompareOp.getGeometry();
				final GeoConstraintsWrapper geoConstraints = GeometryUtils
						.basicGeoConstraintsWrapperFromGeometry(geometry);

				Constraints constraints = geoConstraints.getConstraints();
				final CompareOperation extractedCompareOp = geometryAndCompareOp.getCompareOp();
				if ((timeConstraintSet != null) && !timeConstraintSet.isEmpty()) {
					// determine which time constraints are associated with an
					// indexable
					// field
					final TemporalConstraints temporalConstraints = TimeUtils.getTemporalConstraintsForDescriptors(
							adapter.getTimeDescriptors(),
							timeConstraintSet);
					// convert to constraints
					final Constraints timeConstraints = SpatialTemporalQuery.createConstraints(
							temporalConstraints,
							false);
					constraints = geoConstraints.getConstraints().merge(
							timeConstraints);
				}
				// TODO: this actually doesn't boost performance much, if at
				// all, and one key is missing - the query geometry has to be
				// topologically equivalent to its envelope and the ingested
				// geometry has to be topologically equivalent to its envelope
				// this could be kept as a statistic on ingest, but considering
				// it doesn't boost performance it may not be worthwhile
				// pursuing

				// if (geoConstraints.isConstraintsMatchGeometry() &&
				// CompareOperation.INTERSECTS.equals(geoCompareOp)) {
				// baseQuery = new BasicQuery(
				// constraints);
				// }
				// else {
				baseQuery = new SpatialQuery(
						constraints,
						geometry,
						extractedCompareOp);

				// ExtractGeometryFilterVisitor sets predicate to NULL when CQL
				// expression
				// involves multiple dissimilar geometric relationships (i.e.
				// "CROSSES(...) AND TOUCHES(...)")
				// In which case, baseQuery is not sufficient to represent CQL
				// expression.
				// By setting Exact flag to false we are forcing CQLQuery to
				// represent CQL expression but use
				// linear constraint from baseQuery
				if (extractedCompareOp == null) {
					baseQuery.setExact(false);
				}
				// }
			}
			else if ((timeConstraintSet != null) && !timeConstraintSet.isEmpty()) {
				// determine which time constraints are associated with an
				// indexable
				// field
				final TemporalConstraints temporalConstraints = TimeUtils.getTemporalConstraintsForDescriptors(
						adapter.getTimeDescriptors(),
						timeConstraintSet);
				baseQuery = new TemporalQuery(
						temporalConstraints);
			}
		}
		// if baseQuery completely represents CQLQuery expression then use that
		if (attrs.isEmpty() && (baseQuery != null) && baseQuery.isExact()) {
			return baseQuery;
		}
		else {
			// baseQuery is passed to CQLQuery just to extract out linear
			// constraints only
			return new ExplicitCQLQuery(
					baseQuery,
					cqlFilter,
					adapter);
		}
	}

	private Filter filter;

	public OptimalCQLQuery() {}

	public OptimalCQLQuery(
			final Filter filter ) {
		this.filter = filter;
	}

	@Override
	public QueryConstraints createQueryConstraints(
			final DataTypeAdapter<?> adapter,
			final Index index ) {
		if (adapter instanceof GeotoolsFeatureDataAdapter) {
			return createOptimalQuery(
					filter,
					(GeotoolsFeatureDataAdapter) adapter,
					index);
		}
		if ((adapter instanceof InternalDataAdapter)
				&& (((InternalDataAdapter) adapter).getAdapter() instanceof GeotoolsFeatureDataAdapter)) {
			return createOptimalQuery(
					filter,
					(GeotoolsFeatureDataAdapter) ((InternalDataAdapter) adapter).getAdapter(),
					index);
		}
		LOGGER.error("Adapter is not a geotools feature adapter.  Cannot apply CQL filter.");
		return null;
	}

	@Override
	public byte[] toBinary() {
		byte[] filterBytes;
		if (filter == null) {
			LOGGER.warn("CQL filter is null");
			filterBytes = new byte[] {};
		}
		else {
			filterBytes = StringUtils.stringToBinary(ECQL.toCQL(filter));
		}
		return filterBytes;
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		try {
			GeometryUtils.initClassLoader();
		}
		catch (final MalformedURLException e) {
			LOGGER.error(
					"Unable to initialize GeoTools class loader",
					e);
		}
		if (bytes.length > 0) {
			final String cql = StringUtils.stringFromBinary(bytes);
			try {
				filter = ECQL.toFilter(cql);
			}
			catch (final Exception e) {
				throw new IllegalArgumentException(
						cql,
						e);
			}
		}
		else {
			LOGGER.warn("CQL filter is empty bytes");
			filter = null;
		}
	}

}
