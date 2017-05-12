package mil.nga.giat.geowave.adapter.vector.query.cql;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.filter.Filter;

import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.adapter.vector.GeotoolsFeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.plugin.ExtractAttributesFilter;
import mil.nga.giat.geowave.adapter.vector.plugin.ExtractGeometryFilterVisitor;
import mil.nga.giat.geowave.adapter.vector.plugin.ExtractGeometryFilterVisitorResult;
import mil.nga.giat.geowave.adapter.vector.plugin.ExtractTimeFilterVisitor;
import mil.nga.giat.geowave.adapter.vector.util.QueryIndexHelper;
import mil.nga.giat.geowave.adapter.vector.utils.TimeDescriptors;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.GeometryUtils.GeoConstraintsWrapper;
import mil.nga.giat.geowave.core.geotime.index.dimension.LatitudeDefinition;
import mil.nga.giat.geowave.core.geotime.index.dimension.TimeDefinition;
import mil.nga.giat.geowave.core.geotime.store.dimension.LatitudeField;
import mil.nga.giat.geowave.core.geotime.store.dimension.LongitudeField;
import mil.nga.giat.geowave.core.geotime.store.dimension.TimeField;
import mil.nga.giat.geowave.core.geotime.store.filter.SpatialQueryFilter.CompareOperation;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialTemporalQuery;
import mil.nga.giat.geowave.core.geotime.store.query.TemporalConstraints;
import mil.nga.giat.geowave.core.geotime.store.query.TemporalConstraintsSet;
import mil.nga.giat.geowave.core.geotime.store.query.TemporalQuery;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndex;
import mil.nga.giat.geowave.core.store.query.BasicQuery;
import mil.nga.giat.geowave.core.store.query.BasicQuery.Constraints;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.Query;

public class CQLQuery implements
		DistributableQuery
{
	private final static Logger LOGGER = LoggerFactory.getLogger(CQLQuery.class);
	private Query baseQuery;
	private CQLQueryFilter filter;
	private Filter cqlFilter;

	protected CQLQuery() {}

	public static Query createOptimalQuery(
			final String cql,
			final GeotoolsFeatureDataAdapter adapter,
			final PrimaryIndex index )
			throws CQLException {
		return createOptimalQuery(
				cql,
				adapter,
				index,
				null);
	}

	public static Query createOptimalQuery(
			final String cql,
			final GeotoolsFeatureDataAdapter adapter,
			final PrimaryIndex index,
			final BasicQuery baseQuery )
			throws CQLException {
		return createOptimalQuery(
				cql,
				adapter,
				CompareOperation.INTERSECTS,
				index,
				baseQuery);
	}

	public static Query createOptimalQuery(
			final String cql,
			final GeotoolsFeatureDataAdapter adapter,
			final CompareOperation geoCompareOp,
			final PrimaryIndex index,
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

	public static Query createOptimalQuery(
			final Filter cqlFilter,
			final GeotoolsFeatureDataAdapter adapter,
			final PrimaryIndex index,
			final BasicQuery baseQuery ) {
		return createOptimalQuery(
				cqlFilter,
				adapter,
				CompareOperation.INTERSECTS,
				index,
				baseQuery);
	}

	public static Query createOptimalQuery(
			final Filter cqlFilter,
			final GeotoolsFeatureDataAdapter adapter,
			final CompareOperation geoCompareOp,
			final PrimaryIndex index,
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
			attrs = new ArrayList<String>();
		}
		// assume the index can't handle spatial or temporal constraints if its
		// null
		final boolean isSpatial = index == null ? false : hasAtLeastSpatial(index);
		final boolean isTemporal = index == null ? false : hasTime(index) && adapter.hasTemporalConstraints();
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
							adapter.getFeatureType().getCoordinateReferenceSystem());
			final TemporalConstraintsSet timeConstraintSet = new ExtractTimeFilterVisitor(
					adapter.getTimeDescriptors()).getConstraints(cqlFilter);

			if (geometryAndCompareOp != null) {
				Geometry geometry = geometryAndCompareOp.getGeometry();
				final GeoConstraintsWrapper geoConstraints = GeometryUtils
						.basicGeoConstraintsWrapperFromGeometry(geometry);

				Constraints constraints = geoConstraints.getConstraints();
				final CompareOperation extractedCompareOp = geometryAndCompareOp.getCompareOp();
				if ((timeConstraintSet != null) && !timeConstraintSet.isEmpty()) {
					// determine which time constraints are associated with an
					// indexable
					// field
					final TemporalConstraints temporalConstraints = QueryIndexHelper
							.getTemporalConstraintsForDescriptors(
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
				final TemporalConstraints temporalConstraints = QueryIndexHelper.getTemporalConstraintsForDescriptors(
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
			return new CQLQuery(
					baseQuery,
					cqlFilter,
					adapter);
		}
	}

	public CQLQuery(
			final Query baseQuery,
			final Filter filter,
			final GeotoolsFeatureDataAdapter adapter ) {
		this.baseQuery = baseQuery;
		cqlFilter = filter;
		this.filter = new CQLQueryFilter(
				filter,
				adapter);
	}

	@Override
	public List<QueryFilter> createFilters(
			final CommonIndexModel indexModel ) {
		List<QueryFilter> queryFilters;
		// note, this assumes the CQL filter covers the baseQuery which *should*
		// be a safe assumption, otherwise we need to add the
		// baseQuery.createFilters to the list of query filters
		queryFilters = new ArrayList<QueryFilter>();
		if (filter != null) {
			queryFilters = new ArrayList<QueryFilter>(
					queryFilters);
			queryFilters.add(filter);
		}
		return queryFilters;
	}

	@Override
	public boolean isSupported(
			final Index<?, ?> index ) {
		if (baseQuery != null) {
			return baseQuery.isSupported(index);
		}
		return true;
	}

	@Override
	public List<MultiDimensionalNumericData> getIndexConstraints(
			final NumericIndexStrategy indexStrategy ) {
		if (baseQuery != null) {
			return baseQuery.getIndexConstraints(indexStrategy);
		}
		return Collections.emptyList();
	}

	@Override
	public byte[] toBinary() {
		byte[] baseQueryBytes;
		if (baseQuery != null) {
			if (!(baseQuery instanceof DistributableQuery)) {
				throw new IllegalArgumentException(
						"Cannot distribute CQL query with base query of type '" + baseQuery.getClass() + "'");
			}
			else {
				baseQueryBytes = PersistenceUtils.toBinary((DistributableQuery) baseQuery);
			}
		}
		else {
			// base query can be null, no reason to log a warning
			baseQueryBytes = new byte[] {};
		}
		final byte[] filterBytes;
		if (filter != null) {
			filterBytes = filter.toBinary();
		}
		else {
			LOGGER.warn("Filter is null");
			filterBytes = new byte[] {};
		}

		final ByteBuffer buf = ByteBuffer.allocate(filterBytes.length + baseQueryBytes.length + 4);
		buf.putInt(filterBytes.length);
		buf.put(filterBytes);
		buf.put(baseQueryBytes);
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int filterBytesLength = buf.getInt();
		final int baseQueryBytesLength = bytes.length - filterBytesLength - 4;
		if (filterBytesLength > 0) {
			final byte[] filterBytes = new byte[filterBytesLength];

			filter = new CQLQueryFilter();
			filter.fromBinary(filterBytes);
		}
		else {
			LOGGER.warn("CQL filter is empty bytes");
			filter = null;
		}
		if (baseQueryBytesLength > 0) {
			final byte[] baseQueryBytes = new byte[baseQueryBytesLength];

			try {
				baseQuery = PersistenceUtils.fromBinary(
						baseQueryBytes,
						DistributableQuery.class);
			}
			catch (final Exception e) {
				throw new IllegalArgumentException(
						e);
			}
		}
		else {
			// base query can be null, no reason to log a warning
			baseQuery = null;
		}
	}

	@Override
	public List<ByteArrayRange> getSecondaryIndexConstraints(
			final SecondaryIndex<?> index ) {
		final PropertyFilterVisitor visitor = new PropertyFilterVisitor();
		final PropertyConstraintSet constraints = (PropertyConstraintSet) cqlFilter.accept(
				visitor,
				null);
		return constraints.getRangesFor(index);
	}

	@Override
	public List<DistributableQueryFilter> getSecondaryQueryFilter(
			final SecondaryIndex<?> index ) {
		final PropertyFilterVisitor visitor = new PropertyFilterVisitor();
		final PropertyConstraintSet constraints = (PropertyConstraintSet) cqlFilter.accept(
				visitor,
				null);
		return constraints.getFiltersFor(index);
	}

	protected static boolean hasAtLeastSpatial(
			final PrimaryIndex index ) {
		if ((index == null) || (index.getIndexModel() == null) || (index.getIndexModel().getDimensions() == null)) {
			return false;
		}
		boolean hasLatitude = false;
		boolean hasLongitude = false;
		for (final NumericDimensionField dimension : index.getIndexModel().getDimensions()) {
			if (dimension instanceof LatitudeField) {
				hasLatitude = true;
			}
			if (dimension instanceof LongitudeField) {
				hasLongitude = true;
			}
		}
		return hasLatitude && hasLongitude;
	}

	protected static boolean hasTime(
			final PrimaryIndex index ) {
		if ((index == null) || (index.getIndexModel() == null) || (index.getIndexModel().getDimensions() == null)) {
			return false;
		}
		for (final NumericDimensionField dimension : index.getIndexModel().getDimensions()) {
			if (dimension instanceof TimeField) {
				return true;
			}
		}
		return false;
	}
}
