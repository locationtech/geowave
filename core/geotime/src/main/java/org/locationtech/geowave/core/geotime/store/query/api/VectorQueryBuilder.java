package org.locationtech.geowave.core.geotime.store.query.api;

import java.util.Date;

import org.locationtech.geowave.core.geotime.store.query.filter.SpatialQueryFilter.CompareOperation;
import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.index.MultiDimensionalCoordinateRangesArray;
import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.api.QueryConstraints;
import org.locationtech.geowave.core.store.query.constraints.BasicQuery.Constraints;
import org.locationtech.geowave.core.store.query.filter.BasicQueryFilter.BasicQueryCompareOperation;
import org.locationtech.geowave.core.store.query.options.CommonQueryOptions;
import org.locationtech.geowave.core.store.query.options.DataTypeQueryOptions;
import org.locationtech.geowave.core.store.query.options.IndexQueryOptions;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;

import com.vividsolutions.jts.geom.Geometry;

public interface VectorQueryBuilder extends
		QueryBuilder<SimpleFeature>
{
	// TODO all these method overrides are just to make sure the builder type
	// stays as VectorQueryBuilder, is there a better way to do this?
	@Override
	VectorQueryBuilder allIndicies();

	@Override
	VectorQueryBuilder index(
			Index index );

	@Override
	VectorQueryBuilder indexId(
			ByteArrayId indexId );

	@Override
	VectorQueryBuilder allTypes();

	@Override
	VectorQueryBuilder addTypeId(
			ByteArrayId typeId );

	@Override
	VectorQueryBuilder typeIds(
			ByteArrayId[] typeIds );

	@Override
	VectorQueryBuilder addType(
			DataTypeAdapter<SimpleFeature> type );

	@Override
	VectorQueryBuilder types(
			DataTypeAdapter<SimpleFeature> types );

	@Override
	VectorQueryBuilder subsetFields(
			DataTypeAdapter<SimpleFeature> type,
			String[] fieldIds );

	@Override
	VectorQueryBuilder allFields();

	@Override
	VectorQueryBuilder addAuthorization(
			String authorization );

	@Override
	VectorQueryBuilder authorizations(
			String[] authorizations );

	@Override
	VectorQueryBuilder subsampling(
			double[] maxResolutionPerDimension );

	@Override
	VectorQueryBuilder noLimit();

	@Override
	VectorQueryBuilder limit(
			int limit );

	@Override
	VectorQueryBuilder maxRanges(
			int maxRangeDecomposition );

	@Override
	VectorQueryBuilder noMaxRanges();

	@Override
	VectorQueryBuilder addDataId(
			ByteArrayId dataId );

	@Override
	VectorQueryBuilder dataIds(
			ByteArrayId[] dataIds );

	@Override
	VectorQueryBuilder prefix(
			ByteArrayId partitionKey,
			ByteArrayId sortKeyPrefix );

	@Override
	VectorQueryBuilder coordinateRanges(
			NumericIndexStrategy indexStrategy,
			MultiDimensionalCoordinateRangesArray[] coordinateRanges );

	@Override
	VectorQueryBuilder constraints(
			Constraints constraints );

	@Override
	VectorQueryBuilder constraints(
			Constraints constraints,
			BasicQueryCompareOperation compareOp );

	@Override
	VectorQueryBuilder noConstraints();

	@Override
	VectorQueryBuilder commonOptions(
			CommonQueryOptions commonQueryOptions );

	@Override
	VectorQueryBuilder dataTypeOptions(
			DataTypeQueryOptions<SimpleFeature> dataTypeOptions );

	@Override
	VectorQueryBuilder indexOptions(
			IndexQueryOptions indexOptions );

	@Override
	VectorQueryBuilder constraints(
			QueryConstraints constraints );

	// more convenience methods for aggregations
	// this will work if geo is part of common index or data adapter is provided
	VectorQueryBuilder bboxOfResults();

	VectorQueryBuilder timeRangeOfResults();

	VectorQueryBuilder spatialConstraint(
			Geometry geometry );

	VectorQueryBuilder spatialConstraintCrs(
			String crsCode );

	VectorQueryBuilder spatialConstraintCompareOperation(
			CompareOperation compareOperation );

	// we can always support open-ended time using beginning of epoch as default
	// start and some end of time such as max long as default end
	VectorQueryBuilder start(
			Date startTime );

	VectorQueryBuilder end(
			Date startTime );

	// these cql expressions should always attempt to use
	// CQLQuery.createOptimalQuery() which requires adapter and index
	VectorQueryBuilder cqlConstraint(
			String cqlExpression );

	VectorQueryBuilder filterConstraint(
			Filter filter );

	static VectorQueryBuilder newBuilder() {
		return null;
	}

}
