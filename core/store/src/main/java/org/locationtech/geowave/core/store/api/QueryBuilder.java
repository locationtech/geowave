package org.locationtech.geowave.core.store.api;

import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.index.MultiDimensionalCoordinateRangesArray;
import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.constraints.BasicQuery.Constraints;
import org.locationtech.geowave.core.store.query.filter.BasicQueryFilter.BasicQueryCompareOperation;
import org.locationtech.geowave.core.store.query.options.CommonQueryOptions;
import org.locationtech.geowave.core.store.query.options.DataTypeQueryOptions;
import org.locationtech.geowave.core.store.query.options.IndexQueryOptions;

public interface QueryBuilder<T>
{

	QueryBuilder<T> allIndicies();

	QueryBuilder<T> index(
			Index index );

	QueryBuilder<T> indexId(
			String indexId );

	QueryBuilder<T> allTypes();

	QueryBuilder<T> addTypeId(
			String dataAdapterTypeId );

	QueryBuilder<T> setTypeIds(
			String[] dataAdapterTypeIds );

	QueryBuilder<T> addType(
			DataTypeAdapter<T> type );

	QueryBuilder<T> setTypes(
			DataTypeAdapter<T>[] types );

	// is it clear that this overrides the type options above?
	//also is this the best way to use the generic considering order matters with this?
	QueryBuilder<T> aggregate(
			DataTypeAdapter<?> type,
			Aggregation<?, T, ?> aggregation );

	// this is a convenience method to set the count aggregation
	QueryBuilder<Long> count();

	// is it clear that this overrides the type options above?
	QueryBuilder<T> subsetFields(
			DataTypeAdapter<T> type,
			String[] fieldIds );

	QueryBuilder<T> allFields();

	QueryBuilder<T> addAuthorization(
			String authorization );

	QueryBuilder<T> setAuthorizations(
			String[] authorizations );

	QueryBuilder<T> subsampling(
			double[] maxResolutionPerDimension );

	QueryBuilder<T> noLimit();

	QueryBuilder<T> limit(
			int limit );

	QueryBuilder<T> maxRanges(
			int maxRangeDecomposition );

	QueryBuilder<T> noMaxRanges();

	QueryBuilder<T> addDataId(
			ByteArrayId dataId );

	QueryBuilder<T> setDataIds(
			ByteArrayId[] dataIds );

	QueryBuilder<T> prefix(
			ByteArrayId partitionKey,
			ByteArrayId sortKeyPrefix );

	QueryBuilder<T> coordinateRanges(
			NumericIndexStrategy indexStrategy,
			MultiDimensionalCoordinateRangesArray[] coordinateRanges );

	QueryBuilder<T> constraints(
			Constraints constraints );

	QueryBuilder<T> constraints(
			Constraints constraints,
			BasicQueryCompareOperation compareOp );

	QueryBuilder<T> noConstraints();

	QueryBuilder<T> commonOptions(
			CommonQueryOptions commonQueryOptions );

	QueryBuilder<T> dataTypeOptions(
			DataTypeQueryOptions<T> dataTypeOptions );

	QueryBuilder<T> indexOptions(
			IndexQueryOptions indexOptions );

	QueryBuilder<T> constraints(
			QueryConstraints constraints );

	Query<T> build();
	
	static <T> QueryBuilder<T> newBuilder() {
		return null;
	}

}