package org.locationtech.geowave.core.store.query;

import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Query;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.index.IndexConstraints;

public class QueryBuilderImpl<T> implements
		QueryBuilder<T>
{

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.locationtech.geowave.core.store.api.QueryBuilder#allIndicies()
	 */
	@Override
	public QueryBuilder<T> allIndicies() {
		return this;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.locationtech.geowave.core.store.api.QueryBuilder#index(org.locationtech
	 * .geowave.core.store.api.Index)
	 */
	@Override
	public QueryBuilder<T> index(
			Index index ) {
		return this;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.locationtech.geowave.core.store.api.QueryBuilder#index(org.locationtech
	 * .geowave.core.index.ByteArrayId)
	 */
	@Override
	public QueryBuilder<T> index(
			ByteArrayId index ) {
		return this;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.locationtech.geowave.core.store.api.QueryBuilder#allTypes()
	 */
	@Override
	public QueryBuilder<T> allTypes() {
		return this;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.locationtech.geowave.core.store.api.QueryBuilder#addTypeId(org.
	 * locationtech.geowave.core.index.ByteArrayId)
	 */
	@Override
	public QueryBuilder<T> addTypeId(
			ByteArrayId typeId ) {
		return this;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.locationtech.geowave.core.store.api.QueryBuilder#typeIds(org.locationtech
	 * .geowave.core.index.ByteArrayId[])
	 */
	@Override
	public QueryBuilder<T> typeIds(
			ByteArrayId[] typeIds ) {
		return this;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.locationtech.geowave.core.store.api.QueryBuilder#addType(org.locationtech
	 * .geowave.core.store.api.DataTypeAdapter)
	 */
	@Override
	public QueryBuilder<T> addType(
			DataTypeAdapter<T> type ) {
		return this;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.locationtech.geowave.core.store.api.QueryBuilder#types(org.locationtech
	 * .geowave.core.store.api.DataTypeAdapter)
	 */
	@Override
	public QueryBuilder<T> types(
			DataTypeAdapter<T> types ) {
		return this;
	}

	// is it clear that this overrides the type options above?
	/*
	 * (non-Javadoc)
	 * 
	 * @see org.locationtech.geowave.core.store.api.QueryBuilder#aggregate(org.
	 * locationtech.geowave.core.store.api.DataTypeAdapter,
	 * org.locationtech.geowave.core.store.api.Aggregation)
	 */
	@Override
	public <T extends Mergeable> QueryBuilder<T> aggregate(
			DataTypeAdapter<?> type,
			Aggregation<?, T, ?> aggregation ) {
		return (QueryBuilder<T>) this;
	}

	// is it clear that this overrides the type options above?
	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.locationtech.geowave.core.store.api.QueryBuilder#subsetFields(org
	 * .locationtech.geowave.core.store.api.DataTypeAdapter, java.lang.String[])
	 */
	@Override
	public QueryBuilder<T> subsetFields(
			DataTypeAdapter<T> type,
			String[] fieldIds ) {
		return this;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.locationtech.geowave.core.store.api.QueryBuilder#allFields()
	 */
	@Override
	public QueryBuilder<T> allFields() {
		return this;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.locationtech.geowave.core.store.api.QueryBuilder#addAuthorization
	 * (java.lang.String)
	 */
	@Override
	public QueryBuilder<T> addAuthorization(
			String authorization ) {
		return this;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.locationtech.geowave.core.store.api.QueryBuilder#authorizations(java
	 * .lang.String[])
	 */
	@Override
	public QueryBuilder<T> authorizations(
			String[] authorizations ) {
		return this;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.locationtech.geowave.core.store.api.QueryBuilder#subsampling(double
	 * [])
	 */
	@Override
	public QueryBuilder<T> subsampling(
			double[] maxResolutionPerDimension ) {
		return this;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.locationtech.geowave.core.store.api.QueryBuilder#noLimit()
	 */
	@Override
	public QueryBuilder<T> noLimit() {
		return this;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.locationtech.geowave.core.store.api.QueryBuilder#limit(int)
	 */
	@Override
	public QueryBuilder<T> limit(
			int limit ) {
		return this;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.locationtech.geowave.core.store.api.QueryBuilder#maxRanges(int)
	 */
	@Override
	public QueryBuilder<T> maxRanges(
			int maxRangeDecomposition ) {
		return this;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.locationtech.geowave.core.store.api.QueryBuilder#noMaxRanges()
	 */
	@Override
	public QueryBuilder<T> noMaxRanges() {
		return this;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.locationtech.geowave.core.store.api.QueryBuilder#constraints(org.
	 * locationtech.geowave.core.store.api.QueryConstraints)
	 */
	@Override
	public QueryBuilder<T> constraints(
			QueryConstraints constraints ) {
		return this;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.locationtech.geowave.core.store.api.QueryBuilder#noConstraints()
	 */
	@Override
	public QueryBuilder<T> noConstraints() {
		return this;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.locationtech.geowave.core.store.api.QueryBuilder#build()
	 */
	@Override
	public Query<T> build() {
		return null;
	}
}
