package org.locationtech.geowave.core.store.query;

import org.apache.commons.lang.ArrayUtils;
import org.locationtech.geowave.core.store.api.Query;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.query.options.FilterByTypeQueryOptions;
import org.locationtech.geowave.core.store.query.options.QuerySingleIndex;

public class QueryBuilderImpl<T, R extends QueryBuilder<T, R>> extends
		BaseQueryBuilderImpl<T, Query<T>, R> implements
		QueryBuilder<T, R>
{
	protected String[] typeNames = new String[0];
	protected String[] fieldNames = null;

	@Override
	public R allTypes() {
		this.typeNames = new String[0];
		return (R) this;
	}

	@Override
	public R addTypeName(
			final String typeName ) {
		ArrayUtils
				.add(
						typeNames,
						typeName);
		return (R) this;
	}

	@Override
	public R setTypeNames(
			final String[] typeNames ) {
		this.typeNames = typeNames;
		return (R) this;
	}

	@Override
	public R subsetFields(
			final String typeName,
			final String... fieldNames ) {
		this.typeNames = new String[] {
			typeName
		};
		this.fieldNames = fieldNames;
		return (R) this;
	}

	@Override
	public R allFields() {
		this.fieldNames = null;
		return (R) this;
	}

	protected FilterByTypeQueryOptions<T> newFilterByTypeQueryOptions() {
		return typeNames.length == 1 ? new FilterByTypeQueryOptions<>(
				typeNames[0],
				fieldNames)
				: new FilterByTypeQueryOptions<>(
						typeNames);
	}

	@Override
	public Query<T> build() {
		return new Query<>(
				newCommonQueryOptions(),
				newFilterByTypeQueryOptions(),
				new QuerySingleIndex(
						indexName),
				constraints);
	}
}
