package org.locationtech.geowave.core.store.api;

import org.locationtech.geowave.core.store.query.BaseQueryBuilder;
import org.locationtech.geowave.core.store.query.QueryBuilderImpl;

public interface QueryBuilder<T, R extends QueryBuilder<T, R>> extends
		BaseQueryBuilder<T, Query<T>, R>
{
	R allTypes();

	R addTypeName(
			String typeName );

	R setTypeNames(
			String[] typeNames );

	// is it clear that this overrides the type options above?
	R subsetFields(
			String typeName,
			String... fieldIds );

	R allFields();

	static <T> QueryBuilder<T, ?> newBuilder() {
		return new QueryBuilderImpl<>();
	}

}