package org.locationtech.geowave.core.store.query;

import org.locationtech.geowave.core.store.api.QueryConstraintsFactory;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraintsFactoryImpl;
import org.locationtech.geowave.core.store.query.options.CommonQueryOptions.HintKey;

public interface BaseQueryBuilder<T, Q extends BaseQuery<T, ?>, R extends BaseQueryBuilder<T, Q, R>>
{
	R allIndicies();

	R indexName(
			String indexName );

	R addAuthorization(
			String authorization );

	R setAuthorizations(
			String[] authorizations );

	R noAuthorizations();

	R noLimit();

	R limit(
			int limit );

	<HintValueType> R addHint(
			HintKey<HintValueType> key,
			HintValueType value );

	R noHints();

	R constraints(
			QueryConstraints constraints );

	default QueryConstraintsFactory constraintsFactory() {
		return QueryConstraintsFactoryImpl.SINGLETON_INSTANCE;
	}

	Q build();
}
