package org.locationtech.geowave.core.store.api;

import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.query.BaseQueryBuilder;
import org.locationtech.geowave.core.store.query.aggregate.AggregationQueryBuilderImpl;

public interface AggregationQueryBuilder<P extends Persistable, R, T, A extends AggregationQueryBuilder<P, R, T, A>>
		extends
		BaseQueryBuilder<R, AggregationQuery<P, R, T>, A>
{
	A aggregate(
			String typeName,
			Aggregation<P, R, T> aggregation );

	// this is a convenience method to set the count aggregation
	// if no type names are given it is assumed to count every type
	A count(
			String... typeNames );

	static <P extends Persistable, R, T, A extends AggregationQueryBuilder<P, R, T, A>> AggregationQueryBuilder<P, R, T, A> newBuilder() {
		return new AggregationQueryBuilderImpl<>();
	}
}
