package org.locationtech.geowave.core.store.api;

import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.query.BaseQueryBuilder;

public interface AggregationQueryBuilder<P extends Persistable, R extends Mergeable, T, A extends AggregationQueryBuilder<P, R, T, A>>
		extends
		BaseQueryBuilder<R, AggregationQuery<P, R, T>, A>
{
	A aggregate(
			String typeName,
			Aggregation<P, R, T> aggregation );

	// this is a convenience method to set the count aggregation
	A count();
}
