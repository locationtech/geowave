package org.locationtech.geowave.core.store.query.aggregate;

import org.locationtech.geowave.core.index.Mergeable;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.api.AggregationQuery;
import org.locationtech.geowave.core.store.api.AggregationQueryBuilder;
import org.locationtech.geowave.core.store.query.BaseQueryBuilderImpl;

public class AggregationQueryBuilderImpl<P extends Persistable, R extends Mergeable, T, A extends AggregationQueryBuilder<P, R, T, A>>
		extends
		BaseQueryBuilderImpl<R, AggregationQuery<P, R, T>, A> implements
		AggregationQueryBuilder<P, R, T, A>
{

	@Override
	public AggregationQuery<P, R, T> build() {
		return null;
	}

}
