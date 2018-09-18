package org.locationtech.geowave.core.store.api;

import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.query.BaseQuery;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.options.AggregateTypeQueryOptions;
import org.locationtech.geowave.core.store.query.options.CommonQueryOptions;
import org.locationtech.geowave.core.store.query.options.IndexQueryOptions;

public class AggregationQuery<P extends Persistable, R, T> extends
		BaseQuery<R, AggregateTypeQueryOptions<P, R, T>>
{
	public AggregationQuery() {
		super();
	}

	public AggregationQuery(
			final CommonQueryOptions commonQueryOptions,
			final AggregateTypeQueryOptions<P, R, T> dataTypeQueryOptions,
			final IndexQueryOptions indexQueryOptions,
			final QueryConstraints queryConstraints ) {
		super(
				commonQueryOptions,
				dataTypeQueryOptions,
				indexQueryOptions,
				queryConstraints);
	}
}
