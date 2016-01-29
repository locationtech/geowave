package mil.nga.giat.geowave.core.store.query.aggregate;

import mil.nga.giat.geowave.core.index.Mergeable;

public interface Aggregation<T> extends
		Mergeable
{
	public void aggregate(
			T entry );
}
