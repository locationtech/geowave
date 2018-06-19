package mil.nga.giat.geowave.analytic.spark;

import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;

public class RDDOptions
{
	private DistributableQuery query = null;
	private QueryOptions queryOptions = null;
	private int minSplits = -1;
	private int maxSplits = -1;

	public RDDOptions() {}

	public DistributableQuery getQuery() {
		return query;
	}

	public void setQuery(
			DistributableQuery query ) {
		this.query = query;
	}

	public QueryOptions getQueryOptions() {
		return queryOptions;
	}

	public void setQueryOptions(
			QueryOptions queryOptions ) {
		this.queryOptions = queryOptions;
	}

	public int getMinSplits() {
		return minSplits;
	}

	public void setMinSplits(
			int minSplits ) {
		this.minSplits = minSplits;
	}

	public int getMaxSplits() {
		return maxSplits;
	}

	public void setMaxSplits(
			int maxSplits ) {
		this.maxSplits = maxSplits;
	}

}
