package mil.nga.giat.geowave.core.store.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.filter.PrefixIdQueryFilter;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.Index;

public class PrefixIdQuery implements
		Query
{
	private final ByteArrayId sortKeyPrefix;
	private final ByteArrayId partitionKey;

	public PrefixIdQuery(
			final ByteArrayId partitionKey,
			final ByteArrayId sortKeyPrefix ) {
		this.partitionKey = partitionKey;
		this.sortKeyPrefix = sortKeyPrefix;
	}

	public ByteArrayId getPartitionKey() {
		return partitionKey;
	}

	public ByteArrayId getSortKeyPrefix() {
		return sortKeyPrefix;
	}

	@Override
	public List<QueryFilter> createFilters(
			final CommonIndexModel indexModel ) {
		final List<QueryFilter> filters = new ArrayList<QueryFilter>();
		filters.add(new PrefixIdQueryFilter(
				partitionKey,
				sortKeyPrefix));
		return filters;
	}

	@Override
	public boolean isSupported(
			final Index index ) {
		return true;
	}

	@Override
	public List<MultiDimensionalNumericData> getIndexConstraints(
			final NumericIndexStrategy indexStrategy ) {
		return Collections.emptyList();
	}

}
