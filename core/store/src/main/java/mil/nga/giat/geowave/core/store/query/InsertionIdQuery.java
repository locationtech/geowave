package mil.nga.giat.geowave.core.store.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.filter.InsertionIdQueryFilter;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.Index;

public class InsertionIdQuery implements
		Query
{
	private final ByteArrayId partitionKey;
	private final ByteArrayId sortKey;
	private final ByteArrayId dataId;

	public InsertionIdQuery(
			final ByteArrayId partitionKey,
			final ByteArrayId sortKey,
			final ByteArrayId dataId ) {
		this.partitionKey = partitionKey;
		this.sortKey = sortKey;
		this.dataId = dataId;
	}

	public ByteArrayId getPartitionKey() {
		return partitionKey;
	}

	public ByteArrayId getSortKey() {
		return sortKey;
	}

	public ByteArrayId getDataId() {
		return dataId;
	}

	@Override
	public List<QueryFilter> createFilters(
			final CommonIndexModel indexModel ) {
		final List<QueryFilter> filters = new ArrayList<QueryFilter>();
		filters.add(new InsertionIdQueryFilter(
				partitionKey,
				sortKey,
				dataId));
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
