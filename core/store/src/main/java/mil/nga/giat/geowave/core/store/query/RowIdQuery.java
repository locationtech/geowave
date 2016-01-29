package mil.nga.giat.geowave.core.store.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.filter.RowIdQueryFilter;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.Index;

public class RowIdQuery implements
		Query
{
	private final List<ByteArrayId> rowIds;

	public RowIdQuery(
			final ByteArrayId rowId ) {
		rowIds = Collections.singletonList(rowId);
	}

	public RowIdQuery(
			final List<ByteArrayId> rowIds ) {
		this.rowIds = new ArrayList<ByteArrayId>(
				rowIds);
	}

	public List<ByteArrayId> getRowIds() {
		return rowIds;
	}

	@Override
	public List<QueryFilter> createFilters(
			final CommonIndexModel indexModel ) {
		final List<QueryFilter> filters = new ArrayList<QueryFilter>();
		filters.add(new RowIdQueryFilter(
				rowIds));
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
