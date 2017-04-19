package mil.nga.giat.geowave.datastore.hbase.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.base.BaseDataStore;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.Query;

public class HBaseRowIdsQuery<T> extends
		HBaseConstraintsQuery
{

	final Collection<ByteArrayId> rows;

	public HBaseRowIdsQuery(
			final BaseDataStore dataStore,
			final DataAdapter<T> adapter,
			final PrimaryIndex index,
			final Collection<ByteArrayId> rows,
			final ScanCallback<T, ?> scanCallback,
			final DedupeFilter dedupFilter,
			final String[] authorizations ) {
		super(
				dataStore,
				Collections.<ByteArrayId> emptyList(),
				index,
				(Query) null,
				dedupFilter,
				scanCallback,
				null,
				null,
				null,
				null,
				null,
				authorizations);
		this.rows = rows;
	}

	@Override
	protected List<ByteArrayRange> getRanges() {
		final List<ByteArrayRange> ranges = new ArrayList<ByteArrayRange>();
		for (final ByteArrayId row : rows) {
			ranges.add(new ByteArrayRange(
					row,
					row,
					true));
		}
		return ranges;
	}

}
