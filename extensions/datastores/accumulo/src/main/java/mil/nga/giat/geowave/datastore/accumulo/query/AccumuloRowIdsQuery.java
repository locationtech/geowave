package mil.nga.giat.geowave.datastore.accumulo.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.store.ScanCallback;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.Query;

/**
 * Represents a query operation for a specific set of Accumulo row IDs.
 * 
 */
public class AccumuloRowIdsQuery<T> extends
		AccumuloConstraintsQuery
{
	final Collection<ByteArrayId> rows;

	public AccumuloRowIdsQuery(
			final DataAdapter<T> adapter,
			final PrimaryIndex index,
			final Collection<ByteArrayId> rows,
			final ScanCallback<T> scanCallback,
			final DedupeFilter dedupFilter,
			final String[] authorizations ) {
		super(
				Collections.<ByteArrayId> emptyList(),
				index,
				(Query) null,
				dedupFilter,
				scanCallback,
				authorizations);
		this.rows = rows;
	}

	public AccumuloRowIdsQuery(
			final List<ByteArrayId> adapterIds,
			final PrimaryIndex index,
			final Collection<ByteArrayId> rows,
			final ScanCallback<T> scanCallback,
			final DedupeFilter dedupFilter,
			final String[] authorizations ) {
		super(
				adapterIds,
				index,
				(Query) null,
				dedupFilter,
				scanCallback,
				authorizations);
		this.rows = rows;
	}

	@Override
	protected List<ByteArrayRange> getRanges() {
		final List<ByteArrayRange> ranges = new ArrayList<ByteArrayRange>();
		for (ByteArrayId row : rows)
			ranges.add(new ByteArrayRange(
					row,
					row,
					true));
		return ranges;
	}
}