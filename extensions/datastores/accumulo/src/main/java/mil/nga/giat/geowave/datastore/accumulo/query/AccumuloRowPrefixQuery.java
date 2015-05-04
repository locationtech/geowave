package mil.nga.giat.geowave.datastore.accumulo.query;

import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.store.ScanCallback;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.datastore.accumulo.util.CloseableIteratorWrapper;

/**
 * Represents a query operation using an Accumulo row prefix.
 * 
 */
public class AccumuloRowPrefixQuery extends
		AbstractAccumuloRowQuery<CloseableIteratorWrapper<?>>
{

	public AccumuloRowPrefixQuery(
			final Index index,
			final ByteArrayId rowPrefix,
			final ScanCallback<CloseableIteratorWrapper<?>> scanCallback,
			final String... authorizations ) {
		super(
				index,
				rowPrefix,
				authorizations,
				scanCallback);
	}

	public AccumuloRowPrefixQuery(
			final Index index,
			final ByteArrayId rowPrefix,
			final String... authorizations ) {
		super(
				index,
				rowPrefix,
				authorizations,
				null);
	}

	@Override
	protected CloseableIteratorWrapper<?> queryResultFromIterator(
			final CloseableIteratorWrapper<?> it ) {
		return it;
	}

	@Override
	protected Integer getScannerLimit() {
		return null;
	}

	@Override
	protected List<ByteArrayRange> getRanges() {
		final List<ByteArrayRange> ranges = new ArrayList<ByteArrayRange>();
		ranges.add(new ByteArrayRange(
				row,
				row,
				false));
		return ranges;
	}

}
