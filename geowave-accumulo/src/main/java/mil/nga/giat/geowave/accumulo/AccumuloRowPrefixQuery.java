package mil.nga.giat.geowave.accumulo;

import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.ByteArrayRange;
import mil.nga.giat.geowave.store.index.Index;

/**
 * Represents a query operation using an Accumulo row prefix.
 * 
 */
public class AccumuloRowPrefixQuery extends
		AbstractAccumuloRowQuery<CloseableIteratorWrapper<?>>
{
	private static class PrefixRange extends
			ByteArrayRange
	{

		public PrefixRange(
				final ByteArrayId prefix ) {
			super(
					prefix,
					prefix);
		}

		@Override
		public boolean isSingleValue() {
			return false;
		}

	}

	public AccumuloRowPrefixQuery(
			final Index index,
			final ByteArrayId rowPrefix ) {
		super(
				index,
				rowPrefix);
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
		ranges.add(new PrefixRange(
				row));
		return ranges;
	}

}
