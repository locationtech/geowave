package mil.nga.giat.geowave.adapter.vector.query.row;

import java.io.IOException;
import java.util.Collection;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

/**
 * This class wraps a source iterator and a row provider to provide the logic
 * for seeks based on the given row provider to the given source iterator. It is
 * dependent on seek calls delegated to it from a parent Iterator.
 * 
 */
public class RowProviderSkippingIterator
{
	private SortedKeyValueIterator<Key, Value> source = null;
	protected Collection<ByteSequence> columnFamilies;

	protected boolean inclusive = false;
	protected Range range;
	protected Key nextRow;
	private AbstractRowProvider rowProvider;

	public RowProviderSkippingIterator() {
		super();
	}

	public RowProviderSkippingIterator(
			final SortedKeyValueIterator<Key, Value> source,
			final AbstractRowProvider rowProvider ) {
		this.source = source;
		this.rowProvider = rowProvider;
	}

	public void setSeekRange(
			final Range range,
			final Collection<ByteSequence> columnFamilies,
			final boolean inclusive ) {
		this.range = range;
		this.columnFamilies = columnFamilies;
		this.inclusive = inclusive;
	}

	public boolean reseekAsNeeded()
			throws IOException {
		consume();

		if ((nextRow == null) && (range.getEndKey() == null)) {
			return false;
		}
		return true;
	}

	protected void consume()
			throws IOException {
		while (skipRows()) {
			// seek to the next row
			reseek(nextRow);
		}
	}

	private boolean skipRows() {
		if (source.hasTop()) {
			rowProvider.setCurrentRow(source.getTopKey());
			if (!rowProvider.skipRows()) {
				return false;
			}
			final Key nextRow = rowProvider.getNextRow();

			this.nextRow = nextRow;
			if ((nextRow == null) && (range.getEndKey() == null)) {
				return false;
			}
			return true;
		}
		return false;
	}

	private void reseek(
			final Key key )
			throws IOException {
		if ((key == null) || range.afterEndKey(key)) {
			range = Range.exact(range.getEndKey().getRow());
			source.seek(
					range,
					columnFamilies,
					inclusive);

		}
		else {
			range = new Range(
					key,
					true,
					range.getEndKey(),
					range.isEndKeyInclusive());
			source.seek(
					range,
					columnFamilies,
					inclusive);
		}
	}
}
