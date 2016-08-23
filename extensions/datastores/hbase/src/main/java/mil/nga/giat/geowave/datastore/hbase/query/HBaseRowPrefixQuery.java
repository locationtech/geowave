package mil.nga.giat.geowave.datastore.hbase.query;

import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

/**
 * Represents a query operation using an HBase row prefix.
 *
 */
public class HBaseRowPrefixQuery<T> extends
		AbstractHBaseRowQuery<T>
{

	final Integer limit;
	final ByteArrayId rowPrefix;

	public HBaseRowPrefixQuery(
			final PrimaryIndex index,
			final ByteArrayId rowPrefix,
			final ScanCallback<T> scanCallback,
			final Integer limit,
			final String[] authorizations ) {
		super(
				index,
				authorizations,
				scanCallback);
		this.limit = limit;
		this.rowPrefix = rowPrefix;
	}

	@Override
	protected Integer getScannerLimit() {
		return limit;
	}

	@Override
	protected List<ByteArrayRange> getRanges() {
		final List<ByteArrayRange> ranges = new ArrayList<ByteArrayRange>();
		ranges.add(new ByteArrayRange(
				rowPrefix,
				rowPrefix,
				false));
		return ranges;
	}

}
