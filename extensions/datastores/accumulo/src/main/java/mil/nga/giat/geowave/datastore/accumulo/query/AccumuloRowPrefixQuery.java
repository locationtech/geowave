package mil.nga.giat.geowave.datastore.accumulo.query;

import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.store.base.BaseDataStore;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

/**
 * Represents a query operation using an Accumulo row prefix.
 *
 */
public class AccumuloRowPrefixQuery<T> extends
		AbstractAccumuloRowQuery<T>
{

	final Integer limit;
	final ByteArrayId rowPrefix;

	public AccumuloRowPrefixQuery(
			final BaseDataStore dataStore,
			final PrimaryIndex index,
			final ByteArrayId rowPrefix,
			final ScanCallback<T, Object> scanCallback,
			final Integer limit,
			final DifferingFieldVisibilityEntryCount visibilityCounts,
			final String[] authorizations ) {
		super(
				dataStore,
				index,
				authorizations,
				scanCallback,
				visibilityCounts);
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
