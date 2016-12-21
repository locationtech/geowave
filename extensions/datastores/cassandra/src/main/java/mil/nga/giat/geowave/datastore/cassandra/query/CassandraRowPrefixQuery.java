package mil.nga.giat.geowave.datastore.cassandra.query;

import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.datastore.cassandra.operations.CassandraOperations;

/**
 * Represents a query operation using an DynamoDB row prefix.
 *
 */
public class CassandraRowPrefixQuery<T> extends
		AbstractCassandraRowQuery<T>
{

	final Integer limit;
	final ByteArrayId rowPrefix;

	public CassandraRowPrefixQuery(
			final CassandraOperations cassandraOperations,
			final PrimaryIndex index,
			final ByteArrayId rowPrefix,
			final ScanCallback<T> scanCallback,
			final Integer limit,
			final DifferingFieldVisibilityEntryCount visibilityCounts,
			final String[] authorizations ) {
		super(
				cassandraOperations,
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
		ranges.add(
				new ByteArrayRange(
						rowPrefix,
						rowPrefix,
						false));
		return ranges;
	}

}
