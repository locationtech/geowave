package mil.nga.giat.geowave.datastore.cassandra.query;

import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.util.NativeEntryIteratorWrapper;
import mil.nga.giat.geowave.datastore.cassandra.CassandraRow;
import mil.nga.giat.geowave.datastore.cassandra.operations.CassandraOperations;

/**
 * Represents a query operation for a Cassandra row. This abstraction is
 * re-usable for both exact row ID queries and row prefix queries.
 *
 */
abstract public class AbstractCassandraRowQuery<T> extends
		CassandraQuery
{
	private static final Logger LOGGER = Logger.getLogger(AbstractCassandraRowQuery.class);
	protected final ScanCallback<T, CassandraRow> scanCallback;

	public AbstractCassandraRowQuery(
			final DataStore dataStore,
			final CassandraOperations operations,
			final PrimaryIndex index,
			final String[] authorizations,
			final ScanCallback<T, CassandraRow> scanCallback,
			final DifferingFieldVisibilityEntryCount visibilityCounts ) {
		super(
				dataStore,
				operations,
				index,
				visibilityCounts,
				authorizations);
		this.scanCallback = scanCallback;
	}

	public CloseableIterator<T> query(
			final double[] maxResolutionSubsamplingPerDimension,
			final AdapterStore adapterStore ) {
		final CloseableIterator<CassandraRow> results = getResults(
				maxResolutionSubsamplingPerDimension,
				getScannerLimit());
		return new CloseableIterator.Wrapper<T>(
				new NativeEntryIteratorWrapper<>(
						dataStore,
						adapterStore,
						index,
						results,
						null,
						this.scanCallback,
						true));
	}

	abstract protected Integer getScannerLimit();
}
