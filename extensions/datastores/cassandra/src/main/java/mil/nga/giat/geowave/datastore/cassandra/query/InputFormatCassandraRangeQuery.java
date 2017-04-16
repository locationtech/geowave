package mil.nga.giat.geowave.datastore.cassandra.query;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.base.BaseDataStore;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.datastore.cassandra.CassandraRow;
import mil.nga.giat.geowave.datastore.cassandra.operations.CassandraOperations;
import mil.nga.giat.geowave.datastore.cassandra.util.CassandraInputFormatIteratorWrapper;

public class InputFormatCassandraRangeQuery extends
		CassandraConstraintsQuery
{
	private final static Logger LOGGER = Logger.getLogger(InputFormatCassandraRangeQuery.class);
	private final ByteArrayRange range;
	private final boolean isOutputWritable;

	private static List<ByteArrayId> getAdapterIds(
			final PrimaryIndex index,
			final AdapterStore adapterStore,
			final QueryOptions queryOptions ) {
		try {
			return queryOptions.getAdapterIds(adapterStore);
		}
		catch (final IOException e) {
			LOGGER.error(
					"Adapter IDs not set and unattainable from the AdapterStore",
					e);
		}
		return Collections.emptyList();
	}

	public InputFormatCassandraRangeQuery(
			final BaseDataStore dataStore,
			final AdapterStore adapterStore,
			final CassandraOperations cassandraOperations,
			final PrimaryIndex index,
			final ByteArrayRange range,
			final List<QueryFilter> queryFilters,
			final boolean isOutputWritable,
			final QueryOptions queryOptions ) {
		super(
				dataStore,
				cassandraOperations,
				getAdapterIds(
						index,
						adapterStore,
						queryOptions),
				index,
				null,
				queryFilters,
				null,
				(ScanCallback<?, CassandraRow>) queryOptions.getScanCallback(),
				null,
				null,
				null,
				null,
				null,
				queryOptions.getAuthorizations());

		this.range = range;
		this.isOutputWritable = isOutputWritable;
	}

	@Override
	protected Iterator initIterator(
			final AdapterStore adapterStore,
			final CloseableIterator<CassandraRow> results ) {
		final List<QueryFilter> filters = getAllFiltersList();
		return new CassandraInputFormatIteratorWrapper(
				dataStore,
				adapterStore,
				index,
				results,
				isOutputWritable,
				filters.isEmpty() ? null : filters.size() == 1 ? filters.get(0)
						: new mil.nga.giat.geowave.core.store.filter.FilterList<QueryFilter>(
								filters));
	}

}
