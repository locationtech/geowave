package mil.nga.giat.geowave.datastore.dynamodb.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.base.BaseDataStore;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBOperations;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBRow;
import mil.nga.giat.geowave.datastore.dynamodb.util.DynamoDBInputFormatIteratorWrapper;

public class InputFormatDynamoDBRangeQuery extends
		DynamoDBConstraintsQuery
{
	private final static Logger LOGGER = Logger.getLogger(InputFormatDynamoDBRangeQuery.class);
	private final ByteArrayRange range;
	private final boolean isOutputWritable;

	private static List<ByteArrayId> getAdapterIds(
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

	public InputFormatDynamoDBRangeQuery(
			final BaseDataStore dataStore,
			final AdapterStore adapterStore,
			final DynamoDBOperations dynamoDBOperations,
			final PrimaryIndex index,
			final ByteArrayRange range,
			final List<QueryFilter> queryFilters,
			final boolean isOutputWritable,
			final QueryOptions queryOptions ) {
		super(
				dataStore,
				dynamoDBOperations,
				getAdapterIds(
						adapterStore,
						queryOptions),
				index,
				null,
				queryFilters != null ? queryFilters : new ArrayList<QueryFilter>(),
				new DedupeFilter(),
				(ScanCallback<?, DynamoDBRow>) queryOptions.getScanCallback(),
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
	protected List<ByteArrayRange> getRanges() {
		return Collections.singletonList(range);
	}

	@Override
	protected Iterator initIterator(
			final AdapterStore adapterStore,
			final Iterator<DynamoDBRow> results ) {
		final List<QueryFilter> filters = getAllFiltersList();
		return new DynamoDBInputFormatIteratorWrapper(
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
