package mil.nga.giat.geowave.datastore.dynamodb.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.RowMergingDataAdapter;
import mil.nga.giat.geowave.core.store.base.BaseDataStore;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.filter.FilterList;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.FilteredIndexQuery;
import mil.nga.giat.geowave.core.store.util.MergingEntryIterator;
import mil.nga.giat.geowave.core.store.util.NativeEntryIteratorWrapper;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBOperations;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBRow;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBRow.GuavaRowTranslationHelper;

public abstract class DynamoDBFilteredIndexQuery extends
		DynamoDBQuery implements
		FilteredIndexQuery
{
	protected List<QueryFilter> clientFilters;
	private final static Logger LOGGER = Logger.getLogger(DynamoDBFilteredIndexQuery.class);
	protected final ScanCallback<?, DynamoDBRow> scanCallback;

	public DynamoDBFilteredIndexQuery(
			final BaseDataStore dataStore,
			final DynamoDBOperations dynamodbOperations,
			final List<ByteArrayId> adapterIds,
			final PrimaryIndex index,
			final List<QueryFilter> queryFilters,
			final DedupeFilter clientDedupeFilter,
			final ScanCallback<?, DynamoDBRow> scanCallback,
			final Pair<List<String>, DataAdapter<?>> fieldIdsAdapterPair,
			final DifferingFieldVisibilityEntryCount visibilityCounts,
			final String... authorizations ) {
		super(
				dataStore,
				dynamodbOperations,
				adapterIds,
				index,
				fieldIdsAdapterPair,
				visibilityCounts,
				authorizations);
		clientFilters = new ArrayList<>();
		if (clientDedupeFilter != null) {
			clientFilters.add(
					0,
					clientDedupeFilter);
		}
		clientFilters.addAll(queryFilters);
		this.scanCallback = scanCallback;
	}

	protected List<QueryFilter> getClientFilters() {
		return clientFilters;
	}

	@Override
	public void setClientFilters(
			final List<QueryFilter> clientFilters ) {}

	@SuppressWarnings("rawtypes")
	public CloseableIterator<Object> query(
			final AdapterStore adapterStore,
			final double[] maxResolutionSubsamplingPerDimension,
			final Integer limit ) {
		boolean exists = false;
		try {
			exists = dynamodbOperations.tableExists(StringUtils.stringFromBinary(index.getId().getBytes()));
		}
		catch (final IOException e) {
			LOGGER.error(
					"table doesn't exist",
					e);
		}
		if (!exists) {
			LOGGER.warn("Table does not exist " + StringUtils.stringFromBinary(index.getId().getBytes()));
			return new CloseableIterator.Empty();
		}

		final Iterator<Map<String, AttributeValue>> results = getResults(
				maxResolutionSubsamplingPerDimension,
				limit,
				adapterStore);

		if (results == null) {
			LOGGER.error("Could not get scanner instance, getScanner returned null");
			return new CloseableIterator.Empty();
		}
		Iterator it = initIterator(
				adapterStore,
				Iterators.transform(
						results,
						new GuavaRowTranslationHelper()));
		if ((limit != null) && (limit > 0)) {
			it = Iterators.limit(
					it,
					limit);
		}
		return new CloseableIterator.Wrapper(
				it);
	}

	protected Iterator initIterator(
			final AdapterStore adapterStore,
			final Iterator<DynamoDBRow> results ) {
		final List<QueryFilter> filters = getAllFiltersList();
		final QueryFilter queryFilter = filters.isEmpty() ? null : filters.size() == 1 ? filters.get(0)
				: new FilterList<QueryFilter>(
						filters);

		final Map<ByteArrayId, RowMergingDataAdapter> mergingAdapters = new HashMap<ByteArrayId, RowMergingDataAdapter>();
		for (final ByteArrayId adapterId : adapterIds) {
			final DataAdapter adapter = adapterStore.getAdapter(adapterId);
			if ((adapter instanceof RowMergingDataAdapter)
					&& (((RowMergingDataAdapter) adapter).getTransform() != null)) {
				mergingAdapters.put(
						adapterId,
						(RowMergingDataAdapter) adapter);
			}
		}
		if (mergingAdapters.isEmpty()) {
			return new NativeEntryIteratorWrapper<>(
					dataStore,
					adapterStore,
					index,
					results,
					queryFilter,
					scanCallback,
					true);
		}
		else {
			return new MergingEntryIterator(
					dataStore,
					adapterStore,
					index,
					results,
					queryFilter,
					scanCallback,
					mergingAdapters);
		}
	}

	protected List<QueryFilter> getAllFiltersList() {
		// This method is so that it can be overridden to also add distributed
		// filter list
		final List<QueryFilter> filters = new ArrayList<QueryFilter>();
		filters.addAll(clientFilters);
		return filters;
	}
}
