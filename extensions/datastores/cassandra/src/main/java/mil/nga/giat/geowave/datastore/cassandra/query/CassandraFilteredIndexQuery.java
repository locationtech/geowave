package mil.nga.giat.geowave.datastore.cassandra.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.filter.FilterList;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.FilteredIndexQuery;
import mil.nga.giat.geowave.core.store.util.NativeEntryIteratorWrapper;
import mil.nga.giat.geowave.datastore.cassandra.CassandraRow;
import mil.nga.giat.geowave.datastore.cassandra.operations.CassandraOperations;

public abstract class CassandraFilteredIndexQuery extends
		CassandraQuery implements
		FilteredIndexQuery
{
	protected List<QueryFilter> clientFilters;
	private final static Logger LOGGER = Logger.getLogger(
			CassandraFilteredIndexQuery.class);
	protected final ScanCallback<?> scanCallback;

	public CassandraFilteredIndexQuery(
			final CassandraOperations cassandraOperations,
			final List<ByteArrayId> adapterIds,
			final PrimaryIndex index,
			final List<QueryFilter> queryFilters,
			final DedupeFilter clientDedupeFilter,
			final ScanCallback<?> scanCallback,
			final Pair<List<String>, DataAdapter<?>> fieldIdsAdapterPair,
			final DifferingFieldVisibilityEntryCount visibilityCounts,
			final String... authorizations ) {
		super(
				cassandraOperations,
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
		clientFilters.addAll(
				queryFilters);
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
		final boolean exists = cassandraOperations.tableExists(
				StringUtils.stringFromBinary(
						index.getId().getBytes()));
		if (!exists) {
			LOGGER.warn(
					"Table does not exist " + StringUtils.stringFromBinary(
							index.getId().getBytes()));
			return new CloseableIterator.Empty();
		}

		final Iterator<CassandraRow> results = getResults(
				maxResolutionSubsamplingPerDimension,
				limit);

		if (results == null) {
			LOGGER.error(
					"Could not get scanner instance, getScanner returned null");
			return new CloseableIterator.Empty();
		}
		Iterator it = initIterator(
				adapterStore,
				results);
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
			final Iterator<CassandraRow> results ) {
		return new NativeEntryIteratorWrapper<>(
				adapterStore,
				index,
				results,
				clientFilters.isEmpty() ? null : clientFilters.size() == 1 ? clientFilters.get(
						0)
						: new FilterList<QueryFilter>(
								clientFilters),
				scanCallback);
	}

}
