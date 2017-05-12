package mil.nga.giat.geowave.core.store.base;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.AdapterToIndexMapping;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.DataStoreOperations;
import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.IndexDependentDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.callback.IngestCallback;
import mil.nga.giat.geowave.core.store.callback.IngestCallbackList;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;
import mil.nga.giat.geowave.core.store.index.writer.IndependentAdapterIndexWriter;
import mil.nga.giat.geowave.core.store.index.writer.IndexCompositeWriter;
import mil.nga.giat.geowave.core.store.memory.MemoryAdapterStore;
import mil.nga.giat.geowave.core.store.query.DataIdQuery;
import mil.nga.giat.geowave.core.store.query.EverythingQuery;
import mil.nga.giat.geowave.core.store.query.PrefixIdQuery;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.core.store.query.RowIdQuery;

public abstract class BaseDataStore
{
	private final static Logger LOGGER = LoggerFactory.getLogger(BaseDataStore.class);

	protected static final String ALT_INDEX_TABLE = "_GEOWAVE_ALT_INDEX";

	protected final IndexStore indexStore;
	protected final AdapterStore adapterStore;
	protected final DataStatisticsStore statisticsStore;
	protected final SecondaryIndexDataStore secondaryIndexDataStore;
	protected final AdapterIndexMappingStore indexMappingStore;
	private final DataStoreOperations baseOperations;
	private final DataStoreOptions baseOptions;

	public BaseDataStore(
			final IndexStore indexStore,
			final AdapterStore adapterStore,
			final DataStatisticsStore statisticsStore,
			final AdapterIndexMappingStore indexMappingStore,
			final SecondaryIndexDataStore secondaryIndexDataStore,
			final DataStoreOperations operations,
			final DataStoreOptions options ) {
		this.indexStore = indexStore;
		this.adapterStore = adapterStore;
		this.statisticsStore = statisticsStore;
		this.indexMappingStore = indexMappingStore;
		this.secondaryIndexDataStore = secondaryIndexDataStore;

		baseOperations = operations;
		baseOptions = options;
	}

	public void store(
			final PrimaryIndex index ) {
		if (baseOptions.isPersistIndex() && !indexStore.indexExists(index.getId())) {
			indexStore.addIndex(index);
		}
	}

	protected synchronized void store(
			final DataAdapter<?> adapter ) {
		if (baseOptions.isPersistAdapter() && !adapterStore.adapterExists(adapter.getAdapterId())) {
			adapterStore.addAdapter(adapter);
		}
	}

	public <T> IndexWriter<T> createWriter(
			final DataAdapter<T> adapter,
			final PrimaryIndex... indices )
			throws MismatchedIndexToAdapterMapping {
		store(adapter);

		indexMappingStore.addAdapterIndexMapping(new AdapterToIndexMapping(
				adapter.getAdapterId(),
				indices));

		final IndexWriter<T>[] writers = new IndexWriter[indices.length];

		int i = 0;
		for (final PrimaryIndex index : indices) {
			final DataStoreCallbackManager callbackManager = new DataStoreCallbackManager(
					statisticsStore,
					secondaryIndexDataStore,
					i == 0);

			callbackManager.setPersistStats(baseOptions.isPersistDataStatistics());

			final List<IngestCallback<T>> callbacks = new ArrayList<IngestCallback<T>>();

			store(index);

			final String indexName = index.getId().getString();

			if (adapter instanceof WritableDataAdapter) {
				if (baseOptions.isUseAltIndex()) {
					addAltIndexCallback(
							callbacks,
							indexName,
							adapter,
							index.getId());
				}
			}
			callbacks.add(callbackManager.getIngestCallback(
					(WritableDataAdapter<T>) adapter,
					index));

			initOnIndexWriterCreate(
					adapter,
					index);

			final IngestCallbackList<T> callbacksList = new IngestCallbackList<T>(
					callbacks);
			writers[i] = createIndexWriter(
					adapter,
					index,
					baseOperations,
					baseOptions,
					callbacksList,
					callbacksList);

			if (adapter instanceof IndexDependentDataAdapter) {
				writers[i] = new IndependentAdapterIndexWriter<T>(
						(IndexDependentDataAdapter<T>) adapter,
						index,
						writers[i]);
			}
			i++;
		}
		return new IndexCompositeWriter(
				writers);

	}

	public <T> CloseableIterator<T> query(
			final QueryOptions queryOptions,
			final Query query ) {
		return internalQuery(
				queryOptions,
				query,
				false);
	}

	/*
	 * Since this general-purpose method crosses multiple adapters, the type of
	 * result cannot be assumed.
	 * 
	 * (non-Javadoc)
	 * 
	 * @see
	 * mil.nga.giat.geowave.core.store.DataStore#query(mil.nga.giat.geowave.
	 * core.store.query.QueryOptions,
	 * mil.nga.giat.geowave.core.store.query.Query)
	 */
	protected <T> CloseableIterator<T> internalQuery(
			final QueryOptions queryOptions,
			final Query query,
			boolean delete ) {
		final List<CloseableIterator<Object>> results = new ArrayList<CloseableIterator<Object>>();
		// all queries will use the same instance of the dedupe filter for
		// client side filtering because the filter needs to be applied across
		// indices
		final QueryOptions sanitizedQueryOptions = (queryOptions == null) ? new QueryOptions() : queryOptions;
		final Query sanitizedQuery = (query == null) ? new EverythingQuery() : query;

		final DedupeFilter filter = new DedupeFilter();
		MemoryAdapterStore tempAdapterStore;
		List<DataStoreCallbackManager> deleteCallbacks = new ArrayList<>();

		try {
			tempAdapterStore = new MemoryAdapterStore(
					sanitizedQueryOptions.getAdaptersArray(adapterStore));
			// keep a list of adapters that have been queried, to only low an
			// adapter to be queried
			// once
			final Set<ByteArrayId> queriedAdapters = new HashSet<ByteArrayId>();
			for (final Pair<PrimaryIndex, List<DataAdapter<Object>>> indexAdapterPair : sanitizedQueryOptions
					.getAdaptersWithMinimalSetOfIndices(
							tempAdapterStore,
							indexMappingStore,
							indexStore)) {
				final List<ByteArrayId> adapterIdsToQuery = new ArrayList<>();
				for (final DataAdapter<Object> adapter : indexAdapterPair.getRight()) {
					if (delete) {
						final DataStoreCallbackManager callbackCache = new DataStoreCallbackManager(
								statisticsStore,
								secondaryIndexDataStore,
								queriedAdapters.add(adapter.getAdapterId()));
						deleteCallbacks.add(callbackCache);
						ScanCallback callback = queryOptions.getScanCallback();

						final PrimaryIndex index = indexAdapterPair.getLeft();
						queryOptions.setScanCallback(new ScanCallback<Object>() {

							@Override
							public void entryScanned(
									DataStoreEntryInfo entryInfo,
									Object entry ) {
								if (callback != null) {
									callback.entryScanned(
											entryInfo,
											entry);
								}
								callbackCache.getDeleteCallback(
										(WritableDataAdapter<Object>) adapter,
										index).entryDeleted(
										entryInfo,
										entry);
							}
						});
					}
					if (sanitizedQuery instanceof RowIdQuery) {
						sanitizedQueryOptions.setLimit(-1);
						results.add(queryRowIds(
								adapter,
								indexAdapterPair.getLeft(),
								((RowIdQuery) sanitizedQuery).getRowIds(),
								filter,
								sanitizedQueryOptions,
								tempAdapterStore,
								delete));
						continue;
					}
					else if (sanitizedQuery instanceof DataIdQuery) {
						final DataIdQuery idQuery = (DataIdQuery) sanitizedQuery;
						if (idQuery.getAdapterId().equals(
								adapter.getAdapterId())) {
							results.add(getEntries(
									indexAdapterPair.getLeft(),
									idQuery.getDataIds(),
									(DataAdapter<Object>) adapterStore.getAdapter(idQuery.getAdapterId()),
									filter,
									(ScanCallback<Object>) sanitizedQueryOptions.getScanCallback(),
									sanitizedQueryOptions.getAuthorizations(),
									sanitizedQueryOptions.getMaxResolutionSubsamplingPerDimension(),
									delete));
						}
						continue;
					}
					else if (sanitizedQuery instanceof PrefixIdQuery) {
						final PrefixIdQuery prefixIdQuery = (PrefixIdQuery) sanitizedQuery;
						results.add(queryRowPrefix(
								indexAdapterPair.getLeft(),
								prefixIdQuery.getRowPrefix(),
								sanitizedQueryOptions,
								tempAdapterStore,
								adapterIdsToQuery,
								delete));
						continue;
					}
					adapterIdsToQuery.add(adapter.getAdapterId());
				}
				// supports querying multiple adapters in a single index
				// in one query instance (one scanner) for efficiency
				if (adapterIdsToQuery.size() > 0) {
					results.add(queryConstraints(
							adapterIdsToQuery,
							indexAdapterPair.getLeft(),
							sanitizedQuery,
							filter,
							sanitizedQueryOptions,
							tempAdapterStore,
							delete));
				}
			}

		}
		catch (final IOException e1) {
			LOGGER.error(
					"Failed to resolve adapter or index for query",
					e1);
		}
		return new CloseableIteratorWrapper<T>(
				new Closeable() {
					@Override
					public void close()
							throws IOException {
						for (final CloseableIterator<Object> result : results) {
							result.close();
						}
						for (DataStoreCallbackManager c : deleteCallbacks) {
							c.close();
						}
					}
				},
				Iterators.concat(new CastIterator<T>(
						results.iterator())));
	}

	@SuppressWarnings("unchecked")
	protected CloseableIterator<Object> getEntries(
			final PrimaryIndex index,
			final List<ByteArrayId> dataIds,
			final DataAdapter<Object> adapter,
			final DedupeFilter dedupeFilter,
			final ScanCallback<Object> callback,
			final String[] authorizations,
			final double[] maxResolutionSubsamplingPerDimension,
			boolean delete )
			throws IOException {
		final String altIdxTableName = index.getId().getString() + ALT_INDEX_TABLE;

		MemoryAdapterStore tempAdapterStore;

		tempAdapterStore = new MemoryAdapterStore(
				new DataAdapter[] {
					adapter
				});

		if (baseOptions.isUseAltIndex() && baseOperations.tableExists(altIdxTableName)) {
			final List<ByteArrayId> rowIds = getAltIndexRowIds(
					altIdxTableName,
					dataIds,
					adapter.getAdapterId());

			if (rowIds.size() > 0) {

				final QueryOptions options = new QueryOptions();
				options.setScanCallback(callback);
				options.setAuthorizations(authorizations);
				options.setMaxResolutionSubsamplingPerDimension(maxResolutionSubsamplingPerDimension);
				options.setLimit(-1);

				return queryRowIds(
						adapter,
						index,
						rowIds,
						dedupeFilter,
						options,
						tempAdapterStore,
						delete);
			}
		}
		else {
			return getEntryRows(
					index,
					tempAdapterStore,
					dataIds,
					adapter,
					callback,
					dedupeFilter,
					authorizations,
					delete);
		}
		return new CloseableIterator.Empty();
	}

	public boolean delete(
			final QueryOptions queryOptions,
			final Query query ) {
		if (((query == null) || (query instanceof EverythingQuery)) && queryOptions.isAllAdapters()) {
			return deleteEverything();
		}

		final AtomicBoolean aOk = new AtomicBoolean(
				true);

		// keep a list of adapters that have been queried, to only low an
		// adapter to be queried
		// once
		final Set<ByteArrayId> queriedAdapters = new HashSet<ByteArrayId>();

		try {
			for (final Pair<PrimaryIndex, List<DataAdapter<Object>>> indexAdapterPair : queryOptions
					.getIndicesForAdapters(
							adapterStore,
							indexMappingStore,
							indexStore)) {
				final PrimaryIndex index = indexAdapterPair.getLeft();
				if (index == null) {
					continue;
				}
				final String indexTableName = index.getId().getString();
				final String altIdxTableName = indexTableName + ALT_INDEX_TABLE;

				final Closeable idxDeleter = createIndexDeleter(
						indexTableName,
						queryOptions.getAuthorizations());

				final Closeable altIdxDelete = baseOptions.isUseAltIndex()
						&& baseOperations.tableExists(altIdxTableName) ? createIndexDeleter(
						altIdxTableName,
						queryOptions.getAuthorizations()) : null;

				for (final DataAdapter<Object> adapter : indexAdapterPair.getRight()) {

					final DataStoreCallbackManager callbackCache = new DataStoreCallbackManager(
							statisticsStore,
							secondaryIndexDataStore,
							queriedAdapters.add(adapter.getAdapterId()));

					callbackCache.setPersistStats(baseOptions.isPersistDataStatistics());

					if (query instanceof EverythingQuery) {
						deleteEntries(
								adapter,
								index,
								queryOptions.getAuthorizations());
						continue;
					}

					final ScanCallback<Object> callback = new ScanCallback<Object>() {
						@Override
						public void entryScanned(
								final DataStoreEntryInfo entryInfo,
								final Object entry ) {
							callbackCache.getDeleteCallback(
									(WritableDataAdapter<Object>) adapter,
									index).entryDeleted(
									entryInfo,
									entry);
							try {
								addToBatch(
										idxDeleter,
										entryInfo.getRowIds());
								if (altIdxDelete != null) {
									addToBatch(
											altIdxDelete,
											Collections.singletonList(adapter.getDataId(entry)));
								}
							}
							catch (final Exception e) {
								LOGGER.error(
										"Failed deletion",
										e);
								aOk.set(false);
							}
						}
					};

					CloseableIterator<?> dataIt = null;
					queryOptions.setScanCallback(callback);
					final List<ByteArrayId> adapterIds = Collections.singletonList(adapter.getAdapterId());
					if (query instanceof RowIdQuery) {
						queryOptions.setLimit(-1);
						dataIt = queryRowIds(
								adapter,
								index,
								((RowIdQuery) query).getRowIds(),
								null,
								queryOptions,
								adapterStore,
								true);
					}
					else if (query instanceof DataIdQuery) {
						final DataIdQuery idQuery = (DataIdQuery) query;
						dataIt = getEntries(
								index,
								idQuery.getDataIds(),
								adapter,
								null,
								callback,
								queryOptions.getAuthorizations(),
								null,
								true);
					}
					else if (query instanceof PrefixIdQuery) {
						dataIt = queryRowPrefix(
								index,
								((PrefixIdQuery) query).getRowPrefix(),
								queryOptions,
								adapterStore,
								adapterIds,
								true);
					}
					else {
						dataIt = queryConstraints(
								adapterIds,
								index,
								query,
								null,
								queryOptions,
								adapterStore,
								true);
					}

					while (dataIt.hasNext()) {
						dataIt.next();
					}
					try {
						dataIt.close();
					}
					catch (final Exception ex) {
						LOGGER.warn(
								"Cannot close iterator",
								ex);
					}
					callbackCache.close();
				}
				if (altIdxDelete != null) {
					altIdxDelete.close();
				}
				idxDeleter.close();
			}

			return aOk.get();
		}
		catch (final Exception e) {
			LOGGER.error(
					"Failed delete operation " + query.toString(),
					e);
			return false;
		}
	}

	protected boolean deleteEverything() {
		try {
			indexStore.removeAll();
			adapterStore.removeAll();
			statisticsStore.removeAll();
			secondaryIndexDataStore.removeAll();
			indexMappingStore.removeAll();

			baseOperations.deleteAll();
			return true;
		}
		catch (final Exception e) {
			LOGGER.error(
					"Unable to delete all tables",
					e);

		}
		return false;
	}

	private <T> void deleteEntries(
			final DataAdapter<T> adapter,
			final PrimaryIndex index,
			final String... additionalAuthorizations )
			throws IOException {
		final String tableName = index.getId().getString();
		final String altIdxTableName = tableName + ALT_INDEX_TABLE;
		final String adapterId = StringUtils.stringFromBinary(adapter.getAdapterId().getBytes());

		try (final CloseableIterator<DataStatistics<?>> it = statisticsStore.getDataStatistics(adapter.getAdapterId())) {

			while (it.hasNext()) {
				final DataStatistics stats = it.next();
				statisticsStore.removeStatistics(
						adapter.getAdapterId(),
						stats.getStatisticsId(),
						additionalAuthorizations);
			}
		}

		// cannot delete because authorizations are not used
		// this.indexMappingStore.remove(adapter.getAdapterId());

		deleteAll(
				tableName,
				adapterId,
				additionalAuthorizations);
		if (baseOptions.isUseAltIndex() && baseOperations.tableExists(altIdxTableName)) {
			deleteAll(
					altIdxTableName,
					adapterId,
					additionalAuthorizations);
		}
	}

	protected abstract boolean deleteAll(
			final String tableName,
			final String columnFamily,
			final String... additionalAuthorizations );

	protected abstract void addToBatch(
			Closeable idxDeleter,
			List<ByteArrayId> rowIds )
			throws Exception;

	protected abstract Closeable createIndexDeleter(
			String indexTableName,
			String[] authorizations )
			throws Exception;

	protected abstract List<ByteArrayId> getAltIndexRowIds(
			final String altIdxTableName,
			final List<ByteArrayId> dataIds,
			final ByteArrayId adapterId,
			final String... authorizations );

	protected abstract CloseableIterator<Object> getEntryRows(
			final PrimaryIndex index,
			final AdapterStore tempAdapterStore,
			final List<ByteArrayId> dataIds,
			final DataAdapter<?> adapter,
			final ScanCallback<Object> callback,
			final DedupeFilter dedupeFilter,
			final String[] authorizations,
			boolean delete );

	protected abstract CloseableIterator<Object> queryConstraints(
			List<ByteArrayId> adapterIdsToQuery,
			PrimaryIndex index,
			Query sanitizedQuery,
			DedupeFilter filter,
			QueryOptions sanitizedQueryOptions,
			AdapterStore tempAdapterStore,
			boolean delete );

	protected abstract CloseableIterator<Object> queryRowPrefix(
			PrimaryIndex index,
			ByteArrayId rowPrefix,
			QueryOptions sanitizedQueryOptions,
			AdapterStore tempAdapterStore,
			List<ByteArrayId> adapterIdsToQuery,
			boolean delete );

	protected abstract CloseableIterator<Object> queryRowIds(
			DataAdapter<Object> adapter,
			PrimaryIndex index,
			List<ByteArrayId> rowIds,
			DedupeFilter filter,
			QueryOptions sanitizedQueryOptions,
			AdapterStore tempAdapterStore,
			boolean delete );

	protected abstract <T> void addAltIndexCallback(
			List<IngestCallback<T>> callbacks,
			String indexName,
			DataAdapter<T> adapter,
			ByteArrayId primaryIndexId );

	protected abstract IndexWriter createIndexWriter(
			DataAdapter adapter,
			PrimaryIndex index,
			DataStoreOperations baseOperations,
			DataStoreOptions baseOptions,
			final IngestCallback callback,
			final Closeable closable );

	protected abstract void initOnIndexWriterCreate(
			final DataAdapter adapter,
			final PrimaryIndex index );

}
