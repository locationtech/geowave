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
import org.apache.log4j.Logger;

import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.InsertionIds;
import mil.nga.giat.geowave.core.store.AdapterToIndexMapping;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.DataStore;
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
import mil.nga.giat.geowave.core.store.adapter.statistics.DuplicateEntryCount;
import mil.nga.giat.geowave.core.store.callback.IngestCallback;
import mil.nga.giat.geowave.core.store.callback.IngestCallbackList;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.index.IndexMetaDataSet;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;
import mil.nga.giat.geowave.core.store.index.writer.IndependentAdapterIndexWriter;
import mil.nga.giat.geowave.core.store.index.writer.IndexCompositeWriter;
import mil.nga.giat.geowave.core.store.memory.MemoryAdapterStore;
import mil.nga.giat.geowave.core.store.operations.DataStoreOperations;
import mil.nga.giat.geowave.core.store.operations.Deleter;
import mil.nga.giat.geowave.core.store.query.DataIdQuery;
import mil.nga.giat.geowave.core.store.query.EverythingQuery;
import mil.nga.giat.geowave.core.store.query.InsertionIdQuery;
import mil.nga.giat.geowave.core.store.query.PrefixIdQuery;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.QueryOptions;

public class BaseDataStore implements
		DataStore
{
	private final static Logger LOGGER = Logger.getLogger(BaseDataStore.class);

	protected static final String ALT_INDEX_TABLE = "_GEOWAVE_ALT_INDEX";

	protected final IndexStore indexStore;
	protected final AdapterStore adapterStore;
	protected final DataStatisticsStore statisticsStore;
	protected final SecondaryIndexDataStore secondaryIndexDataStore;
	protected final AdapterIndexMappingStore indexMappingStore;
	protected final DataStoreOperations baseOperations;
	protected final DataStoreOptions baseOptions;

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

	@Override
	public <T> IndexWriter<T> createWriter(
			final WritableDataAdapter<T> adapter,
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

			if (baseOptions.isUseAltIndex()) {
				addAltIndexCallback(
						callbacks,
						indexName,
						adapter,
						index.getId());
			}
			callbacks.add(callbackManager.getIngestCallback(
					adapter,
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
	@Override
	public <T> CloseableIterator<T> query(
			final QueryOptions queryOptions,
			final Query query ) {
		final List<CloseableIterator<Object>> results = new ArrayList<CloseableIterator<Object>>();
		// all queries will use the same instance of the dedupe filter for
		// client side filtering because the filter needs to be applied across
		// indices
		final QueryOptions sanitizedQueryOptions = (queryOptions == null) ? new QueryOptions() : queryOptions;
		final Query sanitizedQuery = (query == null) ? new EverythingQuery() : query;

		final DedupeFilter filter = new DedupeFilter();
		MemoryAdapterStore tempAdapterStore;
		try {
			tempAdapterStore = new MemoryAdapterStore(
					sanitizedQueryOptions.getAdaptersArray(adapterStore));

			for (final Pair<PrimaryIndex, List<DataAdapter<Object>>> indexAdapterPair : sanitizedQueryOptions
					.getAdaptersWithMinimalSetOfIndices(
							tempAdapterStore,
							indexMappingStore,
							indexStore)) {
				final List<ByteArrayId> adapterIdsToQuery = new ArrayList<>();
				for (final DataAdapter<Object> adapter : indexAdapterPair.getRight()) {
					if (sanitizedQuery instanceof InsertionIdQuery) {
						sanitizedQueryOptions.setLimit(-1);
						results.add(queryInsertionId(
								adapter,
								indexAdapterPair.getLeft(),
								(InsertionIdQuery) sanitizedQuery,
								filter,
								sanitizedQueryOptions,
								tempAdapterStore));
						continue;
					}
					else if (sanitizedQuery instanceof PrefixIdQuery) {
						final PrefixIdQuery prefixIdQuery = (PrefixIdQuery) sanitizedQuery;
						results.add(queryRowPrefix(
								indexAdapterPair.getLeft(),
								prefixIdQuery.getPartitionKey(),
								prefixIdQuery.getSortKeyPrefix(),
								sanitizedQueryOptions,
								tempAdapterStore,
								adapterIdsToQuery));
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
							tempAdapterStore));
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
					}
				},
				Iterators.concat(new CastIterator<T>(
						results.iterator())));
	}

	@Override
	public boolean delete(
			final QueryOptions queryOptions,
			final Query query ) {
		if (((query == null) || (query instanceof EverythingQuery)) && queryOptions.isAllAdapters()) {
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

		final AtomicBoolean aOk = new AtomicBoolean(
				true);

		// keep a list of adapters that have been queried, to only low an
		// adapter to be queried
		// once
		final Set<ByteArrayId> queriedAdapters = new HashSet<ByteArrayId>();
		Deleter idxDeleter = null, altIdxDeleter = null;
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

				idxDeleter = baseOperations.createDeleter(
						index.getId(),
						queryOptions.getAuthorizations());

				altIdxDeleter = baseOptions.isUseAltIndex() && baseOperations.indexExists(new ByteArrayId(
						altIdxTableName)) ? baseOperations.createDeleter(
						new ByteArrayId(
								altIdxTableName),
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
					final Deleter internalIdxDeleter = idxDeleter;
					final Deleter internalAltIdxDeleter = altIdxDeleter;
					final ScanCallback<Object, GeoWaveRow> callback = new ScanCallback<Object, GeoWaveRow>() {
						@Override
						public void entryScanned(
								final Object entry,
								final GeoWaveRow row ) {
							callbackCache.getDeleteCallback(
									(WritableDataAdapter<Object>) adapter,
									index).entryDeleted(
									entry,
									row);
							try {
								internalIdxDeleter.delete(
										row,
										adapter);
								if (internalAltIdxDeleter != null) {
									internalAltIdxDeleter.delete(
											row,
											adapter);
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
					if (query instanceof InsertionIdQuery) {
						queryOptions.setLimit(-1);
						dataIt = queryInsertionId(
								adapter,
								index,
								(InsertionIdQuery) query,
								null,
								queryOptions,
								adapterStore);
					}
					else if (query instanceof PrefixIdQuery) {
						dataIt = queryRowPrefix(
								index,
								((PrefixIdQuery) query).getPartitionKey(),
								((PrefixIdQuery) query).getSortKeyPrefix(),
								queryOptions,
								adapterStore,
								adapterIds);
					}
					else {
						dataIt = queryConstraints(
								adapterIds,
								index,
								query,
								null,
								queryOptions,
								adapterStore);
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
			}

			return aOk.get();
		}
		catch (final Exception e) {
			LOGGER.error(
					"Failed delete operation " + query.toString(),
					e);
			e.printStackTrace();
			return false;
		}
		finally {
			try {
				if (idxDeleter != null) {
					idxDeleter.close();
				}
				if (altIdxDeleter != null) {
					altIdxDeleter.close();
				}
			}
			catch (final Exception e) {
				LOGGER.warn(
						"Unable to close deleter",
						e);
			}
		}

	}

	private <T> void deleteEntries(
			final DataAdapter<T> adapter,
			final PrimaryIndex index,
			final String... additionalAuthorizations )
			throws IOException {
		final String altIdxTableName = index.getId().getString() + ALT_INDEX_TABLE;

		try (final CloseableIterator<DataStatistics<?>> it = statisticsStore.getDataStatistics(adapter.getAdapterId())) {

			while (it.hasNext()) {
				final DataStatistics<?> stats = it.next();
				statisticsStore.removeStatistics(
						adapter.getAdapterId(),
						stats.getStatisticsId(),
						additionalAuthorizations);
			}
		}

		// cannot delete because authorizations are not used
		// this.indexMappingStore.remove(adapter.getAdapterId());

		baseOperations.deleteAll(
				index.getId(),
				adapter.getAdapterId(),
				additionalAuthorizations);
		if (baseOptions.isUseAltIndex() && baseOperations.indexExists(new ByteArrayId(
				altIdxTableName))) {
			baseOperations.deleteAll(
					new ByteArrayId(
							altIdxTableName),
					adapter.getAdapterId(),
					additionalAuthorizations);
		}
	}

	protected InsertionIds getAltIndexInsertionIds(
			final String altIdxTableName,
			final List<ByteArrayId> dataIds,
			final ByteArrayId adapterId,
			final String... authorizations ) {
		// TODO: GEOWAVE-1018 - this really should be a secondary index and not
		// special cased
		return new InsertionIds();

	}

	protected CloseableIterator<Object> getEntryRows(
			final PrimaryIndex index,
			final AdapterStore tempAdapterStore,
			final List<ByteArrayId> dataIds,
			final DataAdapter<?> adapter,
			final DedupeFilter dedupeFilter,
			final QueryOptions queryOptions ) {
		return queryConstraints(
				Collections.singletonList(adapter.getAdapterId()),
				index,
				new DataIdQuery(
						dataIds),
				dedupeFilter,
				queryOptions,
				tempAdapterStore);
	}

	protected CloseableIterator<Object> queryConstraints(
			final List<ByteArrayId> adapterIdsToQuery,
			final PrimaryIndex index,
			final Query sanitizedQuery,
			final DedupeFilter filter,
			final QueryOptions sanitizedQueryOptions,
			final AdapterStore tempAdapterStore ) {
		final BaseConstraintsQuery constraintsQuery = new BaseConstraintsQuery(
				this,
				adapterIdsToQuery,
				index,
				sanitizedQuery,
				filter,
				sanitizedQueryOptions.getScanCallback(),
				sanitizedQueryOptions.getAggregation(),
				sanitizedQueryOptions.getFieldIdsAdapterPair(),
				IndexMetaDataSet.getIndexMetadata(
						index,
						adapterIdsToQuery,
						statisticsStore,
						sanitizedQueryOptions.getAuthorizations()),
				DuplicateEntryCount.getDuplicateCounts(
						index,
						adapterIdsToQuery,
						statisticsStore,
						sanitizedQueryOptions.getAuthorizations()),
				DifferingFieldVisibilityEntryCount.getVisibilityCounts(
						index,
						adapterIdsToQuery,
						statisticsStore,
						sanitizedQueryOptions.getAuthorizations()),
				sanitizedQueryOptions.getAuthorizations());

		return constraintsQuery.query(
				baseOperations,
				baseOptions,
				tempAdapterStore,
				sanitizedQueryOptions.getMaxResolutionSubsamplingPerDimension(),
				sanitizedQueryOptions.getLimit());
	}

	protected CloseableIterator<Object> queryRowPrefix(
			final PrimaryIndex index,
			final ByteArrayId partitionKey,
			final ByteArrayId sortPrefix,
			final QueryOptions sanitizedQueryOptions,
			final AdapterStore tempAdapterStore,
			final List<ByteArrayId> adapterIdsToQuery ) {
		final BaseRowPrefixQuery<Object> prefixQuery = new BaseRowPrefixQuery<Object>(
				this,
				index,
				partitionKey,
				sortPrefix,
				(ScanCallback<Object, ?>) sanitizedQueryOptions.getScanCallback(),
				sanitizedQueryOptions.getLimit(),
				DifferingFieldVisibilityEntryCount.getVisibilityCounts(
						index,
						adapterIdsToQuery,
						statisticsStore,
						sanitizedQueryOptions.getAuthorizations()),
				sanitizedQueryOptions.getAuthorizations());

		return prefixQuery.query(
				baseOperations,
				baseOptions,
				sanitizedQueryOptions.getMaxResolutionSubsamplingPerDimension(),
				tempAdapterStore);

	}

	protected CloseableIterator<Object> queryInsertionId(
			final DataAdapter<Object> adapter,
			final PrimaryIndex index,
			final InsertionIdQuery query,
			final DedupeFilter filter,
			final QueryOptions sanitizedQueryOptions,
			final AdapterStore tempAdapterStore ) {
		final DifferingFieldVisibilityEntryCount visibilityCounts = DifferingFieldVisibilityEntryCount
				.getVisibilityCounts(
						index,
						Collections.singletonList(adapter.getAdapterId()),
						statisticsStore,
						sanitizedQueryOptions.getAuthorizations());

		final BaseInsertionIdQuery<Object> q = new BaseInsertionIdQuery<Object>(
				this,
				adapter,
				index,
				query,
				(ScanCallback<Object, ?>) sanitizedQueryOptions.getScanCallback(),
				filter,
				visibilityCounts,
				sanitizedQueryOptions.getAuthorizations());

		return q.query(
				baseOperations,
				baseOptions,
				tempAdapterStore,
				sanitizedQueryOptions.getMaxResolutionSubsamplingPerDimension(),
				sanitizedQueryOptions.getLimit());
	}

	protected <T> IndexWriter<T> createIndexWriter(
			final WritableDataAdapter<T> adapter,
			final PrimaryIndex index,
			final DataStoreOperations baseOperations,
			final DataStoreOptions baseOptions,
			final IngestCallback<T> callback,
			final Closeable closable ) {
		return new BaseIndexWriter<T>(
				adapter,
				index,
				baseOperations,
				baseOptions,
				callback,
				closable);
	}

	protected <T> void initOnIndexWriterCreate(
			final DataAdapter<T> adapter,
			final PrimaryIndex index ) {}

	protected <T> void addAltIndexCallback(
			final List<IngestCallback<T>> callbacks,
			final String indexName,
			final DataAdapter<T> adapter,
			final ByteArrayId primaryIndexId ) {
		try {
			callbacks.add(new AltIndexCallback<T>(
					indexName,
					(WritableDataAdapter<T>) adapter,
					primaryIndexId));

		}
		catch (final Exception e) {
			LOGGER.error(
					"Unable to create table table for alt index to  [" + indexName + "]",
					e);
		}
	}

	private class AltIndexCallback<T> implements
			IngestCallback<T>
	{
		private final ByteArrayId EMPTY_VISIBILITY = new ByteArrayId(
				new byte[0]);
		private final ByteArrayId EMPTY_FIELD_ID = new ByteArrayId(
				new byte[0]);
		private final WritableDataAdapter<T> adapter;
		private final String altIdxTableName;
		private final ByteArrayId primaryIndexId;
		private final ByteArrayId altIndexId;

		public AltIndexCallback(
				final String indexName,
				final WritableDataAdapter<T> adapter,
				final ByteArrayId primaryIndexId ) {
			this.adapter = adapter;
			altIdxTableName = indexName + ALT_INDEX_TABLE;
			altIndexId = new ByteArrayId(
					altIdxTableName);
			this.primaryIndexId = primaryIndexId;
			try {
				if (baseOperations.indexExists(new ByteArrayId(
						indexName))) {
					if (!baseOperations.indexExists(new ByteArrayId(
							altIdxTableName))) {
						throw new IllegalArgumentException(
								"Requested alternate index table does not exist.");
					}
				}
				else {
					// index table does not exist yet
					if (baseOperations.indexExists(new ByteArrayId(
							altIdxTableName))) {
						baseOperations.deleteAll(
								new ByteArrayId(
										altIdxTableName),
								null);
						LOGGER.warn("Deleting current alternate index table [" + altIdxTableName
								+ "] as main table does not yet exist.");
					}
				}
			}
			catch (final IOException e) {
				LOGGER.error("Exception checking for index " + indexName + ": " + e);
			}
		}

		@Override
		public void entryIngested(
				final T entry,
				final GeoWaveRow... geowaveRows ) {
			for (final GeoWaveRow geowaveRow : geowaveRows) {
				final ByteArrayId dataId = adapter.getDataId(entry);
				if ((dataId != null) && (dataId.getBytes() != null) && (dataId.getBytes().length > 0)) {
					secondaryIndexDataStore.storeJoinEntry(
							altIndexId,
							dataId,
							adapter.getAdapterId(),
							EMPTY_FIELD_ID,
							primaryIndexId,
							new ByteArrayId(
									geowaveRow.getPartitionKey()),
							new ByteArrayId(
									geowaveRow.getSortKey()),
							EMPTY_VISIBILITY);
				}
			}
		}
	}
}
