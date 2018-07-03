/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
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
import mil.nga.giat.geowave.core.index.InsertionIds;
import mil.nga.giat.geowave.core.store.AdapterToIndexMapping;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.IndexDependentDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.InternalAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.InternalDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.InternalDataAdapterWrapper;
import mil.nga.giat.geowave.core.store.adapter.PersistentAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;
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
import mil.nga.giat.geowave.core.store.memory.MemoryPersistentAdapterStore;
import mil.nga.giat.geowave.core.store.operations.DataStoreOperations;
import mil.nga.giat.geowave.core.store.operations.Deleter;
import mil.nga.giat.geowave.core.store.query.AdapterQuery;
import mil.nga.giat.geowave.core.store.query.EverythingQuery;
import mil.nga.giat.geowave.core.store.query.InsertionIdQuery;
import mil.nga.giat.geowave.core.store.query.PrefixIdQuery;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.QueryOptions;

public class BaseDataStore implements
		DataStore
{
	private final static Logger LOGGER = LoggerFactory.getLogger(BaseDataStore.class);

	protected static final String ALT_INDEX_TABLE = "_GEOWAVE_ALT_INDEX";

	protected final IndexStore indexStore;
	protected final PersistentAdapterStore adapterStore;
	protected final DataStatisticsStore statisticsStore;
	protected final SecondaryIndexDataStore secondaryIndexDataStore;
	protected final AdapterIndexMappingStore indexMappingStore;
	protected final DataStoreOperations baseOperations;
	protected final DataStoreOptions baseOptions;
	protected final InternalAdapterStore internalAdapterStore;

	public BaseDataStore(
			final IndexStore indexStore,
			final PersistentAdapterStore adapterStore,
			final DataStatisticsStore statisticsStore,
			final AdapterIndexMappingStore indexMappingStore,
			final SecondaryIndexDataStore secondaryIndexDataStore,
			final DataStoreOperations operations,
			final DataStoreOptions options,
			final InternalAdapterStore internalAdapterStore ) {
		this.indexStore = indexStore;
		this.adapterStore = adapterStore;
		this.statisticsStore = statisticsStore;
		this.indexMappingStore = indexMappingStore;
		this.secondaryIndexDataStore = secondaryIndexDataStore;
		this.internalAdapterStore = internalAdapterStore;

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
			final InternalDataAdapter<?> internalAdapter ) {
		if (baseOptions.isPersistAdapter() && !adapterStore.adapterExists(internalAdapter.getInternalAdapterId())) {
			adapterStore.addAdapter(internalAdapter);
		}

	}

	@Override
	public <T> IndexWriter<T> createWriter(
			final WritableDataAdapter<T> adapter,
			final PrimaryIndex... indices )
			throws MismatchedIndexToAdapterMapping {
		adapter.init(indices);
		// add internal adapter
		final InternalDataAdapter<T> internalAdapter = new InternalDataAdapterWrapper<T>(
				adapter,
				internalAdapterStore.addAdapterId(adapter.getAdapterId()));
		store(internalAdapter);
		indexMappingStore.addAdapterIndexMapping(new AdapterToIndexMapping(
				internalAdapter.getInternalAdapterId(),
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
					internalAdapter,
					index));

			initOnIndexWriterCreate(
					internalAdapter,
					index);

			final IngestCallbackList<T> callbacksList = new IngestCallbackList<T>(
					callbacks);
			writers[i] = createIndexWriter(
					internalAdapter,
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

	@Override
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
			final boolean delete ) {
		final List<CloseableIterator<Object>> results = new ArrayList<CloseableIterator<Object>>();
		// all queries will use the same instance of the dedupe filter for
		// client side filtering because the filter needs to be applied across
		// indices
		final BaseQueryOptions sanitizedQueryOptions = new BaseQueryOptions(
				(queryOptions == null) ? new QueryOptions() : queryOptions,
				internalAdapterStore);

		// If CQL filter is set
		if (query instanceof AdapterQuery) {
			final ByteArrayId CQlAdapterId = ((AdapterQuery) query).getAdapterId();

			if ((sanitizedQueryOptions.getAdapterIds() == null) || (sanitizedQueryOptions.getAdapterIds().isEmpty())) {
				sanitizedQueryOptions.setInternalAdapterId(internalAdapterStore.getInternalAdapterId(CQlAdapterId));
			}
			else if (sanitizedQueryOptions.getAdapterIds().size() == 1) {
				if (!sanitizedQueryOptions.getAdapterIds().iterator().next().equals(
						internalAdapterStore.getInternalAdapterId(CQlAdapterId))) {
					LOGGER.error("CQL Query AdapterID does not match Query Options AdapterId");
					throw new RuntimeException(
							"CQL Query AdapterID does not match Query Options AdapterId");
				}
			}
			else {
				// Throw exception when QueryOptions has more than one adapter
				// and CQL Adapter is set.
				LOGGER.error("CQL Query AdapterID does not match Query Options AdapterId");
				throw new RuntimeException(
						"CQL Query AdapterID does not match Query Options AdapterId");
			}

		}

		final Query sanitizedQuery = (query == null) ? new EverythingQuery() : query;

		final DedupeFilter filter = new DedupeFilter();
		MemoryPersistentAdapterStore tempAdapterStore;
		final List<DataStoreCallbackManager> deleteCallbacks = new ArrayList<>();

		try {
			tempAdapterStore = new MemoryPersistentAdapterStore(
					sanitizedQueryOptions.getAdaptersArray(adapterStore));
			// keep a list of adapters that have been queried, to only load an
			// adapter to be queried once
			final Set<Short> queriedAdapters = new HashSet<Short>();
			for (final Pair<PrimaryIndex, List<InternalDataAdapter<?>>> indexAdapterPair : sanitizedQueryOptions
					.getAdaptersWithMinimalSetOfIndices(
							tempAdapterStore,
							indexMappingStore,
							indexStore)) {
				final List<Short> adapterIdsToQuery = new ArrayList<>();
				for (final InternalDataAdapter adapter : indexAdapterPair.getRight()) {
					if (delete) {
						final DataStoreCallbackManager callbackCache = new DataStoreCallbackManager(
								statisticsStore,
								secondaryIndexDataStore,
								queriedAdapters.add(adapter.getInternalAdapterId()));
						deleteCallbacks.add(callbackCache);
						final ScanCallback callback = sanitizedQueryOptions.getScanCallback();

						final PrimaryIndex index = indexAdapterPair.getLeft();
						sanitizedQueryOptions.setScanCallback(new ScanCallback<Object, GeoWaveRow>() {

							@Override
							public void entryScanned(
									final Object entry,
									final GeoWaveRow row ) {
								if (callback != null) {
									callback.entryScanned(
											entry,
											row);
								}
								callbackCache.getDeleteCallback(
										adapter,
										index).entryDeleted(
										entry,
										row);
							}
						});
					}
					if (sanitizedQuery instanceof InsertionIdQuery) {
						sanitizedQueryOptions.setLimit(-1);
						results.add(queryInsertionId(
								adapter,
								indexAdapterPair.getLeft(),
								(InsertionIdQuery) sanitizedQuery,
								filter,
								sanitizedQueryOptions,
								tempAdapterStore,
								delete));
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
								adapterIdsToQuery,
								delete));
						continue;
					}
					adapterIdsToQuery.add(adapter.getInternalAdapterId());
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
						for (final DataStoreCallbackManager c : deleteCallbacks) {
							c.close();
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
			return deleteEverything();
		}

		final BaseQueryOptions sanitizedQueryOptions = new BaseQueryOptions(
				queryOptions,
				internalAdapterStore);
		final AtomicBoolean aOk = new AtomicBoolean(
				true);

		// If CQL filter is set
		if (query instanceof AdapterQuery) {
			final ByteArrayId CQlAdapterId = ((AdapterQuery) query).getAdapterId();

			if ((sanitizedQueryOptions.getAdapterIds() == null) || (sanitizedQueryOptions.getAdapterIds().isEmpty())) {
				sanitizedQueryOptions.setInternalAdapterId(internalAdapterStore.getInternalAdapterId(CQlAdapterId));
			}
			else if (sanitizedQueryOptions.getAdapterIds().size() == 1) {
				if (!sanitizedQueryOptions.getAdapterIds().iterator().next().equals(
						internalAdapterStore.getInternalAdapterId(CQlAdapterId))) {
					LOGGER.error("CQL Query AdapterID does not match Query Options AdapterId");
					throw new RuntimeException(
							"CQL Query AdapterID does not match Query Options AdapterId");
				}
			}
			else {
				// Throw exception when QueryOptions has more than one adapter
				// and CQL Adapter
				// is set.
				LOGGER.error("CQL Query AdapterID does not match Query Options AdapterIds");
				throw new RuntimeException(
						"CQL Query AdapterID does not match Query Options AdapterIds");
			}

		}

		// keep a list of adapters that have been queried, to only low an
		// adapter to be queried
		// once
		final Set<Short> queriedAdapters = new HashSet<Short>();
		Deleter idxDeleter = null, altIdxDeleter = null;
		try {
			for (final Pair<PrimaryIndex, List<InternalDataAdapter<?>>> indexAdapterPair : sanitizedQueryOptions
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

				for (final InternalDataAdapter adapter : indexAdapterPair.getRight()) {

					final DataStoreCallbackManager callbackCache = new DataStoreCallbackManager(
							statisticsStore,
							secondaryIndexDataStore,
							queriedAdapters.add(adapter.getInternalAdapterId()));

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
									adapter,
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
					sanitizedQueryOptions.setScanCallback(callback);
					final List<Short> adapterIds = Collections.singletonList(adapter.getInternalAdapterId());
					if (query instanceof InsertionIdQuery) {
						queryOptions.setLimit(-1);
						dataIt = queryInsertionId(
								adapter,
								index,
								(InsertionIdQuery) query,
								null,
								sanitizedQueryOptions,
								adapterStore,
								true);
					}
					else if (query instanceof PrefixIdQuery) {
						dataIt = queryRowPrefix(
								index,
								((PrefixIdQuery) query).getPartitionKey(),
								((PrefixIdQuery) query).getSortKeyPrefix(),
								sanitizedQueryOptions,
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
								sanitizedQueryOptions,
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
			}

			return aOk.get();
		}
		catch (final Exception e) {
			LOGGER.error(
					"Failed delete operation " + query.toString(),
					e);
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

	protected boolean deleteEverything() {
		try {
			indexStore.removeAll();
			adapterStore.removeAll();
			statisticsStore.removeAll();
			internalAdapterStore.removeAll();
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
			final InternalDataAdapter<T> adapter,
			final PrimaryIndex index,
			final String... additionalAuthorizations )
			throws IOException {
		final String altIdxTableName = index.getId().getString() + ALT_INDEX_TABLE;

		statisticsStore.removeAllStatistics(
				adapter.getInternalAdapterId(),
				additionalAuthorizations);

		// cannot delete because authorizations are not used
		// this.indexMappingStore.remove(adapter.getAdapterId());

		baseOperations.deleteAll(
				index.getId(),
				adapter.getInternalAdapterId(),
				additionalAuthorizations);
		if (baseOptions.isUseAltIndex() && baseOperations.indexExists(new ByteArrayId(
				altIdxTableName))) {
			baseOperations.deleteAll(
					new ByteArrayId(
							altIdxTableName),
					adapter.getInternalAdapterId(),
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

	protected CloseableIterator<Object> queryConstraints(
			final List<Short> adapterIdsToQuery,
			final PrimaryIndex index,
			final Query sanitizedQuery,
			final DedupeFilter filter,
			final BaseQueryOptions sanitizedQueryOptions,
			final PersistentAdapterStore tempAdapterStore,
			final boolean delete ) {
		final BaseConstraintsQuery constraintsQuery = new BaseConstraintsQuery(
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
			final BaseQueryOptions sanitizedQueryOptions,
			final PersistentAdapterStore tempAdapterStore,
			final List<Short> adapterIdsToQuery,
			final boolean delete ) {
		final BaseRowPrefixQuery<Object> prefixQuery = new BaseRowPrefixQuery<Object>(
				index,
				partitionKey,
				sortPrefix,
				(ScanCallback<Object, ?>) sanitizedQueryOptions.getScanCallback(),
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
				tempAdapterStore,
				sanitizedQueryOptions.getLimit());

	}

	protected CloseableIterator<Object> queryInsertionId(
			final InternalDataAdapter<?> adapter,
			final PrimaryIndex index,
			final InsertionIdQuery query,
			final DedupeFilter filter,
			final BaseQueryOptions sanitizedQueryOptions,
			final PersistentAdapterStore tempAdapterStore,
			final boolean delete ) {
		final DifferingFieldVisibilityEntryCount visibilityCounts = DifferingFieldVisibilityEntryCount
				.getVisibilityCounts(
						index,
						Collections.singletonList(adapter.getInternalAdapterId()),
						statisticsStore,
						sanitizedQueryOptions.getAuthorizations());

		final BaseInsertionIdQuery<Object> q = new BaseInsertionIdQuery<Object>(
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
			final InternalDataAdapter<T> adapter,
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
			final InternalDataAdapter<T> adapter,
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
					"Unable to create table for alt index to  [" + indexName + "]",
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
					// TODO GEOWAVE-1018 the secondaryIndexDataStore isn't
					// really implemented fully, so this warning will likely
					// occur because there really is no "alt index" without
					// secondary indexing
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
