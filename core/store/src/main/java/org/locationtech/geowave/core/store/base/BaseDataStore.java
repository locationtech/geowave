/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *   
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.core.store.base;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.DataStore;
import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.IndexWriter;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.IndexDependentDataAdapter;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapterWrapper;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.WritableDataAdapter;
import org.locationtech.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.adapter.statistics.DuplicateEntryCount;
import org.locationtech.geowave.core.store.callback.IngestCallback;
import org.locationtech.geowave.core.store.callback.IngestCallbackList;
import org.locationtech.geowave.core.store.callback.ScanCallback;
import org.locationtech.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import org.locationtech.geowave.core.store.data.visibility.FieldVisibilityCount;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.filter.DedupeFilter;
import org.locationtech.geowave.core.store.index.IndexMetaDataSet;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.index.PrimaryIndex;
import org.locationtech.geowave.core.store.index.SecondaryIndexDataStore;
import org.locationtech.geowave.core.store.index.writer.IndependentAdapterIndexWriter;
import org.locationtech.geowave.core.store.index.writer.IndexCompositeWriter;
import org.locationtech.geowave.core.store.memory.MemoryPersistentAdapterStore;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.core.store.query.AdapterQuery;
import org.locationtech.geowave.core.store.query.EverythingQuery;
import org.locationtech.geowave.core.store.query.InsertionIdQuery;
import org.locationtech.geowave.core.store.query.PrefixIdQuery;
import org.locationtech.geowave.core.store.query.Query;
import org.locationtech.geowave.core.store.query.QueryOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;

public class BaseDataStore implements
		DataStore
{
	private final static Logger LOGGER = LoggerFactory.getLogger(BaseDataStore.class);

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

	public DataStatisticsStore getStatisticsStore() {
		return this.statisticsStore;
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
	 * org.locationtech.geowave.core.store.DataStore#query(org.locationtech.
	 * geowave. core.store.query.QueryOptions,
	 * org.locationtech.geowave.core.store.query.Query)
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
			final List<Pair<PrimaryIndex, List<InternalDataAdapter<?>>>> indexAdapterPairList = delete ? sanitizedQueryOptions
					.getIndicesForAdapters(
							tempAdapterStore,
							indexMappingStore,
							indexStore) : sanitizedQueryOptions.getAdaptersWithMinimalSetOfIndices(
					tempAdapterStore,
					indexMappingStore,
					indexStore);
			for (Pair<PrimaryIndex, List<InternalDataAdapter<?>>> indexAdapterPair : indexAdapterPairList) {
				final List<Short> adapterIdsToQuery = new ArrayList<>();
				// this only needs to be done once per index, not once per
				// adapter
				boolean queriedAllAdaptersByPrefix = false;
				for (final InternalDataAdapter adapter : indexAdapterPair.getRight()) {
					if (delete) {
						final DataStoreCallbackManager callbackCache = new DataStoreCallbackManager(
								statisticsStore,
								secondaryIndexDataStore,
								queriedAdapters.add(adapter.getInternalAdapterId()));
						callbackCache.setPersistStats(baseOptions.isPersistDataStatistics());
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
						if (!queriedAllAdaptersByPrefix) {
							final PrefixIdQuery prefixIdQuery = (PrefixIdQuery) sanitizedQuery;
							results.add(queryRowPrefix(
									indexAdapterPair.getLeft(),
									prefixIdQuery.getPartitionKey(),
									prefixIdQuery.getSortKeyPrefix(),
									sanitizedQueryOptions,
									indexAdapterPair.getRight(),
									tempAdapterStore,
									delete));
							queriedAllAdaptersByPrefix = true;
						}
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
		final BaseQueryOptions sanitizedQueryOptions = new BaseQueryOptions(
				queryOptions,
				internalAdapterStore);
		if (((query == null) || (query instanceof EverythingQuery))) {
			if (sanitizedQueryOptions.isAllAdapters()) {
				// TODO what about authorizations here?
				return deleteEverything();
			}
			else {
				try {
					for (final Pair<PrimaryIndex, List<InternalDataAdapter<?>>> indexAdapterPair : sanitizedQueryOptions
							.getIndicesForAdapters(
									adapterStore,
									indexMappingStore,
									indexStore)) {

						for (final InternalDataAdapter adapter : indexAdapterPair.getRight()) {
							try {
								deleteEntries(
										adapter,
										indexAdapterPair.getLeft(),
										sanitizedQueryOptions.getAuthorizations());
							}
							catch (IOException e) {
								LOGGER.warn(
										"Unable to delete by adapter",
										e);
								return false;
							}
						}
					}
				}
				catch (IOException e) {
					LOGGER.warn(
							"Unable to get adapters to delete",
							e);
					return false;
				}
			}
		}
		else {
			try (CloseableIterator<?> dataIt = internalQuery(
					queryOptions,
					query,
					true)) {
				while (dataIt.hasNext()) {
					dataIt.next();
				}
			}
			catch (IOException e) {
				LOGGER.warn(
						"Unable to close deleter",
						e);
				return false;
			}
		}

		return true;
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
		statisticsStore.removeAllStatistics(
				adapter.getInternalAdapterId(),
				additionalAuthorizations);

		// cannot delete because authorizations are not used
		// this.indexMappingStore.remove(adapter.getAdapterId());

		baseOperations.deleteAll(
				index.getId(),
				adapter.getInternalAdapterId(),
				additionalAuthorizations);
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
				FieldVisibilityCount.getVisibilityCounts(
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
				sanitizedQueryOptions.getLimit(),
				sanitizedQueryOptions.getMaxRangeDecomposition(),
				delete);
	}

	protected CloseableIterator<Object> queryRowPrefix(
			final PrimaryIndex index,
			final ByteArrayId partitionKey,
			final ByteArrayId sortPrefix,
			final BaseQueryOptions sanitizedQueryOptions,
			List<InternalDataAdapter<?>> adapters,
			final PersistentAdapterStore tempAdapterStore,
			final boolean delete ) {
		Set<Short> adapterIds = adapters.stream().map(a -> a.getInternalAdapterId()).collect(Collectors.toSet());
		final BaseRowPrefixQuery<Object> prefixQuery = new BaseRowPrefixQuery<Object>(
				index,
				partitionKey,
				sortPrefix,
				(ScanCallback<Object, ?>) sanitizedQueryOptions.getScanCallback(),
				DifferingFieldVisibilityEntryCount.getVisibilityCounts(
						index,
						adapterIds,
						statisticsStore,
						sanitizedQueryOptions.getAuthorizations()),
				FieldVisibilityCount.getVisibilityCounts(
						index,
						adapterIds,
						statisticsStore,
						sanitizedQueryOptions.getAuthorizations()),
				sanitizedQueryOptions.getAuthorizations());

		return prefixQuery.query(
				baseOperations,
				baseOptions,
				sanitizedQueryOptions.getMaxResolutionSubsamplingPerDimension(),
				tempAdapterStore,
				sanitizedQueryOptions.getLimit(),
				sanitizedQueryOptions.getMaxRangeDecomposition(),
				delete);

	}

	protected CloseableIterator<Object> queryInsertionId(
			final InternalDataAdapter<?> adapter,
			final PrimaryIndex index,
			final InsertionIdQuery query,
			final DedupeFilter filter,
			final BaseQueryOptions sanitizedQueryOptions,
			final PersistentAdapterStore tempAdapterStore,
			final boolean delete ) {
		final DifferingFieldVisibilityEntryCount differingVisibilityCounts = DifferingFieldVisibilityEntryCount
				.getVisibilityCounts(
						index,
						Collections.singletonList(adapter.getInternalAdapterId()),
						statisticsStore,
						sanitizedQueryOptions.getAuthorizations());
		FieldVisibilityCount visibilityCounts = FieldVisibilityCount.getVisibilityCounts(
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
				differingVisibilityCounts,
				visibilityCounts,
				sanitizedQueryOptions.getAuthorizations());
		return q.query(
				baseOperations,
				baseOptions,
				tempAdapterStore,
				sanitizedQueryOptions.getMaxResolutionSubsamplingPerDimension(),
				sanitizedQueryOptions.getLimit(),
				sanitizedQueryOptions.getMaxRangeDecomposition(),
				delete);
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

}
