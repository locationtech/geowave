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
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.index.InsertionIds;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.IndexDependentDataAdapter;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapterWrapper;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.adapter.statistics.DuplicateEntryCount;
import org.locationtech.geowave.core.store.adapter.statistics.StatisticsType;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.api.QueryOptions;
import org.locationtech.geowave.core.store.callback.IngestCallback;
import org.locationtech.geowave.core.store.callback.IngestCallbackList;
import org.locationtech.geowave.core.store.callback.ScanCallback;
import org.locationtech.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import org.locationtech.geowave.core.store.data.visibility.FieldVisibilityCount;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.index.IndexMetaDataSet;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.index.SecondaryIndexDataStore;
import org.locationtech.geowave.core.store.index.writer.IndependentAdapterIndexWriter;
import org.locationtech.geowave.core.store.index.writer.IndexCompositeWriter;
import org.locationtech.geowave.core.store.memory.MemoryPersistentAdapterStore;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.core.store.operations.RowDeleter;
import org.locationtech.geowave.core.store.query.constraints.TypeConstraintQuery;
import org.locationtech.geowave.core.store.query.constraints.EverythingQuery;
import org.locationtech.geowave.core.store.query.constraints.InsertionIdQuery;
import org.locationtech.geowave.core.store.query.constraints.PrefixIdQuery;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.filter.DedupeFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;

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
			final Index index ) {
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
	public <T> Writer<T> createWriter(
			final DataTypeAdapter<T> adapter,
			final Index... indices )
			throws MismatchedIndexToAdapterMapping {
		adapter.init(indices);
		// add internal adapter
		final InternalDataAdapter<T> internalAdapter = new InternalDataAdapterWrapper<>(
				adapter,
				internalAdapterStore.addAdapterId(adapter.getAdapterId()));
		store(internalAdapter);
		indexMappingStore.addAdapterIndexMapping(new AdapterToIndexMapping(
				internalAdapter.getInternalAdapterId(),
				indices));

		final Writer<T>[] writers = new Writer[indices.length];

		int i = 0;
		for (final Index index : indices) {
			final DataStoreCallbackManager callbackManager = new DataStoreCallbackManager(
					statisticsStore,
					secondaryIndexDataStore,
					i == 0);

			callbackManager.setPersistStats(baseOptions.isPersistDataStatistics());

			final List<IngestCallback<T>> callbacks = new ArrayList<>();

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

			final IngestCallbackList<T> callbacksList = new IngestCallbackList<>(
					callbacks);
			writers[i] = createIndexWriter(
					internalAdapter,
					index,
					baseOperations,
					baseOptions,
					callbacksList,
					callbacksList);

			if (adapter instanceof IndexDependentDataAdapter) {
				writers[i] = new IndependentAdapterIndexWriter<>(
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
			final QueryOptions<T> queryOptions,
			final QueryConstraints query ) {
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
			final QueryOptions<T> queryOptions,
			final QueryConstraints query,
			final boolean delete ) {
		final List<CloseableIterator<Object>> results = new ArrayList<>();
		// all queries will use the same instance of the dedupe filter for
		// client side filtering because the filter needs to be applied across
		// indices
		final BaseQueryOptions sanitizedQueryOptions = new BaseQueryOptions(
				(queryOptions == null) ? new QueryOptions() : queryOptions,
				internalAdapterStore);

		// If CQL filter is set
		if (query instanceof TypeConstraintQuery) {
			final ByteArrayId CQlAdapterId = ((TypeConstraintQuery) query).getAdapterId();

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

		final QueryConstraints sanitizedQuery = (query == null) ? new EverythingQuery() : query;

		final DedupeFilter filter = new DedupeFilter();
		MemoryPersistentAdapterStore tempAdapterStore;
		final List<DataStoreCallbackManager> deleteCallbacks = new ArrayList<>();

		try {
			tempAdapterStore = new MemoryPersistentAdapterStore(
					sanitizedQueryOptions.getAdaptersArray(adapterStore));
			// keep a list of adapters that have been queried, to only load an
			// adapter to be queried once
			final Set<Short> queriedAdapters = new HashSet<>();
			for (final Pair<Index, List<InternalDataAdapter<?>>> indexAdapterPair : sanitizedQueryOptions
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

						final Index index = indexAdapterPair.getLeft();
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
		return new CloseableIteratorWrapper<>(
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
			final QueryConstraints query ) {
		if (((query == null) || (query instanceof EverythingQuery)) && queryOptions.isAllAdapters()) {
			return deleteEverything();
		}

		final BaseQueryOptions sanitizedQueryOptions = new BaseQueryOptions(
				queryOptions,
				internalAdapterStore);
		final AtomicBoolean aOk = new AtomicBoolean(
				true);

		// If CQL filter is set
		if (query instanceof TypeConstraintQuery) {
			final ByteArrayId CQlAdapterId = ((TypeConstraintQuery) query).getAdapterId();

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
		final Set<Short> queriedAdapters = new HashSet<>();
		RowDeleter idxDeleter = null, altIdxDeleter = null;
		try {
			for (final Pair<Index, List<InternalDataAdapter<?>>> indexAdapterPair : sanitizedQueryOptions
					.getIndicesForAdapters(
							adapterStore,
							indexMappingStore,
							indexStore)) {
				final Index index = indexAdapterPair.getLeft();
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
					final RowDeleter internalIdxDeleter = idxDeleter;
					final RowDeleter internalAltIdxDeleter = altIdxDeleter;
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
			final Index index,
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
			final Index index,
			final QueryConstraints sanitizedQuery,
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
				sanitizedQueryOptions.getMaxRangeDecomposition());
	}

	protected CloseableIterator<Object> queryRowPrefix(
			final Index index,
			final ByteArrayId partitionKey,
			final ByteArrayId sortPrefix,
			final BaseQueryOptions sanitizedQueryOptions,
			final PersistentAdapterStore tempAdapterStore,
			final List<Short> adapterIdsToQuery,
			final boolean delete ) {
		final BaseRowPrefixQuery<Object> prefixQuery = new BaseRowPrefixQuery<>(
				index,
				partitionKey,
				sortPrefix,
				(ScanCallback<Object, ?>) sanitizedQueryOptions.getScanCallback(),
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

		return prefixQuery.query(
				baseOperations,
				baseOptions,
				sanitizedQueryOptions.getMaxResolutionSubsamplingPerDimension(),
				tempAdapterStore,
				sanitizedQueryOptions.getLimit(),
				sanitizedQueryOptions.getMaxRangeDecomposition());

	}

	protected CloseableIterator<Object> queryInsertionId(
			final InternalDataAdapter<?> adapter,
			final Index index,
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
		final FieldVisibilityCount visibilityCounts = FieldVisibilityCount.getVisibilityCounts(
				index,
				Collections.singletonList(adapter.getInternalAdapterId()),
				statisticsStore,
				sanitizedQueryOptions.getAuthorizations());
		final BaseInsertionIdQuery<Object> q = new BaseInsertionIdQuery<>(
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
				sanitizedQueryOptions.getMaxRangeDecomposition());
	}

	protected <T> Writer<T> createIndexWriter(
			final InternalDataAdapter<T> adapter,
			final Index index,
			final DataStoreOperations baseOperations,
			final DataStoreOptions baseOptions,
			final IngestCallback<T> callback,
			final Closeable closable ) {
		return new BaseIndexWriter<>(
				adapter,
				index,
				baseOperations,
				baseOptions,
				callback,
				closable);
	}

	protected <T> void initOnIndexWriterCreate(
			final InternalDataAdapter<T> adapter,
			final Index index ) {}

	protected <T> void addAltIndexCallback(
			final List<IngestCallback<T>> callbacks,
			final String indexName,
			final DataTypeAdapter<T> adapter,
			final ByteArrayId primaryIndexId ) {
		try {
			callbacks.add(new AltIndexCallback<>(
					indexName,
					adapter,
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
		private final DataTypeAdapter<T> adapter;
		private final String altIdxTableName;
		private final ByteArrayId primaryIndexId;
		private final ByteArrayId altIndexId;

		public AltIndexCallback(
				final String indexName,
				final DataTypeAdapter<T> adapter,
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

	@Override
	public InternalDataStatistics<?>[] getStatistics(
			final ByteArrayId adapterId,
			final String... authorizations ) {
		if (adapterId == null) {
			try (CloseableIterator<InternalDataStatistics<?>> it = (CloseableIterator) statisticsStore
					.getAllDataStatistics(authorizations)) {
				return Iterators.toArray(
						it,
						InternalDataStatistics.class);
			}
			catch (IOException e) {
				LOGGER.warn(
						"Unable to close statistics iterator",
						e);
				return new InternalDataStatistics[0];
			}
		}
		final Short internalAdapterId = internalAdapterStore.getInternalAdapterId(adapterId);
		if (internalAdapterId == null) {
			LOGGER.warn("Unable to find adapter '" + adapterId.getString() + "' for stats");
			return new InternalDataStatistics[0];
		}
		try (CloseableIterator<InternalDataStatistics<?>> it = (CloseableIterator) statisticsStore.getDataStatistics(
				internalAdapterId,
				authorizations)) {
			return Iterators.toArray(
					it,
					InternalDataStatistics.class);
		}
		catch (IOException e) {
			LOGGER.warn(
					"Unable to close statistics iterator per adapter",
					e);
			return new InternalDataStatistics[0];
		}
	}

	@Override
	public DataTypeAdapter<?>[] getAdapters() {
		try (CloseableIterator<InternalDataAdapter<?>> it = adapterStore.getAdapters()) {
			return Iterators
					.toArray(
							Iterators
									.transform(
											it,
											a -> (DataTypeAdapter<?>) a.getAdapter()),
							DataTypeAdapter.class);
		}
		catch (final IOException e) {
			LOGGER
					.warn(
							"Unable to close adapter iterator",
							e);
			return new DataTypeAdapter[0];		}
	}

	@Override
	public Index[] getIndices(
			ByteArrayId adapterId ) {
		Short internalAdapterId = internalAdapterStore.getInternalAdapterId(adapterId);
		if (internalAdapterId == null) {
			LOGGER.warn("Unable to find adapter '" + adapterId.getString() + "' for indices");
			return new Index[0];
		}
		AdapterToIndexMapping indices = indexMappingStore.getIndicesForAdapter(internalAdapterId);
		return indices.getIndices(indexStore);
	}

	@Override
	public <R extends InternalDataStatistics<?>> R getStatistics(
			ByteArrayId adapterId,
			StatisticsType<R> statisticsType,
			String... authorizations ) {
		Short internalAdapterId = internalAdapterStore.getInternalAdapterId(adapterId);
		if (internalAdapterId == null) {
			LOGGER.warn("Unable to find adapter '" + adapterId.getString() + "' for statistics");
			return null;
		}
		return (R) statisticsStore.getDataStatistics(
				internalAdapterId,
				statisticsType,
				authorizations);
	}
}
