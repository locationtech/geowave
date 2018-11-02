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
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.IndexDependentDataAdapter;
import org.locationtech.geowave.core.store.adapter.InitializeWithIndicesDataAdapter;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapterWrapper;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.adapter.statistics.DuplicateEntryCount;
import org.locationtech.geowave.core.store.adapter.statistics.InternalDataStatistics;
import org.locationtech.geowave.core.store.adapter.statistics.StatisticsImpl;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.api.AggregationQuery;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.IngestOptions;
import org.locationtech.geowave.core.store.api.Query;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.api.Statistics;
import org.locationtech.geowave.core.store.api.StatisticsQuery;
import org.locationtech.geowave.core.store.api.Writer;
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
import org.locationtech.geowave.core.store.query.aggregate.AdapterAndIndexBasedAggregation;
import org.locationtech.geowave.core.store.query.constraints.AdapterAndIndexBasedQueryConstraints;
import org.locationtech.geowave.core.store.query.constraints.EverythingQuery;
import org.locationtech.geowave.core.store.query.constraints.InsertionIdQuery;
import org.locationtech.geowave.core.store.query.constraints.PrefixIdQuery;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.constraints.TypeConstraintQuery;
import org.locationtech.geowave.core.store.query.filter.DedupeFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterators;
import com.google.common.collect.Streams;

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
		if (secondaryIndexDataStore != null) {
			secondaryIndexDataStore.setDataStore(this);
		}
		baseOperations = operations;
		baseOptions = options;
	}

	public void store(
			final Index index ) {
		if (!indexStore.indexExists(index.getName())) {
			indexStore.addIndex(index);
		}
	}

	protected synchronized void store(
			final InternalDataAdapter<?> adapter ) {
		if (!adapterStore.adapterExists(adapter.getAdapterId())) {
			adapterStore.addAdapter(adapter);
		}

	}

	public DataStatisticsStore getStatisticsStore() {
		return statisticsStore;
	}

	private <T> Writer<T> createWriter(
			final InternalDataAdapter<T> adapter,
			final Index... indices ) {

		final Writer<T>[] writers = new Writer[indices.length];

		int i = 0;
		for (final Index index : indices) {
			final DataStoreCallbackManager callbackManager = new DataStoreCallbackManager(
					statisticsStore,
					secondaryIndexDataStore,
					i == 0);

			callbackManager.setPersistStats(baseOptions.isPersistDataStatistics());

			final List<IngestCallback<T>> callbacks = new ArrayList<>();

			callbacks.add(callbackManager.getIngestCallback(
					adapter,
					index));

			final IngestCallbackList<T> callbacksList = new IngestCallbackList<>(
					callbacks);
			writers[i] = createIndexWriter(
					adapter,
					index,
					baseOperations,
					baseOptions,
					callbacksList,
					callbacksList);

			if (adapter.getAdapter() instanceof IndexDependentDataAdapter) {
				writers[i] = new IndependentAdapterIndexWriter<>(
						(IndexDependentDataAdapter<T>) adapter.getAdapter(),
						index,
						writers[i]);
			}
			i++;
		}
		return new IndexCompositeWriter(
				writers);

	}

	public <T, R extends GeoWaveRow> CloseableIterator<T> query(
			final Query<T> query,
			final ScanCallback<T, R> scanCallback ) {
		return internalQuery(
				query,
				false,
				scanCallback);
	}

	@Override
	public <T> CloseableIterator<T> query(
			final Query<T> query ) {
		return internalQuery(
				query,
				false);
	}

	protected <T> CloseableIterator<T> internalQuery(
			final Query<T> query,
			final boolean delete ) {
		return internalQuery(
				query,
				delete,
				null);
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
			Query<T> query,
			final boolean delete,
			final ScanCallback<T, ?> scanCallback ) {
		if (query == null) {
			query = (Query) QueryBuilder.newBuilder().build();
		}
		// all queries will use the same instance of the dedupe filter for
		// client side filtering because the filter needs to be applied across
		// indices
		final BaseQueryOptions queryOptions = new BaseQueryOptions(
				query,
				adapterStore,
				internalAdapterStore,
				scanCallback);
		return internalQuery(
				query.getQueryConstraints(),
				queryOptions,
				delete);
	}

	protected <T> CloseableIterator<T> internalQuery(
			final QueryConstraints constraints,
			final BaseQueryOptions queryOptions,
			final boolean delete ) {

		final List<CloseableIterator<Object>> results = new ArrayList<>();

		// If CQL filter is set
		if (constraints instanceof TypeConstraintQuery) {
			final String constraintTypeName = ((TypeConstraintQuery) constraints).getTypeName();

			if ((queryOptions.getAdapterIds() == null) || (queryOptions.getAdapterIds().length == 0)) {
				queryOptions.setAdapterId(internalAdapterStore.getAdapterId(constraintTypeName));
			}
			else if (queryOptions.getAdapterIds().length == 1) {
				final Short adapterId = internalAdapterStore.getAdapterId(constraintTypeName);
				if ((adapterId == null) || (queryOptions.getAdapterIds()[0] != adapterId.shortValue())) {
					LOGGER.error("Constraint Query Type name does not match Query Options Type Name");
					throw new RuntimeException(
							"Constraint Query Type name does not match Query Options Type Name");
				}
			}
			else {
				// Throw exception when QueryOptions has more than one adapter
				// and CQL Adapter is set.
				LOGGER.error("Constraint Query Type name does not match Query Options Type Name");
				throw new RuntimeException(
						"Constraint Query Type name does not match Query Options Type Name");
			}

		}

		final QueryConstraints sanitizedConstraints = (constraints == null) ? new EverythingQuery() : constraints;
		final boolean isConstraintsAdapterIndexSpecific = sanitizedConstraints instanceof AdapterAndIndexBasedQueryConstraints;
		final boolean isAggregationAdapterIndexSpecific = (queryOptions.getAggregation() != null)
				&& (queryOptions.getAggregation().getRight() instanceof AdapterAndIndexBasedAggregation);
		final DedupeFilter filter = new DedupeFilter();
		MemoryPersistentAdapterStore tempAdapterStore;
		final List<DataStoreCallbackManager> deleteCallbacks = new ArrayList<>();

		try {
			tempAdapterStore = new MemoryPersistentAdapterStore(
					queryOptions.getAdaptersArray(adapterStore));
			// keep a list of adapters that have been queried, to only load an
			// adapter to be queried once
			final Set<Short> queriedAdapters = new HashSet<>();
			final List<Pair<Index, List<InternalDataAdapter<?>>>> indexAdapterPairList = delete ? queryOptions
					.getIndicesForAdapters(
							tempAdapterStore,
							indexMappingStore,
							indexStore) : queryOptions.getAdaptersWithMinimalSetOfIndices(
					tempAdapterStore,
					indexMappingStore,
					indexStore);
			final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation = queryOptions.getAggregation();
			for (final Pair<Index, List<InternalDataAdapter<?>>> indexAdapterPair : indexAdapterPairList) {
				final List<Short> adapterIdsToQuery = new ArrayList<>();
				// this only needs to be done once per index, not once per
				// adapter
				boolean queriedAllAdaptersByPrefix = false;
				for (final InternalDataAdapter adapter : indexAdapterPair.getRight()) {
					if (delete) {
						final DataStoreCallbackManager callbackCache = new DataStoreCallbackManager(
								statisticsStore,
								secondaryIndexDataStore,
								queriedAdapters.add(adapter.getAdapterId()));
						callbackCache.setPersistStats(baseOptions.isPersistDataStatistics());
						deleteCallbacks.add(callbackCache);
						final ScanCallback callback = queryOptions.getScanCallback();

						final Index index = indexAdapterPair.getLeft();
						queryOptions.setScanCallback(new ScanCallback<Object, GeoWaveRow>() {

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
					QueryConstraints adapterIndexConstraints;
					if (isConstraintsAdapterIndexSpecific) {
						adapterIndexConstraints = ((AdapterAndIndexBasedQueryConstraints) sanitizedConstraints)
								.createQueryConstraints(
										adapter,
										indexAdapterPair.getLeft());
					}
					else {
						adapterIndexConstraints = sanitizedConstraints;
					}
					if (isAggregationAdapterIndexSpecific) {
						queryOptions.setAggregation(
								((AdapterAndIndexBasedAggregation) aggregation.getRight()).createAggregation(
										adapter,
										indexAdapterPair.getLeft()),
								aggregation.getLeft());
					}
					if (adapterIndexConstraints instanceof InsertionIdQuery) {
						queryOptions.setLimit(-1);
						results.add(queryInsertionId(
								adapter,
								indexAdapterPair.getLeft(),
								(InsertionIdQuery) adapterIndexConstraints,
								filter,
								queryOptions,
								tempAdapterStore,
								delete));
						continue;
					}
					else if (adapterIndexConstraints instanceof PrefixIdQuery) {
						if (!queriedAllAdaptersByPrefix) {
							final PrefixIdQuery prefixIdQuery = (PrefixIdQuery) adapterIndexConstraints;
							results.add(queryRowPrefix(
									indexAdapterPair.getLeft(),
									prefixIdQuery.getPartitionKey(),
									prefixIdQuery.getSortKeyPrefix(),
									queryOptions,
									indexAdapterPair.getRight(),
									tempAdapterStore,
									delete));
							queriedAllAdaptersByPrefix = true;
						}
						continue;
					}
					else if (isConstraintsAdapterIndexSpecific || isAggregationAdapterIndexSpecific) {
						// can't query multiple adapters in the same scan
						results.add(queryConstraints(
								Collections.singletonList(adapter.getAdapterId()),
								indexAdapterPair.getLeft(),
								adapterIndexConstraints,
								filter,
								queryOptions,
								tempAdapterStore,
								delete));
						continue;
					}
					// finally just add it to a list to query multiple adapters
					// in on scan
					adapterIdsToQuery.add(adapter.getAdapterId());
				}
				// supports querying multiple adapters in a single index
				// in one query instance (one scanner) for efficiency
				if (adapterIdsToQuery.size() > 0) {
					results.add(queryConstraints(
							adapterIdsToQuery,
							indexAdapterPair.getLeft(),
							sanitizedConstraints,
							filter,
							queryOptions,
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

	private boolean isAllAdapters(
			final String[] typeNames ) {
		return Arrays.equals(
				internalAdapterStore.getTypeNames(),
				typeNames);
	}

	public <T> boolean delete(
			Query<T> query,
			final ScanCallback<T, ?> scanCallback ) {
		if (query == null) {
			query = (Query) QueryBuilder.newBuilder().build();
		}
		if (((query.getQueryConstraints() == null) || (query.getQueryConstraints() instanceof EverythingQuery))) {
			if ((query.getDataTypeQueryOptions().getTypeNames() == null)
					|| (query.getDataTypeQueryOptions().getTypeNames().length == 0)
					|| isAllAdapters(query.getDataTypeQueryOptions().getTypeNames())) {
				// TODO what about authorizations here?
				return deleteEverything();
			}
			else {
				try {
					final BaseQueryOptions sanitizedQueryOptions = new BaseQueryOptions(
							query,
							adapterStore,
							internalAdapterStore);
					for (final Pair<Index, List<InternalDataAdapter<?>>> indexAdapterPair : sanitizedQueryOptions
							.getIndicesForAdapters(
									adapterStore,
									indexMappingStore,
									indexStore)) {

						for (final InternalDataAdapter adapter : indexAdapterPair.getRight()) {
							try {
								deleteEntries(
										adapter,
										indexAdapterPair.getLeft(),
										query.getCommonQueryOptions().getAuthorizations());
							}
							catch (final IOException e) {
								LOGGER.warn(
										"Unable to delete by adapter",
										e);
								return false;
							}
						}
					}
				}
				catch (final IOException e) {
					LOGGER.warn(
							"Unable to get adapters to delete",
							e);
					return false;
				}
			}
		}
		else {
			try (CloseableIterator<?> dataIt = internalQuery(
					query,
					true,
					scanCallback)) {
				while (dataIt.hasNext()) {
					dataIt.next();
				}
			}
		}

		return true;
	}

	@Override
	public <T> boolean delete(
			final Query<T> query ) {
		return delete(
				query,
				null);
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
		statisticsStore.removeAllStatistics(
				adapter.getAdapterId(),
				additionalAuthorizations);

		// cannot delete because authorizations are not used
		// this.indexMappingStore.remove(adapter.getAdapterId());

		baseOperations.deleteAll(
				index.getName(),
				adapter.getTypeName(),
				adapter.getAdapterId(),
				additionalAuthorizations);
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
				ArrayUtils.toPrimitive(adapterIdsToQuery.toArray(new Short[0])),
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
				internalAdapterStore,
				sanitizedQueryOptions.getMaxResolutionSubsamplingPerDimension(),
				sanitizedQueryOptions.getTargetResolutionPerDimensionForHierarchicalIndex(),
				sanitizedQueryOptions.getLimit(),
				sanitizedQueryOptions.getMaxRangeDecomposition(),
				delete);
	}

	protected CloseableIterator<Object> queryRowPrefix(
			final Index index,
			final ByteArray partitionKey,
			final ByteArray sortPrefix,
			final BaseQueryOptions sanitizedQueryOptions,
			final List<InternalDataAdapter<?>> adapters,
			final PersistentAdapterStore tempAdapterStore,
			final boolean delete ) {
		final Set<Short> adapterIds = adapters
				.stream()
				.map(
						a -> a.getAdapterId())
				.collect(
						Collectors.toSet());
		final BaseRowPrefixQuery<Object> prefixQuery = new BaseRowPrefixQuery<>(
				index,
				partitionKey,
				sortPrefix,
				(ScanCallback<Object, ?>) sanitizedQueryOptions.getScanCallback(),
				DifferingFieldVisibilityEntryCount
						.getVisibilityCounts(
								index,
								adapterIds,
								statisticsStore,
								sanitizedQueryOptions.getAuthorizations()),
				FieldVisibilityCount
						.getVisibilityCounts(
								index,
								adapterIds,
								statisticsStore,
								sanitizedQueryOptions.getAuthorizations()),
				sanitizedQueryOptions.getAuthorizations());

		return prefixQuery
				.query(
						baseOperations,
						baseOptions,
						sanitizedQueryOptions.getMaxResolutionSubsamplingPerDimension(),
						sanitizedQueryOptions.getTargetResolutionPerDimensionForHierarchicalIndex(),
						tempAdapterStore,
						internalAdapterStore,
						sanitizedQueryOptions.getLimit(),
						sanitizedQueryOptions.getMaxRangeDecomposition(),
						delete);

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
						Collections.singletonList(adapter.getAdapterId()),
						statisticsStore,
						sanitizedQueryOptions.getAuthorizations());
		final FieldVisibilityCount visibilityCounts = FieldVisibilityCount.getVisibilityCounts(
				index,
				Collections.singletonList(adapter.getAdapterId()),
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
				internalAdapterStore,
				sanitizedQueryOptions.getMaxResolutionSubsamplingPerDimension(),
				sanitizedQueryOptions.getTargetResolutionPerDimensionForHierarchicalIndex(),
				sanitizedQueryOptions.getLimit(),
				sanitizedQueryOptions.getMaxRangeDecomposition(),
				delete);
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

	/**
	 * Get all the adapters that have been used within this data store
	 *
	 * @return An array of the adapters used within this datastore.
	 */
	@Override
	public DataTypeAdapter<?>[] getTypes() {
		try (CloseableIterator<InternalDataAdapter<?>> it = adapterStore.getAdapters()) {
			return Iterators
					.toArray(
							Iterators
									.transform(
											it,
											a -> (DataTypeAdapter<?>) a.getAdapter()),
							DataTypeAdapter.class);
		}
	}

	// public Statistics<?>[] getStatistics(
	// @Nullable String typeName,
	// String... authorizations ) {
	// if (typeName == null) {
	// try (CloseableIterator<InternalDataStatistics<?, ?,?>> it =
	// (CloseableIterator) statisticsStore
	// .getAllDataStatistics(
	// authorizations)) {
	// return Iterators
	// .toArray(
	// Iterators.transform(it,s -> new StatisticsImpl<>(result, statsType,
	// statsId, typeName) ),
	// InternalDataStatistics.class);
	// }
	// catch (final IOException e) {
	// LOGGER
	// .warn(
	// "Unable to close statistics iterator",
	// e);
	// return new InternalDataStatistics[0];
	// }
	// }
	// final Short internalAdapterId = internalAdapterStore
	// .getAdapterId(
	// typeName);
	// if (internalAdapterId == null) {
	// LOGGER
	// .warn(
	// "Unable to find adapter '" + typeName + "' for stats");
	// return new InternalDataStatistics[0];
	// }
	// try (CloseableIterator<InternalDataStatistics<?,?,?>> it =
	// (CloseableIterator) statisticsStore
	// .getDataStatistics(
	// internalAdapterId,
	// authorizations)) {
	// return Iterators
	// .toArray(
	// it,
	// InternalDataStatistics.class);
	// }
	// catch (final IOException e) {
	// LOGGER
	// .warn(
	// "Unable to close statistics iterator per adapter",
	// e);
	// return new InternalDataStatistics[0];
	// }
	// }
	//
	// public <R> R getStatisticsResult(
	// String typeName,
	// StatisticsType<R,?> statisticsType,
	// String... authorizations ) {
	//
	// final Short internalAdapterId = internalAdapterStore
	// .getAdapterId(
	// typeName);
	// if (internalAdapterId == null) {
	// LOGGER
	// .warn(
	// "Unable to find adapter '" + typeName + "' for statistics");
	// return null;
	// }
	// return (R) statisticsStore
	// .getDataStatistics(
	// internalAdapterId,
	// statisticsType,
	// authorizations);
	// }

	@Override
	public Index[] getIndices() {
		return getIndices(null);
	}

	@Override
	public Index[] getIndices(
			final String typeName ) {
		if (typeName == null) {
			final List<Index> indexList = new ArrayList<>();
			try (CloseableIterator<Index> indexIt = indexStore.getIndices()) {
				while (indexIt.hasNext()) {
					indexList.add(indexIt.next());
				}
				return indexList.toArray(new Index[0]);
			}
		}
		final Short internalAdapterId = internalAdapterStore.getAdapterId(typeName);
		if (internalAdapterId == null) {
			LOGGER.warn("Unable to find adapter '" + typeName + "' for indices");
			return new Index[0];
		}
		final AdapterToIndexMapping indices = indexMappingStore.getIndicesForAdapter(internalAdapterId);
		return indices.getIndices(indexStore);
	}

	@Override
	public void addIndex(
			final String typeName,
			final Index... indices ) {
		if (indices.length == 0) {
			LOGGER
					.warn(
							"At least one index must be provided.");
			return;
		}
		final Short adapterId = internalAdapterStore
				.getAdapterId(
						typeName);
		if (adapterId == null) {
			LOGGER
					.warn(
							"DataTypeAdapter does not exist for type '" + typeName
									+ "'. Add it using addType(<dataTypeAdapter>) and then add the indices again.");
			return;
		}
		else {
			final InternalDataAdapter<?> adapter = adapterStore
					.getAdapter(
							adapterId);
			if (adapter == null) {
				LOGGER
						.warn(
								"DataTypeAdapter is undefined for type '" + typeName
										+ "'. Add it using addType(<dataTypeAdapter>) and then add the indices again.");
				return;
			}
			final AdapterToIndexMapping existingMapping = indexMappingStore
					.getIndicesForAdapter(
							adapterId);
			if ((existingMapping != null) && (existingMapping.getIndexNames().length > 0)) {
				// reduce the provided indices to only those that don't already
				// exist
				final Index[] newIndices = Arrays
						.stream(
								indices)
						.filter(
								i -> !ArrayUtils
										.contains(
												existingMapping.getIndexNames(),
												i.getName()))
						.toArray(
								size -> new Index[size]);
				if (newIndices.length > 0) {
					LOGGER
							.info(
									"Indices already available for type '" + typeName
											+ "'. Writing existing data to new indices for consistency.");

					internalAddIndices(
							adapter,
							newIndices,
							true);
					try (Writer writer = createWriter(
							adapter,
							newIndices)) {
						try (
								// TODO what about authorizations
								final CloseableIterator it = query(
										QueryBuilder.newBuilder().build())) {
							while (it.hasNext()) {
								writer
										.write(
												it.next());
							}
						}
					}
				}
				else if (LOGGER.isInfoEnabled()) {
					LOGGER
							.info(
									"Indices " + ArrayUtils
											.toString(
													indices)
											+ " already added.");
				}
			}
			else {
				internalAddIndices(
						adapter,
						indices,
						true);
			}
		}
	}

	private void internalAddIndices(
			final InternalDataAdapter<?> adapter,
			final Index[] indices,
			final boolean updateAdapter ) {
		if (adapter.getAdapter() instanceof InitializeWithIndicesDataAdapter) {
			if (((InitializeWithIndicesDataAdapter) adapter.getAdapter()).init(indices) && updateAdapter) {
				adapterStore.removeAdapter(adapter.getAdapterId());
				adapterStore.addAdapter(adapter);
			}
		}
		indexMappingStore.addAdapterIndexMapping(new AdapterToIndexMapping(
				internalAdapterStore.addTypeName(adapter.getTypeName()),
				indices));
		for (final Index index : indices) {
			store(index);
			initOnIndexWriterCreate(
					adapter,
					index);
		}
	}

	@Override
	public <T> void addType(
			final DataTypeAdapter<T> dataTypeAdapter,
			final Index... initialIndices ) {
		// add internal adapter
		final InternalDataAdapter<T> adapter = new InternalDataAdapterWrapper<>(
				dataTypeAdapter,
				internalAdapterStore.addTypeName(dataTypeAdapter.getTypeName()));
		internalAddIndices(
				adapter,
				initialIndices,
				false);
		store(adapter);
	}

	/**
	 * Returns an index writer to perform batched write operations for the given
	 * typename
	 *
	 */
	@Override
	public <T> Writer<T> createWriter(
			final String typeName ) {
		final Short adapterId = internalAdapterStore.getAdapterId(typeName);
		if (adapterId == null) {
			LOGGER.warn("DataTypeAdapter does not exist for type '" + typeName
					+ "'. Add it using addType(<dataTypeAdapter>).");
			return null;
		}
		final InternalDataAdapter<T> adapter = (InternalDataAdapter<T>) adapterStore.getAdapter(adapterId);
		if (adapter == null) {
			LOGGER.warn("DataTypeAdapter is undefined for type '" + typeName
					+ "'. Add it using addType(<dataTypeAdapter>).");
			return null;
		}
		final AdapterToIndexMapping mapping = indexMappingStore.getIndicesForAdapter(adapterId);
		if (mapping == null) {
			LOGGER.warn("No indices for type '" + typeName + "'. Add indices using addIndex(<typename>, <indices>).");
			return null;
		}
		return createWriter(
				adapter,
				mapping.getIndices(indexStore));
	}

	@Override
	public <T> void ingest(
			final URL url,
			final Index... index )
			throws MismatchedIndexToAdapterMapping {
		ingest(
				url,
				null,
				index);
	}

	@Override
	public <T> void ingest(
			final URL url,
			final IngestOptions<T> options,
			final Index... index )
			throws MismatchedIndexToAdapterMapping {
		// TODO Issue #1442 likely need to move logic from LocalFileIngestDriver
		// into core-store
	}

	@Override
	public <P extends Persistable, R, T> R aggregate(
			final AggregationQuery<P, R, T> query ) {
		if (query == null) {
			LOGGER.warn("Aggregation must be defined");
			return null;
		}
		R results = null;

		final Aggregation<P, R, T> aggregation = query.getDataTypeQueryOptions().getAggregation();
		try (CloseableIterator<R> resultsIt = internalQuery(
				query.getQueryConstraints(),
				new BaseQueryOptions(
						query,
						adapterStore,
						internalAdapterStore),
				false)) {
			while (resultsIt.hasNext()) {
				final R next = resultsIt.next();
				if (results == null) {
					results = next;
				}
				else {
					results = aggregation.merge(
							results,
							next);
				}
			}
		}
		if (results == null) {
			aggregation.clearResult();
			return aggregation.getResult();
		}
		else {
			return results;
		}
	}

	protected <R> CloseableIterator<InternalDataStatistics<?, R, ?>> internalQueryStatistics(
			final StatisticsQuery<R> query ) {
		// sanity check, although suing the builders should disallow this type
		// of query
		if ((query.getStatsType() == null) && (query.getExtendedId() != null) && (query.getExtendedId().length() > 0)) {
			LOGGER
					.error(
							"Cannot query by extended ID '" + query.getExtendedId()
									+ "' if statistic type is not provided");
			return new CloseableIterator.Empty<>();
		}
		CloseableIterator<InternalDataStatistics<?, R, ?>> it = null;
		if ((query.getTypeName() != null) && (query.getTypeName().length() > 0)) {
			final Short adapterId = internalAdapterStore
					.getAdapterId(
							query.getTypeName());
			if (adapterId == null) {
				LOGGER
						.error(
								"DataTypeAdapter does not exist for type '" + query.getTypeName() + "'");
				return new CloseableIterator.Empty<>();
			}
			if (query.getStatsType() != null) {
				if ((query.getExtendedId() != null) && (query.getExtendedId().length() > 0)) {

					it = (CloseableIterator) statisticsStore
							.getDataStatistics(
									adapterId,
									query.getExtendedId(),
									query.getStatsType(),
									query.getAuthorizations());
				}
				else {
					it = (CloseableIterator) statisticsStore
							.getDataStatistics(
									adapterId,
									query.getStatsType(),
									query.getAuthorizations());
				}
			}
			else {
				it = (CloseableIterator) statisticsStore
						.getDataStatistics(
								adapterId,
								query.getAuthorizations());
				if (query.getExtendedId() != null) {
					it = new CloseableIteratorWrapper<>(
							it,
							Iterators
									.filter(
											it,
											s -> s
													.getExtendedId()
													.startsWith(
															query.getExtendedId())));
				}
			}
		}
		else {
			if (query.getStatsType() != null) {
				if (query.getExtendedId() != null) {
					it = (CloseableIterator) statisticsStore
							.getDataStatistics(
									query.getExtendedId(),
									query.getStatsType(),
									query.getAuthorizations());
				}
				else {
					it = (CloseableIterator) statisticsStore
							.getDataStatistics(
									query.getStatsType(),
									query.getAuthorizations());
				}
			}
			else {
				it = (CloseableIterator) statisticsStore
						.getAllDataStatistics(
								query.getAuthorizations());
			}
		}
		return it;

	}

	@Override
	public <R> Statistics<R>[] queryStatistics(
			final StatisticsQuery<R> query ) {
		try (CloseableIterator<InternalDataStatistics<?, R, ?>> it = internalQueryStatistics(
				query)) {
			return Streams
					.stream(
							it)
					.map(
							s -> new StatisticsImpl<>(
									s.getResult(),
									s.getType(),
									s.getExtendedId(),
									internalAdapterStore
											.getTypeName(
													s.getAdapterId())))
					.toArray(
							size -> new Statistics[size]);
		}
	}

	@Override
	public <R> R aggregateStatistics(
			final StatisticsQuery<R> query ) {
		if (query.getStatsType() == null) {
			LOGGER
					.error(
							"Statistic Type must be provided for a statistical aggregation");
			return null;
		}
		try (CloseableIterator<InternalDataStatistics<?, R, ?>> it = internalQueryStatistics(
				query)) {
			final Optional<InternalDataStatistics<?, R, ?>> result = Streams
					.stream(
							it)
					.reduce(
							InternalDataStatistics::reduce);
			if (result.isPresent()) {
				return result.get().getResult();
			}
			LOGGER
					.warn(
							"No statistics found matching query criteria for statistical aggregation");
			return null;
		}
	}

	@Override
	public void copyTo(
			final DataStore other ) {

	}

	@Override
	public void copyTo(
			final DataStore other,
			final Query<?> query ) {
		if (query == null) {
			copyTo(other);
		}
		// TODO issue #1440 addresses filling out this method
	}

	@Override
	public void removeIndex(
			final String indexName ) {
		// TODO Auto-generated method stub

	}

	@Override
	public void removeIndex(
			final String typeName,
			final String indexName ) {
		// TODO Auto-generated method stub

	}

	@Override
	public void removeType(
			final String typeName ) {
		// TODO Auto-generated method stub

	}

	@Override
	public void deleteAll() {
		deleteEverything();
	}
}
