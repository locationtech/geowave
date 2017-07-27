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
package mil.nga.giat.geowave.datastore.accumulo;

import java.io.IOException;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.hadoop.mapreduce.InputSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.RowMergingDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.metadata.AdapterIndexMappingStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.AdapterStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.DataStatisticsStoreImpl;
import mil.nga.giat.geowave.core.store.metadata.IndexStoreImpl;
import mil.nga.giat.geowave.core.store.memory.MemoryAdapterStore;
import mil.nga.giat.geowave.core.store.query.DataIdQuery;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.core.store.query.RowIdQuery;
import mil.nga.giat.geowave.core.store.query.aggregate.CountAggregation;
import mil.nga.giat.geowave.core.store.query.aggregate.CountResult;
import mil.nga.giat.geowave.core.store.util.DataAdapterAndIndexCache;
import mil.nga.giat.geowave.datastore.accumulo.cli.config.AccumuloOptions;
import mil.nga.giat.geowave.datastore.accumulo.index.secondary.AccumuloSecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.AccumuloSplitsProvider;
import mil.nga.giat.geowave.datastore.accumulo.operations.AccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.util.AccumuloUtils;
import mil.nga.giat.geowave.mapreduce.BaseMapReduceDataStore;

/**
 * This is the Accumulo implementation of the data store. It requires an
 * AccumuloOperations instance that describes how to connect (read/write data)
 * to Apache Accumulo. It can create default implementations of the IndexStore
 * and AdapterStore based on the operations which will persist configuration
 * information to Accumulo tables, or an implementation of each of these stores
 * can be passed in A DataStore can both ingest and query data based on
 * persisted indices and data adapters. When the data is ingested it is
 * explicitly given an index and a data adapter which is then persisted to be
 * used in subsequent queries.
 */
public class AccumuloDataStore extends
		BaseMapReduceDataStore
{
	private final static Logger LOGGER = LoggerFactory.getLogger(AccumuloDataStore.class);

	private final AccumuloOperations accumuloOperations;
	private final AccumuloOptions accumuloOptions;

	private final AccumuloSplitsProvider splitsProvider = new AccumuloSplitsProvider();

	private static class DupTracker
	{
		HashMap<ByteArrayId, ByteArrayId> idMap;
		HashMap<ByteArrayId, Integer> dupCountMap;

		public DupTracker() {
			idMap = new HashMap<>();
			dupCountMap = new HashMap<>();
		}
	}

	public AccumuloDataStore(
			final AccumuloOperations accumuloOperations,
			final AccumuloOptions accumuloOptions ) {
		this(
				new IndexStoreImpl(
						accumuloOperations,
						accumuloOptions),
				new AdapterStoreImpl(
						accumuloOperations,
						accumuloOptions),
				new DataStatisticsStoreImpl(
						accumuloOperations,
						accumuloOptions),
				new AccumuloSecondaryIndexDataStore(
						accumuloOperations,
						accumuloOptions),
				new AdapterIndexMappingStoreImpl(
						accumuloOperations,
						accumuloOptions),
				accumuloOperations,
				accumuloOptions);
	}

	public AccumuloDataStore(
			final IndexStore indexStore,
			final AdapterStore adapterStore,
			final DataStatisticsStore statisticsStore,
			final AccumuloSecondaryIndexDataStore secondaryIndexDataStore,
			final AdapterIndexMappingStore indexMappingStore,
			final AccumuloOperations accumuloOperations ) {
		this(
				indexStore,
				adapterStore,
				statisticsStore,
				secondaryIndexDataStore,
				indexMappingStore,
				accumuloOperations,
				new AccumuloOptions());
	}

	public AccumuloDataStore(
			final IndexStore indexStore,
			final AdapterStore adapterStore,
			final DataStatisticsStore statisticsStore,
			final AccumuloSecondaryIndexDataStore secondaryIndexDataStore,
			final AdapterIndexMappingStore indexMappingStore,
			final AccumuloOperations accumuloOperations,
			final AccumuloOptions accumuloOptions ) {
		super(
				indexStore,
				adapterStore,
				statisticsStore,
				indexMappingStore,
				secondaryIndexDataStore,
				accumuloOperations,
				accumuloOptions);

		this.accumuloOperations = accumuloOperations;
		this.accumuloOptions = accumuloOptions;
		secondaryIndexDataStore.setDataStore(this);
	}

	@Override
	protected void initOnIndexWriterCreate(
			final DataAdapter adapter,
			final PrimaryIndex index ) {

		final String indexName = index.getId().getString();

		try {
			if (adapter instanceof RowMergingDataAdapter) {
				if (!DataAdapterAndIndexCache.getInstance(
						RowMergingAdapterOptionProvider.ROW_MERGING_ADAPTER_CACHE_ID).add(
						adapter.getAdapterId(),
						indexName)) {
					AccumuloUtils.attachRowMergingIterators(
							((RowMergingDataAdapter<?, ?>) adapter),
							accumuloOperations,
							accumuloOptions,
							indexName);
				}
			}

			final byte[] adapterId = adapter.getAdapterId().getBytes();
			if (accumuloOptions.isUseLocalityGroups() && !accumuloOperations.localityGroupExists(
					indexName,
					adapterId)) {
				accumuloOperations.addLocalityGroup(
						indexName,
						adapterId);
			}
		}
		catch (AccumuloException | TableNotFoundException | AccumuloSecurityException e) {
			LOGGER.error(
					"Unable to determine existence of locality group [" + adapter.getAdapterId().getString() + "]",
					e);
		}

	}

	@Override
	public List<InputSplit> getSplits(
			final DistributableQuery query,
			final QueryOptions queryOptions,
			final AdapterStore adapterStore,
			final DataStatisticsStore statsStore,
			final IndexStore indexStore,
			final Integer minSplits,
			final Integer maxSplits )
			throws IOException,
			InterruptedException {
		return splitsProvider.getSplits(
				accumuloOperations,
				query,
				queryOptions,
				adapterStore,
				statsStore,
				indexStore,
				indexMappingStore,
				minSplits,
				maxSplits);
	}

	@Override
	public boolean delete(
			final QueryOptions queryOptions,
			final Query query ) {
		// Normal mass wipeout
		if (((query == null) || (query instanceof EverythingQuery)) && queryOptions.isAllAdapters()) {
			return deleteEverything();
		}

		// Delete by data ID works best the old way
		if (query instanceof DataIdQuery) {
			return super.delete(
					queryOptions,
					query);
		}

		// Clean up inputs
		final QueryOptions sanitizedQueryOptions = (queryOptions == null) ? new QueryOptions() : queryOptions;
		final Query sanitizedQuery = (query == null) ? new EverythingQuery() : query;

		// Get the index-adapter pairs
		MemoryAdapterStore tempAdapterStore;
		List<Pair<PrimaryIndex, List<DataAdapter<Object>>>> indexAdapterPairs;

		try {
			tempAdapterStore = new MemoryAdapterStore(
					sanitizedQueryOptions.getAdaptersArray(adapterStore));

			indexAdapterPairs = sanitizedQueryOptions.getAdaptersWithMinimalSetOfIndices(
					tempAdapterStore,
					indexMappingStore,
					indexStore);
		}
		catch (final IOException e1) {
			LOGGER.error(
					"Failed to resolve adapter or index for query",
					e1);

			return false;
		}

		// Setup the stats for update
		final DataStoreCallbackManager callbackCache = new DataStoreCallbackManager(
				statisticsStore,
				secondaryIndexDataStore,
				true);

		callbackCache.setPersistStats(accumuloOptions.isPersistDataStatistics());

		final DupTracker dupTracker = new DupTracker();

		// Get BatchDeleters for the query
		CloseableIterator<Object> deleteIt = getBatchDeleters(
				callbackCache,
				tempAdapterStore,
				indexAdapterPairs,
				sanitizedQueryOptions,
				sanitizedQuery,
				dupTracker);

		// Iterate through deleters
		while (deleteIt.hasNext()) {
			deleteIt.next();
		}

		try {
			deleteIt.close();
			callbackCache.close();
		}
		catch (IOException e) {
			LOGGER.error(
					"Failed to close delete iterator",
					e);
		}

		// Have to delete dups by data ID if any
		if (!dupTracker.dupCountMap.isEmpty()) {
			LOGGER.warn("Need to delete duplicates by data ID");
			int dupDataCount = 0;
			int dupFailCount = 0;
			boolean deleteByIdSuccess = true;

			for (ByteArrayId dataId : dupTracker.dupCountMap.keySet()) {
				if (!super.delete(
						new QueryOptions(),
						new DataIdQuery(
								dupTracker.idMap.get(dataId),
								dataId))) {
					deleteByIdSuccess = false;
					dupFailCount++;
				}
				else {
					dupDataCount++;
				}
			}

			if (deleteByIdSuccess) {
				LOGGER.warn("Deleted " + dupDataCount + " duplicates by data ID");
			}
			else {
				LOGGER.warn("Failed to delete " + dupFailCount + " duplicates by data ID");
			}
		}

		boolean countAggregation = (sanitizedQuery instanceof DataIdQuery ? false : true);

		// Count after the delete. Should always be zero
		int undeleted = getCount(
				indexAdapterPairs,
				sanitizedQueryOptions,
				sanitizedQuery,
				countAggregation);

		// Expected outcome
		if (undeleted == 0) {
			return true;
		}

		LOGGER.warn("Accumulo bulk delete failed to remove " + undeleted + " rows");

		// Fallback: delete duplicates via callback using base delete method
		return super.delete(
				queryOptions,
				query);
	}

	protected int getCount(
			List<Pair<PrimaryIndex, List<DataAdapter<Object>>>> indexAdapterPairs,
			final QueryOptions sanitizedQueryOptions,
			final Query sanitizedQuery,
			final boolean countAggregation ) {
		int count = 0;

		for (final Pair<PrimaryIndex, List<DataAdapter<Object>>> indexAdapterPair : indexAdapterPairs) {
			for (DataAdapter dataAdapter : indexAdapterPair.getRight()) {
				QueryOptions countOptions = new QueryOptions(
						sanitizedQueryOptions);
				if (countAggregation) {
					countOptions.setAggregation(
							new CountAggregation(),
							dataAdapter);
				}

				try (final CloseableIterator<Object> it = query(
						countOptions,
						sanitizedQuery)) {
					while (it.hasNext()) {
						if (countAggregation) {
							final CountResult result = ((CountResult) (it.next()));
							if (result != null) {
								count += result.getCount();
							}
						}
						else {
							it.next();
							count++;
						}
					}

					it.close();
				}
				catch (final Exception e) {
					LOGGER.warn(
							"Error running count aggregation",
							e);
					return count;
				}
			}
		}

		return count;
	}

	protected <T> CloseableIterator<T> getBatchDeleters(
			final DataStoreCallbackManager callbackCache,
			final MemoryAdapterStore tempAdapterStore,
			final List<Pair<PrimaryIndex, List<DataAdapter<Object>>>> indexAdapterPairs,
			final QueryOptions sanitizedQueryOptions,
			final Query sanitizedQuery,
			final DupTracker dupTracker ) {
		final boolean DELETE = true; // for readability
		final List<CloseableIterator<Object>> results = new ArrayList<CloseableIterator<Object>>();

		for (final Pair<PrimaryIndex, List<DataAdapter<Object>>> indexAdapterPair : indexAdapterPairs) {
			final List<ByteArrayId> adapterIdsToQuery = new ArrayList<>();
			for (final DataAdapter<Object> adapter : indexAdapterPair.getRight()) {

				// Add scan callback for bookkeeping
				final ScanCallback<Object> callback = new ScanCallback<Object>() {
					@Override
					public void entryScanned(
							final DataStoreEntryInfo entryInfo,
							final Object entry ) {
						updateDupCounts(
								dupTracker,
								adapter.getAdapterId(),
								entryInfo);

						callbackCache.getDeleteCallback(
								(WritableDataAdapter<Object>) adapter,
								indexAdapterPair.getLeft()).entryDeleted(
								entryInfo,
								entry);
					}
				};

				sanitizedQueryOptions.setScanCallback(callback);

				if (sanitizedQuery instanceof RowIdQuery) {
					sanitizedQueryOptions.setLimit(-1);
					results.add(queryRowIds(
							adapter,
							indexAdapterPair.getLeft(),
							((RowIdQuery) sanitizedQuery).getRowIds(),
							null,
							sanitizedQueryOptions,
							tempAdapterStore,
							DELETE));
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
							DELETE));
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
						null,
						sanitizedQueryOptions,
						tempAdapterStore,
						DELETE));
			}
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

	protected void updateDupCounts(
			final DupTracker dupTracker,
			final ByteArrayId adapterId,
			DataStoreEntryInfo entryInfo ) {
		ByteArrayId dataId = new ByteArrayId(
				entryInfo.getDataId());

		for (ByteArrayId rowId : entryInfo.getRowIds()) {
			GeowaveRowId rowData = new GeowaveRowId(
					rowId.getBytes());
			int rowDups = rowData.getNumberOfDuplicates();

			if (rowDups > 0) {
				if (dupTracker.idMap.get(dataId) == null) {
					dupTracker.idMap.put(
							dataId,
							adapterId);
				}

				Integer mapDups = dupTracker.dupCountMap.get(dataId);

				if (mapDups == null) {
					dupTracker.dupCountMap.put(
							dataId,
							rowDups);
				}
				else if (mapDups == 1) {
					dupTracker.dupCountMap.remove(dataId);
				}
				else {
					dupTracker.dupCountMap.put(
							dataId,
							mapDups - 1);
				}
			}
		}

	}
}
