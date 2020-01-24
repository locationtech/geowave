/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.base;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
import org.locationtech.geowave.core.store.base.dataidx.DataIndexUtils;
import org.locationtech.geowave.core.store.callback.DeleteCallbackList;
import org.locationtech.geowave.core.store.callback.DeleteOtherIndicesCallback;
import org.locationtech.geowave.core.store.callback.DuplicateDeletionCallback;
import org.locationtech.geowave.core.store.callback.IngestCallback;
import org.locationtech.geowave.core.store.callback.IngestCallbackList;
import org.locationtech.geowave.core.store.callback.ScanCallback;
import org.locationtech.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import org.locationtech.geowave.core.store.data.visibility.FieldVisibilityCount;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.entities.GeoWaveRowMergingTransform;
import org.locationtech.geowave.core.store.index.IndexMetaDataSet;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.index.writer.IndependentAdapterIndexWriter;
import org.locationtech.geowave.core.store.index.writer.IndexCompositeWriter;
import org.locationtech.geowave.core.store.ingest.BaseDataStoreIngestDriver;
import org.locationtech.geowave.core.store.memory.MemoryPersistentAdapterStore;
import org.locationtech.geowave.core.store.operations.DataIndexReaderParamsBuilder;
import org.locationtech.geowave.core.store.operations.DataStoreOperations;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import org.locationtech.geowave.core.store.operations.MetadataReader;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.core.store.operations.MetadataWriter;
import org.locationtech.geowave.core.store.operations.ReaderParamsBuilder;
import org.locationtech.geowave.core.store.operations.RowReader;
import org.locationtech.geowave.core.store.operations.RowWriter;
import org.locationtech.geowave.core.store.query.aggregate.AdapterAndIndexBasedAggregation;
import org.locationtech.geowave.core.store.query.constraints.AdapterAndIndexBasedQueryConstraints;
import org.locationtech.geowave.core.store.query.constraints.DataIdQuery;
import org.locationtech.geowave.core.store.query.constraints.DataIdRangeQuery;
import org.locationtech.geowave.core.store.query.constraints.EverythingQuery;
import org.locationtech.geowave.core.store.query.constraints.InsertionIdQuery;
import org.locationtech.geowave.core.store.query.constraints.PrefixIdQuery;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.constraints.TypeConstraintQuery;
import org.locationtech.geowave.core.store.query.filter.DedupeFilter;
import org.locationtech.geowave.core.store.util.NativeEntryIteratorWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Iterators;
import com.google.common.collect.Streams;

public class BaseDataStore implements DataStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseDataStore.class);

  protected final IndexStore indexStore;
  protected final PersistentAdapterStore adapterStore;
  protected final DataStatisticsStore statisticsStore;
  protected final AdapterIndexMappingStore indexMappingStore;
  protected final DataStoreOperations baseOperations;
  protected final DataStoreOptions baseOptions;
  protected final InternalAdapterStore internalAdapterStore;

  protected enum DeletionMode {
    DONT_DELETE, DELETE, DELETE_WITH_DUPLICATES;
  }

  public BaseDataStore(
      final IndexStore indexStore,
      final PersistentAdapterStore adapterStore,
      final DataStatisticsStore statisticsStore,
      final AdapterIndexMappingStore indexMappingStore,
      final DataStoreOperations operations,
      final DataStoreOptions options,
      final InternalAdapterStore internalAdapterStore) {
    this.indexStore = indexStore;
    this.adapterStore = adapterStore;
    this.statisticsStore = statisticsStore;
    this.indexMappingStore = indexMappingStore;
    this.internalAdapterStore = internalAdapterStore;
    baseOperations = operations;
    baseOptions = options;
  }

  public void store(final Index index) {
    if (!indexStore.indexExists(index.getName())) {
      indexStore.addIndex(index);
    }
  }

  protected synchronized void store(final InternalDataAdapter<?> adapter) {
    if (!adapterStore.adapterExists(adapter.getAdapterId())) {
      adapterStore.addAdapter(adapter);
    }
  }

  public DataStatisticsStore getStatisticsStore() {
    return statisticsStore;
  }

  public Short getAdapterId(final String typeName) {
    return internalAdapterStore.getAdapterId(typeName);
  }

  private <T> Writer<T> createWriter(
      final InternalDataAdapter<T> adapter,
      final boolean writingOriginalData,
      final Index... indices) {
    final boolean secondaryIndex =
        writingOriginalData
            && baseOptions.isSecondaryIndexing()
            && DataIndexUtils.adapterSupportsDataIndex(adapter);
    final Writer<T>[] writers = new Writer[secondaryIndex ? indices.length + 1 : indices.length];

    int i = 0;
    if (secondaryIndex) {
      final DataStoreCallbackManager callbackManager =
          new DataStoreCallbackManager(statisticsStore, true);
      final List<IngestCallback<T>> callbacks =
          Collections.singletonList(
              callbackManager.getIngestCallback(adapter, DataIndexUtils.DATA_ID_INDEX));

      final IngestCallbackList<T> callbacksList = new IngestCallbackList<>(callbacks);
      writers[i++] =
          createDataIndexWriter(adapter, baseOperations, baseOptions, callbacksList, callbacksList);
    }
    for (final Index index : indices) {
      final DataStoreCallbackManager callbackManager =
          new DataStoreCallbackManager(statisticsStore, i == 0);
      callbackManager.setPersistStats(baseOptions.isPersistDataStatistics());

      final List<IngestCallback<T>> callbacks =
          writingOriginalData
              ? Collections.singletonList(callbackManager.getIngestCallback(adapter, index))
              : Collections.emptyList();

      final IngestCallbackList<T> callbacksList = new IngestCallbackList<>(callbacks);
      writers[i] =
          createIndexWriter(
              adapter,
              index,
              baseOperations,
              baseOptions,
              callbacksList,
              callbacksList);

      if (adapter.getAdapter() instanceof IndexDependentDataAdapter) {
        writers[i] =
            new IndependentAdapterIndexWriter<>(
                (IndexDependentDataAdapter<T>) adapter.getAdapter(),
                index,
                writers[i]);
      }
      i++;
    }
    return new IndexCompositeWriter(writers);
  }

  public <T, R extends GeoWaveRow> CloseableIterator<T> query(
      final Query<T> query,
      final ScanCallback<T, R> scanCallback) {
    return internalQuery(query, DeletionMode.DONT_DELETE, scanCallback);
  }

  @Override
  public <T> CloseableIterator<T> query(final Query<T> query) {
    return internalQuery(query, DeletionMode.DONT_DELETE);
  }

  protected <T> CloseableIterator<T> internalQuery(
      final Query<T> query,
      final DeletionMode delete) {
    return internalQuery(query, delete, null);
  }

  /*
   * Since this general-purpose method crosses multiple adapters, the type of result cannot be
   * assumed.
   *
   * (non-Javadoc)
   *
   * @see org.locationtech.geowave.core.store.DataStore#query(org.locationtech. geowave.
   * core.store.query.QueryOptions, org.locationtech.geowave.core.store.query.Query)
   */
  protected <T> CloseableIterator<T> internalQuery(
      Query<T> query,
      final DeletionMode delete,
      final ScanCallback<T, ?> scanCallback) {
    if (query == null) {
      query = (Query) QueryBuilder.newBuilder().build();
    }
    final BaseQueryOptions queryOptions =
        new BaseQueryOptions(query, adapterStore, internalAdapterStore, scanCallback);
    return internalQuery(query.getQueryConstraints(), queryOptions, delete);
  }

  protected <T> CloseableIterator<T> internalQuery(
      final QueryConstraints constraints,
      final BaseQueryOptions queryOptions,
      final DeletionMode deleteMode) {
    // Note: The DeletionMode option is provided to avoid recursively
    // adding DuplicateDeletionCallbacks when actual duplicates are removed
    // via the DuplicateDeletionCallback. The callback should only be added
    // during the initial deletion query.
    final boolean delete =
        ((deleteMode == DeletionMode.DELETE)
            || (deleteMode == DeletionMode.DELETE_WITH_DUPLICATES));

    final List<CloseableIterator<Object>> results = new ArrayList<>();

    // If CQL filter is set
    if (constraints instanceof TypeConstraintQuery) {
      final String constraintTypeName = ((TypeConstraintQuery) constraints).getTypeName();

      if ((queryOptions.getAdapterIds() == null) || (queryOptions.getAdapterIds().length == 0)) {
        queryOptions.setAdapterId(internalAdapterStore.getAdapterId(constraintTypeName));
      } else if (queryOptions.getAdapterIds().length == 1) {
        final Short adapterId = internalAdapterStore.getAdapterId(constraintTypeName);
        if ((adapterId == null) || (queryOptions.getAdapterIds()[0] != adapterId.shortValue())) {
          LOGGER.error("Constraint Query Type name does not match Query Options Type Name");
          throw new RuntimeException(
              "Constraint Query Type name does not match Query Options Type Name");
        }
      } else {
        // Throw exception when QueryOptions has more than one adapter
        // and CQL Adapter is set.
        LOGGER.error("Constraint Query Type name does not match Query Options Type Name");
        throw new RuntimeException(
            "Constraint Query Type name does not match Query Options Type Name");
      }
    }

    final QueryConstraints sanitizedConstraints =
        (constraints == null) ? new EverythingQuery() : constraints;
    final List<DataStoreCallbackManager> deleteCallbacks = new ArrayList<>();
    final Map<Short, Set<ByteArray>> dataIdsToDelete;
    if (DeletionMode.DELETE_WITH_DUPLICATES.equals(deleteMode)
        && (baseOptions.isSecondaryIndexing())) {
      dataIdsToDelete = new HashMap<>();
    } else {
      dataIdsToDelete = null;
    }
    final boolean dataIdIndexIsBest =
        baseOptions.isSecondaryIndexing()
            && ((sanitizedConstraints instanceof DataIdQuery)
                || (sanitizedConstraints instanceof DataIdRangeQuery)
                || (sanitizedConstraints instanceof EverythingQuery));
    if (!delete && dataIdIndexIsBest) {
      try {
        // just grab the values directly from the Data Index
        InternalDataAdapter<?>[] adapters = queryOptions.getAdaptersArray(adapterStore);
        if (!queryOptions.isAllIndices()) {
          final Set<Short> adapterIds =
              new HashSet<>(
                  Arrays.asList(
                      ArrayUtils.toObject(
                          queryOptions.getValidAdapterIds(
                              internalAdapterStore,
                              indexMappingStore))));
          adapters =
              Arrays.stream(adapters).filter(a -> adapterIds.contains(a.getAdapterId())).toArray(
                  i -> new InternalDataAdapter<?>[i]);
        }
        // TODO test whether aggregations work in this case
        for (final InternalDataAdapter<?> adapter : adapters) {

          RowReader<GeoWaveRow> rowReader;
          if (sanitizedConstraints instanceof DataIdQuery) {
            rowReader =
                DataIndexUtils.getRowReader(
                    baseOperations,
                    adapterStore,
                    internalAdapterStore,
                    queryOptions.getFieldIdsAdapterPair(),
                    queryOptions.getAggregation(),
                    queryOptions.getAuthorizations(),
                    adapter.getAdapterId(),
                    ((DataIdQuery) sanitizedConstraints).getDataIds());
          } else if (sanitizedConstraints instanceof DataIdRangeQuery) {
            rowReader =
                DataIndexUtils.getRowReader(
                    baseOperations,
                    adapterStore,
                    internalAdapterStore,
                    queryOptions.getFieldIdsAdapterPair(),
                    queryOptions.getAggregation(),
                    queryOptions.getAuthorizations(),
                    adapter.getAdapterId(),
                    ((DataIdRangeQuery) sanitizedConstraints).getStartDataIdInclusive(),
                    ((DataIdRangeQuery) sanitizedConstraints).getEndDataIdInclusive());
          } else {
            rowReader =
                DataIndexUtils.getRowReader(
                    baseOperations,
                    adapterStore,
                    internalAdapterStore,
                    queryOptions.getFieldIdsAdapterPair(),
                    queryOptions.getAggregation(),
                    queryOptions.getAuthorizations(),
                    adapter.getAdapterId());
          }
          results.add(
              new CloseableIteratorWrapper(
                  rowReader,
                  new NativeEntryIteratorWrapper(
                      adapterStore,
                      DataIndexUtils.DATA_ID_INDEX,
                      rowReader,
                      null,
                      queryOptions.getScanCallback(),
                      BaseDataStoreUtils.getFieldBitmask(
                          queryOptions.getFieldIdsAdapterPair(),
                          DataIndexUtils.DATA_ID_INDEX),
                      queryOptions.getMaxResolutionSubsamplingPerDimension(),
                      !BaseDataStoreUtils.isCommonIndexAggregation(queryOptions.getAggregation()),
                      null)));
        }
        if (BaseDataStoreUtils.isAggregation(queryOptions.getAggregation())) {
          return BaseDataStoreUtils.aggregate(new CloseableIteratorWrapper(new Closeable() {
            @Override
            public void close() throws IOException {
              for (final CloseableIterator<Object> result : results) {
                result.close();
              }
            }
          }, Iterators.concat(results.iterator())),
              (Aggregation) queryOptions.getAggregation().getRight());
        }
      } catch (final IOException e1) {
        LOGGER.error("Failed to resolve adapter or index for query", e1);
      }
    } else {
      final boolean isConstraintsAdapterIndexSpecific =
          sanitizedConstraints instanceof AdapterAndIndexBasedQueryConstraints;
      final boolean isAggregationAdapterIndexSpecific =
          (queryOptions.getAggregation() != null)
              && (queryOptions.getAggregation().getRight() instanceof AdapterAndIndexBasedAggregation);

      // all queries will use the same instance of the dedupe filter for
      // client side filtering because the filter needs to be applied across
      // indices
      final DedupeFilter filter = new DedupeFilter();
      MemoryPersistentAdapterStore tempAdapterStore;

      tempAdapterStore =
          new MemoryPersistentAdapterStore(queryOptions.getAdaptersArray(adapterStore));
      // keep a list of adapters that have been queried, to only load an
      // adapter to be queried once
      final Set<Short> queriedAdapters = new HashSet<>();
      // if its an ordered constraints then it is dependent on the index selected, if its
      // secondary indexing its inefficient to delete by constraints
      final boolean deleteAllIndicesByConstraints =
          ((delete
              && ((constraints == null) || !constraints.indexMustBeSpecified())
              && !baseOptions.isSecondaryIndexing()));
      final List<Pair<Index, List<InternalDataAdapter<?>>>> indexAdapterPairList =
          (deleteAllIndicesByConstraints)
              ? queryOptions.getIndicesForAdapters(tempAdapterStore, indexMappingStore, indexStore)
              : queryOptions.getBestQueryIndices(
                  tempAdapterStore,
                  indexMappingStore,
                  indexStore,
                  sanitizedConstraints);
      Map<Short, List<Index>> additionalIndicesToDelete = null;
      if (DeletionMode.DELETE_WITH_DUPLICATES.equals(deleteMode)
          && !deleteAllIndicesByConstraints) {
        additionalIndicesToDelete = new HashMap<>();
        // we have to make sure to delete from the other indices if they exist
        final List<Pair<Index, List<InternalDataAdapter<?>>>> allIndices =
            queryOptions.getIndicesForAdapters(tempAdapterStore, indexMappingStore, indexStore);
        for (final Pair<Index, List<InternalDataAdapter<?>>> allPair : allIndices) {
          for (final Pair<Index, List<InternalDataAdapter<?>>> constraintPair : indexAdapterPairList) {
            if (((constraintPair.getKey() == null) && (allPair.getKey() == null))
                || constraintPair.getKey().equals(allPair.getKey())) {
              allPair.getRight().removeAll(constraintPair.getRight());
              break;
            }
          }
          for (final InternalDataAdapter<?> adapter : allPair.getRight()) {
            List<Index> indices = additionalIndicesToDelete.get(adapter.getAdapterId());
            if (indices == null) {
              indices = new ArrayList<>();
              additionalIndicesToDelete.put(adapter.getAdapterId(), indices);
            }
            indices.add(allPair.getLeft());
          }
        }
      }
      final Pair<InternalDataAdapter<?>, Aggregation<?, ?, ?>> aggregation =
          queryOptions.getAggregation();
      for (final Pair<Index, List<InternalDataAdapter<?>>> indexAdapterPair : indexAdapterPairList) {
        if (indexAdapterPair.getKey() == null) {
          // this indicates there are no indices that satisfy this set of adapters
          // we can still satisfy it with the data ID index if its available for certain types of
          // queries
          if (dataIdIndexIsBest) {
            // and in fact this must be a deletion operation otherwise it would have been caught in
            // prior logic for !delete
            for (final InternalDataAdapter adapter : indexAdapterPair.getRight()) {
              // this must be a data index only adapter, just worry about updating statistics and
              // not other indices or duplicates
              final ScanCallback callback = queryOptions.getScanCallback();
              ScanCallback scanCallback = callback;
              if (baseOptions.isPersistDataStatistics()) {
                final DataStoreCallbackManager callbackCache =
                    new DataStoreCallbackManager(
                        statisticsStore,
                        queriedAdapters.add(adapter.getAdapterId()));
                deleteCallbacks.add(callbackCache);
                scanCallback = new ScanCallback<Object, GeoWaveRow>() {

                  @Override
                  public void entryScanned(final Object entry, final GeoWaveRow row) {
                    if (callback != null) {
                      callback.entryScanned(entry, row);
                    }
                    callbackCache.getDeleteCallback(adapter, null).entryDeleted(entry, row);
                  }
                };
              }
              if (sanitizedConstraints instanceof DataIdQuery) {
                DataIndexUtils.delete(
                    baseOperations,
                    adapterStore,
                    internalAdapterStore,
                    queryOptions.getFieldIdsAdapterPair(),
                    queryOptions.getAggregation(),
                    queryOptions.getAuthorizations(),
                    scanCallback,
                    adapter.getAdapterId(),
                    ((DataIdQuery) sanitizedConstraints).getDataIds());
              } else if (sanitizedConstraints instanceof DataIdRangeQuery) {
                DataIndexUtils.delete(
                    baseOperations,
                    adapterStore,
                    internalAdapterStore,
                    queryOptions.getFieldIdsAdapterPair(),
                    queryOptions.getAggregation(),
                    queryOptions.getAuthorizations(),
                    scanCallback,
                    adapter.getAdapterId(),
                    ((DataIdRangeQuery) sanitizedConstraints).getStartDataIdInclusive(),
                    ((DataIdRangeQuery) sanitizedConstraints).getEndDataIdInclusive());
              } else {
                DataIndexUtils.delete(
                    baseOperations,
                    adapterStore,
                    internalAdapterStore,
                    queryOptions.getFieldIdsAdapterPair(),
                    queryOptions.getAggregation(),
                    queryOptions.getAuthorizations(),
                    scanCallback,
                    adapter.getAdapterId());
              }
            }
          } else {
            final String[] typeNames =
                indexAdapterPair.getRight().stream().map(a -> a.getAdapter().getTypeName()).toArray(
                    k -> new String[k]);
            LOGGER.warn(
                "Data types '"
                    + ArrayUtils.toString(typeNames)
                    + "' do not have an index that satisfies the query");
          }

          continue;
        }
        final List<Short> adapterIdsToQuery = new ArrayList<>();
        // this only needs to be done once per index, not once per
        // adapter
        boolean queriedAllAdaptersByPrefix = false;
        // maintain a set of data IDs if deleting using secondary indexing
        for (final InternalDataAdapter adapter : indexAdapterPair.getRight()) {
          if (delete) {
            final DataStoreCallbackManager callbackCache =
                new DataStoreCallbackManager(
                    statisticsStore,
                    queriedAdapters.add(adapter.getAdapterId()));

            // the duplicate deletion callback utilizes insertion id
            // query to clean up the dupes, in this case we do not
            // want the stats to change
            if (!(constraints instanceof InsertionIdQuery)) {
              callbackCache.setPersistStats(baseOptions.isPersistDataStatistics());
            } else {
              callbackCache.setPersistStats(false);
            }

            deleteCallbacks.add(callbackCache);
            final ScanCallback callback = queryOptions.getScanCallback();

            final Index index = indexAdapterPair.getLeft();
            if (deleteMode == DeletionMode.DELETE_WITH_DUPLICATES) {
              final DeleteCallbackList<T, GeoWaveRow> delList =
                  (DeleteCallbackList<T, GeoWaveRow>) callbackCache.getDeleteCallback(
                      adapter,
                      index);

              final DuplicateDeletionCallback<T> dupDeletionCallback =
                  new DuplicateDeletionCallback<>(this, adapter, index);
              delList.addCallback(dupDeletionCallback);
              if ((additionalIndicesToDelete != null)
                  && (additionalIndicesToDelete.get(adapter.getAdapterId()) != null)) {
                delList.addCallback(
                    new DeleteOtherIndicesCallback<>(
                        baseOperations,
                        adapter,
                        additionalIndicesToDelete.get(adapter.getAdapterId()),
                        adapterStore,
                        internalAdapterStore,
                        queryOptions.getAuthorizations()));
              }
            }
            final Map<Short, Set<ByteArray>> internalDataIdsToDelete = dataIdsToDelete;
            queryOptions.setScanCallback(new ScanCallback<Object, GeoWaveRow>() {

              @Override
              public void entryScanned(final Object entry, final GeoWaveRow row) {
                if (callback != null) {
                  callback.entryScanned(entry, row);
                }
                if (internalDataIdsToDelete != null) {
                  final ByteArray dataId = new ByteArray(row.getDataId());
                  Set<ByteArray> currentDataIdsToDelete =
                      internalDataIdsToDelete.get(row.getAdapterId());
                  if (currentDataIdsToDelete == null) {
                    currentDataIdsToDelete = Collections.synchronizedSet(new HashSet<>());
                    internalDataIdsToDelete.put(row.getAdapterId(), currentDataIdsToDelete);
                  }
                  currentDataIdsToDelete.add(dataId);
                }
                callbackCache.getDeleteCallback(adapter, index).entryDeleted(entry, row);
              }
            });
          }
          QueryConstraints adapterIndexConstraints;
          if (isConstraintsAdapterIndexSpecific) {
            adapterIndexConstraints =
                ((AdapterAndIndexBasedQueryConstraints) sanitizedConstraints).createQueryConstraints(
                    adapter,
                    indexAdapterPair.getLeft());
            if (adapterIndexConstraints == null) {
              continue;
            }
          } else {
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
            results.add(
                queryInsertionId(
                    adapter,
                    indexAdapterPair.getLeft(),
                    (InsertionIdQuery) adapterIndexConstraints,
                    filter,
                    queryOptions,
                    tempAdapterStore,
                    delete));
            continue;
          } else if (adapterIndexConstraints instanceof PrefixIdQuery) {
            if (!queriedAllAdaptersByPrefix) {
              final PrefixIdQuery prefixIdQuery = (PrefixIdQuery) adapterIndexConstraints;
              results.add(
                  queryRowPrefix(
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
          } else if (isConstraintsAdapterIndexSpecific || isAggregationAdapterIndexSpecific) {
            // can't query multiple adapters in the same scan
            results.add(
                queryConstraints(
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
          results.add(
              queryConstraints(
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
    return new CloseableIteratorWrapper<>(new Closeable() {

      @Override
      public void close() throws IOException {
        for (final CloseableIterator<Object> result : results) {
          result.close();
        }
        for (final DataStoreCallbackManager c : deleteCallbacks) {
          c.close();
        }
        if ((dataIdsToDelete != null) && !dataIdsToDelete.isEmpty()) {
          if (baseOptions.isSecondaryIndexing()) {
            deleteFromDataIndex(dataIdsToDelete, queryOptions.getAuthorizations());
          }

        }
      }

    }, Iterators.concat(new CastIterator<T>(results.iterator())));
  }

  protected void deleteFromDataIndex(
      final Map<Short, Set<ByteArray>> dataIdsToDelete,
      final String... authorizations) {
    for (final Entry<Short, Set<ByteArray>> entry : dataIdsToDelete.entrySet()) {
      final Short adapterId = entry.getKey();
      baseOperations.delete(
          new DataIndexReaderParamsBuilder<>(
              adapterStore,
              internalAdapterStore).additionalAuthorizations(
                  authorizations).isAuthorizationsLimiting(false).adapterId(adapterId).dataIds(
                      entry.getValue().stream().map(b -> b.getBytes()).toArray(
                          i -> new byte[i][])).build());
    }
  }


  private boolean isAllAdapters(final String[] typeNames) {
    return Arrays.equals(internalAdapterStore.getTypeNames(), typeNames);
  }

  private Short[] getAdaptersForIndex(final String indexName) {
    final ArrayList<Short> markedAdapters = new ArrayList<>();
    // remove the given index for all types
    try (final CloseableIterator<InternalDataAdapter<?>> it = adapterStore.getAdapters()) {

      while (it.hasNext()) {

        final InternalDataAdapter<?> dataAdapter = it.next();
        final AdapterToIndexMapping adapterIndexMap =
            indexMappingStore.getIndicesForAdapter(dataAdapter.getAdapterId());
        final String[] indexNames = adapterIndexMap.getIndexNames();
        for (int i = 0; i < indexNames.length; i++) {
          if (indexNames[i].equals(indexName)) {
            // check if it is the only index for the current adapter
            if (indexNames.length == 1) {
              throw new IllegalStateException(
                  "Index removal failed. Adapters require at least one index.");
            } else {
              // mark the index for removal
              markedAdapters.add(adapterIndexMap.getAdapterId());
            }
          }
        }
      }
    }
    final Short[] adapters = new Short[markedAdapters.size()];
    return markedAdapters.toArray(adapters);
  }

  public <T> boolean delete(
      Query<T> query,
      final ScanCallback<T, ?> scanCallback,
      final boolean deleteDuplicates) {
    if (query == null) {
      query = (Query) QueryBuilder.newBuilder().build();
    }
    if (((query.getQueryConstraints() == null)
        || (query.getQueryConstraints() instanceof EverythingQuery))) {
      if ((query.getDataTypeQueryOptions().getTypeNames() == null)
          || (query.getDataTypeQueryOptions().getTypeNames().length == 0)
          || isAllAdapters(query.getDataTypeQueryOptions().getTypeNames())) {
        // TODO what about authorizations here?
        return deleteEverything();
      } else {
        try {
          final BaseQueryOptions sanitizedQueryOptions =
              new BaseQueryOptions(query, adapterStore, internalAdapterStore);
          for (final Pair<Index, List<InternalDataAdapter<?>>> indexAdapterPair : sanitizedQueryOptions.getIndicesForAdapters(
              adapterStore,
              indexMappingStore,
              indexStore)) {
            if (indexAdapterPair.getLeft() != null) {
              for (final InternalDataAdapter adapter : indexAdapterPair.getRight()) {
                try {
                  deleteEntries(
                      adapter,
                      indexAdapterPair.getLeft(),
                      query.getCommonQueryOptions().getAuthorizations());
                } catch (final IOException e) {
                  LOGGER.warn("Unable to delete by adapter", e);
                  return false;
                }
              }
            }
          }
          if (baseOptions.isSecondaryIndexing()) {
            for (final InternalDataAdapter adapter : sanitizedQueryOptions.getAdaptersArray(
                adapterStore)) {
              deleteEntries(
                  adapter,
                  DataIndexUtils.DATA_ID_INDEX,
                  query.getCommonQueryOptions().getAuthorizations());
            }
          }
        } catch (final IOException e) {
          LOGGER.warn("Unable to get adapters to delete", e);
          return false;
        }
      }
    } else {
      try (CloseableIterator<?> dataIt =
          internalQuery(
              query,
              deleteDuplicates ? DeletionMode.DELETE_WITH_DUPLICATES : DeletionMode.DELETE,
              scanCallback)) {
        while (dataIt.hasNext()) {
          dataIt.next();
        }
      }
    }

    return true;
  }

  @Override
  public <T> boolean delete(final Query<T> query) {
    return delete(query, null, true);
  }

  public <T> boolean delete(final Query<T> query, final ScanCallback<T, ?> scanCallback) {
    return delete(query, scanCallback, true);
  }

  public <T> boolean delete(final Query<T> query, final boolean deleteDuplicates) {
    return delete(query, null, deleteDuplicates);
  }

  protected boolean deleteEverything() {
    try {
      indexStore.removeAll();
      adapterStore.removeAll();
      statisticsStore.removeAll();
      internalAdapterStore.removeAll();
      indexMappingStore.removeAll();

      baseOperations.deleteAll();
      return true;
    } catch (final Exception e) {
      LOGGER.error("Unable to delete all tables", e);
    }
    return false;
  }

  private <T> void deleteEntries(
      final InternalDataAdapter<T> adapter,
      final Index index,
      final String... additionalAuthorizations) throws IOException {
    statisticsStore.removeAllStatistics(adapter.getAdapterId(), additionalAuthorizations);

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
      final boolean delete) {
    final BaseConstraintsQuery constraintsQuery =
        new BaseConstraintsQuery(
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
            DataIndexUtils.getDataIndexRetrieval(
                baseOperations,
                adapterStore,
                internalAdapterStore,
                index,
                sanitizedQueryOptions.getFieldIdsAdapterPair(),
                sanitizedQueryOptions.getAggregation(),
                sanitizedQueryOptions.getAuthorizations(),
                baseOptions.getDataIndexBatchSize()),
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
      final byte[] partitionKey,
      final byte[] sortPrefix,
      final BaseQueryOptions sanitizedQueryOptions,
      final List<InternalDataAdapter<?>> adapters,
      final PersistentAdapterStore tempAdapterStore,
      final boolean delete) {
    final Set<Short> adapterIds =
        adapters.stream().map(a -> a.getAdapterId()).collect(Collectors.toSet());
    final BaseRowPrefixQuery<Object> prefixQuery =
        new BaseRowPrefixQuery<>(
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
            DataIndexUtils.getDataIndexRetrieval(
                baseOperations,
                adapterStore,
                internalAdapterStore,
                index,
                sanitizedQueryOptions.getFieldIdsAdapterPair(),
                sanitizedQueryOptions.getAggregation(),
                sanitizedQueryOptions.getAuthorizations(),
                baseOptions.getDataIndexBatchSize()),
            sanitizedQueryOptions.getAuthorizations());

    return prefixQuery.query(
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
      final boolean delete) {
    final DifferingFieldVisibilityEntryCount differingVisibilityCounts =
        DifferingFieldVisibilityEntryCount.getVisibilityCounts(
            index,
            Collections.singletonList(adapter.getAdapterId()),
            statisticsStore,
            sanitizedQueryOptions.getAuthorizations());
    final FieldVisibilityCount visibilityCounts =
        FieldVisibilityCount.getVisibilityCounts(
            index,
            Collections.singletonList(adapter.getAdapterId()),
            statisticsStore,
            sanitizedQueryOptions.getAuthorizations());
    final BaseInsertionIdQuery<Object> q =
        new BaseInsertionIdQuery<>(
            adapter,
            index,
            query,
            (ScanCallback<Object, ?>) sanitizedQueryOptions.getScanCallback(),
            filter,
            differingVisibilityCounts,
            visibilityCounts,
            DataIndexUtils.getDataIndexRetrieval(
                baseOperations,
                adapterStore,
                internalAdapterStore,
                index,
                sanitizedQueryOptions.getFieldIdsAdapterPair(),
                sanitizedQueryOptions.getAggregation(),
                sanitizedQueryOptions.getAuthorizations(),
                baseOptions.getDataIndexBatchSize()),
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

  protected <T> Writer<T> createDataIndexWriter(
      final InternalDataAdapter<T> adapter,
      final DataStoreOperations baseOperations,
      final DataStoreOptions baseOptions,
      final IngestCallback<T> callback,
      final Closeable closable) {
    return new BaseDataIndexWriter<>(adapter, baseOperations, baseOptions, callback, closable);
  }

  protected <T> Writer<T> createIndexWriter(
      final InternalDataAdapter<T> adapter,
      final Index index,
      final DataStoreOperations baseOperations,
      final DataStoreOptions baseOptions,
      final IngestCallback<T> callback,
      final Closeable closable) {
    return new BaseIndexWriter<>(adapter, index, baseOperations, baseOptions, callback, closable);
  }

  protected <T> void initOnIndexWriterCreate(
      final InternalDataAdapter<T> adapter,
      final Index index) {}

  /**
   * Get all the adapters that have been used within this data store
   *
   * @return An array of the adapters used within this datastore.
   */
  @Override
  public DataTypeAdapter<?>[] getTypes() {
    try (CloseableIterator<InternalDataAdapter<?>> it = adapterStore.getAdapters()) {
      return Iterators.toArray(
          Iterators.transform(it, a -> (DataTypeAdapter<?>) a.getAdapter()),
          DataTypeAdapter.class);
    }
  }

  @Override
  public Index[] getIndices() {
    return getIndices(null);
  }

  @Override
  public Index[] getIndices(final String typeName) {
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
  public void addIndex(final String typeName, final Index... indices) {
    if (indices.length == 0) {
      LOGGER.warn("At least one index must be provided.");
      return;
    }
    final Short adapterId = internalAdapterStore.getAdapterId(typeName);
    if (adapterId == null) {
      LOGGER.warn(
          "DataTypeAdapter does not exist for type '"
              + typeName
              + "'. Add it using addType(<dataTypeAdapter>) and then add the indices again.");
      return;
    } else {
      final InternalDataAdapter<?> adapter = adapterStore.getAdapter(adapterId);
      if (adapter == null) {
        LOGGER.warn(
            "DataTypeAdapter is undefined for type '"
                + typeName
                + "'. Add it using addType(<dataTypeAdapter>) and then add the indices again.");
        return;
      }
      final AdapterToIndexMapping existingMapping =
          indexMappingStore.getIndicesForAdapter(adapterId);
      if ((existingMapping != null) && (existingMapping.getIndexNames().length > 0)) {
        // reduce the provided indices to only those that don't already
        // exist
        final Index[] newIndices =
            Arrays.stream(indices).filter(
                i -> !ArrayUtils.contains(existingMapping.getIndexNames(), i.getName())).toArray(
                    size -> new Index[size]);
        if (newIndices.length > 0) {
          LOGGER.info(
              "Indices already available for type '"
                  + typeName
                  + "'. Writing existing data to new indices for consistency.");

          internalAddIndices(adapter, newIndices, true);
          try (Writer writer = createWriter(adapter, false, newIndices)) {
            try (
                // TODO what about authorizations
                final CloseableIterator it =
                    query(QueryBuilder.newBuilder().addTypeName(adapter.getTypeName()).build())) {
              while (it.hasNext()) {
                writer.write(it.next());
              }
            }
          }
        } else if (LOGGER.isInfoEnabled()) {
          LOGGER.info("Indices " + ArrayUtils.toString(indices) + " already added.");
        }
      } else {
        internalAddIndices(adapter, indices, true);
      }
    }
  }

  private void internalAddIndices(
      final InternalDataAdapter<?> adapter,
      final Index[] indices,
      final boolean updateAdapter) {
    if (adapter.getAdapter() instanceof InitializeWithIndicesDataAdapter) {
      if (((InitializeWithIndicesDataAdapter) adapter.getAdapter()).init(indices)
          && updateAdapter) {
        adapterStore.removeAdapter(adapter.getAdapterId());
        adapterStore.addAdapter(adapter);
      }
    }
    indexMappingStore.addAdapterIndexMapping(
        new AdapterToIndexMapping(
            internalAdapterStore.addTypeName(adapter.getTypeName()),
            indices));
    for (final Index index : indices) {
      store(index);
      initOnIndexWriterCreate(adapter, index);
    }
  }

  @Override
  public <T> void addType(final DataTypeAdapter<T> dataTypeAdapter, final Index... initialIndices) {
    // add internal adapter
    final InternalDataAdapter<T> adapter =
        new InternalDataAdapterWrapper<>(
            dataTypeAdapter,
            internalAdapterStore.addTypeName(dataTypeAdapter.getTypeName()));
    final Index[] initialIndicesUnique =
        Arrays.stream(initialIndices).distinct().toArray(size -> new Index[size]);
    internalAddIndices(adapter, initialIndicesUnique, false);
    store(adapter);
  }

  /** Returns an index writer to perform batched write operations for the given typename */
  @Override
  public <T> Writer<T> createWriter(final String typeName) {
    final Short adapterId = internalAdapterStore.getAdapterId(typeName);
    if (adapterId == null) {
      LOGGER.warn(
          "DataTypeAdapter does not exist for type '"
              + typeName
              + "'. Add it using addType(<dataTypeAdapter>).");
      return null;
    }
    final InternalDataAdapter<T> adapter =
        (InternalDataAdapter<T>) adapterStore.getAdapter(adapterId);
    if (adapter == null) {
      LOGGER.warn(
          "DataTypeAdapter is undefined for type '"
              + typeName
              + "'. Add it using addType(<dataTypeAdapter>).");
      return null;
    }
    final AdapterToIndexMapping mapping = indexMappingStore.getIndicesForAdapter(adapterId);
    if (mapping == null) {
      LOGGER.warn(
          "No indices for type '"
              + typeName
              + "'. Add indices using addIndex(<typename>, <indices>).");
      return null;
    }
    return createWriter(adapter, true, mapping.getIndices(indexStore));
  }

  @Override
  public <T> void ingest(final String inputPath, final Index... index) {
    ingest(inputPath, null, index);
  }

  @Override
  public <T> void ingest(
      final String inputPath,
      final IngestOptions<T> options,
      final Index... index) {
    // Driver
    final BaseDataStoreIngestDriver driver =
        new BaseDataStoreIngestDriver(
            this,
            options == null ? IngestOptions.newBuilder().build() : options,
            index);

    // Execute
    if (!driver.runOperation(inputPath, null)) {
      throw new RuntimeException("Ingest failed to execute");
    }
  }

  @Override
  public <P extends Persistable, R, T> R aggregate(final AggregationQuery<P, R, T> query) {
    if (query == null) {
      LOGGER.warn("Aggregation must be defined");
      return null;
    }
    R results = null;

    final Aggregation<P, R, T> aggregation = query.getDataTypeQueryOptions().getAggregation();
    try (CloseableIterator<R> resultsIt =
        internalQuery(
            query.getQueryConstraints(),
            new BaseQueryOptions(query, adapterStore, internalAdapterStore),
            DeletionMode.DONT_DELETE)) {
      while (resultsIt.hasNext()) {
        final R next = resultsIt.next();
        if (results == null) {
          results = next;
        } else {
          results = aggregation.merge(results, next);
        }
      }
    }
    if (results == null) {
      aggregation.clearResult();
      return aggregation.getResult();
    } else {
      return results;
    }
  }

  protected <R> CloseableIterator<InternalDataStatistics<?, R, ?>> internalQueryStatistics(
      final StatisticsQuery<R> query) {
    // sanity check, although using the builders should disallow this type
    // of query
    if ((query.getStatsType() == null)
        && (query.getExtendedId() != null)
        && (query.getExtendedId().length() > 0)) {
      LOGGER.error(
          "Cannot query by extended ID '"
              + query.getExtendedId()
              + "' if statistic type is not provided");
      return new CloseableIterator.Empty<>();
    }
    CloseableIterator<InternalDataStatistics<?, R, ?>> it = null;
    if ((query.getTypeName() != null) && (query.getTypeName().length() > 0)) {
      final Short adapterId = internalAdapterStore.getAdapterId(query.getTypeName());
      if (adapterId == null) {
        LOGGER.error("DataTypeAdapter does not exist for type '" + query.getTypeName() + "'");
        return new CloseableIterator.Empty<>();
      }
      if (query.getStatsType() != null) {
        if ((query.getExtendedId() != null) && (query.getExtendedId().length() > 0)) {

          it =
              (CloseableIterator) statisticsStore.getDataStatistics(
                  adapterId,
                  query.getExtendedId(),
                  query.getStatsType(),
                  query.getAuthorizations());
        } else {
          it =
              (CloseableIterator) statisticsStore.getDataStatistics(
                  adapterId,
                  query.getStatsType(),
                  query.getAuthorizations());
        }
      } else {
        it =
            (CloseableIterator) statisticsStore.getDataStatistics(
                adapterId,
                query.getAuthorizations());
        if (query.getExtendedId() != null) {
          it =
              new CloseableIteratorWrapper<>(
                  it,
                  Iterators.filter(it, s -> s.getExtendedId().startsWith(query.getExtendedId())));
        }
      }
    } else {
      if (query.getStatsType() != null) {
        if (query.getExtendedId() != null) {
          it =
              (CloseableIterator) statisticsStore.getDataStatistics(
                  query.getExtendedId(),
                  query.getStatsType(),
                  query.getAuthorizations());
        } else {
          it =
              (CloseableIterator) statisticsStore.getDataStatistics(
                  query.getStatsType(),
                  query.getAuthorizations());
        }
      } else {
        it = (CloseableIterator) statisticsStore.getAllDataStatistics(query.getAuthorizations());
      }
    }
    return it;
  }

  @Override
  public <R> Statistics<R>[] queryStatistics(final StatisticsQuery<R> query) {
    try (CloseableIterator<InternalDataStatistics<?, R, ?>> it = internalQueryStatistics(query)) {
      return Streams.stream(it).map(
          s -> new StatisticsImpl<>(
              s.getResult(),
              s.getType(),
              s.getExtendedId(),
              internalAdapterStore.getTypeName(s.getAdapterId()))).toArray(
                  size -> new Statistics[size]);
    }
  }

  @Override
  public <R> R aggregateStatistics(final StatisticsQuery<R> query) {
    if (query.getStatsType() == null) {
      LOGGER.error("Statistic Type must be provided for a statistical aggregation");
      return null;
    }
    try (CloseableIterator<InternalDataStatistics<?, R, ?>> it = internalQueryStatistics(query)) {
      final Optional<InternalDataStatistics<?, R, ?>> result =
          Streams.stream(it).reduce(InternalDataStatistics::reduce);
      if (result.isPresent()) {
        return result.get().getResult();
      }
      LOGGER.warn("No statistics found matching query criteria for statistical aggregation");
      return null;
    }
  }

  @Override
  public void copyTo(final DataStore other) {
    if (other instanceof BaseDataStore) {
      // if we have access to datastoreoperations for "other" we can more
      // efficiently copy underlying GeoWaveRow and GeoWaveMetadata
      for (final MetadataType metadataType : MetadataType.values()) {
        try (MetadataWriter writer =
            ((BaseDataStore) other).baseOperations.createMetadataWriter(metadataType)) {
          final MetadataReader reader = baseOperations.createMetadataReader(metadataType);
          try (
              CloseableIterator<GeoWaveMetadata> it = reader.query(new MetadataQuery(null, null))) {
            while (it.hasNext()) {
              writer.write(it.next());
            }
          }
        } catch (final Exception e) {
          LOGGER.error("Unable to write metadata on copy", e);
        }
      }
      try (CloseableIterator<InternalDataAdapter<?>> it = adapterStore.getAdapters()) {
        while (it.hasNext()) {
          final InternalDataAdapter<?> adapter = it.next();
          for (final Index index : indexMappingStore.getIndicesForAdapter(
              adapter.getAdapterId()).getIndices(indexStore)) {
            final boolean rowMerging = BaseDataStoreUtils.isRowMerging(adapter);
            final ReaderParamsBuilder bldr =
                new ReaderParamsBuilder(
                    index,
                    adapterStore,
                    internalAdapterStore,
                    rowMerging
                        ? new GeoWaveRowMergingTransform(
                            BaseDataStoreUtils.getRowMergingAdapter(adapter),
                            adapter.getAdapterId())
                        : GeoWaveRowIteratorTransformer.NO_OP_TRANSFORMER);
            bldr.adapterIds(new short[] {adapter.getAdapterId()});
            bldr.isClientsideRowMerging(rowMerging);
            try (RowReader<GeoWaveRow> reader = baseOperations.createReader(bldr.build())) {
              try (RowWriter writer =
                  ((BaseDataStore) other).baseOperations.createWriter(index, adapter)) {
                while (reader.hasNext()) {
                  writer.write(reader.next());
                }
              }
            } catch (final Exception e) {
              LOGGER.error("Unable to write metadata on copy", e);
            }
          }
        }
      }
    } else {
      final DataTypeAdapter<?>[] sourceTypes = getTypes();

      // add all the types that the destination store doesn't have yet
      final DataTypeAdapter<?>[] destTypes = other.getTypes();
      for (int i = 0; i < sourceTypes.length; i++) {
        boolean found = false;
        for (int k = 0; k < destTypes.length; k++) {
          if (destTypes[k].getTypeName().compareTo(sourceTypes[i].getTypeName()) == 0) {
            found = true;
            break;
          }
        }
        if (!found) {
          other.addType(sourceTypes[i]);
        }
      }

      // add the indices for each type
      for (int i = 0; i < sourceTypes.length; i++) {
        final String typeName = sourceTypes[i].getTypeName();
        final short adapterId = internalAdapterStore.getAdapterId(typeName);
        final AdapterToIndexMapping indicesForAdapter =
            indexMappingStore.getIndicesForAdapter(adapterId);
        final Index[] indices = indicesForAdapter.getIndices(indexStore);
        other.addIndex(typeName, indices);

        final QueryBuilder<?, ?> qb = QueryBuilder.newBuilder().addTypeName(typeName);
        try (CloseableIterator<?> it = query(qb.build())) {
          try (final Writer writer = other.createWriter(typeName)) {
            while (it.hasNext()) {
              writer.write(it.next());
            }
          }
        }
      }
    }
  }

  @Override
  public void copyTo(final DataStore other, final Query<?> query) {
    // check for 'everything' query
    if (query == null) {
      copyTo(other);
      return;
    }

    final String[] typeNames = query.getDataTypeQueryOptions().getTypeNames();
    final String indexName = query.getIndexQueryOptions().getIndexName();
    final boolean isAllIndices = query.getIndexQueryOptions().isAllIndices();
    final List<DataTypeAdapter<?>> typesToCopy;

    // if typeNames are not specified, then it means 'everything' as well
    if (((typeNames == null) || (typeNames.length == 0))) {
      if ((query.getQueryConstraints() == null)
          || (query.getQueryConstraints() instanceof EverythingQuery)) {
        copyTo(other);
        return;
      } else {
        typesToCopy = Arrays.asList(getTypes());
      }
    } else {
      // make sure the types requested exist in the source store (this)
      // before trying to copy!
      final DataTypeAdapter<?>[] sourceTypes = getTypes();
      typesToCopy = new ArrayList<>();
      for (int i = 0; i < typeNames.length; i++) {
        boolean found = false;
        for (int k = 0; k < sourceTypes.length; k++) {
          if (sourceTypes[k].getTypeName().compareTo(typeNames[i]) == 0) {
            found = true;
            typesToCopy.add(sourceTypes[k]);
            break;
          }
        }
        if (!found) {
          throw new IllegalArgumentException(
              "Some type names specified in the query do not exist in the source database and thus cannot be copied.");
        }
      }
    }

    // if there is an index requested in the query, make sure it exists in
    // the source store before trying to copy as well!
    final Index[] sourceIndices = getIndices();
    Index indexToCopy = null;

    if (!isAllIndices) {
      // just add the one index specified by the query
      // first make sure source index exists though
      boolean found = false;
      for (int i = 0; i < sourceIndices.length; i++) {
        if (sourceIndices[i].getName().compareTo(indexName) == 0) {
          found = true;
          indexToCopy = sourceIndices[i];
          break;
        }
      }
      if (!found) {
        throw new IllegalArgumentException(
            "The index specified in the query does not exist in the source database and thus cannot be copied.");
      }

      // also make sure the types/index mapping for the query are legit
      for (int i = 0; i < typeNames.length; i++) {
        final short adapterId = internalAdapterStore.getAdapterId(typeNames[i]);
        final AdapterToIndexMapping indexMap = indexMappingStore.getIndicesForAdapter(adapterId);
        final String[] mapIndexNames = indexMap.getIndexNames();
        found = false;
        for (int k = 0; k < mapIndexNames.length; k++) {
          if (mapIndexNames[k].compareTo(indexName) == 0) {
            found = true;
            break;
          }
        }
        if (!found) {
          throw new IllegalArgumentException(
              "The index "
                  + indexName
                  + " and the type "
                  + typeNames[i]
                  + " specified by the query are not associated in the source database");
        }
      }
    }

    // add all the types that the destination store doesn't have yet
    final DataTypeAdapter<?>[] destTypes = other.getTypes();
    for (int i = 0; i < typesToCopy.size(); i++) {
      boolean found = false;
      for (int k = 0; k < destTypes.length; k++) {
        if (destTypes[k].getTypeName().compareTo(typesToCopy.get(i).getTypeName()) == 0) {
          found = true;
          break;
        }
      }
      if (!found) {
        other.addType(typesToCopy.get(i));
      }
    }

    // add all the indices that the destination store doesn't have yet
    if (isAllIndices) {
      // in this case, all indices from the types requested by the query
      for (int i = 0; i < typesToCopy.size(); i++) {
        final String typeName = typesToCopy.get(i).getTypeName();
        final short adapterId = internalAdapterStore.getAdapterId(typeName);
        final AdapterToIndexMapping indicesForAdapter =
            indexMappingStore.getIndicesForAdapter(adapterId);
        final Index[] indices = indicesForAdapter.getIndices(indexStore);
        other.addIndex(typeName, indices);

        final QueryBuilder<?, ?> qb =
            QueryBuilder.newBuilder().addTypeName(typeName).constraints(
                query.getQueryConstraints());
        try (CloseableIterator<?> it = query(qb.build())) {
          try (Writer writer = other.createWriter(typeName)) {
            while (it.hasNext()) {
              writer.write(it.next());
            }
          }
        }
      }
    } else {
      // otherwise, add just the one index to the types specified by the
      // query
      for (int i = 0; i < typesToCopy.size(); i++) {
        other.addIndex(typesToCopy.get(i).getTypeName(), indexToCopy);
      }

      // Write out / copy the data. We must do this on a per-type basis so
      // we can write appropriately
      for (int k = 0; k < typesToCopy.size(); k++) {
        final InternalDataAdapter<?> adapter =
            adapterStore.getAdapter(
                internalAdapterStore.getAdapterId(typesToCopy.get(k).getTypeName()));
        final QueryBuilder<?, ?> qb =
            QueryBuilder.newBuilder().addTypeName(adapter.getTypeName()).indexName(
                indexToCopy.getName()).constraints(query.getQueryConstraints());
        try (CloseableIterator<?> it = query(qb.build())) {
          try (Writer writer = other.createWriter(adapter.getTypeName())) {
            while (it.hasNext()) {
              writer.write(it.next());
            }
          }
        }
      }
    }
  }

  @Override
  public void removeIndex(final String indexName) {
    // remove the given index for all types

    // this is a little convoluted and requires iterating over all the
    // adapters, getting each adapter's index map, checking if the index is
    // there, and
    // then mark it for removal from both the map and from the index store.
    // If this index is the only index remaining for a given type, then we
    // need
    // to throw an exception first (no deletion will occur).

    final ArrayList<Short> markedAdapters = new ArrayList<>();
    try (CloseableIterator<InternalDataAdapter<?>> it = adapterStore.getAdapters()) {
      while (it.hasNext()) {

        final InternalDataAdapter<?> dataAdapter = it.next();
        final AdapterToIndexMapping adapterIndexMap =
            indexMappingStore.getIndicesForAdapter(dataAdapter.getAdapterId());
        final String[] indexNames = adapterIndexMap.getIndexNames();
        for (int i = 0; i < indexNames.length; i++) {
          if (indexNames[i].equals(indexName) && !baseOptions.isSecondaryIndexing()) {
            // check if it is the only index for the current adapter
            if (indexNames.length == 1) {
              throw new IllegalStateException(
                  "Index removal failed. Adapters require at least one index.");
            } else {
              // mark the index for removal and continue looking
              // for
              // others
              markedAdapters.add(adapterIndexMap.getAdapterId());
              continue;
            }
          }
        }
      }
    }

    // take out the index from the data statistics, and mapping
    for (int i = 0; i < markedAdapters.size(); i++) {
      final short adapterId = markedAdapters.get(i);
      baseOperations.deleteAll(indexName, internalAdapterStore.getTypeName(adapterId), adapterId);
      // need to be careful only deleting stats for this index and not any others for the adapter
      final List<InternalDataStatistics<?, ?, ?>> statsToRemove = new ArrayList<>();
      try (CloseableIterator<InternalDataStatistics<?, ?, ?>> it =
          statisticsStore.getDataStatistics(adapterId)) {
        while (it.hasNext()) {
          final InternalDataStatistics<?, ?, ?> stat = it.next();
          if ((stat.getExtendedId() != null) && stat.getExtendedId().startsWith(indexName)) {
            statsToRemove.add(stat);
          }
        }
      }
      statsToRemove.forEach(
          stat -> statisticsStore.removeStatistics(
              adapterId,
              stat.getType(),
              stat.getExtendedId()));
      indexMappingStore.remove(adapterId, indexName);
    }
    // remove the actual index
    indexStore.removeIndex(indexName);
  }

  @Override
  public void removeIndex(final String typeName, final String indexName)
      throws IllegalStateException {

    // First make sure the adapter exists and that this is not the only
    // index left for the given adapter. If it is, we should throw an
    // exception.
    final short adapterId = internalAdapterStore.getAdapterId(typeName);
    final AdapterToIndexMapping adapterIndexMap = indexMappingStore.getIndicesForAdapter(adapterId);

    if (adapterIndexMap == null) {
      throw new IllegalArgumentException(
          "No adapter with typeName " + typeName + "could be found.");
    }

    final String[] indexNames = adapterIndexMap.getIndexNames();
    if ((indexNames.length == 1) && !baseOptions.isSecondaryIndexing()) {
      throw new IllegalStateException("Index removal failed. Adapters require at least one index.");
    }

    // Remove all the data for the adapter and index
    baseOperations.deleteAll(indexName, typeName, adapterId);

    // If this is the last adapter/type associated with the index, then we
    // can remove the actual index too.
    final Short[] adapters = getAdaptersForIndex(indexName);
    if (adapters.length == 1) {
      indexStore.removeIndex(indexName);
    }

    // Finally, remove the mapping
    indexMappingStore.remove(adapterId, indexName);
  }

  @Override
  public void removeType(final String typeName) {
    // Removing a type requires removing the data associated with the type,
    // the index mapping for the type, and we also need to remove stats for
    // the type.
    final Short adapterId = internalAdapterStore.getAdapterId(typeName);

    if (adapterId != null) {
      final AdapterToIndexMapping mapping = indexMappingStore.getIndicesForAdapter(adapterId);
      final String[] indexNames = mapping.getIndexNames();

      // remove all the data for each index paired to this adapter
      for (int i = 0; i < indexNames.length; i++) {
        baseOperations.deleteAll(indexNames[i], typeName, adapterId);
      }
      if (baseOptions.isSecondaryIndexing()) {
        baseOperations.deleteAll(DataIndexUtils.DATA_ID_INDEX.getName(), typeName, adapterId);
      }

      statisticsStore.removeAllStatistics(adapterId);
      indexMappingStore.remove(adapterId);
      internalAdapterStore.remove(adapterId);
      adapterStore.removeAdapter(adapterId);
    }
  }

  @Override
  public void deleteAll() {
    deleteEverything();
  }
}
