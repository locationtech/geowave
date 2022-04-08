/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.store.AdapterToIndexMapping;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.DataStoreProperty;
import org.locationtech.geowave.core.store.PropertyStore;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.FieldDescriptor;
import org.locationtech.geowave.core.store.adapter.IndexDependentDataAdapter;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.api.AggregationQuery;
import org.locationtech.geowave.core.store.api.BinConstraints;
import org.locationtech.geowave.core.store.api.BinConstraints.ByteArrayConstraints;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.DataTypeStatistic;
import org.locationtech.geowave.core.store.api.FieldStatistic;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.IndexStatistic;
import org.locationtech.geowave.core.store.api.IngestOptions;
import org.locationtech.geowave.core.store.api.Query;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.api.Statistic;
import org.locationtech.geowave.core.store.api.StatisticQuery;
import org.locationtech.geowave.core.store.api.StatisticValue;
import org.locationtech.geowave.core.store.api.VisibilityHandler;
import org.locationtech.geowave.core.store.api.Writer;
import org.locationtech.geowave.core.store.base.dataidx.DataIndexUtils;
import org.locationtech.geowave.core.store.callback.DeleteCallbackList;
import org.locationtech.geowave.core.store.callback.DeleteOtherIndicesCallback;
import org.locationtech.geowave.core.store.callback.DuplicateDeletionCallback;
import org.locationtech.geowave.core.store.callback.IngestCallback;
import org.locationtech.geowave.core.store.callback.IngestCallbackList;
import org.locationtech.geowave.core.store.callback.ScanCallback;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.entities.GeoWaveRowMergingTransform;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.index.writer.IndependentAdapterIndexWriter;
import org.locationtech.geowave.core.store.index.writer.IndexCompositeWriter;
import org.locationtech.geowave.core.store.ingest.BaseDataStoreIngestDriver;
import org.locationtech.geowave.core.store.memory.MemoryAdapterIndexMappingStore;
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
import org.locationtech.geowave.core.store.query.gwql.ResultSet;
import org.locationtech.geowave.core.store.query.gwql.parse.GWQLParser;
import org.locationtech.geowave.core.store.query.gwql.statement.Statement;
import org.locationtech.geowave.core.store.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.statistics.DefaultStatisticsProvider;
import org.locationtech.geowave.core.store.statistics.InternalStatisticsHelper;
import org.locationtech.geowave.core.store.statistics.StatisticId;
import org.locationtech.geowave.core.store.statistics.StatisticType;
import org.locationtech.geowave.core.store.statistics.StatisticUpdateCallback;
import org.locationtech.geowave.core.store.statistics.adapter.DataTypeStatisticType;
import org.locationtech.geowave.core.store.statistics.field.FieldStatisticType;
import org.locationtech.geowave.core.store.statistics.index.DifferingVisibilityCountStatistic.DifferingVisibilityCountValue;
import org.locationtech.geowave.core.store.statistics.index.FieldVisibilityCountStatistic.FieldVisibilityCountValue;
import org.locationtech.geowave.core.store.statistics.index.IndexStatisticType;
import org.locationtech.geowave.core.store.statistics.query.DataTypeStatisticQuery;
import org.locationtech.geowave.core.store.statistics.query.FieldStatisticQuery;
import org.locationtech.geowave.core.store.statistics.query.IndexStatisticQuery;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import org.locationtech.geowave.core.store.util.NativeEntryIteratorWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.internal.Maps;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class BaseDataStore implements DataStore {
  private static final Logger LOGGER = LoggerFactory.getLogger(BaseDataStore.class);

  protected final IndexStore indexStore;
  protected final PersistentAdapterStore adapterStore;
  protected final DataStatisticsStore statisticsStore;
  protected final AdapterIndexMappingStore indexMappingStore;
  protected final DataStoreOperations baseOperations;
  protected final DataStoreOptions baseOptions;
  protected final InternalAdapterStore internalAdapterStore;
  protected final PropertyStore propertyStore;

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
      final InternalAdapterStore internalAdapterStore,
      final PropertyStore propertyStore) {
    this.indexStore = indexStore;
    this.adapterStore = adapterStore;
    this.statisticsStore = statisticsStore;
    this.indexMappingStore = indexMappingStore;
    this.internalAdapterStore = internalAdapterStore;
    this.propertyStore = propertyStore;
    baseOperations = operations;
    baseOptions = options;
  }

  public void store(final Index index) {
    checkNewDataStore();
    if (!indexStore.indexExists(index.getName())) {
      indexStore.addIndex(index);
      if (index instanceof DefaultStatisticsProvider) {
        ((DefaultStatisticsProvider) index).getDefaultStatistics().forEach(
            stat -> statisticsStore.addStatistic(stat));
      }
    }
  }

  protected synchronized void store(final InternalDataAdapter<?> adapter) {
    checkNewDataStore();
    if (!adapterStore.adapterExists(adapter.getAdapterId())) {
      adapterStore.addAdapter(adapter);
      if (adapter.getAdapter() instanceof DefaultStatisticsProvider) {
        ((DefaultStatisticsProvider) adapter.getAdapter()).getDefaultStatistics().forEach(
            stat -> statisticsStore.addStatistic(stat));
      }
    }
  }

  private void checkNewDataStore() {
    if ((propertyStore.getProperty(BaseDataStoreUtils.DATA_VERSION_PROPERTY) == null)
        && !BaseDataStoreUtils.hasMetadata(baseOperations, MetadataType.ADAPTER)
        && !BaseDataStoreUtils.hasMetadata(baseOperations, MetadataType.INDEX)) {
      // Only set the data version if no adapters and indices have already been added
      propertyStore.setProperty(
          new DataStoreProperty(
              BaseDataStoreUtils.DATA_VERSION_PROPERTY,
              BaseDataStoreUtils.DATA_VERSION));
    }
  }

  public DataStatisticsStore getStatisticsStore() {
    return statisticsStore;
  }

  public Short getAdapterId(final String typeName) {
    return internalAdapterStore.getAdapterId(typeName);
  }

  private VisibilityHandler resolveVisibilityHandler(
      final InternalDataAdapter<?> adapter,
      final VisibilityHandler visibilityHandler) {
    if (visibilityHandler != null) {
      return visibilityHandler;
    }
    if (adapter.getVisibilityHandler() != null) {
      return adapter.getVisibilityHandler();
    }
    final DataStoreProperty globalVis =
        propertyStore.getProperty(BaseDataStoreUtils.GLOBAL_VISIBILITY_PROPERTY);
    if (globalVis != null) {
      return (VisibilityHandler) globalVis.getValue();
    }
    return DataStoreUtils.UNCONSTRAINED_VISIBILITY;
  }

  @SuppressWarnings("unchecked")
  private <T> Writer<T> createWriter(
      final InternalDataAdapter<T> adapter,
      final VisibilityHandler visibilityHandler,
      final boolean writingOriginalData,
      final Index... indices) {
    final boolean secondaryIndex =
        writingOriginalData
            && baseOptions.isSecondaryIndexing()
            && DataIndexUtils.adapterSupportsDataIndex(adapter);
    final Writer<T>[] writers = new Writer[secondaryIndex ? indices.length + 1 : indices.length];
    final VisibilityHandler resolvedVisibilityHandler =
        resolveVisibilityHandler(adapter, visibilityHandler);

    int i = 0;
    if (secondaryIndex) {
      final DataStoreCallbackManager callbackManager =
          new DataStoreCallbackManager(statisticsStore, true);
      final AdapterToIndexMapping indexMapping =
          indexMappingStore.getMapping(
              adapter.getAdapterId(),
              DataIndexUtils.DATA_ID_INDEX.getName());
      final List<IngestCallback<T>> callbacks =
          Collections.singletonList(
              callbackManager.getIngestCallback(
                  adapter,
                  indexMapping,
                  DataIndexUtils.DATA_ID_INDEX));

      final IngestCallbackList<T> callbacksList = new IngestCallbackList<>(callbacks);
      writers[i++] =
          createDataIndexWriter(
              adapter,
              indexMapping,
              resolvedVisibilityHandler,
              baseOperations,
              baseOptions,
              callbacksList,
              callbacksList);
    }
    for (final Index index : indices) {
      final DataStoreCallbackManager callbackManager =
          new DataStoreCallbackManager(statisticsStore, i == 0);
      callbackManager.setPersistStats(baseOptions.isPersistDataStatistics());
      final AdapterToIndexMapping indexMapping =
          indexMappingStore.getMapping(adapter.getAdapterId(), index.getName());
      final List<IngestCallback<T>> callbacks =
          writingOriginalData
              ? Collections.singletonList(
                  callbackManager.getIngestCallback(adapter, indexMapping, index))
              : Collections.emptyList();

      final IngestCallbackList<T> callbacksList = new IngestCallbackList<>(callbacks);
      writers[i] =
          createIndexWriter(
              adapter,
              indexMapping,
              index,
              resolvedVisibilityHandler,
              baseOperations,
              baseOptions,
              callbacksList,
              callbacksList);

      if (adapter.getAdapter() instanceof IndexDependentDataAdapter) {
        writers[i] =
            new IndependentAdapterIndexWriter<>(
                (IndexDependentDataAdapter<T>) adapter.getAdapter(),
                index,
                resolvedVisibilityHandler,
                writers[i]);
      }
      i++;
    }
    return new IndexCompositeWriter<>(writers);
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

  @Override
  public ResultSet query(final String queryStr, final String... authorizations) {
    final Statement statement = GWQLParser.parseStatement(this, queryStr);
    return statement.execute(authorizations);
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
      dataIdsToDelete = new ConcurrentHashMap<>();
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
                    indexMappingStore,
                    internalAdapterStore,
                    queryOptions.getFieldIdsAdapterPair(),
                    queryOptions.getAggregation(),
                    queryOptions.getAuthorizations(),
                    adapter.getAdapterId(),
                    ((DataIdQuery) sanitizedConstraints).getDataIds());
          } else if (sanitizedConstraints instanceof DataIdRangeQuery) {
            if (((DataIdRangeQuery) sanitizedConstraints).isReverse()
                && !isReverseIterationSupported()) {
              throw new UnsupportedOperationException(
                  "Currently the underlying datastore does not support reverse iteration");
            }
            rowReader =
                DataIndexUtils.getRowReader(
                    baseOperations,
                    adapterStore,
                    indexMappingStore,
                    internalAdapterStore,
                    queryOptions.getFieldIdsAdapterPair(),
                    queryOptions.getAggregation(),
                    queryOptions.getAuthorizations(),
                    adapter.getAdapterId(),
                    ((DataIdRangeQuery) sanitizedConstraints).getStartDataIdInclusive(),
                    ((DataIdRangeQuery) sanitizedConstraints).getEndDataIdInclusive(),
                    ((DataIdRangeQuery) sanitizedConstraints).isReverse());
          } else {
            rowReader =
                DataIndexUtils.getRowReader(
                    baseOperations,
                    adapterStore,
                    indexMappingStore,
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
                      indexMappingStore,
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
              (Aggregation) queryOptions.getAggregation().getRight(),
              (DataTypeAdapter) queryOptions.getAggregation().getLeft());
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
      DedupeFilter dedupeFilter = new DedupeFilter();
      MemoryPersistentAdapterStore tempAdapterStore =
          new MemoryPersistentAdapterStore(queryOptions.getAdaptersArray(adapterStore));
      MemoryAdapterIndexMappingStore memoryMappingStore = new MemoryAdapterIndexMappingStore();
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
                  statisticsStore,
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
      final ScanCallback callback = queryOptions.getScanCallback();
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
                    callbackCache.getDeleteCallback(adapter, null, null).entryDeleted(entry, row);
                  }
                };
              }
              if (sanitizedConstraints instanceof DataIdQuery) {
                DataIndexUtils.delete(
                    baseOperations,
                    adapterStore,
                    indexMappingStore,
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
                    indexMappingStore,
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
                    indexMappingStore,
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
          final Index index = indexAdapterPair.getLeft();
          final AdapterToIndexMapping indexMapping =
              indexMappingStore.getMapping(adapter.getAdapterId(), index.getName());
          memoryMappingStore.addAdapterIndexMapping(indexMapping);
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

            if (deleteMode == DeletionMode.DELETE_WITH_DUPLICATES) {
              final DeleteCallbackList<T, GeoWaveRow> delList =
                  (DeleteCallbackList<T, GeoWaveRow>) callbackCache.getDeleteCallback(
                      adapter,
                      indexMapping,
                      index);

              final DuplicateDeletionCallback<T> dupDeletionCallback =
                  new DuplicateDeletionCallback<>(this, adapter, indexMapping, index);
              delList.addCallback(dupDeletionCallback);
              if ((additionalIndicesToDelete != null)
                  && (additionalIndicesToDelete.get(adapter.getAdapterId()) != null)) {
                delList.addCallback(
                    new DeleteOtherIndicesCallback<>(
                        baseOperations,
                        adapter,
                        additionalIndicesToDelete.get(adapter.getAdapterId()),
                        adapterStore,
                        indexMappingStore,
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
                    synchronized (internalDataIdsToDelete) {
                      currentDataIdsToDelete = internalDataIdsToDelete.get(row.getAdapterId());
                      if (currentDataIdsToDelete == null) {
                        currentDataIdsToDelete = Sets.newConcurrentHashSet();
                        internalDataIdsToDelete.put(row.getAdapterId(), currentDataIdsToDelete);
                      }
                    }
                  }
                  currentDataIdsToDelete.add(dataId);
                }
                callbackCache.getDeleteCallback(adapter, indexMapping, index).entryDeleted(
                    entry,
                    row);
              }
            });
          }
          QueryConstraints adapterIndexConstraints;
          if (isConstraintsAdapterIndexSpecific) {
            adapterIndexConstraints =
                ((AdapterAndIndexBasedQueryConstraints) sanitizedConstraints).createQueryConstraints(
                    adapter,
                    indexAdapterPair.getLeft(),
                    indexMapping);
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
                    indexMapping,
                    index),
                aggregation.getLeft());
          }
          if (adapterIndexConstraints instanceof InsertionIdQuery) {
            queryOptions.setLimit(-1);
            results.add(
                queryInsertionId(
                    adapter,
                    index,
                    (InsertionIdQuery) adapterIndexConstraints,
                    dedupeFilter,
                    queryOptions,
                    tempAdapterStore,
                    delete));
            continue;
          } else if (adapterIndexConstraints instanceof PrefixIdQuery) {
            if (!queriedAllAdaptersByPrefix) {
              final PrefixIdQuery prefixIdQuery = (PrefixIdQuery) adapterIndexConstraints;
              results.add(
                  queryRowPrefix(
                      index,
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
                    index,
                    adapterIndexConstraints,
                    dedupeFilter,
                    queryOptions,
                    tempAdapterStore,
                    memoryMappingStore,
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
                  dedupeFilter,
                  queryOptions,
                  tempAdapterStore,
                  memoryMappingStore,
                  delete));
        }
        if (DeletionMode.DELETE_WITH_DUPLICATES.equals(deleteMode)) {
          // Make sure each index query has a clean dedupe filter so that entries from other indices
          // get deleted
          dedupeFilter = new DedupeFilter();
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
              indexMappingStore,
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
    final InternalDataAdapter<?>[] adapters = adapterStore.getAdapters();

    for (final InternalDataAdapter<?> dataAdapter : adapters) {
      final AdapterToIndexMapping[] adapterIndexMap =
          indexMappingStore.getIndicesForAdapter(dataAdapter.getAdapterId());
      for (int i = 0; i < adapterIndexMap.length; i++) {
        if (adapterIndexMap[i].getIndexName().equals(indexName)) {
          // check if it is the only index for the current adapter
          if (adapterIndexMap.length == 1) {
            throw new IllegalStateException(
                "Index removal failed. Adapters require at least one index.");
          } else {
            // mark the index for removal
            markedAdapters.add(dataAdapter.getAdapterId());
          }
        }
      }
    }

    final Short[] adapterIds = new Short[markedAdapters.size()];
    return markedAdapters.toArray(adapterIds);
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
    try (CloseableIterator<? extends Statistic<? extends StatisticValue<?>>> adapterStats =
        statisticsStore.getDataTypeStatistics(adapter.getAdapter(), null, null)) {
      statisticsStore.removeStatistics(adapterStats);
    }

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
      final AdapterIndexMappingStore mappingStore,
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
            InternalStatisticsHelper.getIndexMetadata(
                index,
                adapterIdsToQuery,
                tempAdapterStore,
                statisticsStore,
                sanitizedQueryOptions.getAuthorizations()),
            InternalStatisticsHelper.getDuplicateCounts(
                index,
                adapterIdsToQuery,
                tempAdapterStore,
                statisticsStore,
                sanitizedQueryOptions.getAuthorizations()),
            InternalStatisticsHelper.getDifferingVisibilityCounts(
                index,
                adapterIdsToQuery,
                tempAdapterStore,
                statisticsStore,
                sanitizedQueryOptions.getAuthorizations()),
            InternalStatisticsHelper.getVisibilityCounts(
                index,
                adapterIdsToQuery,
                tempAdapterStore,
                statisticsStore,
                sanitizedQueryOptions.getAuthorizations()),
            DataIndexUtils.getDataIndexRetrieval(
                baseOperations,
                adapterStore,
                indexMappingStore,
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
        mappingStore,
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
            InternalStatisticsHelper.getDifferingVisibilityCounts(
                index,
                adapterIds,
                tempAdapterStore,
                statisticsStore,
                sanitizedQueryOptions.getAuthorizations()),
            InternalStatisticsHelper.getVisibilityCounts(
                index,
                adapterIds,
                tempAdapterStore,
                statisticsStore,
                sanitizedQueryOptions.getAuthorizations()),
            DataIndexUtils.getDataIndexRetrieval(
                baseOperations,
                adapterStore,
                indexMappingStore,
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
        indexMappingStore,
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
    final DifferingVisibilityCountValue differingVisibilityCounts =
        InternalStatisticsHelper.getDifferingVisibilityCounts(
            index,
            Collections.singletonList(adapter.getAdapterId()),
            tempAdapterStore,
            statisticsStore,
            sanitizedQueryOptions.getAuthorizations());
    final FieldVisibilityCountValue visibilityCounts =
        InternalStatisticsHelper.getVisibilityCounts(
            index,
            Collections.singletonList(adapter.getAdapterId()),
            tempAdapterStore,
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
                indexMappingStore,
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
        indexMappingStore,
        internalAdapterStore,
        sanitizedQueryOptions.getMaxResolutionSubsamplingPerDimension(),
        sanitizedQueryOptions.getTargetResolutionPerDimensionForHierarchicalIndex(),
        sanitizedQueryOptions.getLimit(),
        sanitizedQueryOptions.getMaxRangeDecomposition(),
        delete);
  }

  protected <T> Writer<T> createDataIndexWriter(
      final InternalDataAdapter<T> adapter,
      final AdapterToIndexMapping indexMapping,
      final VisibilityHandler visibilityHandler,
      final DataStoreOperations baseOperations,
      final DataStoreOptions baseOptions,
      final IngestCallback<T> callback,
      final Closeable closable) {
    return new BaseDataIndexWriter<>(
        adapter,
        indexMapping,
        visibilityHandler,
        baseOperations,
        baseOptions,
        callback,
        closable);
  }

  protected <T> Writer<T> createIndexWriter(
      final InternalDataAdapter<T> adapter,
      final AdapterToIndexMapping indexMapping,
      final Index index,
      final VisibilityHandler visibilityHandler,
      final DataStoreOperations baseOperations,
      final DataStoreOptions baseOptions,
      final IngestCallback<T> callback,
      final Closeable closable) {
    return new BaseIndexWriter<>(
        adapter,
        indexMapping,
        index,
        visibilityHandler,
        baseOperations,
        baseOptions,
        callback,
        closable);
  }

  protected <T> void initOnIndexWriterCreate(
      final InternalDataAdapter<T> adapter,
      final Index index) {}

  @Override
  public DataTypeAdapter<?> getType(final String typeName) {
    final InternalDataAdapter<?> internalDataAdapter = getInternalAdapter(typeName);
    if (internalDataAdapter == null) {
      return null;
    }
    return internalDataAdapter.getAdapter();
  }

  private InternalDataAdapter<?> getInternalAdapter(final String typeName) {
    final Short internalAdapterId = internalAdapterStore.getAdapterId(typeName);
    if (internalAdapterId == null) {
      return null;
    }
    return adapterStore.getAdapter(internalAdapterId);
  }

  /**
   * Get all the adapters that have been used within this data store
   *
   * @return An array of the adapters used within this datastore.
   */
  @Override
  public DataTypeAdapter<?>[] getTypes() {
    final InternalDataAdapter<?>[] adapters = adapterStore.getAdapters();
    return Arrays.stream(adapters).map(InternalDataAdapter::getAdapter).toArray(
        DataTypeAdapter<?>[]::new);
  }

  @Override
  public void addIndex(final Index index) {
    store(index);
  }

  @Override
  public Index[] getIndices() {
    return getIndices(null);
  }

  @Override
  public Index getIndex(final String indexName) {
    return indexStore.getIndex(indexName);
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
    final AdapterToIndexMapping[] indices =
        indexMappingStore.getIndicesForAdapter(internalAdapterId);
    return Arrays.stream(indices).map(indexMapping -> indexMapping.getIndex(indexStore)).toArray(
        Index[]::new);
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
      final AdapterToIndexMapping[] existingMappings =
          indexMappingStore.getIndicesForAdapter(adapterId);
      if ((existingMappings != null) && (existingMappings.length > 0)) {
        // reduce the provided indices to only those that don't already
        // exist
        final Set<String> indexNames =
            Arrays.stream(existingMappings).map(AdapterToIndexMapping::getIndexName).collect(
                Collectors.toSet());
        final Index[] newIndices =
            Arrays.stream(indices).filter(i -> !indexNames.contains(i.getName())).toArray(
                size -> new Index[size]);
        if (newIndices.length > 0) {
          internalAddIndices(adapter, newIndices);
          try (Writer writer =
              createWriter(adapter, adapter.getVisibilityHandler(), false, newIndices)) {
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
        internalAddIndices(adapter, indices);
      }
    }
  }

  private void internalAddIndices(final InternalDataAdapter<?> adapter, final Index[] indices) {
    for (final Index index : indices) {
      indexMappingStore.addAdapterIndexMapping(
          BaseDataStoreUtils.mapAdapterToIndex(adapter, index));
      store(index);
      initOnIndexWriterCreate(adapter, index);
    }
  }

  @Override
  public <T> void addType(final DataTypeAdapter<T> dataTypeAdapter, final Index... initialIndices) {
    addTypeInternal(dataTypeAdapter, null, initialIndices);
  }

  @Override
  public <T> void addType(
      final DataTypeAdapter<T> dataTypeAdapter,
      final List<Statistic<?>> statistics,
      final Index... initialIndices) {
    addType(dataTypeAdapter, null, statistics, initialIndices);
  }


  @Override
  public <T> void addType(
      final DataTypeAdapter<T> dataTypeAdapter,
      final VisibilityHandler visibilityHandler,
      final List<Statistic<?>> statistics,
      final Index... initialIndices) {
    if (addTypeInternal(dataTypeAdapter, visibilityHandler, initialIndices)) {
      statistics.stream().forEach(stat -> statisticsStore.addStatistic(stat));
    }
  }


  protected <T> boolean addTypeInternal(
      final DataTypeAdapter<T> dataTypeAdapter,
      final VisibilityHandler visibilityHandler,
      final Index... initialIndices) {
    // add internal adapter
    final InternalDataAdapter<T> adapter =
        dataTypeAdapter.asInternalAdapter(
            internalAdapterStore.addTypeName(dataTypeAdapter.getTypeName()),
            visibilityHandler);
    final boolean newAdapter = !adapterStore.adapterExists(adapter.getAdapterId());
    final Index[] initialIndicesUnique =
        Arrays.stream(initialIndices).distinct().toArray(size -> new Index[size]);
    internalAddIndices(adapter, initialIndicesUnique);
    store(adapter);
    return newAdapter;
  }

  /** Returns an index writer to perform batched write operations for the given typename */
  @Override
  public <T> Writer<T> createWriter(final String typeName) {
    return createWriter(typeName, null);
  }

  /** Returns an index writer to perform batched write operations for the given typename */
  @Override
  public <T> Writer<T> createWriter(
      final String typeName,
      final VisibilityHandler visibilityHandler) {
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
    final AdapterToIndexMapping[] mappings = indexMappingStore.getIndicesForAdapter(adapterId);
    if ((mappings.length == 0) && !baseOptions.isSecondaryIndexing()) {
      LOGGER.warn(
          "No indices for type '"
              + typeName
              + "'. Add indices using addIndex(<typename>, <indices>).");
      return null;
    }
    final Index[] indices =
        Arrays.stream(mappings).map(mapping -> mapping.getIndex(indexStore)).toArray(Index[]::new);
    return createWriter(adapter, visibilityHandler, true, indices);
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

  @SuppressWarnings("unchecked")
  protected <V extends StatisticValue<R>, R> CloseableIterator<V> internalQueryStatistics(
      final StatisticQuery<V, R> query) {
    final List<Statistic<V>> statistics = Lists.newLinkedList();
    if (query instanceof IndexStatisticQuery) {
      final IndexStatisticQuery<V, R> statQuery = (IndexStatisticQuery<V, R>) query;
      if (statQuery.indexName() == null) {
        final Index[] indices = getIndices();
        for (final Index index : indices) {
          getIndexStatistics(index, statQuery, statistics);

        }
      } else {
        final Index index = indexStore.getIndex(statQuery.indexName());
        if (index != null) {
          getIndexStatistics(index, statQuery, statistics);
        }
      }
    } else if (query instanceof DataTypeStatisticQuery) {
      final DataTypeStatisticQuery<V, R> statQuery = (DataTypeStatisticQuery<V, R>) query;
      if (statQuery.typeName() == null) {
        final DataTypeAdapter<?>[] adapters = getTypes();
        for (final DataTypeAdapter<?> adapter : adapters) {
          getAdapterStatistics(adapter, statQuery, statistics);
        }
      } else {
        final DataTypeAdapter<?> adapter = getType(statQuery.typeName());
        if (adapter != null) {
          getAdapterStatistics(adapter, statQuery, statistics);
        }
      }
    } else if (query instanceof FieldStatisticQuery) {
      final FieldStatisticQuery<V, R> statQuery = (FieldStatisticQuery<V, R>) query;
      if (statQuery.typeName() == null) {
        final DataTypeAdapter<?>[] adapters = getTypes();
        for (final DataTypeAdapter<?> adapter : adapters) {
          getFieldStatistics(adapter, statQuery, statistics);
        }
      } else {
        final DataTypeAdapter<?> adapter = getType(statQuery.typeName());
        if (adapter != null) {
          getFieldStatistics(adapter, statQuery, statistics);
        }
      }
    }

    if (query.binConstraints() != null) {
      final List<CloseableIterator<? extends StatisticValue<?>>> iterators = new ArrayList<>();
      for (final Statistic<?> stat : statistics) {
        if (stat.getBinningStrategy() != null) {
          final ByteArrayConstraints bins = query.binConstraints().constraints(stat);
          // we really don't need to check if the binning strategy supports the class considering
          // the binning strategy won't return bin constraints if it doesn't support the object
          if ((bins != null) && ((bins.getBins().length > 0) || bins.isAllBins())) {
            iterators.add(
                statisticsStore.getStatisticValues(
                    statistics.iterator(),
                    bins,
                    query.authorizations()));
          }
        }
      }
      return (CloseableIterator<V>) new CloseableIteratorWrapper<>(
          () -> iterators.forEach(CloseableIterator::close),
          Iterators.concat(iterators.iterator()));
    } else {
      return (CloseableIterator<V>) statisticsStore.getStatisticValues(
          statistics.iterator(),
          null,
          query.authorizations());
    }
  }

  @Override
  public <V extends StatisticValue<R>, R> CloseableIterator<V> queryStatistics(
      final StatisticQuery<V, R> query) {
    return internalQueryStatistics(query);
  }

  @Override
  public <V extends StatisticValue<R>, R> V aggregateStatistics(final StatisticQuery<V, R> query) {
    if (query.statisticType() == null) {
      LOGGER.error("Statistic Type must be provided for a statistical aggregation");
      return null;
    }
    try (CloseableIterator<V> values = internalQueryStatistics(query)) {
      V value = null;
      while (values.hasNext()) {
        if (value == null) {
          value = values.next();
        } else {
          value.merge(values.next());
        }
      }
      return value;
    }
  }

  @SuppressWarnings("unchecked")
  private <V extends StatisticValue<R>, R> void getIndexStatistics(
      final Index index,
      final IndexStatisticQuery<V, R> query,
      final List<Statistic<V>> statistics) {
    try (CloseableIterator<? extends Statistic<?>> statsIter =
        statisticsStore.getIndexStatistics(index, query.statisticType(), query.tag())) {
      while (statsIter.hasNext()) {
        statistics.add((Statistic<V>) statsIter.next());
      }
    }
  }

  @SuppressWarnings("unchecked")
  private <V extends StatisticValue<R>, R> void getAdapterStatistics(
      final DataTypeAdapter<?> adapter,
      final DataTypeStatisticQuery<V, R> query,
      final List<Statistic<V>> statistics) {
    try (CloseableIterator<? extends Statistic<?>> statsIter =
        statisticsStore.getDataTypeStatistics(adapter, query.statisticType(), query.tag())) {
      while (statsIter.hasNext()) {
        statistics.add((Statistic<V>) statsIter.next());
      }
    }
  }

  @SuppressWarnings("unchecked")
  private <V extends StatisticValue<R>, R> void getFieldStatistics(
      final DataTypeAdapter<?> adapter,
      final FieldStatisticQuery<V, R> query,
      final List<Statistic<V>> statistics) {
    try (CloseableIterator<? extends Statistic<?>> statsIter =
        statisticsStore.getFieldStatistics(
            adapter,
            query.statisticType(),
            query.fieldName(),
            query.tag())) {
      while (statsIter.hasNext()) {
        statistics.add((Statistic<V>) statsIter.next());
      }
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
          try (CloseableIterator<GeoWaveMetadata> it = reader.query(new MetadataQuery())) {
            while (it.hasNext()) {
              writer.write(it.next());
            }
          }
        } catch (final Exception e) {
          LOGGER.error("Unable to write metadata on copy", e);
        }
      }
      final InternalDataAdapter<?>[] adapters = adapterStore.getAdapters();
      for (final InternalDataAdapter<?> adapter : adapters) {
        final AdapterToIndexMapping[] mappings =
            indexMappingStore.getIndicesForAdapter(adapter.getAdapterId());
        for (final AdapterToIndexMapping mapping : mappings) {
          final Index index = mapping.getIndex(indexStore);
          final boolean rowMerging = BaseDataStoreUtils.isRowMerging(adapter);
          final ReaderParamsBuilder<GeoWaveRow> bldr =
              new ReaderParamsBuilder<>(
                  index,
                  adapterStore,
                  indexMappingStore,
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
        final AdapterToIndexMapping[] indexMappings =
            indexMappingStore.getIndicesForAdapter(adapterId);
        final Index[] indices =
            Arrays.stream(indexMappings).map(mapping -> mapping.getIndex(indexStore)).toArray(
                Index[]::new);
        other.addIndex(typeName, indices);

        final QueryBuilder<?, ?> qb = QueryBuilder.newBuilder().addTypeName(typeName);
        try (CloseableIterator<?> it = query(qb.build())) {
          try (final Writer<Object> writer = other.createWriter(typeName)) {
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
        final AdapterToIndexMapping[] indexMappings =
            indexMappingStore.getIndicesForAdapter(adapterId);
        found = false;
        for (int k = 0; k < indexMappings.length; k++) {
          if (indexMappings[k].getIndexName().compareTo(indexName) == 0) {
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
        final AdapterToIndexMapping[] indexMappings =
            indexMappingStore.getIndicesForAdapter(adapterId);
        final Index[] indices =
            Arrays.stream(indexMappings).map(mapping -> mapping.getIndex(indexStore)).toArray(
                Index[]::new);
        other.addIndex(typeName, indices);

        final QueryBuilder<?, ?> qb =
            QueryBuilder.newBuilder().addTypeName(typeName).constraints(
                query.getQueryConstraints());
        try (CloseableIterator<?> it = query(qb.build())) {
          try (Writer<Object> writer = other.createWriter(typeName)) {
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
          try (Writer<Object> writer = other.createWriter(adapter.getTypeName())) {
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
    final Index index = indexStore.getIndex(indexName);
    if (index == null) {
      LOGGER.warn("Unable to remove index '" + indexName + "' because it was not found.");
      return;
    }
    final ArrayList<Short> markedAdapters = new ArrayList<>();
    final InternalDataAdapter<?>[] adapters = adapterStore.getAdapters();
    for (final InternalDataAdapter<?> dataAdapter : adapters) {
      final AdapterToIndexMapping[] indexMappings =
          indexMappingStore.getIndicesForAdapter(dataAdapter.getAdapterId());
      for (int i = 0; i < indexMappings.length; i++) {
        if (indexMappings[i].getIndexName().equals(indexName)
            && !baseOptions.isSecondaryIndexing()) {
          // check if it is the only index for the current adapter
          if (indexMappings.length == 1) {
            throw new IllegalStateException(
                "Index removal failed. Adapters require at least one index.");
          } else {
            // mark the index for removal and continue looking
            // for
            // others
            markedAdapters.add(dataAdapter.getAdapterId());
            continue;
          }
        }
      }
    }

    // take out the index from the data statistics, and mapping
    for (int i = 0; i < markedAdapters.size(); i++) {
      final short adapterId = markedAdapters.get(i);
      baseOperations.deleteAll(indexName, internalAdapterStore.getTypeName(adapterId), adapterId);
      indexMappingStore.remove(adapterId, indexName);
    }

    statisticsStore.removeStatistics(index);

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
    final AdapterToIndexMapping[] indexMappings = indexMappingStore.getIndicesForAdapter(adapterId);

    if (indexMappings.length == 0) {
      throw new IllegalArgumentException(
          "No adapter with typeName " + typeName + "could be found.");
    }

    if ((indexMappings.length == 1) && !baseOptions.isSecondaryIndexing()) {
      throw new IllegalStateException("Index removal failed. Adapters require at least one index.");
    }

    // Remove all the data for the adapter and index
    baseOperations.deleteAll(indexName, typeName, adapterId);

    // If this is the last adapter/type associated with the index, then we
    // can remove the actual index too.
    final Short[] adapters = getAdaptersForIndex(indexName);
    if (adapters.length == 1) {
      indexStore.removeIndex(indexName);
    } else {
      try (CloseableIterator<? extends Statistic<?>> iter =
          statisticsStore.getIndexStatistics(getIndex(indexName), null, null)) {
        while (iter.hasNext()) {
          statisticsStore.removeTypeSpecificStatisticValues(
              (IndexStatistic<?>) iter.next(),
              typeName);
        }
      }
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
      final AdapterToIndexMapping[] indexMappings =
          indexMappingStore.getIndicesForAdapter(adapterId);

      // remove all the data for each index paired to this adapter
      for (int i = 0; i < indexMappings.length; i++) {
        baseOperations.deleteAll(indexMappings[i].getIndexName(), typeName, adapterId);
      }
      if (baseOptions.isSecondaryIndexing()) {
        baseOperations.deleteAll(DataIndexUtils.DATA_ID_INDEX.getName(), typeName, adapterId);
      }

      statisticsStore.removeStatistics(adapterStore.getAdapter(adapterId));
      indexMappingStore.remove(adapterId);
      internalAdapterStore.remove(adapterId);
      adapterStore.removeAdapter(adapterId);
    }
  }

  @Override
  public void deleteAll() {
    deleteEverything();
  }

  public IndexStore getIndexStore() {
    return indexStore;
  }

  public PersistentAdapterStore getAdapterStore() {
    return adapterStore;
  }

  public AdapterIndexMappingStore getIndexMappingStore() {
    return indexMappingStore;
  }

  public DataStoreOperations getBaseOperations() {
    return baseOperations;
  }

  public InternalAdapterStore getInternalAdapterStore() {
    return internalAdapterStore;
  }

  public boolean isReverseIterationSupported() {
    return false;
  }

  private void addStatistics(
      final Statistic<? extends StatisticValue<?>>[] statistics,
      final boolean calculateStats) {
    if ((statistics == null) || (statistics.length == 0)) {
      return;
    }
    // grouping stats is separated from calculating stats primarily because regardless of whether
    // stats are calculated they should be validated before adding them to the statistics store
    final Pair<Map<Index, List<IndexStatistic<?>>>, Map<InternalDataAdapter<?>, List<Statistic<? extends StatisticValue<?>>>>> groupedStats =
        groupAndValidateStats(statistics, false);
    final Map<Index, List<IndexStatistic<?>>> indexStatsToAdd = groupedStats.getLeft();
    final Map<InternalDataAdapter<?>, List<Statistic<? extends StatisticValue<?>>>> otherStatsToAdd =
        groupedStats.getRight();
    for (final List<IndexStatistic<?>> indexStats : indexStatsToAdd.values()) {
      indexStats.forEach(indexStat -> statisticsStore.addStatistic(indexStat));
    }
    for (final List<Statistic<? extends StatisticValue<?>>> otherStats : otherStatsToAdd.values()) {
      otherStats.forEach(statistic -> statisticsStore.addStatistic(statistic));
    }

    if (calculateStats) {
      calcStats(indexStatsToAdd, otherStatsToAdd);
    }
  }

  private Pair<Map<Index, List<IndexStatistic<?>>>, Map<InternalDataAdapter<?>, List<Statistic<? extends StatisticValue<?>>>>> groupAndValidateStats(
      final Statistic<? extends StatisticValue<?>>[] statistics,
      final boolean allowExisting) {
    final Map<Index, List<IndexStatistic<?>>> indexStatsToAdd = Maps.newHashMap();
    final Map<InternalDataAdapter<?>, List<Statistic<? extends StatisticValue<?>>>> otherStatsToAdd =
        Maps.newHashMap();
    for (final Statistic<? extends StatisticValue<?>> statistic : statistics) {
      if (!allowExisting && statisticsStore.exists(statistic)) {
        throw new IllegalArgumentException(
            "The statistic already exists.  If adding it is still desirable, use a 'tag' to make the statistic unique.");
      }
      if (statistic instanceof IndexStatistic) {
        final IndexStatistic<?> indexStat = (IndexStatistic<?>) statistic;
        if (indexStat.getIndexName() == null) {
          throw new IllegalArgumentException("No index specified.");
        }
        final Index index = indexStore.getIndex(indexStat.getIndexName());
        if (index == null) {
          throw new IllegalArgumentException("No index named " + indexStat.getIndexName() + ".");
        }
        if (!indexStatsToAdd.containsKey(index)) {
          indexStatsToAdd.put(index, Lists.newArrayList());
        }
        indexStatsToAdd.get(index).add(indexStat);
      } else if (statistic instanceof DataTypeStatistic) {
        final DataTypeStatistic<?> adapterStat = (DataTypeStatistic<?>) statistic;
        if (adapterStat.getTypeName() == null) {
          throw new IllegalArgumentException("No type specified.");
        }
        final InternalDataAdapter<?> adapter = getInternalAdapter(adapterStat.getTypeName());
        if (adapter == null) {
          throw new IllegalArgumentException("No type named " + adapterStat.getTypeName() + ".");
        }
        if (!otherStatsToAdd.containsKey(adapter)) {
          otherStatsToAdd.put(adapter, Lists.newArrayList());
        }
        otherStatsToAdd.get(adapter).add(adapterStat);
      } else if (statistic instanceof FieldStatistic) {
        final FieldStatistic<?> fieldStat = (FieldStatistic<?>) statistic;
        if (fieldStat.getTypeName() == null) {
          throw new IllegalArgumentException("No type specified.");
        }
        final InternalDataAdapter<?> adapter = getInternalAdapter(fieldStat.getTypeName());
        if (adapter == null) {
          throw new IllegalArgumentException("No type named " + fieldStat.getTypeName() + ".");
        }
        if (fieldStat.getFieldName() == null) {
          throw new IllegalArgumentException("No field specified.");
        }
        boolean foundMatch = false;
        final FieldDescriptor<?>[] fields = adapter.getFieldDescriptors();
        for (int i = 0; i < fields.length; i++) {
          if (fieldStat.getFieldName().equals(fields[i].fieldName())) {
            foundMatch = true;
            break;
          }
        }
        if (!foundMatch) {
          throw new IllegalArgumentException(
              "No field named "
                  + fieldStat.getFieldName()
                  + " was found on the type "
                  + fieldStat.getTypeName()
                  + ".");
        }
        if (!otherStatsToAdd.containsKey(adapter)) {
          otherStatsToAdd.put(adapter, Lists.newArrayList());
        }
        otherStatsToAdd.get(adapter).add(fieldStat);
      } else {
        throw new IllegalArgumentException("Unrecognized statistic type.");
      }
    }
    return Pair.of(indexStatsToAdd, otherStatsToAdd);
  }

  @SuppressWarnings("unchecked")
  private void calcStats(
      final Map<Index, List<IndexStatistic<?>>> indexStatsToAdd,
      final Map<InternalDataAdapter<?>, List<Statistic<? extends StatisticValue<?>>>> otherStatsToAdd) {
    for (final Entry<Index, List<IndexStatistic<?>>> indexStats : indexStatsToAdd.entrySet()) {
      final Index index = indexStats.getKey();
      final ArrayList<Short> indexAdapters = new ArrayList<>();
      final InternalDataAdapter<?>[] adapters = adapterStore.getAdapters();
      for (final InternalDataAdapter<?> dataAdapter : adapters) {
        final AdapterToIndexMapping[] adapterIndexMap =
            indexMappingStore.getIndicesForAdapter(dataAdapter.getAdapterId());
        for (int i = 0; i < adapterIndexMap.length; i++) {
          if (adapterIndexMap[i].getIndexName().equals(index.getName())) {
            indexAdapters.add(adapterIndexMap[i].getAdapterId());
            break;
          }
        }
      }

      // Scan all adapters used on the index
      for (int i = 0; i < indexAdapters.size(); i++) {
        final short adapterId = indexAdapters.get(i);
        final InternalDataAdapter<?> adapter = adapterStore.getAdapter(adapterId);
        final Query<Object> query =
            QueryBuilder.newBuilder().addTypeName(adapter.getTypeName()).indexName(
                index.getName()).build();
        final List<Statistic<? extends StatisticValue<?>>> statsToUpdate =
            Lists.newArrayList(indexStats.getValue());
        if (otherStatsToAdd.containsKey(adapter)) {
          statsToUpdate.addAll(otherStatsToAdd.get(adapter));
          // Adapter-specific stats only need to be computed once, so remove them once they've
          // been processed
          otherStatsToAdd.remove(adapter);
        }
        final AdapterToIndexMapping indexMapping =
            indexMappingStore.getMapping(adapterId, index.getName());
        try (StatisticUpdateCallback<?> updateCallback =
            new StatisticUpdateCallback<>(
                statsToUpdate,
                statisticsStore,
                index,
                indexMapping,
                adapter)) {
          try (CloseableIterator<?> entryIt =
              this.query(query, (ScanCallback<Object, GeoWaveRow>) updateCallback)) {
            while (entryIt.hasNext()) {
              entryIt.next();
            }
          }
        }
      }
    }
    for (final Entry<InternalDataAdapter<?>, List<Statistic<? extends StatisticValue<?>>>> otherStats : otherStatsToAdd.entrySet()) {
      final InternalDataAdapter<?> adapter = otherStats.getKey();
      final String typeName = adapter.getTypeName();
      final Index[] indices = getIndices(typeName);
      if (indices.length == 0) {
        // If there are no indices, then there is nothing to calculate.
        return;
      }
      final Query<Object> query =
          QueryBuilder.newBuilder().addTypeName(typeName).indexName(indices[0].getName()).build();
      final AdapterToIndexMapping indexMapping =
          indexMappingStore.getMapping(adapter.getAdapterId(), indices[0].getName());
      try (StatisticUpdateCallback<?> updateCallback =
          new StatisticUpdateCallback<>(
              otherStats.getValue(),
              statisticsStore,
              indices[0],
              indexMapping,
              adapter)) {
        try (CloseableIterator<?> entryIt =
            this.query(query, (ScanCallback<Object, GeoWaveRow>) updateCallback)) {
          while (entryIt.hasNext()) {
            entryIt.next();
          }
        }
      }
    }
  }

  @Override
  public void removeStatistic(final Statistic<?>... statistic) {
    final boolean removed = statisticsStore.removeStatistics(Arrays.asList(statistic).iterator());
    if (!removed) {
      throw new IllegalArgumentException(
          "Statistic could not be removed because it was not found.");
    }
  }

  @Override
  public void addEmptyStatistic(final Statistic<?>... statistic) {
    addStatistics(statistic, false);
  }

  @Override
  public void addStatistic(final Statistic<?>... statistic) {
    addStatistics(statistic, true);
  }

  @Override
  public void recalcStatistic(final Statistic<?>... statistic) {
    for (final Statistic<?> stat : statistic) {
      if (!statisticsStore.exists(stat)) {
        throw new IllegalArgumentException("The statistic " + stat.toString() + " doesn't exist.");
      }
    }

    final Pair<Map<Index, List<IndexStatistic<?>>>, Map<InternalDataAdapter<?>, List<Statistic<? extends StatisticValue<?>>>>> groupedStats =
        groupAndValidateStats(statistic, true);
    // Remove old statistic values
    for (final Statistic<?> stat : statistic) {
      statisticsStore.removeStatisticValues(stat);
    }
    calcStats(groupedStats.getLeft(), groupedStats.getRight());
  }

  @Override
  public DataTypeStatistic<?>[] getDataTypeStatistics(final String typeName) {
    final Short adapterId = internalAdapterStore.getAdapterId(typeName);
    if (adapterId == null) {
      throw new IllegalArgumentException(typeName + " doesn't exist");
    }
    final List<DataTypeStatistic<?>> retVal = new ArrayList<>();
    try (CloseableIterator<? extends DataTypeStatistic<?>> it =
        statisticsStore.getDataTypeStatistics(adapterStore.getAdapter(adapterId), null, null)) {
      while (it.hasNext()) {
        retVal.add(it.next());
      }
    }
    return retVal.toArray(new DataTypeStatistic<?>[retVal.size()]);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <V extends StatisticValue<R>, R> DataTypeStatistic<V> getDataTypeStatistic(
      final StatisticType<V> statisticType,
      final String typeName,
      final String tag) {
    if (!(statisticType instanceof DataTypeStatisticType)) {
      throw new IllegalArgumentException("Statistic type must be a data type statistic.");
    }
    final Short adapterId = internalAdapterStore.getAdapterId(typeName);
    if (adapterId == null) {
      throw new IllegalArgumentException(typeName + " doesn't exist");
    }
    final InternalDataAdapter<?> adapter = adapterStore.getAdapter(adapterId);
    if (adapter == null) {
      throw new IllegalArgumentException(typeName + " is null");
    }
    DataTypeStatistic<V> retVal = null;
    if (tag == null) {
      retVal =
          internalGetDataTypeStatistic(
              (DataTypeStatisticType<V>) statisticType,
              adapter,
              Statistic.DEFAULT_TAG);
      if (retVal == null) {
        retVal =
            internalGetDataTypeStatistic(
                (DataTypeStatisticType<V>) statisticType,
                adapter,
                Statistic.INTERNAL_TAG);
      }
      if (retVal == null) {
        try (CloseableIterator<DataTypeStatistic<V>> iter =
            (CloseableIterator<DataTypeStatistic<V>>) statisticsStore.getDataTypeStatistics(
                adapter,
                statisticType,
                null)) {
          if (iter.hasNext()) {
            retVal = iter.next();
            if (iter.hasNext()) {
              throw new IllegalArgumentException(
                  "Multiple statistics with different tags were found.  A tag must be specified.");
            }
          }
        }
      }
    } else {
      retVal = internalGetDataTypeStatistic((DataTypeStatisticType<V>) statisticType, adapter, tag);
    }
    return retVal;
  }

  private <V extends StatisticValue<R>, R> DataTypeStatistic<V> internalGetDataTypeStatistic(
      final DataTypeStatisticType<V> statisticType,
      final DataTypeAdapter<?> adapter,
      final String tag) {
    final StatisticId<V> statId =
        DataTypeStatistic.generateStatisticId(adapter.getTypeName(), statisticType, tag);
    return (DataTypeStatistic<V>) statisticsStore.getStatisticById(statId);
  }

  @Override
  public IndexStatistic<?>[] getIndexStatistics(final String indexName) {
    final Index index = getIndex(indexName);
    if (index == null) {
      throw new IllegalArgumentException(indexName + " doesn't exist");
    }
    final List<IndexStatistic<?>> retVal = new ArrayList<>();
    try (CloseableIterator<? extends IndexStatistic<?>> it =
        statisticsStore.getIndexStatistics(index, null, null)) {
      while (it.hasNext()) {
        retVal.add(it.next());
      }
    }
    return retVal.toArray(new IndexStatistic<?>[retVal.size()]);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <V extends StatisticValue<R>, R> IndexStatistic<V> getIndexStatistic(
      final StatisticType<V> statisticType,
      final String indexName,
      final String tag) {
    if (!(statisticType instanceof IndexStatisticType)) {
      throw new IllegalArgumentException("Statistic type must be an index statistic.");
    }
    final Index index = getIndex(indexName);
    if (index == null) {
      throw new IllegalArgumentException(indexName + " doesn't exist");
    }
    IndexStatistic<V> retVal = null;
    if (tag == null) {
      retVal =
          internalGetIndexStatistic(
              (IndexStatisticType<V>) statisticType,
              index,
              Statistic.DEFAULT_TAG);
      if (retVal == null) {
        retVal =
            internalGetIndexStatistic(
                (IndexStatisticType<V>) statisticType,
                index,
                Statistic.INTERNAL_TAG);
      }
      if (retVal == null) {
        try (CloseableIterator<IndexStatistic<V>> iter =
            (CloseableIterator<IndexStatistic<V>>) statisticsStore.getIndexStatistics(
                index,
                statisticType,
                null)) {
          if (iter.hasNext()) {
            retVal = iter.next();
            if (iter.hasNext()) {
              throw new IllegalArgumentException(
                  "Multiple statistics with different tags were found.  A tag must be specified.");
            }
          }
        }
      }
    } else {
      retVal = internalGetIndexStatistic((IndexStatisticType<V>) statisticType, index, tag);
    }
    return retVal;
  }

  private <V extends StatisticValue<R>, R> IndexStatistic<V> internalGetIndexStatistic(
      final IndexStatisticType<V> statisticType,
      final Index index,
      final String tag) {
    final StatisticId<V> statId =
        IndexStatistic.generateStatisticId(index.getName(), statisticType, tag);
    return (IndexStatistic<V>) statisticsStore.getStatisticById(statId);
  }

  @Override
  public FieldStatistic<?>[] getFieldStatistics(final String typeName, final String fieldName) {
    final Short adapterId = internalAdapterStore.getAdapterId(typeName);
    if (adapterId == null) {
      throw new IllegalArgumentException(typeName + " doesn't exist");
    }
    final List<FieldStatistic<?>> retVal = new ArrayList<>();
    try (CloseableIterator<? extends FieldStatistic<?>> it =
        statisticsStore.getFieldStatistics(
            adapterStore.getAdapter(adapterId),
            null,
            fieldName,
            null)) {
      while (it.hasNext()) {
        retVal.add(it.next());
      }
    }
    return retVal.toArray(new FieldStatistic<?>[retVal.size()]);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <V extends StatisticValue<R>, R> FieldStatistic<V> getFieldStatistic(
      final StatisticType<V> statisticType,
      final String typeName,
      final String fieldName,
      final String tag) {
    if (!(statisticType instanceof FieldStatisticType)) {
      throw new IllegalArgumentException("Statistic type must be a field statistic.");
    }
    final Short adapterId = internalAdapterStore.getAdapterId(typeName);
    if (adapterId == null) {
      throw new IllegalArgumentException(typeName + " doesn't exist");
    }
    final InternalDataAdapter<?> adapter = adapterStore.getAdapter(adapterId);
    if (adapter == null) {
      throw new IllegalArgumentException(typeName + " is null");
    }
    FieldStatistic<V> retVal = null;
    if (tag == null) {
      retVal =
          internalGetFieldStatistic(
              (FieldStatisticType<V>) statisticType,
              adapter,
              fieldName,
              Statistic.DEFAULT_TAG);
      if (retVal == null) {
        retVal =
            internalGetFieldStatistic(
                (FieldStatisticType<V>) statisticType,
                adapter,
                fieldName,
                Statistic.INTERNAL_TAG);
      }
      if (retVal == null) {
        try (CloseableIterator<FieldStatistic<V>> iter =
            (CloseableIterator<FieldStatistic<V>>) statisticsStore.getFieldStatistics(
                adapter,
                statisticType,
                fieldName,
                null)) {
          if (iter.hasNext()) {
            retVal = iter.next();
            if (iter.hasNext()) {
              throw new IllegalArgumentException(
                  "Multiple statistics with different tags were found.  A tag must be specified.");
            }
          }
        }
      }
    } else {
      retVal =
          internalGetFieldStatistic((FieldStatisticType<V>) statisticType, adapter, fieldName, tag);
    }
    return retVal;
  }

  private <V extends StatisticValue<R>, R> FieldStatistic<V> internalGetFieldStatistic(
      final FieldStatisticType<V> statisticType,
      final DataTypeAdapter<?> adapter,
      final String fieldName,
      final String tag) {
    final StatisticId<V> statId =
        FieldStatistic.generateStatisticId(adapter.getTypeName(), statisticType, fieldName, tag);
    return (FieldStatistic<V>) statisticsStore.getStatisticById(statId);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <V extends StatisticValue<R>, R> R getStatisticValue(
      final Statistic<V> stat,
      BinConstraints binConstraints) {
    if (stat == null) {
      throw new IllegalArgumentException("Statistic must be non-null");
    }
    if (binConstraints == null) {
      LOGGER.warn("Constraints are null, assuming all bins should match.");
      binConstraints = BinConstraints.allBins();
    }
    try (CloseableIterator<V> values =
        (CloseableIterator<V>) statisticsStore.getStatisticValues(
            Iterators.forArray(stat),
            binConstraints.constraints(stat))) {
      final V value = stat.createEmpty();
      while (values.hasNext()) {
        value.merge(values.next());
      }
      return value.getValue();
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public <V extends StatisticValue<R>, R> CloseableIterator<Pair<ByteArray, R>> getBinnedStatisticValues(
      final Statistic<V> stat,
      BinConstraints binConstraints) {
    if (stat == null) {
      throw new IllegalArgumentException("Statistic must be non-null");
    }
    if (binConstraints == null) {
      LOGGER.warn("Constraints are null, assuming all bins should match.");
      binConstraints = BinConstraints.allBins();
    }
    final CloseableIterator<V> values =
        (CloseableIterator<V>) statisticsStore.getStatisticValues(
            Iterators.forArray(stat),
            binConstraints.constraints(stat));
    return new CloseableIteratorWrapper<>(
        values,
        Iterators.transform(values, (v) -> Pair.of(v.getBin(), v.getValue())));
  }
}
