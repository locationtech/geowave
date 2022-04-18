/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.kudu.operations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.AsyncKuduScanner.AsyncKuduScannerBuilder;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.Delete;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduScanner.KuduScannerBuilder;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;
import org.locationtech.geowave.core.index.SinglePartitionQueryRanges;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.base.dataidx.DataIndexUtils;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.metadata.AbstractGeoWavePersistence;
import org.locationtech.geowave.core.store.operations.DataIndexReaderParams;
import org.locationtech.geowave.core.store.operations.Deleter;
import org.locationtech.geowave.core.store.operations.MetadataDeleter;
import org.locationtech.geowave.core.store.operations.MetadataReader;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.core.store.operations.MetadataWriter;
import org.locationtech.geowave.core.store.operations.ReaderParams;
import org.locationtech.geowave.core.store.operations.RowDeleter;
import org.locationtech.geowave.core.store.operations.RowReader;
import org.locationtech.geowave.core.store.operations.RowWriter;
import org.locationtech.geowave.datastore.kudu.KuduDataIndexRow;
import org.locationtech.geowave.datastore.kudu.KuduDataIndexRow.KuduDataIndexField;
import org.locationtech.geowave.datastore.kudu.KuduMetadataRow.KuduMetadataField;
import org.locationtech.geowave.datastore.kudu.KuduRow.KuduField;
import org.locationtech.geowave.datastore.kudu.PersistentKuduRow;
import org.locationtech.geowave.datastore.kudu.config.KuduRequiredOptions;
import org.locationtech.geowave.datastore.kudu.util.AsyncClientPool;
import org.locationtech.geowave.datastore.kudu.util.ClientPool;
import org.locationtech.geowave.datastore.kudu.util.KuduUtils;
import org.locationtech.geowave.mapreduce.MapReduceDataStoreOperations;
import org.locationtech.geowave.mapreduce.splits.RecordReaderParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;

public class KuduOperations implements MapReduceDataStoreOperations {
  private static final Logger LOGGER = LoggerFactory.getLogger(KuduOperations.class);
  private static final int KUDU_IDENTIFIER_MAX_LENGTH = 256;

  private final String gwNamespace;
  protected final KuduRequiredOptions options;

  private final KuduClient client;
  private final AsyncKuduClient asyncClient;
  private final Object CREATE_TABLE_MUTEX = new Object();

  public KuduOperations(final KuduRequiredOptions options) {
    if ((options.getGeoWaveNamespace() == null) || options.getGeoWaveNamespace().equals("")) {
      gwNamespace = "geowave";
    } else {
      gwNamespace = options.getGeoWaveNamespace();
    }
    this.options = options;
    client = ClientPool.getInstance().getClient(options.getKuduMaster());
    asyncClient = AsyncClientPool.getInstance().getClient(options.getKuduMaster());
  }

  @Override
  public boolean indexExists(final String indexName) throws IOException {
    return client.tableExists(getKuduQualifiedName(indexName));
  }

  @Override
  public boolean metadataExists(final MetadataType type) throws IOException {
    return client.tableExists(getKuduQualifiedName(getMetadataTableName(type)));
  }

  @Override
  public void deleteAll() throws Exception {
    for (final String table : client.getTablesList(gwNamespace).getTablesList()) {
      client.deleteTable(table);
    }
  }

  @Override
  public boolean deleteAll(
      final String indexName,
      final String typeName,
      final Short adapterId,
      final String... additionalAuthorizations) {
    // TODO: this deletion does not currently take into account the typeName, and authorizations are
    // not supported
    // KuduSession session = getSession();
    // try {
    // KuduTable table = getTable(indexName);
    // Schema schema = table.getSchema();
    // List<KuduPredicate> preds =
    // Collections.singletonList(
    // KuduPredicate.newComparisonPredicate(
    // schema.getColumn(KuduField.GW_ADAPTER_ID_KEY.getFieldName()),
    // KuduPredicate.ComparisonOp.EQUAL,
    // adapterId));
    // for (Delete delete : getDeletions(table, preds, KuduRow::new)) {
    // OperationResponse resp = session.apply(delete);
    // if (resp.hasRowError()) {
    // LOGGER.error("Encountered error while deleting all: {}", resp.getRowError());
    // }
    // }
    // return true;
    // } catch (KuduException e) {
    // LOGGER.error("Encountered error while deleting all", e);
    // return false;
    // } finally {
    // try {
    // session.close();
    // } catch (KuduException e) {
    // LOGGER.error("Encountered error while closing Kudu session", e);
    // }
    // }
    return false;
  }

  @Override
  public RowWriter createWriter(final Index index, final InternalDataAdapter<?> adapter) {
    createIndexTable(index.getName(), index.getIndexStrategy().getPredefinedSplits().length);
    return new KuduWriter(index.getName(), this);
  }

  @Override
  public RowWriter createDataIndexWriter(final InternalDataAdapter<?> adapter) {
    return createWriter(DataIndexUtils.DATA_ID_INDEX, adapter);
  }

  @Override
  public MetadataWriter createMetadataWriter(final MetadataType metadataType) {
    synchronized (CREATE_TABLE_MUTEX) {
      try {
        if (!metadataExists(metadataType)) {
          final List<ColumnSchema> columns = new ArrayList<>();
          for (final KuduMetadataField f : KuduMetadataField.values()) {
            f.addColumn(columns);
          }
          client.createTable(
              getKuduQualifiedName(getMetadataTableName(metadataType)),
              new Schema(columns),
              new CreateTableOptions().addHashPartitions(
                  Collections.singletonList(KuduMetadataField.GW_PRIMARY_ID_KEY.getFieldName()),
                  KuduUtils.KUDU_DEFAULT_BUCKETS).setNumReplicas(KuduUtils.KUDU_DEFAULT_REPLICAS));
        }
      } catch (final IOException e) {
        LOGGER.error(
            "Unable to create metadata table '{}'",
            getKuduQualifiedName(getMetadataTableName(metadataType)),
            e);
      }
    }
    return new KuduMetadataWriter(this, metadataType);
  }

  @Override
  public MetadataReader createMetadataReader(final MetadataType metadataType) {
    return new KuduMetadataReader(this, metadataType);
  }

  @Override
  public MetadataDeleter createMetadataDeleter(final MetadataType metadataType) {
    return new KuduMetadataDeleter(this, metadataType);
  }

  @Override
  public <T> RowReader<T> createReader(final ReaderParams<T> readerParams) {
    return new KuduReader<>(readerParams, this, options.getStoreOptions().isVisibilityEnabled());
  }

  @Override
  public RowReader<GeoWaveRow> createReader(final RecordReaderParams readerParams) {
    return new KuduReader<>(readerParams, this, options.getStoreOptions().isVisibilityEnabled());
  }

  @Override
  public RowDeleter createRowDeleter(
      final String indexName,
      final PersistentAdapterStore adapterStore,
      final InternalAdapterStore internalAdapterStore,
      final String... authorizations) {
    return new KuduDeleter(this, indexName);
  }

  @Override
  public RowReader<GeoWaveRow> createReader(final DataIndexReaderParams readerParams) {
    return new KuduReader<>(readerParams, this, options.getStoreOptions().isVisibilityEnabled());
  }

  @Override
  public void delete(final DataIndexReaderParams readerParams) {
    final byte[][] dataIds = readerParams.getDataIds();
    final short adapterId = readerParams.getAdapterId();
    final String tableName = DataIndexUtils.DATA_ID_INDEX.getName();
    final KuduSession session = getSession();
    try {
      final KuduTable table = getTable(tableName);
      for (final byte[] dataId : dataIds) {
        final Delete delete = table.newDelete();
        final KuduDataIndexRow row = new KuduDataIndexRow(dataId, adapterId, null);
        row.populatePartialRowPrimaryKey(delete.getRow());
        final OperationResponse resp = session.apply(delete);
        if (resp.hasRowError()) {
          LOGGER.error("Encountered error while deleting row: {}", resp.getRowError());
        }
      }
    } catch (final KuduException e) {
      LOGGER.error("Encountered error while deleting row", e);
    } finally {
      try {
        session.close();
      } catch (final KuduException e) {
        LOGGER.error("Encountered error while closing Kudu session", e);
      }
    }
  }

  private boolean createIndexTable(final String indexName, final int numPartitions) {
    synchronized (CREATE_TABLE_MUTEX) {
      try {
        if (!indexExists(indexName)) {
          final List<ColumnSchema> columns = new ArrayList<>();
          final boolean isDataIndex = DataIndexUtils.isDataIndex(indexName);
          final String hashPartitionColumn;
          if (isDataIndex) {
            for (final KuduDataIndexField f : KuduDataIndexField.values()) {
              f.addColumn(columns);
            }
            hashPartitionColumn = KuduDataIndexField.GW_PARTITION_ID_KEY.getFieldName();
          } else {
            for (final KuduField f : KuduField.values()) {
              f.addColumn(columns);
            }
            hashPartitionColumn = KuduField.GW_PARTITION_ID_KEY.getFieldName();
          }
          client.createTable(
              getKuduQualifiedName(indexName),
              new Schema(columns),
              new CreateTableOptions().addHashPartitions(
                  Collections.singletonList(hashPartitionColumn),
                  Math.max(numPartitions, KuduUtils.KUDU_DEFAULT_BUCKETS)).setNumReplicas(
                      KuduUtils.KUDU_DEFAULT_REPLICAS));
          return true;
        }
      } catch (final IOException e) {
        LOGGER.error("Unable to create table '{}'", getKuduQualifiedName(indexName), e);
      }
      return false;
    }
  }

  public <T> KuduRangeRead<T> getKuduRangeRead(
      final String indexName,
      final short[] adapterIds,
      final Collection<SinglePartitionQueryRanges> ranges,
      final boolean rowMerging,
      final GeoWaveRowIteratorTransformer<T> rowTransformer,
      final Predicate<GeoWaveRow> rowFilter,
      final boolean visibilityEnabled) throws KuduException {
    final KuduTable table = getTable(indexName);
    return new KuduRangeRead<>(
        ranges,
        adapterIds,
        table,
        this,
        visibilityEnabled,
        rowFilter,
        rowTransformer,
        rowMerging);
  }

  @Override
  public <T> Deleter<T> createDeleter(final ReaderParams<T> readerParams) {
    // TODO use QueryAndDeleteByRow with a synchronous reader (all kudu readers are async now)
    // GEOWAVE Issue #1573
    return MapReduceDataStoreOperations.super.createDeleter(readerParams);
  }

  public <T> KuduDataIndexRead<T> getKuduDataIndexRead(
      final String indexName,
      final short adapterId,
      final byte[][] dataIds,
      final Predicate<GeoWaveRow> rowFilter,
      final boolean visibilityEnabled) throws KuduException {
    final KuduTable table = getTable(indexName);
    return new KuduDataIndexRead<>(adapterId, dataIds, table, this, visibilityEnabled, rowFilter);
  }

  public KuduScannerBuilder getScannerBuilder(final KuduTable table) {
    return client.newScannerBuilder(table);
  }

  public AsyncKuduScannerBuilder getAsyncScannerBuilder(final KuduTable table) {
    return asyncClient.newScannerBuilder(table);
  }

  public KuduTable getTable(final String tableName) throws KuduException {
    return client.openTable(getKuduQualifiedName(tableName));
  }

  public KuduSession getSession() {
    return client.newSession();
  }

  /**
   * Returns a modified table name that includes the geowave namespace.
   */
  private String getQualifiedName(final String name) {
    return (gwNamespace == null) ? name : gwNamespace + "_" + name;
  }

  private String getKuduSafeName(final String name) {
    if (name.length() > KUDU_IDENTIFIER_MAX_LENGTH) {
      return name.substring(0, KUDU_IDENTIFIER_MAX_LENGTH);
    }
    return name;
  }

  public String getKuduQualifiedName(final String name) {
    return getKuduSafeName(getQualifiedName(name));
  }

  public String getMetadataTableName(final MetadataType metadataType) {
    return metadataType.id() + "_" + AbstractGeoWavePersistence.METADATA_TABLE;
  }

  public List<Delete> getDeletions(
      final KuduTable table,
      final List<KuduPredicate> predicates,
      final Function<RowResult, PersistentKuduRow> adapter) throws KuduException {
    // TODO: Kudu Java API does not support deleting with predicates, so we first perform a scan and
    // then perform individual row deletions with the full primary key. This is inefficient, because
    // we need to read in entire rows in order to perform deletions.
    final KuduScannerBuilder scannerBuilder = getScannerBuilder(table);
    for (final KuduPredicate pred : predicates) {
      scannerBuilder.addPredicate(pred);
    }
    final KuduScanner scanner = scannerBuilder.build();
    final List<RowResultIterator> allResults = new ArrayList<>();
    while (scanner.hasMoreRows()) {
      allResults.add(scanner.nextRows());
    }
    final Iterator<Delete> deletions =
        Streams.stream(Iterators.concat(allResults.iterator())).map(result -> {
          final PersistentKuduRow row = adapter.apply(result);
          final Delete delete = table.newDelete();
          row.populatePartialRowPrimaryKey(delete.getRow());
          return delete;
        }).iterator();
    return Lists.newArrayList(deletions);
  }
}
