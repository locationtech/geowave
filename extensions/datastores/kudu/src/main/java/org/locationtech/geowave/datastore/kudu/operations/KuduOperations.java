/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.kudu.operations;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.Delete;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduScanner.KuduScannerBuilder;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
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
import org.locationtech.geowave.core.store.operations.MetadataDeleter;
import org.locationtech.geowave.core.store.operations.MetadataReader;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.core.store.operations.MetadataWriter;
import org.locationtech.geowave.core.store.operations.ReaderParams;
import org.locationtech.geowave.core.store.operations.RowDeleter;
import org.locationtech.geowave.core.store.operations.RowReader;
import org.locationtech.geowave.core.store.operations.RowWriter;
import org.locationtech.geowave.datastore.kudu.KuduRow;
import org.locationtech.geowave.datastore.kudu.PersistentKuduRow;
import org.locationtech.geowave.datastore.kudu.config.KuduRequiredOptions;
import org.locationtech.geowave.mapreduce.MapReduceDataStoreOperations;
import org.locationtech.geowave.mapreduce.splits.RecordReaderParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import static org.locationtech.geowave.datastore.kudu.KuduRow.KuduField;

public class KuduOperations implements MapReduceDataStoreOperations {
  private static final Logger LOGGER = LoggerFactory.getLogger(KuduOperations.class);
  private static final int KUDU_IDENTIFIER_MAX_LENGTH = 256;

  private final String gwNamespace;
  private final KuduRequiredOptions options;

  private final KuduClient client;
  private final Object CREATE_TABLE_MUTEX = new Object();

  public KuduOperations(final KuduRequiredOptions options) {
    if ((options.getGeoWaveNamespace() == null) || options.getGeoWaveNamespace().equals("")) {
      gwNamespace = "geowave";
    } else {
      gwNamespace = options.getGeoWaveNamespace();
    }
    this.options = options;
    client = new KuduClient.KuduClientBuilder(options.getKuduMaster()).build();
  }

  @Override
  public boolean indexExists(final String indexName) throws IOException {
    String tableName = getKuduSafeName(indexName);
    return client.tableExists(tableName);
  }

  @Override
  public boolean metadataExists(final MetadataType type) throws IOException {
    return indexExists(getMetadataTableName(type));
  }

  @Override
  public void deleteAll() throws Exception {
    for (String table : client.getTablesList().getTablesList()) {
      client.deleteTable(table);
    }
  }

  @Override
  public boolean deleteAll(
      final String indexName,
      final String typeName,
      final Short adapterId,
      final String... additionalAuthorizations) {
    try {
      KuduSession session = getSession();
      KuduTable table = getTable(indexName);
      Schema schema = table.getSchema();
      List<KuduPredicate> preds =
          Collections.singletonList(
              KuduPredicate.newComparisonPredicate(
                  schema.getColumn(KuduField.GW_ADAPTER_ID_KEY.getFieldName()),
                  KuduPredicate.ComparisonOp.EQUAL,
                  adapterId));
      for (Delete delete : getDeletions(table, preds, KuduRow::new)) {
        session.apply(delete);
      }
    } catch (KuduException e) {
      LOGGER.error("Encountered error while deleting all", e);
    }
    return true;
  }

  @Override
  public boolean ensureAuthorizations(final String clientUser, final String... authorizations) {
    return true;
  }

  @Override
  public RowWriter createWriter(final Index index, final InternalDataAdapter<?> adapter) {
    createTable(index.getName(), index.getIndexStrategy().getPredefinedSplits().length);
    return new KuduWriter(index.getName(), this);
  }

  @Override
  public RowWriter createDataIndexWriter(final InternalDataAdapter<?> adapter) {
    return createWriter(DataIndexUtils.DATA_ID_INDEX, adapter);
  }

  @Override
  public MetadataWriter createMetadataWriter(final MetadataType metadataType) {
    return null;
  }

  @Override
  public MetadataReader createMetadataReader(final MetadataType metadataType) {
    return null;
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
    return null;
  }

  @Override
  public void delete(final DataIndexReaderParams readerParams) {
    try {
      byte[][] dataIds = readerParams.getDataIds();
      short adapterId = readerParams.getAdapterId();
      KuduSession session = getSession();
      String tableName = DataIndexUtils.DATA_ID_INDEX.getName();
      KuduTable table = getTable(tableName);
      for (byte[] dataId : dataIds) {
        Delete delete = table.newDelete();
        PartialRow partialRow = delete.getRow();
        partialRow.addBinary(KuduField.GW_PARTITION_ID_KEY.getFieldName(), dataId);
        partialRow.addShort(KuduField.GW_ADAPTER_ID_KEY.getFieldName(), adapterId);
        session.apply(delete);
      }
    } catch (KuduException e) {
      LOGGER.error("Encountered error while deleting row", e);
    }
  }

  public KuduSession getSession() {
    return client.newSession();
  }

  private boolean createTable(final String indexName, int numPartitions) {
    final String tableName = getKuduSafeName(indexName);
    synchronized (CREATE_TABLE_MUTEX) {
      try {
        if (!indexExists(indexName)) {
          List<ColumnSchema> columns = new ArrayList<>();
          KuduField[] fields = KuduField.values();
          if (DataIndexUtils.isDataIndex(tableName)) {
            fields =
                Arrays.stream(fields).filter(KuduField::isDataIndexColumn).toArray(
                    KuduField[]::new);
          }
          for (final KuduField f : fields) {
            f.addColumn(columns);
          }
          client.createTable(
              tableName,
              new Schema(columns),
              new CreateTableOptions().addHashPartitions(
                  Collections.singletonList(KuduField.GW_PARTITION_ID_KEY.getFieldName()),
                  numPartitions));
          return true;
        }
      } catch (final IOException e) {
        LOGGER.error("Unable to create table '" + indexName + "'", e);
      }
      return false;
    }
  }

  private static String getKuduSafeName(final String name) {
    if (name.length() > KUDU_IDENTIFIER_MAX_LENGTH) {
      return name.substring(0, KUDU_IDENTIFIER_MAX_LENGTH);
    }
    return name;
  }

  public KuduTable getTable(String tableName) throws KuduException {
    return client.openTable(getKuduSafeName(tableName));
  }

  public <T> KuduRangeRead getKuduRangeRead(
      final String tableName,
      final short[] adapterIds,
      final Collection<SinglePartitionQueryRanges> ranges,
      final boolean rowMerging,
      final GeoWaveRowIteratorTransformer<T> rowTransformer,
      final Predicate<GeoWaveRow> rowFilter,
      final boolean visibilityEnabled) throws KuduException {
    KuduTable table = getTable(tableName);
    return new KuduRangeRead(
        ranges,
        adapterIds,
        table,
        this,
        visibilityEnabled,
        rowFilter,
        rowTransformer,
        rowMerging);
  }

  public KuduScannerBuilder getScannerBuilder(KuduTable table) {
    return client.newScannerBuilder(table);
  }

  public String getMetadataTableName(final MetadataType metadataType) {
    final String tableName = metadataType.name() + "_" + AbstractGeoWavePersistence.METADATA_TABLE;
    return tableName;
  }

  public List<Delete> getDeletions(
      KuduTable table,
      List<KuduPredicate> predicates,
      Function<RowResult, PersistentKuduRow> adapter) throws KuduException {
    // TODO: Kudu Java API does not support deleting with predicates, so we first perform a scan and
    // then perform individual row deletions with the full primary key. This is inefficient, because
    // we need to read in entire rows in order to perform deletions.
    KuduScannerBuilder scannerBuilder = getScannerBuilder(table);
    for (KuduPredicate pred : predicates) {
      scannerBuilder.addPredicate(pred);
    }
    KuduScanner scanner = scannerBuilder.build();
    List<RowResultIterator> allResults = new ArrayList<>();
    while (scanner.hasMoreRows()) {
      allResults.add(scanner.nextRows());
    }
    Iterator<Delete> deletions =
        Streams.stream(Iterators.concat(allResults.iterator())).map(result -> {
          PersistentKuduRow row = adapter.apply(result);
          Delete delete = table.newDelete();
          row.populatePartialRowPrimaryKey(delete.getRow());
          return delete;
        }).iterator();
    return Lists.newArrayList(deletions);
  }

}
