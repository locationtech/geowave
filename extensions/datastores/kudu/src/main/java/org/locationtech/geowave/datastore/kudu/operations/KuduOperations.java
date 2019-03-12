/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.kudu.operations;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.*;
import org.apache.kudu.client.KuduScanner.KuduScannerBuilder;
import org.locationtech.geowave.core.index.SinglePartitionQueryRanges;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.base.dataidx.DataIndexUtils;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.operations.*;
import org.locationtech.geowave.datastore.kudu.KuduRow;
import org.locationtech.geowave.datastore.kudu.config.KuduOptions;
import org.locationtech.geowave.datastore.kudu.config.KuduRequiredOptions;
import org.locationtech.geowave.mapreduce.MapReduceDataStoreOperations;
import org.locationtech.geowave.mapreduce.splits.RecordReaderParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.function.Predicate;
import static org.locationtech.geowave.datastore.kudu.KuduRow.*;

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
    return true;
  }

  @Override
  public boolean metadataExists(final MetadataType type) throws IOException {
    return true;
  }

  @Override
  public void deleteAll() throws Exception {}

  @Override
  public boolean deleteAll(
      final String indexName,
      final String typeName,
      final Short adapterId,
      final String... additionalAuthorizations) {
    try {
      KuduSession session = this.getSession();
      Delete delete = this.getDelete(indexName);
      PartialRow partialRow = delete.getRow();
      addAdapterIdToPartialRow(partialRow, null, adapterId);
      session.apply(delete);
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
    createTable(index.getName());
    return new KuduWriter(index.getName(), this);
  }

  @Override
  public RowWriter createDataIndexWriter(final InternalDataAdapter<?> adapter) {
    return null;
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
    return null;
  }

  @Override
  public <T> RowReader<T> createReader(final ReaderParams<T> readerParams) {
    return null;
  }

  @Override
  public <T> Deleter<T> createDeleter(final ReaderParams<T> readerParams) {
    return null;
  }

  @Override
  public RowReader<GeoWaveRow> createReader(final RecordReaderParams readerParams) {
    return null;
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
    // TODO: check data_id vs partition_id for delete
    try {
      byte[][] dataIds = readerParams.getDataIds();
      short adapterId = readerParams.getAdapterId();
      KuduSession session = this.getSession();
      String tableName = DataIndexUtils.DATA_ID_INDEX.getName();
      for (byte[] dataId : dataIds) {
        Delete delete = this.getDelete(tableName);
        PartialRow partialRow = delete.getRow();
        addAdapterIdToPartialRow(partialRow, dataId, adapterId);
        session.apply(delete);
      }
    } catch (KuduException e) {
      LOGGER.error("Encountered error while deleting row", e);
    }
  }

  public KuduSession getSession() {
    return client.newSession();
  }

  private boolean createTable(final String indexName) {
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
          // TODO: get number of buckets based on num partitions specified for index
          client.createTable(
              tableName,
              new Schema(columns),
              new CreateTableOptions().addHashPartitions(
                  Collections.singletonList(KuduField.GW_PARTITION_ID_KEY.getFieldName()),
                  1));
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

  public Insert getInsert(String tableName) throws KuduException {
    KuduTable table = client.openTable(getKuduSafeName(tableName));
    return table.newInsert();
  }

  public Delete getDelete(String tableName) throws KuduException {
    KuduTable table = client.openTable(getKuduSafeName(tableName));
    return table.newDelete();
  }

  public void addToPartialRow(
      GeoWaveRow row,
      GeoWaveValue value,
      PartialRow partialRow,
      ByteBuffer nanoBuffer) {
    byte[] partitionKey = row.getPartitionKey();
    short adapterId = row.getAdapterId();
    byte[] sortKey = row.getSortKey();
    byte[] dataId = row.getDataId();
    int numDuplicates = row.getNumberOfDuplicates();
    partialRow.addBinary(KuduField.GW_PARTITION_ID_KEY.getFieldName(), partitionKey);
    partialRow.addShort(KuduField.GW_ADAPTER_ID_KEY.getFieldName(), adapterId);
    partialRow.addBinary(KuduField.GW_SORT_KEY.getFieldName(), sortKey);
    partialRow.addBinary(KuduField.GW_DATA_ID_KEY.getFieldName(), dataId);
    partialRow.addBinary(KuduField.GW_FIELD_VISIBILITY_KEY.getFieldName(), value.getVisibility());
    partialRow.addBinary(KuduField.GW_FIELD_MASK_KEY.getFieldName(), value.getFieldMask());
    partialRow.addBinary(KuduField.GW_VALUE_KEY.getFieldName(), value.getValue());
    partialRow.addByte(KuduField.GW_NUM_DUPLICATES_KEY.getFieldName(), (byte) numDuplicates);
    if (nanoBuffer != null) {
      partialRow.addBinary(KuduField.GW_NANO_TIME_KEY.getFieldName(), nanoBuffer);
    }
  }

  public void addAdapterIdToPartialRow(PartialRow partialRow, byte[] dataId, short adapterId) {
    if (dataId != null) {
      partialRow.addBinary(KuduField.GW_DATA_ID_KEY.getFieldName(), dataId);
    }
    partialRow.addShort(KuduField.GW_ADAPTER_ID_KEY.getFieldName(), adapterId);
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

  public KuduTable getTable(String tableName) throws KuduException {
    return client.openTable(getKuduSafeName(tableName));
  }

  public KuduScannerBuilder getScannerBuilder(KuduTable table) {
    return client.newScannerBuilder(table);
  }

}
