/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.dynamodb.operations;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.base.dataidx.DataIndexUtils;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.metadata.AbstractGeoWavePersistence;
import org.locationtech.geowave.core.store.operations.DataIndexReaderParams;
import org.locationtech.geowave.core.store.operations.MetadataDeleter;
import org.locationtech.geowave.core.store.operations.MetadataReader;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.core.store.operations.MetadataWriter;
import org.locationtech.geowave.core.store.operations.ReaderParams;
import org.locationtech.geowave.core.store.operations.RowDeleter;
import org.locationtech.geowave.core.store.operations.RowReader;
import org.locationtech.geowave.core.store.operations.RowReaderWrapper;
import org.locationtech.geowave.core.store.operations.RowWriter;
import org.locationtech.geowave.core.store.query.filter.ClientVisibilityFilter;
import org.locationtech.geowave.datastore.dynamodb.DynamoDBClientPool;
import org.locationtech.geowave.datastore.dynamodb.DynamoDBRow;
import org.locationtech.geowave.datastore.dynamodb.config.DynamoDBOptions;
import org.locationtech.geowave.mapreduce.MapReduceDataStoreOperations;
import org.locationtech.geowave.mapreduce.splits.RecordReaderParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemResult;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableStatus;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.amazonaws.services.dynamodbv2.util.TableUtils.TableNeverTransitionedToStateException;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;

public class DynamoDBOperations implements MapReduceDataStoreOperations {
  private final Logger LOGGER = LoggerFactory.getLogger(DynamoDBOperations.class);
  public static final int MAX_ROWS_FOR_BATCHGETITEM = 100;

  public static final int MAX_ROWS_FOR_BATCHWRITER = 25;

  public static final String METADATA_PRIMARY_ID_KEY = "I";
  public static final String METADATA_SECONDARY_ID_KEY = "S";
  public static final String METADATA_TIMESTAMP_KEY = "T";
  public static final String METADATA_VISIBILITY_KEY = "A";
  public static final String METADATA_VALUE_KEY = "V";

  private final AmazonDynamoDBAsync client;
  private final String gwNamespace;
  private final DynamoDBOptions options;
  public static Map<String, Boolean> tableExistsCache = new HashMap<>();

  public DynamoDBOperations(final DynamoDBOptions options) {
    this.options = options;
    client = DynamoDBClientPool.getInstance().getClient(options);
    gwNamespace = options.getGeoWaveNamespace();
  }

  public static DynamoDBOperations createOperations(final DynamoDBOptions options)
      throws IOException {
    return new DynamoDBOperations(options);
  }

  public DynamoDBOptions getOptions() {
    return options;
  }

  public AmazonDynamoDBAsync getClient() {
    return client;
  }

  public String getQualifiedTableName(final String tableName) {
    return gwNamespace == null ? tableName : gwNamespace + "_" + tableName;
  }

  public String getMetadataTableName(final MetadataType metadataType) {
    final String tableName = metadataType.id() + "_" + AbstractGeoWavePersistence.METADATA_TABLE;
    return getQualifiedTableName(tableName);
  }

  @Override
  public void deleteAll() throws Exception {
    final ListTablesResult tables = client.listTables();
    for (final String tableName : tables.getTableNames()) {
      if ((gwNamespace == null) || tableName.startsWith(gwNamespace)) {
        client.deleteTable(new DeleteTableRequest(tableName));
      }
    }
    tableExistsCache.clear();
  }

  @Override
  public boolean indexExists(final String indexName) throws IOException {
    try {
      return TableStatus.ACTIVE.name().equals(
          client.describeTable(getQualifiedTableName(indexName)).getTable().getTableStatus());
    } catch (final AmazonDynamoDBException e) {
      LOGGER.info("Unable to check existence of table", e);
    }
    return false;
  }

  @Override
  public boolean deleteAll(
      final String indexName,
      final String typeName,
      final Short adapterId,
      final String... additionalAuthorizations) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public RowWriter createWriter(final Index index, final InternalDataAdapter<?> adapter) {
    final boolean isDataIndex = DataIndexUtils.isDataIndex(index.getName());
    String qName = getQualifiedTableName(index.getName());
    if (isDataIndex) {
      qName = adapter.getTypeName() + "_" + qName;
    }
    final DynamoDBWriter writer = new DynamoDBWriter(client, qName, isDataIndex);

    createTable(qName, isDataIndex);
    return writer;
  }

  @Override
  public RowWriter createDataIndexWriter(final InternalDataAdapter<?> adapter) {
    return createWriter(DataIndexUtils.DATA_ID_INDEX, adapter);
  }

  @Override
  public void delete(final DataIndexReaderParams readerParams) {
    final String typeName =
        readerParams.getInternalAdapterStore().getTypeName(readerParams.getAdapterId());
    if (typeName == null) {
      return;
    }
    deleteRowsFromDataIndex(readerParams.getDataIds(), readerParams.getAdapterId(), typeName);
  }

  public void deleteRowsFromDataIndex(
      final byte[][] dataIds,
      final short adapterId,
      final String typeName) {
    final String tableName =
        typeName + "_" + getQualifiedTableName(DataIndexUtils.DATA_ID_INDEX.getName());
    final Iterator<byte[]> dataIdIterator = Arrays.stream(dataIds).iterator();
    while (dataIdIterator.hasNext()) {
      final List<WriteRequest> deleteRequests = new ArrayList<>();
      int i = 0;
      while (dataIdIterator.hasNext() && (i < MAX_ROWS_FOR_BATCHWRITER)) {
        deleteRequests.add(
            new WriteRequest(
                new DeleteRequest(
                    Collections.singletonMap(
                        DynamoDBRow.GW_PARTITION_ID_KEY,
                        new AttributeValue().withB(ByteBuffer.wrap(dataIdIterator.next()))))));
        i++;
      }

      client.batchWriteItem(Collections.singletonMap(tableName, deleteRequests));
    }
  }

  @Override
  public RowReader<GeoWaveRow> createReader(final DataIndexReaderParams readerParams) {
    final String typeName =
        readerParams.getInternalAdapterStore().getTypeName(readerParams.getAdapterId());
    if (typeName == null) {
      return new RowReaderWrapper<>(new CloseableIterator.Empty<GeoWaveRow>());
    }
    byte[][] dataIds;
    Iterator<GeoWaveRow> iterator;
    if (readerParams.getDataIds() != null) {
      dataIds = readerParams.getDataIds();
      iterator = getRowsFromDataIndex(dataIds, readerParams.getAdapterId(), typeName);
    } else {
      if ((readerParams.getStartInclusiveDataId() != null)
          || (readerParams.getEndInclusiveDataId() != null)) {
        final List<byte[]> intermediaries = new ArrayList<>();
        ByteArrayUtils.addAllIntermediaryByteArrays(
            intermediaries,
            new ByteArrayRange(
                readerParams.getStartInclusiveDataId(),
                readerParams.getEndInclusiveDataId()));
        dataIds = intermediaries.toArray(new byte[0][]);
        iterator = getRowsFromDataIndex(dataIds, readerParams.getAdapterId(), typeName);
      } else {
        iterator = getRowsFromDataIndex(readerParams.getAdapterId(), typeName);
      }
    }
    if (options.getBaseOptions().isVisibilityEnabled()) {
      Stream<GeoWaveRow> stream = Streams.stream(iterator);
      final Set<String> authorizations =
          Sets.newHashSet(readerParams.getAdditionalAuthorizations());
      stream = stream.filter(new ClientVisibilityFilter(authorizations));
      iterator = stream.iterator();
    }
    return new RowReaderWrapper<>(new CloseableIterator.Wrapper<>(iterator));
  }

  public Iterator<GeoWaveRow> getRowsFromDataIndex(final short adapterId, final String typeName) {
    final List<GeoWaveRow> resultList = new ArrayList<>();
    // fill result list
    ScanResult result =
        getResults(
            typeName + "_" + getQualifiedTableName(DataIndexUtils.DATA_ID_INDEX.getName()),
            adapterId,
            resultList,
            null);
    while ((result.getLastEvaluatedKey() != null) && !result.getLastEvaluatedKey().isEmpty()) {
      result =
          getResults(
              typeName + "_" + getQualifiedTableName(DataIndexUtils.DATA_ID_INDEX.getName()),
              adapterId,
              resultList,
              result.getLastEvaluatedKey());
    }
    return resultList.iterator();
  }

  public Iterator<GeoWaveRow> getRowsFromDataIndex(
      final byte[][] dataIds,
      final short adapterId,
      final String typeName) {
    final Map<ByteArray, GeoWaveRow> resultMap = new HashMap<>();
    final Iterator<byte[]> dataIdIterator = Arrays.stream(dataIds).iterator();
    while (dataIdIterator.hasNext()) {
      // fill result map
      final Collection<Map<String, AttributeValue>> dataIdsForRequest = new ArrayList<>();
      int i = 0;
      while (dataIdIterator.hasNext() && (i < MAX_ROWS_FOR_BATCHGETITEM)) {
        dataIdsForRequest.add(
            Collections.singletonMap(
                DynamoDBRow.GW_PARTITION_ID_KEY,
                new AttributeValue().withB(ByteBuffer.wrap(dataIdIterator.next()))));
        i++;
      }
      BatchGetItemResult result =
          getResults(
              Collections.singletonMap(
                  typeName + "_" + getQualifiedTableName(DataIndexUtils.DATA_ID_INDEX.getName()),
                  new KeysAndAttributes().withKeys(dataIdsForRequest)),
              adapterId,
              resultMap);
      while (!result.getUnprocessedKeys().isEmpty()) {
        result = getResults(result.getUnprocessedKeys(), adapterId, resultMap);
      }
    }
    return Arrays.stream(dataIds).map(d -> resultMap.get(new ByteArray(d))).filter(
        r -> r != null).iterator();
  }

  private ScanResult getResults(
      final String tableName,
      final short adapterId,
      final List<GeoWaveRow> resultList,
      final Map<String, AttributeValue> lastEvaluatedKey) {
    final ScanRequest request = new ScanRequest(tableName);
    if ((lastEvaluatedKey != null) && !lastEvaluatedKey.isEmpty()) {
      request.setExclusiveStartKey(lastEvaluatedKey);
    }
    final ScanResult result = client.scan(request);
    result.getItems().forEach(objMap -> {
      final byte[] dataId = objMap.get(DynamoDBRow.GW_PARTITION_ID_KEY).getB().array();
      final AttributeValue valueAttr = objMap.get(DynamoDBRow.GW_VALUE_KEY);
      final byte[] value = valueAttr == null ? null : valueAttr.getB().array();
      final AttributeValue visAttr = objMap.get(DynamoDBRow.GW_VISIBILITY_KEY);
      final byte[] vis = visAttr == null ? new byte[0] : visAttr.getB().array();

      resultList.add(DataIndexUtils.deserializeDataIndexRow(dataId, adapterId, value, vis));
    });
    return result;
  }

  private BatchGetItemResult getResults(
      final Map<String, KeysAndAttributes> requestItems,
      final short adapterId,
      final Map<ByteArray, GeoWaveRow> resultMap) {
    final BatchGetItemRequest request = new BatchGetItemRequest(requestItems);

    final BatchGetItemResult result = client.batchGetItem(request);
    result.getResponses().values().forEach(results -> results.stream().forEach(objMap -> {
      final byte[] dataId = objMap.get(DynamoDBRow.GW_PARTITION_ID_KEY).getB().array();
      final AttributeValue valueAttr = objMap.get(DynamoDBRow.GW_VALUE_KEY);
      final byte[] value = valueAttr == null ? null : valueAttr.getB().array();
      final AttributeValue visAttr = objMap.get(DynamoDBRow.GW_VISIBILITY_KEY);
      final byte[] vis = visAttr == null ? new byte[0] : visAttr.getB().array();
      resultMap.put(
          new ByteArray(dataId),
          DataIndexUtils.deserializeDataIndexRow(dataId, adapterId, value, vis));
    }));
    return result;
  }

  private boolean createTable(final String qName, final boolean dataIndexTable) {
    return createTable(
        qName,
        dataIndexTable
            ? () -> new CreateTableRequest().withTableName(qName).withAttributeDefinitions(
                new AttributeDefinition(
                    DynamoDBRow.GW_PARTITION_ID_KEY,
                    ScalarAttributeType.B)).withKeySchema(
                        new KeySchemaElement(DynamoDBRow.GW_PARTITION_ID_KEY, KeyType.HASH))
            : () -> new CreateTableRequest().withTableName(qName).withAttributeDefinitions(
                new AttributeDefinition(DynamoDBRow.GW_PARTITION_ID_KEY, ScalarAttributeType.B),
                new AttributeDefinition(
                    DynamoDBRow.GW_RANGE_KEY,
                    ScalarAttributeType.B)).withKeySchema(
                        new KeySchemaElement(DynamoDBRow.GW_PARTITION_ID_KEY, KeyType.HASH),
                        new KeySchemaElement(DynamoDBRow.GW_RANGE_KEY, KeyType.RANGE)));
  }

  private boolean createTable(final String qName, final Supplier<CreateTableRequest> tableRequest) {
    synchronized (tableExistsCache) {
      final Boolean tableExists = tableExistsCache.get(qName);
      if ((tableExists == null) || !tableExists) {
        final boolean tableCreated =
            TableUtils.createTableIfNotExists(
                client,
                tableRequest.get().withProvisionedThroughput(
                    new ProvisionedThroughput(
                        Long.valueOf(options.getReadCapacity()),
                        Long.valueOf(options.getWriteCapacity()))));
        if (tableCreated) {
          try {
            TableUtils.waitUntilActive(client, qName);
          } catch (TableNeverTransitionedToStateException | InterruptedException e) {
            LOGGER.error("Unable to wait for active table '" + qName + "'", e);
          }
        }
        tableExistsCache.put(qName, true);
        return true;
      }
    }
    return false;
  }

  public void dropMetadataTable(MetadataType type) {
    String tableName = getMetadataTableName(type);
    synchronized (DynamoDBOperations.tableExistsCache) {
      final Boolean tableExists = DynamoDBOperations.tableExistsCache.get(tableName);
      if (tableExists == null || tableExists) {
        final boolean tableDropped =
            TableUtils.deleteTableIfExists(client, new DeleteTableRequest(tableName));
        if (tableDropped) {
          DynamoDBOperations.tableExistsCache.put(tableName, false);
        }
      }
    }
  }

  public void ensureTableExists(final String tableName) {
    synchronized (DynamoDBOperations.tableExistsCache) {
      final Boolean tableExists = DynamoDBOperations.tableExistsCache.get(tableName);
      if ((tableExists == null) || !tableExists) {
        final boolean tableCreated =
            TableUtils.createTableIfNotExists(client, new CreateTableRequest() //
                .withTableName(tableName) //
                .withAttributeDefinitions(
                    new AttributeDefinition(METADATA_PRIMARY_ID_KEY, ScalarAttributeType.B)) //
                .withKeySchema(new KeySchemaElement(METADATA_PRIMARY_ID_KEY, KeyType.HASH)) //
                .withAttributeDefinitions(
                    new AttributeDefinition(METADATA_TIMESTAMP_KEY, ScalarAttributeType.N)) //
                .withKeySchema(new KeySchemaElement(METADATA_TIMESTAMP_KEY, KeyType.RANGE)) //
                .withProvisionedThroughput(
                    new ProvisionedThroughput(Long.valueOf(5), Long.valueOf(5))));
        if (tableCreated) {
          try {
            TableUtils.waitUntilActive(client, tableName);
          } catch (TableNeverTransitionedToStateException | InterruptedException e) {
            LOGGER.error("Unable to wait for active table '" + tableName + "'", e);
          }
        }
        DynamoDBOperations.tableExistsCache.put(tableName, true);
      }
    }
  }

  @Override
  public MetadataWriter createMetadataWriter(final MetadataType metadataType) {
    final String tableName = getMetadataTableName(metadataType);
    ensureTableExists(tableName);
    return new DynamoDBMetadataWriter(this, tableName);
  }

  @Override
  public MetadataReader createMetadataReader(final MetadataType metadataType) {
    final String tableName = getMetadataTableName(metadataType);
    ensureTableExists(tableName);
    return new DynamoDBMetadataReader(this, metadataType);
  }

  @Override
  public MetadataDeleter createMetadataDeleter(final MetadataType metadataType) {
    final String tableName = getMetadataTableName(metadataType);
    ensureTableExists(tableName);
    return new DynamoDBMetadataDeleter(this, metadataType);
  }

  @Override
  public <T> RowReader<T> createReader(final ReaderParams<T> readerParams) {
    return new DynamoDBReader<>(readerParams, this, options.getBaseOptions().isVisibilityEnabled());
  }

  @Override
  public RowReader<GeoWaveRow> createReader(final RecordReaderParams recordReaderParams) {
    return new DynamoDBReader<>(
        recordReaderParams,
        this,
        options.getBaseOptions().isVisibilityEnabled());
  }

  @Override
  public RowDeleter createRowDeleter(
      final String indexName,
      final PersistentAdapterStore adapterStore,
      final InternalAdapterStore internalAdapterStore,
      final String... authorizations) {
    return new DynamoDBDeleter(this, getQualifiedTableName(indexName));
  }

  @Override
  public boolean metadataExists(final MetadataType type) throws IOException {
    try {
      return TableStatus.ACTIVE.name().equals(
          client.describeTable(getMetadataTableName(type)).getTable().getTableStatus());
    } catch (final AmazonDynamoDBException e) {
      LOGGER.info("Unable to check existence of table", e);
    }
    return false;
  }

  public boolean createIndex(final Index index) throws IOException {
    final String indexName = index.getName();
    return createTable(getQualifiedTableName(indexName), DataIndexUtils.isDataIndex(indexName));
  }
}
