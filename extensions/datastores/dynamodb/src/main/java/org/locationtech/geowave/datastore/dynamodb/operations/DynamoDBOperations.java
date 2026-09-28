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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
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
import org.locationtech.geowave.datastore.dynamodb.util.DynamoDBUtils;
import org.locationtech.geowave.mapreduce.MapReduceDataStoreOperations;
import org.locationtech.geowave.mapreduce.splits.RecordReaderParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;
import software.amazon.awssdk.services.dynamodb.waiters.DynamoDbWaiter;

public class DynamoDBOperations implements MapReduceDataStoreOperations {
  private final Logger LOGGER = LoggerFactory.getLogger(DynamoDBOperations.class);
  public static final int MAX_ROWS_FOR_BATCHGETITEM = 100;

  public static final int MAX_ROWS_FOR_BATCHWRITER = 25;

  public static final String METADATA_PRIMARY_ID_KEY = "I";
  public static final String METADATA_SECONDARY_ID_KEY = "S";
  public static final String METADATA_TIMESTAMP_KEY = "T";
  public static final String METADATA_VISIBILITY_KEY = "A";
  public static final String METADATA_VALUE_KEY = "V";

  private final DynamoDbClient client;
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

  public DynamoDbClient getClient() {
    return client;
  }

  public String getQualifiedTableName(final String tableName) {
    return gwNamespace == null ? tableName : gwNamespace + "_" + tableName;
  }

  private String getDataIndexTableName(final String typeName) {
    return typeName + "_" + getQualifiedTableName(DataIndexUtils.DATA_ID_INDEX.getName());
  }

  public String getMetadataTableName(final MetadataType metadataType) {
    final String tableName = metadataType.id() + "_" + AbstractGeoWavePersistence.METADATA_TABLE;
    return getQualifiedTableName(tableName);
  }

  @Override
  public void deleteAll() throws Exception {
    for (final String tableName : client.listTablesPaginator().tableNames()) {
      if ((gwNamespace == null) || tableName.startsWith(gwNamespace)) {
        client.deleteTable(b -> b.tableName(tableName));
      }
    }
    tableExistsCache.clear();
  }

  @Override
  public boolean indexExists(final String indexName) throws IOException {
    return isTableActive(getQualifiedTableName(indexName));
  }

  private boolean isTableActive(final String tableName) {
    try {
      return TableStatus.ACTIVE.equals(
          client.describeTable(b -> b.tableName(tableName)).table().tableStatus());
    } catch (final DynamoDbException e) {
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
    final String tableName = getDataIndexTableName(typeName);
    final Iterator<byte[]> dataIdIterator = Arrays.stream(dataIds).iterator();
    while (dataIdIterator.hasNext()) {
      final List<WriteRequest> deleteRequests = new ArrayList<>();
      int i = 0;
      while (dataIdIterator.hasNext() && (i < MAX_ROWS_FOR_BATCHWRITER)) {
        final Map<String, AttributeValue> key =
            Collections.singletonMap(
                DynamoDBRow.GW_PARTITION_ID_KEY,
                DynamoDBUtils.binaryValue(dataIdIterator.next()));
        deleteRequests.add(WriteRequest.builder().deleteRequest(d -> d.key(key)).build());
        i++;
      }

      DynamoDBUtils.batchWriteItem(client, Collections.singletonMap(tableName, deleteRequests));
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
    final String tableName = getDataIndexTableName(typeName);
    final List<GeoWaveRow> resultList = new ArrayList<>();
    for (final Map<String, AttributeValue> item : client.scanPaginator(
        b -> b.tableName(tableName)).items()) {
      resultList.add(toDataIndexRow(item, adapterId));
    }
    return resultList.iterator();
  }

  public Iterator<GeoWaveRow> getRowsFromDataIndex(
      final byte[][] dataIds,
      final short adapterId,
      final String typeName) {
    final Map<ByteArray, GeoWaveRow> resultMap = new HashMap<>();
    final Consumer<Map<String, AttributeValue>> addToResults = item -> {
      final GeoWaveRow row = toDataIndexRow(item, adapterId);
      resultMap.put(new ByteArray(row.getDataId()), row);
    };
    final Iterator<byte[]> dataIdIterator = Arrays.stream(dataIds).iterator();
    while (dataIdIterator.hasNext()) {
      // fill result map
      final Collection<Map<String, AttributeValue>> dataIdsForRequest = new ArrayList<>();
      int i = 0;
      while (dataIdIterator.hasNext() && (i < MAX_ROWS_FOR_BATCHGETITEM)) {
        dataIdsForRequest.add(
            Collections.singletonMap(
                DynamoDBRow.GW_PARTITION_ID_KEY,
                DynamoDBUtils.binaryValue(dataIdIterator.next())));
        i++;
      }
      DynamoDBUtils.batchGetItem(
          client,
          Collections.singletonMap(
              getDataIndexTableName(typeName),
              KeysAndAttributes.builder().keys(dataIdsForRequest).build()),
          addToResults);
    }
    return Arrays.stream(dataIds).map(d -> resultMap.get(new ByteArray(d))).filter(
        r -> r != null).iterator();
  }

  private static GeoWaveRow toDataIndexRow(
      final Map<String, AttributeValue> item,
      final short adapterId) {
    final byte[] vis = DynamoDBUtils.bytes(item.get(DynamoDBRow.GW_VISIBILITY_KEY));
    return DataIndexUtils.deserializeDataIndexRow(
        DynamoDBUtils.bytes(item.get(DynamoDBRow.GW_PARTITION_ID_KEY)),
        adapterId,
        DynamoDBUtils.bytes(item.get(DynamoDBRow.GW_VALUE_KEY)),
        vis == null ? new byte[0] : vis);
  }

  static CreateTableRequest indexTableRequest(
      final String qName,
      final boolean dataIndexTable,
      final long readCapacity,
      final long writeCapacity) {
    final CreateTableRequest.Builder request =
        CreateTableRequest.builder().tableName(qName).provisionedThroughput(
            t -> t.readCapacityUnits(readCapacity).writeCapacityUnits(writeCapacity));
    if (dataIndexTable) {
      return request.attributeDefinitions(
          binaryAttribute(DynamoDBRow.GW_PARTITION_ID_KEY)).keySchema(
              key(DynamoDBRow.GW_PARTITION_ID_KEY, KeyType.HASH)).build();
    }
    return request.attributeDefinitions(
        binaryAttribute(DynamoDBRow.GW_PARTITION_ID_KEY),
        binaryAttribute(DynamoDBRow.GW_RANGE_KEY)).keySchema(
            key(DynamoDBRow.GW_PARTITION_ID_KEY, KeyType.HASH),
            key(DynamoDBRow.GW_RANGE_KEY, KeyType.RANGE)).build();
  }

  /** Keyed on the primary ID and the write timestamp together. */
  static CreateTableRequest metadataTableRequest(final String tableName) {
    return CreateTableRequest.builder().tableName(tableName).attributeDefinitions(
        binaryAttribute(METADATA_PRIMARY_ID_KEY),
        AttributeDefinition.builder().attributeName(METADATA_TIMESTAMP_KEY).attributeType(
            ScalarAttributeType.N).build()).keySchema(
                key(METADATA_PRIMARY_ID_KEY, KeyType.HASH),
                key(METADATA_TIMESTAMP_KEY, KeyType.RANGE)).provisionedThroughput(
                    t -> t.readCapacityUnits(5L).writeCapacityUnits(5L)).build();
  }

  private static AttributeDefinition binaryAttribute(final String name) {
    return AttributeDefinition.builder().attributeName(name).attributeType(
        ScalarAttributeType.B).build();
  }

  private static KeySchemaElement key(final String name, final KeyType type) {
    return KeySchemaElement.builder().attributeName(name).keyType(type).build();
  }

  private boolean createTable(final String qName, final boolean dataIndexTable) {
    synchronized (tableExistsCache) {
      final Boolean tableExists = tableExistsCache.get(qName);
      if ((tableExists == null) || !tableExists) {
        createTableIfNotExists(
            indexTableRequest(
                qName,
                dataIndexTable,
                options.getReadCapacity(),
                options.getWriteCapacity()));
        tableExistsCache.put(qName, true);
        return true;
      }
    }
    return false;
  }

  private void createTableIfNotExists(final CreateTableRequest request) {
    try {
      client.createTable(request);
    } catch (final ResourceInUseException e) {
      // it already exists
      return;
    }
    try (DynamoDbWaiter waiter = client.waiter()) {
      waiter.waitUntilTableExists(b -> b.tableName(request.tableName()));
    } catch (final SdkClientException e) {
      LOGGER.error("Unable to wait for active table '" + request.tableName() + "'", e);
    }
  }

  public void dropMetadataTable(final MetadataType type) {
    final String tableName = getMetadataTableName(type);
    synchronized (DynamoDBOperations.tableExistsCache) {
      final Boolean tableExists = DynamoDBOperations.tableExistsCache.get(tableName);
      if ((tableExists == null) || tableExists) {
        try {
          client.deleteTable(b -> b.tableName(tableName));
          DynamoDBOperations.tableExistsCache.put(tableName, false);
        } catch (final ResourceNotFoundException e) {
          // already gone
        }
      }
    }
  }

  public void ensureTableExists(final String tableName) {
    synchronized (DynamoDBOperations.tableExistsCache) {
      final Boolean tableExists = DynamoDBOperations.tableExistsCache.get(tableName);
      if ((tableExists == null) || !tableExists) {
        createTableIfNotExists(metadataTableRequest(tableName));
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
    return isTableActive(getMetadataTableName(type));
  }

  public boolean createIndex(final Index index) throws IOException {
    final String indexName = index.getName();
    return createTable(getQualifiedTableName(indexName), DataIndexUtils.isDataIndex(indexName));
  }
}
