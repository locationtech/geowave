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
package org.locationtech.geowave.datastore.dynamodb.operations;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.entities.GeoWaveRowMergingIterator;
import org.locationtech.geowave.core.store.index.PrimaryIndex;
import org.locationtech.geowave.core.store.metadata.AbstractGeoWavePersistence;
import org.locationtech.geowave.core.store.operations.RowDeleter;
import org.locationtech.geowave.core.store.operations.Deleter;
import org.locationtech.geowave.core.store.operations.MetadataDeleter;
import org.locationtech.geowave.core.store.operations.MetadataReader;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.core.store.operations.MetadataWriter;
import org.locationtech.geowave.core.store.operations.QueryAndDeleteByRow;
import org.locationtech.geowave.core.store.operations.Reader;
import org.locationtech.geowave.core.store.operations.ReaderParams;
import org.locationtech.geowave.core.store.operations.Writer;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import org.locationtech.geowave.datastore.dynamodb.DynamoDBClientPool;
import org.locationtech.geowave.datastore.dynamodb.DynamoDBOptions;
import org.locationtech.geowave.datastore.dynamodb.DynamoDBRow;
import org.locationtech.geowave.datastore.dynamodb.util.LazyPaginatedScan;
import org.locationtech.geowave.mapreduce.MapReduceDataStoreOperations;
import org.locationtech.geowave.mapreduce.splits.RecordReaderParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsync;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableStatus;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.amazonaws.services.dynamodbv2.util.TableUtils.TableNeverTransitionedToStateException;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;

public class DynamoDBOperations implements
		MapReduceDataStoreOperations
{
	private final Logger LOGGER = LoggerFactory.getLogger(DynamoDBOperations.class);

	public static final String METADATA_PRIMARY_ID_KEY = "I";
	public static final String METADATA_SECONDARY_ID_KEY = "S";
	public static final String METADATA_TIMESTAMP_KEY = "T";
	public static final String METADATA_VISIBILITY_KEY = "A";
	public static final String METADATA_VALUE_KEY = "V";

	private final AmazonDynamoDBAsync client;
	private final String gwNamespace;
	private final DynamoDBOptions options;
	public static Map<String, Boolean> tableExistsCache = new HashMap<>();

	public DynamoDBOperations(
			final DynamoDBOptions options ) {
		this.options = options;
		client = DynamoDBClientPool.getInstance().getClient(
				options);
		gwNamespace = options.getGeowaveNamespace();
	}

	public static DynamoDBOperations createOperations(
			final DynamoDBOptions options )
			throws IOException {
		return new DynamoDBOperations(
				options);
	}

	public DynamoDBOptions getOptions() {
		return options;
	}

	public AmazonDynamoDBAsync getClient() {
		return client;
	}

	public String getQualifiedTableName(
			final String tableName ) {
		return gwNamespace == null ? tableName : gwNamespace + "_" + tableName;
	}

	public String getMetadataTableName(
			final MetadataType metadataType ) {
		final String tableName = metadataType.name() + "_" + AbstractGeoWavePersistence.METADATA_TABLE;
		return getQualifiedTableName(tableName);
	}

	protected Iterator<DynamoDBRow> getRows(
			final String tableName,
			final byte[][] dataIds,
			final byte[] adapterId,
			final String... additionalAuthorizations ) {
		final String qName = getQualifiedTableName(tableName);
		final Short adapterIdObj = ByteArrayUtils.byteArrayToShort(adapterId);
		final Set<ByteArrayId> dataIdsSet = new HashSet<>(
				dataIds.length);
		for (int i = 0; i < dataIds.length; i++) {
			dataIdsSet.add(new ByteArrayId(
					dataIds[i]));
		}
		final ScanRequest request = new ScanRequest(
				qName);
		final ScanResult scanResult = client.scan(request);
		final Iterator<DynamoDBRow> everything = new GeoWaveRowMergingIterator<DynamoDBRow>(
				Iterators.transform(
						new LazyPaginatedScan(
								scanResult,
								request,
								client),
						new DynamoDBRow.GuavaRowTranslationHelper()));
		return Iterators.filter(
				everything,
				new Predicate<DynamoDBRow>() {

					@Override
					public boolean apply(
							final DynamoDBRow input ) {
						return dataIdsSet.contains(new ByteArrayId(
								input.getDataId())) && Short.valueOf(
								input.getInternalAdapterId()).equals(
								adapterIdObj);
					}
				});
	}

	@Override
	public void deleteAll()
			throws Exception {
		final ListTablesResult tables = client.listTables();
		for (final String tableName : tables.getTableNames()) {
			if ((gwNamespace == null) || tableName.startsWith(gwNamespace)) {
				client.deleteTable(new DeleteTableRequest(
						tableName));
			}
		}
		tableExistsCache.clear();
	}

	@Override
	public boolean indexExists(
			final ByteArrayId indexId )
			throws IOException {
		try {
			return TableStatus.ACTIVE.name().equals(
					client.describeTable(
							getQualifiedTableName(indexId.getString())).getTable().getTableStatus());
		}
		catch (final AmazonDynamoDBException e) {
			LOGGER.info(
					"Unable to check existence of table",
					e);
		}
		return false;
	}

	@Override
	public boolean deleteAll(
			final ByteArrayId indexId,
			Short internalAdapterId,
			final String... additionalAuthorizations ) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean ensureAuthorizations(
			final String clientUser,
			final String... authorizations ) {
		return true;
	}

	@Override
	public Writer createWriter(
			final PrimaryIndex index,
			short internalAdapterId ) {
		final String qName = getQualifiedTableName(index.getId().getString());

		final DynamoDBWriter writer = new DynamoDBWriter(
				client,
				qName);

		createTable(qName);
		return writer;
	}

	private boolean createTable(
			String qName ) {
		if (options.getStoreOptions().isCreateTable()) {
			synchronized (tableExistsCache) {
				final Boolean tableExists = tableExistsCache.get(qName);
				if ((tableExists == null) || !tableExists) {
					final boolean tableCreated = TableUtils.createTableIfNotExists(
							client,
							new CreateTableRequest().withTableName(
									qName).withAttributeDefinitions(
									new AttributeDefinition(
											DynamoDBRow.GW_PARTITION_ID_KEY,
											ScalarAttributeType.B),
									new AttributeDefinition(
											DynamoDBRow.GW_RANGE_KEY,
											ScalarAttributeType.B)).withKeySchema(
									new KeySchemaElement(
											DynamoDBRow.GW_PARTITION_ID_KEY,
											KeyType.HASH),
									new KeySchemaElement(
											DynamoDBRow.GW_RANGE_KEY,
											KeyType.RANGE)).withProvisionedThroughput(
									new ProvisionedThroughput(
											Long.valueOf(options.getReadCapacity()),
											Long.valueOf(options.getWriteCapacity()))));
					if (tableCreated) {
						try {
							TableUtils.waitUntilActive(
									client,
									qName);
						}
						catch (TableNeverTransitionedToStateException | InterruptedException e) {
							LOGGER.error(
									"Unable to wait for active table '" + qName + "'",
									e);
						}
					}
					tableExistsCache.put(
							qName,
							true);
					return true;
				}
			}
		}
		return false;

	}

	@Override
	public MetadataWriter createMetadataWriter(
			final MetadataType metadataType ) {
		final String tableName = getMetadataTableName(metadataType);

		if (options.getStoreOptions().isCreateTable()) {
			synchronized (DynamoDBOperations.tableExistsCache) {
				final Boolean tableExists = DynamoDBOperations.tableExistsCache.get(tableName);
				if ((tableExists == null) || !tableExists) {
					final boolean tableCreated = TableUtils.createTableIfNotExists(
							client,
							new CreateTableRequest().withTableName(
									tableName).withAttributeDefinitions(
									new AttributeDefinition(
											METADATA_PRIMARY_ID_KEY,
											ScalarAttributeType.B)).withKeySchema(
									new KeySchemaElement(
											METADATA_PRIMARY_ID_KEY,
											KeyType.HASH)).withAttributeDefinitions(
									new AttributeDefinition(
											METADATA_TIMESTAMP_KEY,
											ScalarAttributeType.N)).withKeySchema(
									new KeySchemaElement(
											METADATA_TIMESTAMP_KEY,
											KeyType.RANGE)).withProvisionedThroughput(
									new ProvisionedThroughput(
											Long.valueOf(5),
											Long.valueOf(5))));
					if (tableCreated) {
						try {
							TableUtils.waitUntilActive(
									client,
									tableName);
						}
						catch (TableNeverTransitionedToStateException | InterruptedException e) {
							LOGGER.error(
									"Unable to wait for active table '" + tableName + "'",
									e);
						}
					}
					DynamoDBOperations.tableExistsCache.put(
							tableName,
							true);
				}
			}
		}

		return new DynamoDBMetadataWriter(
				this,
				tableName);
	}

	@Override
	public MetadataReader createMetadataReader(
			final MetadataType metadataType ) {
		return new DynamoDBMetadataReader(
				this,
				metadataType);
	}

	@Override
	public MetadataDeleter createMetadataDeleter(
			final MetadataType metadataType ) {
		return new DynamoDBMetadataDeleter(
				this,
				metadataType);
	}

	@Override
	public <T> Reader<T> createReader(
			final ReaderParams<T> readerParams ) {
		return new DynamoDBReader<T>(
				readerParams,
				this);
	}

	@Override
	public <T> Reader<T> createReader(
			final RecordReaderParams<T> recordReaderParams ) {
		return new DynamoDBReader<T>(
				recordReaderParams,
				this);
	}

	public RowDeleter createDeleter(
			final ByteArrayId indexId,
			final String... authorizations ) {
		return new DynamoDBDeleter(
				this,
				getQualifiedTableName(indexId.getString()));
	}

	@Override
	public boolean mergeData(
			final PrimaryIndex index,
			PersistentAdapterStore adapterStore,
			final AdapterIndexMappingStore adapterIndexMappingStore ) {
		return DataStoreUtils.mergeData(
				index,
				adapterStore,
				adapterIndexMappingStore);
	}

	@Override
	public boolean mergeStats(
			DataStatisticsStore statsStore,
			InternalAdapterStore internalAdapterStore ) {
		return DataStoreUtils.mergeStats(
				statsStore,
				internalAdapterStore);
	}

	@Override
	public boolean metadataExists(
			final MetadataType type )
			throws IOException {
		try {
			return TableStatus.ACTIVE.name().equals(
					client.describeTable(
							getMetadataTableName(type)).getTable().getTableStatus());
		}
		catch (final AmazonDynamoDBException e) {
			LOGGER.info(
					"Unable to check existence of table",
					e);
		}
		return false;
	}

	@Override
	public boolean createIndex(
			PrimaryIndex index )
			throws IOException {
		return createTable(getQualifiedTableName(index.getId().getString()));
	}

	@Override
	public <T> Deleter<T> createDeleter(
			ReaderParams<T> readerParams ) {
		return new QueryAndDeleteByRow<>(
				createDeleter(
						readerParams.getIndex().getId(),
						readerParams.getAdditionalAuthorizations()),
				createReader(readerParams));
	}
}
