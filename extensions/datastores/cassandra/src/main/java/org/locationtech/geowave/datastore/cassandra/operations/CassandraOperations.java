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
package org.locationtech.geowave.datastore.cassandra.operations;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.SinglePartitionQueryRanges;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.BaseDataStoreOptions;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.metadata.AbstractGeoWavePersistence;
import org.locationtech.geowave.core.store.operations.Deleter;
import org.locationtech.geowave.core.store.operations.MetadataDeleter;
import org.locationtech.geowave.core.store.operations.MetadataReader;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.core.store.operations.MetadataWriter;
import org.locationtech.geowave.core.store.operations.QueryAndDeleteByRow;
import org.locationtech.geowave.core.store.operations.ReaderParams;
import org.locationtech.geowave.core.store.operations.RowDeleter;
import org.locationtech.geowave.core.store.operations.RowReader;
import org.locationtech.geowave.core.store.operations.RowWriter;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import org.locationtech.geowave.datastore.cassandra.CassandraRow;
import org.locationtech.geowave.datastore.cassandra.CassandraRow.CassandraField;
import org.locationtech.geowave.datastore.cassandra.config.CassandraOptions;
import org.locationtech.geowave.datastore.cassandra.config.CassandraRequiredOptions;
import org.locationtech.geowave.datastore.cassandra.util.KeyspaceStatePool;
import org.locationtech.geowave.datastore.cassandra.util.KeyspaceStatePool.KeyspaceState;
import org.locationtech.geowave.datastore.cassandra.util.SessionPool;
import org.locationtech.geowave.mapreduce.MapReduceDataStoreOperations;
import org.locationtech.geowave.mapreduce.splits.RecordReaderParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;

public class CassandraOperations implements
		MapReduceDataStoreOperations
{
	private final static Logger LOGGER = LoggerFactory.getLogger(CassandraOperations.class);
	private final Session session;
	private final String gwNamespace;
	private final static int WRITE_RESPONSE_THREAD_SIZE = 16;
	private final static int READ_RESPONSE_THREAD_SIZE = 16;
	protected final static ExecutorService WRITE_RESPONSE_THREADS = MoreExecutors
			.getExitingExecutorService((ThreadPoolExecutor) Executors.newFixedThreadPool(WRITE_RESPONSE_THREAD_SIZE));
	protected final static ExecutorService READ_RESPONSE_THREADS = MoreExecutors
			.getExitingExecutorService((ThreadPoolExecutor) Executors.newFixedThreadPool(READ_RESPONSE_THREAD_SIZE));

	private static final Object CREATE_TABLE_MUTEX = new Object();
	private final CassandraOptions options;
	private final KeyspaceState state;

	public CassandraOperations(
			final CassandraRequiredOptions options ) {
		if ((options.getGeowaveNamespace() == null) || options.getGeowaveNamespace().equals(
				"")) {
			gwNamespace = "geowave";
		}
		else {
			gwNamespace = getCassandraSafeName(options.getGeowaveNamespace());
		}
		session = SessionPool.getInstance().getSession(
				options.getContactPoint());
		state = KeyspaceStatePool.getInstance().getCachedState(
				options.getContactPoint(),
				gwNamespace);
		this.options = (CassandraOptions) options.getStoreOptions();
		initKeyspace();
	}

	private static String getCassandraSafeName(
			final String name ) {
		// valid characters are alphanumeric or underscore
		// replace invalid characters with an underscore
		return name.replaceAll(
				"[^a-zA-Z\\d_]",
				"_");
	}

	public void initKeyspace() {
		// TODO consider exposing important keyspace options through commandline
		// such as understanding how to properly enable cassandra in production
		// - with data centers and snitch, for now because this is only creating
		// a keyspace "if not exists" a user can create a keyspace matching
		// their geowave namespace with any settings they want manually
		session.execute(SchemaBuilder.createKeyspace(
				gwNamespace).ifNotExists().with().replication(
				ImmutableMap.of(
						"class",
						"SimpleStrategy",
						"replication_factor",
						options.getReplicationFactor())).durableWrites(
				options.isDurableWrites()));
	}

	public Session getSession() {
		return session;
	}

	private Create getCreateTable(
			final String safeTableName ) {
		return SchemaBuilder.createTable(
				gwNamespace,
				safeTableName).ifNotExists();
	}

	private void executeCreateTable(
			final Create create,
			final String safeTableName ) {
		session.execute(create);
		state.tableExistsCache.put(
				safeTableName,
				true);
	}

	public Insert getInsert(
			final String table ) {
		return QueryBuilder.insertInto(
				gwNamespace,
				getCassandraSafeName(table));
	}

	public Delete getDelete(
			final String table ) {
		return QueryBuilder.delete().from(
				gwNamespace,
				getCassandraSafeName(table));
	}

	public Select getSelect(
			final String table,
			final String... columns ) {
		return (columns.length == 0 ? QueryBuilder.select() : QueryBuilder.select(columns)).from(
				gwNamespace,
				getCassandraSafeName(table));
	}

	public BaseDataStoreOptions getOptions() {
		return options;
	}

	public BatchedWrite getBatchedWrite(
			final String tableName ) {
		PreparedStatement preparedWrite;
		final String safeTableName = getCassandraSafeName(tableName);
		synchronized (state.preparedWritesPerTable) {
			preparedWrite = state.preparedWritesPerTable.get(safeTableName);
			if (preparedWrite == null) {
				final Insert insert = getInsert(safeTableName);
				for (final CassandraField f : CassandraField.values()) {
					insert.value(
							f.getFieldName(),
							QueryBuilder.bindMarker(f.getBindMarkerName()));
				}
				preparedWrite = session.prepare(insert);
				state.preparedWritesPerTable.put(
						safeTableName,
						preparedWrite);
			}
		}
		return new BatchedWrite(
				session,
				preparedWrite,
				options.getBatchWriteSize());
	}

	public BatchedRangeRead getBatchedRangeRead(
			final String tableName,
			final short[] adapterIds,
			final Collection<SinglePartitionQueryRanges> ranges,
			final GeoWaveRowIteratorTransformer<?> rowTransformer,
			final Predicate<GeoWaveRow> rowFilter ) {
		PreparedStatement preparedRead;
		final String safeTableName = getCassandraSafeName(tableName);
		synchronized (state.preparedRangeReadsPerTable) {
			preparedRead = state.preparedRangeReadsPerTable.get(safeTableName);
			if (preparedRead == null) {
				final Select select = getSelect(safeTableName);
				select
						.where(
								QueryBuilder.eq(
										CassandraRow.CassandraField.GW_PARTITION_ID_KEY.getFieldName(),
										QueryBuilder.bindMarker(CassandraRow.CassandraField.GW_PARTITION_ID_KEY
												.getBindMarkerName())))
						.and(
								QueryBuilder.in(
										CassandraRow.CassandraField.GW_ADAPTER_ID_KEY.getFieldName(),
										QueryBuilder.bindMarker(CassandraRow.CassandraField.GW_ADAPTER_ID_KEY
												.getBindMarkerName())))
						.and(
								QueryBuilder.gte(
										CassandraRow.CassandraField.GW_SORT_KEY.getFieldName(),
										QueryBuilder.bindMarker(CassandraRow.CassandraField.GW_SORT_KEY
												.getLowerBoundBindMarkerName())))
						.and(
								QueryBuilder.lt(
										CassandraRow.CassandraField.GW_SORT_KEY.getFieldName(),
										QueryBuilder.bindMarker(CassandraRow.CassandraField.GW_SORT_KEY
												.getUpperBoundBindMarkerName())));
				preparedRead = session.prepare(select);
				state.preparedRangeReadsPerTable.put(
						safeTableName,
						preparedRead);
			}
		}

		return new BatchedRangeRead(
				preparedRead,
				this,
				adapterIds,
				ranges,
				rowTransformer,
				rowFilter);
	}

	public RowRead getRowRead(
			final String tableName,
			final byte[] partitionKey,
			final byte[] sortKey,
			final Short internalAdapterId ) {
		PreparedStatement preparedRead;
		final String safeTableName = getCassandraSafeName(tableName);
		synchronized (state.preparedRowReadPerTable) {
			preparedRead = state.preparedRowReadPerTable.get(safeTableName);
			if (preparedRead == null) {
				final Select select = getSelect(safeTableName);
				select
						.where(
								QueryBuilder.eq(
										CassandraRow.CassandraField.GW_PARTITION_ID_KEY.getFieldName(),
										QueryBuilder.bindMarker(CassandraRow.CassandraField.GW_PARTITION_ID_KEY
												.getBindMarkerName())))
						.and(
								QueryBuilder.in(
										CassandraRow.CassandraField.GW_ADAPTER_ID_KEY.getFieldName(),
										QueryBuilder.bindMarker(CassandraRow.CassandraField.GW_ADAPTER_ID_KEY
												.getBindMarkerName())))
						.and(
								QueryBuilder.eq(
										CassandraRow.CassandraField.GW_SORT_KEY.getFieldName(),
										QueryBuilder.bindMarker(CassandraRow.CassandraField.GW_SORT_KEY
												.getBindMarkerName())));
				preparedRead = session.prepare(select);
				state.preparedRowReadPerTable.put(
						safeTableName,
						preparedRead);
			}
		}

		return new RowRead(
				preparedRead,
				this,
				partitionKey,
				sortKey,
				internalAdapterId);

	}

	public CloseableIterator<CassandraRow> executeQuery(
			final Statement... statements ) {
		final Iterator<Iterator<Row>> results = Iterators
				.transform(
						Arrays
								.asList(
										statements)
								.iterator(),
						s -> session
								.execute(
										s)
								.iterator());
		final Iterator<Row> rows = Iterators
				.concat(
						results);
		return new CloseableIterator.Wrapper<>(
				Iterators
						.transform(
								rows,
								r -> new CassandraRow(
										r)));
	}

	@Override
	public void deleteAll()
			throws Exception {
		state.tableExistsCache.clear();
		state.preparedRangeReadsPerTable.clear();
		state.preparedRowReadPerTable.clear();
		state.preparedWritesPerTable.clear();
		session.execute(SchemaBuilder.dropKeyspace(
				gwNamespace).ifExists());
	}

	public boolean deleteAll(
			final String tableName,
			final byte[] adapterId,
			final String... additionalAuthorizations ) {
		// TODO does this actually work? It seems to violate Cassandra rules of
		// always including at least Hash keys on where clause
		session.execute(QueryBuilder.delete().from(
				gwNamespace,
				getCassandraSafeName(tableName)).where(
				QueryBuilder.eq(
						CassandraField.GW_ADAPTER_ID_KEY.getFieldName(),
						ByteBuffer.wrap(adapterId))));
		return true;
	}

	public boolean deleteRows(
			final String tableName,
			final byte[][] dataIds,
			final short internalAdapterId,
			final String... additionalAuthorizations ) {
		session.execute(QueryBuilder.delete().from(
				gwNamespace,
				getCassandraSafeName(tableName)).where(
				QueryBuilder.eq(
						CassandraField.GW_ADAPTER_ID_KEY.getFieldName(),
						internalAdapterId)).and(
				QueryBuilder.in(
						CassandraField.GW_DATA_ID_KEY.getFieldName(),
						Lists.transform(
								Arrays.asList(dataIds),
								new ByteArrayToByteBuffer()))));
		return true;
	}

	public CloseableIterator<CassandraRow> getRows(
			final String tableName,
			final byte[][] dataIds,
			final Short internalAdapterId,
			final String... additionalAuthorizations ) {
		final Set<ByteArray> dataIdsSet = new HashSet<>(
				dataIds.length);
		for (int i = 0; i < dataIds.length; i++) {
			dataIdsSet.add(new ByteArray(
					dataIds[i]));
		}
		final CloseableIterator<CassandraRow> everything = executeQuery(QueryBuilder.select().from(
				gwNamespace,
				getCassandraSafeName(tableName)).allowFiltering());
		return new CloseableIteratorWrapper<>(
				everything,
				Iterators.filter(
						everything,
						new Predicate<GeoWaveRow>() {

							@Override
							public boolean apply(
									final GeoWaveRow input ) {
								return dataIdsSet.contains(new ByteArray(
										input.getDataId())) && (input.getAdapterId() == internalAdapterId);
							}
						}));
	}

	public boolean deleteRow(
			final String tableName,
			final GeoWaveRow row,
			final String... additionalAuthorizations ) {
		boolean exhausted = true;
		for (int i = 0; i < row.getFieldValues().length; i++) {
			final ResultSet rs = session.execute(QueryBuilder.delete().from(
					gwNamespace,
					getCassandraSafeName(tableName)).where(
					QueryBuilder.eq(
							CassandraField.GW_PARTITION_ID_KEY.getFieldName(),
							ByteBuffer.wrap(row.getPartitionKey()))).and(
					QueryBuilder.eq(
							CassandraField.GW_SORT_KEY.getFieldName(),
							ByteBuffer.wrap(row.getSortKey()))).and(
					QueryBuilder.eq(
							CassandraField.GW_ADAPTER_ID_KEY.getFieldName(),
							row.getAdapterId())).and(
					QueryBuilder.eq(
							CassandraField.GW_DATA_ID_KEY.getFieldName(),
							ByteBuffer.wrap(row.getDataId()))).and(
					QueryBuilder.eq(
							CassandraField.GW_FIELD_VISIBILITY_KEY.getFieldName(),
							ByteBuffer.wrap(row.getFieldValues()[i].getVisibility()))));
			exhausted &= rs.isExhausted();
		}

		return !exhausted;
	}

	private static class ByteArrayToByteBuffer implements
			Function<byte[], ByteBuffer>
	{
		@Override
		public ByteBuffer apply(
				final byte[] input ) {
			return ByteBuffer.wrap(input);
		}
	};

	public static class ByteArrayIdToByteBuffer implements
			Function<ByteArray, ByteBuffer>
	{
		@Override
		public ByteBuffer apply(
				final ByteArray input ) {
			return ByteBuffer.wrap(input.getBytes());
		}
	}

	public static class StringToByteBuffer implements
			Function<String, ByteBuffer>
	{
		@Override
		public ByteBuffer apply(
				final String input ) {
			return ByteBuffer.wrap(StringUtils.stringToBinary(input));
		}
	}

	@Override
	public boolean indexExists(
			final String indexName )
			throws IOException {
		final String tableName = getCassandraSafeName(indexName);
		Boolean tableExists = state.tableExistsCache.get(tableName);
		if (tableExists == null) {
			final KeyspaceMetadata keyspace = session.getCluster().getMetadata().getKeyspace(
					gwNamespace);
			if (keyspace != null) {
				tableExists = keyspace.getTable(tableName) != null;
			}
			else {
				tableExists = false;
			}
			state.tableExistsCache.put(
					tableName,
					tableExists);
		}
		return tableExists;
	}

	@Override
	public boolean deleteAll(
			final String indexName,
			final String typeName,
			final Short adapterId,
			final String... additionalAuthorizations ) {
		return false;
	}

	@Override
	public boolean ensureAuthorizations(
			final String clientUser,
			final String... authorizations ) {
		return true;
	}

	@Override
	public RowWriter createWriter(
			final Index index,
			InternalDataAdapter<?> adapter ) {
		createTable(index.getName());
		return new CassandraWriter(
				index.getName(),
				this);
	}

	private boolean createTable(
			final String indexName ) {
		synchronized (CREATE_TABLE_MUTEX) {
			try {
				if (!indexExists(indexName)) {
					final String tableName = getCassandraSafeName(indexName);
					final Create create = getCreateTable(tableName);
					for (final CassandraField f : CassandraField.values()) {
						f.addColumn(create);
					}
					executeCreateTable(
							create,
							tableName);
					return true;
				}
			}
			catch (final IOException e) {
				LOGGER.error(
						"Unable to create table '" + indexName + "'",
						e);
			}
		}
		return false;
	}

	@Override
	public MetadataWriter createMetadataWriter(
			final MetadataType metadataType ) {
		final String tableName = getMetadataTableName(metadataType);
		// this checks for existence prior to create
		synchronized (CREATE_TABLE_MUTEX) {
			try {
				if (!indexExists(tableName)) {
					// create table
					final Create create = getCreateTable(tableName);
					create.addPartitionKey(
							CassandraMetadataWriter.PRIMARY_ID_KEY,
							DataType.blob());
					if (MetadataType.STATS.equals(metadataType) || MetadataType.INTERNAL_ADAPTER.equals(metadataType)) {
						create.addClusteringColumn(
								CassandraMetadataWriter.SECONDARY_ID_KEY,
								DataType.blob());
						create.addClusteringColumn(
								CassandraMetadataWriter.TIMESTAMP_ID_KEY,
								DataType.timeuuid());
						if (MetadataType.STATS.equals(metadataType)) {
							create.addColumn(
									CassandraMetadataWriter.VISIBILITY_KEY,
									DataType.blob());
						}
					}
					create.addColumn(
							CassandraMetadataWriter.VALUE_KEY,
							DataType.blob());
					executeCreateTable(
							create,
							tableName);
				}
			}
			catch (final IOException e) {
				LOGGER.warn(
						"Unable to check if table exists",
						e);
			}
		}
		return new CassandraMetadataWriter(
				this,
				tableName);
	}

	@Override
	public MetadataReader createMetadataReader(
			final MetadataType metadataType ) {
		return new CassandraMetadataReader(
				this,
				metadataType);
	}

	@Override
	public MetadataDeleter createMetadataDeleter(
			final MetadataType metadataType ) {
		return new CassandraMetadataDeleter(
				this,
				metadataType);
	}

	@Override
	public <T> RowReader<T> createReader(
			final ReaderParams<T> readerParams ) {
		return new CassandraReader<>(
				readerParams,
				this);
	}

	@Override
	public RowDeleter createRowDeleter(
			final String indexName,
			final PersistentAdapterStore adapterStore,
			final InternalAdapterStore internalAdapterStore,
			final String... authorizations ) {
		return new CassandraDeleter(
				this,
				indexName);
	}

	@Override
	public boolean mergeData(
			final Index index,
			final PersistentAdapterStore adapterStore,
			final InternalAdapterStore internalAdapterStore,
			final AdapterIndexMappingStore adapterIndexMappingStore ) {
		return DataStoreUtils.mergeData(
				this,
				options,
				index,
				adapterStore,
				internalAdapterStore,
				adapterIndexMappingStore);
	}

	@Override
	public <T> RowReader<T> createReader(
			final RecordReaderParams<T> recordReaderParams ) {
		return new CassandraReader<>(
				recordReaderParams,
				this);
	}

	@Override
	public boolean metadataExists(
			final MetadataType metadataType )
			throws IOException {
		return indexExists(getMetadataTableName(metadataType));
	}

	public String getMetadataTableName(
			final MetadataType metadataType ) {
		final String tableName = metadataType.name() + "_" + AbstractGeoWavePersistence.METADATA_TABLE;
		return tableName;
	}

	@Override
	public boolean createIndex(
			final Index index )
			throws IOException {
		return createTable(index.getName());
	}

	@Override
	public boolean mergeStats(
			final DataStatisticsStore statsStore,
			final InternalAdapterStore internalAdapterStore ) {
		return DataStoreUtils.mergeStats(
				statsStore,
				internalAdapterStore);
	}

	@Override
	public <T> Deleter<T> createDeleter(
			final ReaderParams<T> readerParams ) {
		return new QueryAndDeleteByRow<>(
				createRowDeleter(
						readerParams.getIndex().getName(),
						readerParams.getAdapterStore(),
						readerParams.getInternalAdapterStore(),
						readerParams.getAdditionalAuthorizations()),
				createReader(readerParams));
	}
}
