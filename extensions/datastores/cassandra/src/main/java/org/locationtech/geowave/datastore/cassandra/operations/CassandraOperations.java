/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.cassandra.operations;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.SinglePartitionQueryRanges;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.store.BaseDataStoreOptions;
import org.locationtech.geowave.core.store.CloseableIterator;
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
import org.locationtech.geowave.core.store.operations.RowReaderWrapper;
import org.locationtech.geowave.core.store.operations.RowWriter;
import org.locationtech.geowave.core.store.query.filter.ClientVisibilityFilter;
import org.locationtech.geowave.datastore.cassandra.CassandraRow;
import org.locationtech.geowave.datastore.cassandra.CassandraRow.CassandraField;
import org.locationtech.geowave.datastore.cassandra.config.CassandraOptions;
import org.locationtech.geowave.datastore.cassandra.config.CassandraRequiredOptions;
import org.locationtech.geowave.datastore.cassandra.util.CassandraUtils;
import org.locationtech.geowave.datastore.cassandra.util.KeyspaceStatePool;
import org.locationtech.geowave.datastore.cassandra.util.KeyspaceStatePool.KeyspaceState;
import org.locationtech.geowave.datastore.cassandra.util.SessionPool;
import org.locationtech.geowave.mapreduce.MapReduceDataStoreOperations;
import org.locationtech.geowave.mapreduce.splits.RecordReaderParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.MoreExecutors;

public class CassandraOperations implements MapReduceDataStoreOperations {
  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraOperations.class);
  private final Session session;
  private final String gwNamespace;
  private static final int WRITE_RESPONSE_THREAD_SIZE = 16;
  private static final int READ_RESPONSE_THREAD_SIZE = 16;
  protected static final ExecutorService WRITE_RESPONSE_THREADS =
      MoreExecutors.getExitingExecutorService(
          (ThreadPoolExecutor) Executors.newFixedThreadPool(WRITE_RESPONSE_THREAD_SIZE));
  protected static final ExecutorService READ_RESPONSE_THREADS =
      MoreExecutors.getExitingExecutorService(
          (ThreadPoolExecutor) Executors.newFixedThreadPool(READ_RESPONSE_THREAD_SIZE));

  private static final Object CREATE_TABLE_MUTEX = new Object();
  private final CassandraOptions options;
  private final KeyspaceState state;

  public CassandraOperations(final CassandraRequiredOptions options) {
    if ((options.getGeoWaveNamespace() == null) || options.getGeoWaveNamespace().equals("")) {
      gwNamespace = "geowave";
    } else {
      gwNamespace = getCassandraSafeName(options.getGeoWaveNamespace());
    }
    session = SessionPool.getInstance().getSession(options.getContactPoint());
    state = KeyspaceStatePool.getInstance().getCachedState(options.getContactPoint(), gwNamespace);
    this.options = (CassandraOptions) options.getStoreOptions();
    initKeyspace();
  }

  private static String getCassandraSafeName(final String name) {
    // valid characters are alphanumeric or underscore
    // replace invalid characters with an underscore
    return name.replaceAll("[^a-zA-Z\\d_]", "_");
  }

  public void initKeyspace() {
    // TODO consider exposing important keyspace options through commandline
    // such as understanding how to properly enable cassandra in production
    // - with data centers and snitch, for now because this is only creating
    // a keyspace "if not exists" a user can create a keyspace matching
    // their geowave namespace with any settings they want manually
    session.execute(
        SchemaBuilder.createKeyspace(gwNamespace).ifNotExists().with().replication(
            ImmutableMap.of(
                "class",
                "SimpleStrategy",
                "replication_factor",
                options.getReplicationFactor())).durableWrites(options.isDurableWrites()));
  }

  public Session getSession() {
    return session;
  }

  private Create getCreateTable(final String safeTableName) {
    return SchemaBuilder.createTable(gwNamespace, safeTableName).ifNotExists();
  }

  private void executeCreateTable(final Create create, final String safeTableName) {
    session.execute(create);
    state.tableExistsCache.put(safeTableName, true);
  }

  public Insert getInsert(final String table) {
    return QueryBuilder.insertInto(gwNamespace, getCassandraSafeName(table));
  }

  public Delete getDelete(final String table) {
    return QueryBuilder.delete().from(gwNamespace, getCassandraSafeName(table));
  }

  public Select getSelect(final String table, final String... columns) {
    return (columns.length == 0 ? QueryBuilder.select() : QueryBuilder.select(columns)).from(
        gwNamespace,
        getCassandraSafeName(table));
  }

  public BaseDataStoreOptions getOptions() {
    return options;
  }

  public BatchedWrite getBatchedWrite(final String tableName) {
    PreparedStatement preparedWrite;
    final String safeTableName = getCassandraSafeName(tableName);
    final boolean isDataIndex = DataIndexUtils.isDataIndex(tableName);
    synchronized (state.preparedWritesPerTable) {
      preparedWrite = state.preparedWritesPerTable.get(safeTableName);
      if (preparedWrite == null) {
        final Insert insert = getInsert(safeTableName);
        CassandraField[] fields = CassandraField.values();
        if (isDataIndex) {
          fields =
              Arrays.stream(fields).filter(f -> f.isDataIndexColumn()).toArray(
                  i -> new CassandraField[i]);
        }
        for (final CassandraField f : fields) {
          insert.value(f.getFieldName(), QueryBuilder.bindMarker(f.getBindMarkerName()));
        }
        preparedWrite = session.prepare(insert);
        state.preparedWritesPerTable.put(safeTableName, preparedWrite);
      }
    }
    return new BatchedWrite(
        session,
        preparedWrite,
        isDataIndex ? 1 : options.getBatchWriteSize(),
        isDataIndex,
        options.isVisibilityEnabled());
  }

  @Override
  public RowWriter createDataIndexWriter(final InternalDataAdapter<?> adapter) {
    return createWriter(DataIndexUtils.DATA_ID_INDEX, adapter);
  }

  public BatchedRangeRead getBatchedRangeRead(
      final String tableName,
      final short[] adapterIds,
      final Collection<SinglePartitionQueryRanges> ranges,
      final boolean rowMerging,
      final GeoWaveRowIteratorTransformer<?> rowTransformer,
      final Predicate<GeoWaveRow> rowFilter) {
    PreparedStatement preparedRead;
    final String safeTableName = getCassandraSafeName(tableName);
    synchronized (state.preparedRangeReadsPerTable) {
      preparedRead = state.preparedRangeReadsPerTable.get(safeTableName);
      if (preparedRead == null) {
        final Select select = getSelect(safeTableName);
        select.where(
            QueryBuilder.eq(
                CassandraRow.CassandraField.GW_PARTITION_ID_KEY.getFieldName(),
                QueryBuilder.bindMarker(
                    CassandraRow.CassandraField.GW_PARTITION_ID_KEY.getBindMarkerName()))).and(
                        QueryBuilder.in(
                            CassandraRow.CassandraField.GW_ADAPTER_ID_KEY.getFieldName(),
                            QueryBuilder.bindMarker(
                                CassandraRow.CassandraField.GW_ADAPTER_ID_KEY.getBindMarkerName()))).and(
                                    QueryBuilder.gte(
                                        CassandraRow.CassandraField.GW_SORT_KEY.getFieldName(),
                                        QueryBuilder.bindMarker(
                                            CassandraRow.CassandraField.GW_SORT_KEY.getLowerBoundBindMarkerName()))).and(
                                                QueryBuilder.lt(
                                                    CassandraRow.CassandraField.GW_SORT_KEY.getFieldName(),
                                                    QueryBuilder.bindMarker(
                                                        CassandraRow.CassandraField.GW_SORT_KEY.getUpperBoundBindMarkerName())));
        preparedRead = session.prepare(select);
        state.preparedRangeReadsPerTable.put(safeTableName, preparedRead);
      }
    }

    return new BatchedRangeRead(
        preparedRead,
        this,
        adapterIds,
        ranges,
        rowMerging,
        rowTransformer,
        rowFilter);
  }

  public CloseableIterator<CassandraRow> executeQuery(final Statement... statements) {
    final Iterator<Iterator<Row>> results =
        Iterators.transform(
            Arrays.asList(statements).iterator(),
            s -> session.execute(s).iterator());
    final Iterator<Row> rows = Iterators.concat(results);
    return new CloseableIterator.Wrapper<>(Iterators.transform(rows, r -> new CassandraRow(r)));
  }

  @Override
  public void deleteAll() throws Exception {
    state.tableExistsCache.clear();
    state.preparedRangeReadsPerTable.clear();
    state.preparedRowReadPerTable.clear();
    state.preparedWritesPerTable.clear();
    session.execute(SchemaBuilder.dropKeyspace(gwNamespace).ifExists());
  }

  public boolean deleteAll(
      final String tableName,
      final byte[] adapterId,
      final String... additionalAuthorizations) {
    // TODO does this actually work? It seems to violate Cassandra rules of
    // always including at least Hash keys on where clause
    session.execute(
        QueryBuilder.delete().from(gwNamespace, getCassandraSafeName(tableName)).where(
            QueryBuilder.eq(
                CassandraField.GW_ADAPTER_ID_KEY.getFieldName(),
                ByteBuffer.wrap(adapterId))));
    return true;
  }

  public boolean deleteRows(
      final String tableName,
      final byte[][] dataIds,
      final short internalAdapterId,
      final String... additionalAuthorizations) {
    session.execute(
        QueryBuilder.delete().from(gwNamespace, getCassandraSafeName(tableName)).where(
            QueryBuilder.eq(
                CassandraField.GW_ADAPTER_ID_KEY.getFieldName(),
                internalAdapterId)).and(
                    QueryBuilder.in(
                        CassandraField.GW_DATA_ID_KEY.getFieldName(),
                        Arrays.stream(dataIds).map(new ByteArrayToByteBuffer()))));
    return true;
  }

  public boolean deleteRow(
      final String tableName,
      final GeoWaveRow row,
      final String... additionalAuthorizations) {
    boolean exhausted = true;
    for (int i = 0; i < row.getFieldValues().length; i++) {
      final ResultSet rs =
          session.execute(
              QueryBuilder.delete().from(gwNamespace, getCassandraSafeName(tableName)).where(
                  QueryBuilder.eq(
                      CassandraField.GW_PARTITION_ID_KEY.getFieldName(),
                      ByteBuffer.wrap(
                          CassandraUtils.getCassandraSafePartitionKey(row.getPartitionKey())))).and(
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
                                                          ByteBuffer.wrap(
                                                              row.getFieldValues()[i].getVisibility()))));
      exhausted &= rs.isExhausted();
    }

    return !exhausted;
  }

  private static class ByteArrayToByteBuffer implements Function<byte[], ByteBuffer> {
    @Override
    public ByteBuffer apply(final byte[] input) {
      return ByteBuffer.wrap(input);
    }
  };

  public static class ByteArrayIdToByteBuffer implements Function<ByteArray, ByteBuffer> {
    @Override
    public ByteBuffer apply(final ByteArray input) {
      return ByteBuffer.wrap(input.getBytes());
    }
  }

  public static class StringToByteBuffer implements Function<String, ByteBuffer> {
    @Override
    public ByteBuffer apply(final String input) {
      return ByteBuffer.wrap(StringUtils.stringToBinary(input));
    }
  }

  @Override
  public boolean indexExists(final String indexName) throws IOException {
    final String tableName = getCassandraSafeName(indexName);
    Boolean tableExists = state.tableExistsCache.get(tableName);
    if (tableExists == null) {
      final KeyspaceMetadata keyspace = session.getCluster().getMetadata().getKeyspace(gwNamespace);
      if (keyspace != null) {
        tableExists = keyspace.getTable(tableName) != null;
      } else {
        tableExists = false;
      }
      state.tableExistsCache.put(tableName, tableExists);
    }
    return tableExists;
  }

  @Override
  public boolean deleteAll(
      final String indexName,
      final String typeName,
      final Short adapterId,
      final String... additionalAuthorizations) {
    return false;
  }

  @Override
  public boolean ensureAuthorizations(final String clientUser, final String... authorizations) {
    return true;
  }

  @Override
  public RowWriter createWriter(final Index index, final InternalDataAdapter<?> adapter) {
    createTable(index.getName());
    return new CassandraWriter(index.getName(), this);
  }

  private boolean createTable(final String indexName) {
    synchronized (CREATE_TABLE_MUTEX) {
      try {
        if (!indexExists(indexName)) {
          final String tableName = getCassandraSafeName(indexName);
          final Create create = getCreateTable(tableName);
          CassandraField[] fields = CassandraField.values();
          if (DataIndexUtils.isDataIndex(tableName)) {
            fields =
                Arrays.stream(fields).filter(f -> f.isDataIndexColumn()).toArray(
                    i -> new CassandraField[i]);
          }
          for (final CassandraField f : fields) {
            f.addColumn(create);
          }
          executeCreateTable(create, tableName);
          return true;
        }
      } catch (final IOException e) {
        LOGGER.error("Unable to create table '" + indexName + "'", e);
      }
    }
    return false;
  }

  @Override
  public MetadataWriter createMetadataWriter(final MetadataType metadataType) {
    final String tableName = getMetadataTableName(metadataType);
    // this checks for existence prior to create
    synchronized (CREATE_TABLE_MUTEX) {
      try {
        if (!indexExists(tableName)) {
          // create table
          final Create create = getCreateTable(tableName);
          create.addPartitionKey(CassandraMetadataWriter.PRIMARY_ID_KEY, DataType.blob());
          if (MetadataType.STATS.equals(metadataType)
              || MetadataType.INTERNAL_ADAPTER.equals(metadataType)) {
            create.addClusteringColumn(CassandraMetadataWriter.SECONDARY_ID_KEY, DataType.blob());
            create.addClusteringColumn(
                CassandraMetadataWriter.TIMESTAMP_ID_KEY,
                DataType.timeuuid());
            if (MetadataType.STATS.equals(metadataType)) {
              create.addColumn(CassandraMetadataWriter.VISIBILITY_KEY, DataType.blob());
            }
          }
          create.addColumn(CassandraMetadataWriter.VALUE_KEY, DataType.blob());
          executeCreateTable(create, tableName);
        }
      } catch (final IOException e) {
        LOGGER.warn("Unable to check if table exists", e);
      }
    }
    return new CassandraMetadataWriter(this, tableName);
  }

  @Override
  public MetadataReader createMetadataReader(final MetadataType metadataType) {
    return new CassandraMetadataReader(this, metadataType);
  }

  @Override
  public MetadataDeleter createMetadataDeleter(final MetadataType metadataType) {
    return new CassandraMetadataDeleter(this, metadataType);
  }

  @Override
  public <T> RowReader<T> createReader(final ReaderParams<T> readerParams) {
    return new CassandraReader<>(readerParams, this, options.isVisibilityEnabled());
  }

  @Override
  public RowReader<GeoWaveRow> createReader(final DataIndexReaderParams readerParams) {
    final byte[][] dataIds;
    Iterator<GeoWaveRow> iterator;
    if (readerParams.getDataIds() == null) {
      if ((readerParams.getStartInclusiveDataId() != null)
          || (readerParams.getEndInclusiveDataId() != null)) {
        final List<byte[]> intermediaries = new ArrayList<>();
        ByteArrayUtils.addAllIntermediaryByteArrays(
            intermediaries,
            new ByteArrayRange(
                readerParams.getStartInclusiveDataId(),
                readerParams.getEndInclusiveDataId()));
        dataIds = intermediaries.toArray(new byte[0][]);
        iterator = getRows(dataIds, readerParams.getAdapterId());
      } else {
        iterator = getRows(readerParams.getAdapterId());
      }
    } else {
      dataIds = readerParams.getDataIds();
      iterator = getRows(dataIds, readerParams.getAdapterId());
    }
    if (options.isVisibilityEnabled()) {
      Stream<GeoWaveRow> stream = Streams.stream(iterator);
      final Set<String> authorizations =
          Sets.newHashSet(readerParams.getAdditionalAuthorizations());
      stream = stream.filter(new ClientVisibilityFilter(authorizations));
      iterator = stream.iterator();
    }
    return new RowReaderWrapper<>(new CloseableIterator.Wrapper(iterator));
  }

  public Iterator<GeoWaveRow> getRows(final short adapterId) {
    final String tableName = DataIndexUtils.DATA_ID_INDEX.getName();
    final String safeTableName = getCassandraSafeName(tableName);

    // the datastax client API does not allow for unconstrained partition keys (not a recommended
    // usage, but this interface must support it)
    // so CQL is built manually here
    final ResultSet results =
        getSession().execute(
            "select * from "
                + gwNamespace
                + "."
                + safeTableName
                + " where "
                + CassandraRow.CassandraField.GW_ADAPTER_ID_KEY.getFieldName()
                + " = "
                + adapterId
                + " ALLOW FILTERING");
    return Streams.stream(results.iterator()).map(r -> {
      final byte[] d = r.getBytes(CassandraField.GW_PARTITION_ID_KEY.getFieldName()).array();
      final byte[] v = r.getBytes(CassandraField.GW_VALUE_KEY.getFieldName()).array();

      return DataIndexUtils.deserializeDataIndexRow(d, adapterId, v, options.isVisibilityEnabled());
    }).iterator();
  }

  public Iterator<GeoWaveRow> getRows(final byte[][] dataIds, final short adapterId) {
    PreparedStatement preparedRead;
    final String tableName = DataIndexUtils.DATA_ID_INDEX.getName();
    final String safeTableName = getCassandraSafeName(tableName);
    synchronized (state.preparedRangeReadsPerTable) {
      preparedRead = state.preparedRangeReadsPerTable.get(safeTableName);
      if (preparedRead == null) {
        final Select select = getSelect(safeTableName);
        select.where(
            QueryBuilder.in(
                CassandraRow.CassandraField.GW_PARTITION_ID_KEY.getFieldName(),
                QueryBuilder.bindMarker(
                    CassandraRow.CassandraField.GW_PARTITION_ID_KEY.getBindMarkerName()))).and(
                        QueryBuilder.eq(
                            CassandraRow.CassandraField.GW_ADAPTER_ID_KEY.getFieldName(),
                            QueryBuilder.bindMarker(
                                CassandraRow.CassandraField.GW_ADAPTER_ID_KEY.getBindMarkerName())));
        preparedRead = session.prepare(select);
        state.preparedRangeReadsPerTable.put(safeTableName, preparedRead);
      }
    }
    final BoundStatement statement = new BoundStatement(preparedRead);
    statement.set(
        CassandraField.GW_ADAPTER_ID_KEY.getBindMarkerName(),
        adapterId,
        TypeCodec.smallInt());
    statement.set(
        CassandraField.GW_PARTITION_ID_KEY.getBindMarkerName(),
        Arrays.stream(dataIds).map(d -> ByteBuffer.wrap(d)).collect(Collectors.toList()),
        TypeCodec.list(TypeCodec.blob()));
    final ResultSet results = getSession().execute(statement);
    final Map<ByteArray, GeoWaveRow> resultsMap = new HashMap<>();
    results.forEach(r -> {
      final byte[] d = r.getBytes(CassandraField.GW_PARTITION_ID_KEY.getFieldName()).array();
      final byte[] v = r.getBytes(CassandraField.GW_VALUE_KEY.getFieldName()).array();
      resultsMap.put(
          new ByteArray(d),
          DataIndexUtils.deserializeDataIndexRow(d, adapterId, v, options.isVisibilityEnabled()));
    });
    return Arrays.stream(dataIds).map(d -> resultsMap.get(new ByteArray(d))).filter(
        r -> r != null).iterator();
  }

  @Override
  public RowDeleter createRowDeleter(
      final String indexName,
      final PersistentAdapterStore adapterStore,
      final InternalAdapterStore internalAdapterStore,
      final String... authorizations) {
    return new CassandraDeleter(this, indexName);
  }

  @Override
  public RowReader<GeoWaveRow> createReader(final RecordReaderParams recordReaderParams) {
    return new CassandraReader<>(recordReaderParams, this, options.isVisibilityEnabled());
  }

  @Override
  public boolean metadataExists(final MetadataType metadataType) throws IOException {
    return indexExists(getMetadataTableName(metadataType));
  }

  public String getMetadataTableName(final MetadataType metadataType) {
    final String tableName = metadataType.name() + "_" + AbstractGeoWavePersistence.METADATA_TABLE;
    return tableName;
  }

  public boolean createIndex(final Index index) throws IOException {
    return createTable(index.getName());
  }

  @Override
  public void delete(final DataIndexReaderParams readerParams) {
    deleteRowsFromDataIndex(readerParams.getDataIds(), readerParams.getAdapterId());
  }

  public void deleteRowsFromDataIndex(final byte[][] dataIds, final short adapterId) {
    session.execute(
        QueryBuilder.delete().from(
            gwNamespace,
            getCassandraSafeName(DataIndexUtils.DATA_ID_INDEX.getName())).where(
                QueryBuilder.in(
                    CassandraField.GW_PARTITION_ID_KEY.getFieldName(),
                    Arrays.stream(dataIds).map(d -> ByteBuffer.wrap(d)).collect(
                        Collectors.toList()))).and(
                            QueryBuilder.eq(
                                CassandraField.GW_ADAPTER_ID_KEY.getFieldName(),
                                adapterId)));
  }
}
