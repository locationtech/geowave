/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.cassandra.operations;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.codehaus.jackson.map.ObjectMapper;
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
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.querybuilder.Literal;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.DeleteSelection;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableStart;
import com.datastax.oss.driver.api.querybuilder.schema.Drop;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.select.SelectFrom;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.MoreExecutors;

public class CassandraOperations implements MapReduceDataStoreOperations {
  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraOperations.class);
  private final CqlSession session;
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
    this(
        options,
        SessionPool.getInstance().getSession(options.getContactPoints(), options.getDatacenter()));
  }

  public CassandraOperations(final CassandraRequiredOptions options, final CqlSession session) {
    if ((options.getGeoWaveNamespace() == null) || options.getGeoWaveNamespace().equals("")) {
      gwNamespace = "geowave";
    } else {
      gwNamespace = getCassandraSafeName(options.getGeoWaveNamespace());
    }
    this.session = session;
    state = KeyspaceStatePool.getInstance().getCachedState(options.getContactPoints(), gwNamespace);
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
        SchemaBuilder.createKeyspace(gwNamespace).ifNotExists().withReplicationOptions(
            ImmutableMap.of(
                "class",
                "SimpleStrategy",
                "replication_factor",
                options.getReplicationFactor())).withDurableWrites(
                    options.isDurableWrites()).build());
  }

  public CqlSession getSession() {
    return session;
  }

  private CreateTableStart getCreateTable(final String safeTableName) {
    return SchemaBuilder.createTable(gwNamespace, safeTableName).ifNotExists();
  }

  private void executeCreateTable(final CreateTable create, final String safeTableName) {
    session.execute(create.build());
    state.tableExistsCache.put(safeTableName, true);
  }

  private void executeDropTable(final Drop drop, final String safeTableName) {
    // drop table is extremely slow, need to increase timeout
    session.execute(drop.build().setTimeout(Duration.of(12L, ChronoUnit.HOURS)));
    state.tableExistsCache.put(safeTableName, false);
  }

  public InsertInto getInsert(final String table) {
    return QueryBuilder.insertInto(gwNamespace, getCassandraSafeName(table));
  }

  public DeleteSelection getDelete(final String table) {
    return QueryBuilder.deleteFrom(gwNamespace, getCassandraSafeName(table));
  }

  public Select getSelect(final String table, final String... columns) {
    final SelectFrom select = QueryBuilder.selectFrom(gwNamespace, getCassandraSafeName(table));
    return columns.length == 0 ? select.all() : select.columns(columns);
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
        final InsertInto insert = getInsert(safeTableName);
        CassandraField[] fields = CassandraField.values();

        if (isDataIndex) {
          fields =
              Arrays.stream(fields).filter(f -> f.isDataIndexColumn()).toArray(
                  i -> new CassandraField[i]);
        }
        RegularInsert regInsert = null;
        for (final CassandraField f : fields) {
          regInsert =
              (regInsert != null ? regInsert : insert).value(
                  f.getFieldName(),
                  QueryBuilder.bindMarker(f.getBindMarkerName()));
        }
        preparedWrite = session.prepare(regInsert.build());
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

        preparedRead =
            session.prepare(
                getSelect(safeTableName).whereColumn(
                    CassandraRow.CassandraField.GW_PARTITION_ID_KEY.getFieldName()).isEqualTo(
                        QueryBuilder.bindMarker(
                            CassandraRow.CassandraField.GW_PARTITION_ID_KEY.getBindMarkerName())).whereColumn(
                                CassandraRow.CassandraField.GW_ADAPTER_ID_KEY.getFieldName()).in(
                                    QueryBuilder.bindMarker(
                                        CassandraRow.CassandraField.GW_ADAPTER_ID_KEY.getBindMarkerName())).whereColumn(
                                            CassandraRow.CassandraField.GW_SORT_KEY.getFieldName()).isGreaterThanOrEqualTo(
                                                QueryBuilder.bindMarker(
                                                    CassandraRow.CassandraField.GW_SORT_KEY.getLowerBoundBindMarkerName())).whereColumn(
                                                        CassandraRow.CassandraField.GW_SORT_KEY.getFieldName()).isLessThan(
                                                            QueryBuilder.bindMarker(
                                                                CassandraRow.CassandraField.GW_SORT_KEY.getUpperBoundBindMarkerName())).build());
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
    final SimpleStatement statement = SchemaBuilder.dropKeyspace(gwNamespace).ifExists().build();
    // drop keyspace is extremely slow, need to increase timeout
    session.execute(statement.setTimeout(Duration.of(12L, ChronoUnit.HOURS)));
  }

  public boolean deleteAll(
      final String tableName,
      final byte[] adapterId,
      final String... additionalAuthorizations) {
    // TODO does this actually work? It seems to violate Cassandra rules of
    // always including at least Hash keys on where clause
    session.execute(
        QueryBuilder.deleteFrom(gwNamespace, getCassandraSafeName(tableName)).whereColumn(
            CassandraField.GW_ADAPTER_ID_KEY.getFieldName()).isEqualTo(
                QueryBuilder.literal(ByteBuffer.wrap(adapterId))).build());
    return true;
  }

  public boolean deleteRows(
      final String tableName,
      final byte[][] dataIds,
      final short internalAdapterId,
      final String... additionalAuthorizations) {
    session.execute(
        QueryBuilder.deleteFrom(gwNamespace, getCassandraSafeName(tableName)).whereColumn(
            CassandraField.GW_ADAPTER_ID_KEY.getFieldName()).isEqualTo(
                QueryBuilder.literal(internalAdapterId)).whereColumn(
                    CassandraField.GW_DATA_ID_KEY.getFieldName()).in(
                        Arrays.stream(dataIds).map(new ByteArrayToByteBuffer()).map(
                            QueryBuilder::literal).toArray(Literal[]::new)).build());
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
              QueryBuilder.deleteFrom(gwNamespace, getCassandraSafeName(tableName)).whereColumn(
                  CassandraField.GW_PARTITION_ID_KEY.getFieldName()).isEqualTo(
                      QueryBuilder.literal(
                          ByteBuffer.wrap(
                              CassandraUtils.getCassandraSafePartitionKey(
                                  row.getPartitionKey())))).whereColumn(
                                      CassandraField.GW_SORT_KEY.getFieldName()).isEqualTo(
                                          QueryBuilder.literal(
                                              ByteBuffer.wrap(row.getSortKey()))).whereColumn(
                                                  CassandraField.GW_ADAPTER_ID_KEY.getFieldName()).isEqualTo(
                                                      QueryBuilder.literal(
                                                          row.getAdapterId())).whereColumn(
                                                              CassandraField.GW_DATA_ID_KEY.getFieldName()).isEqualTo(
                                                                  QueryBuilder.literal(
                                                                      ByteBuffer.wrap(
                                                                          row.getDataId()))).whereColumn(
                                                                              CassandraField.GW_FIELD_VISIBILITY_KEY.getFieldName()).isEqualTo(
                                                                                  QueryBuilder.literal(
                                                                                      ByteBuffer.wrap(
                                                                                          row.getFieldValues()[i].getVisibility()))).build());
      exhausted &= rs.isFullyFetched();
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
      final Optional<KeyspaceMetadata> keyspace = session.getMetadata().getKeyspace(gwNamespace);
      if (keyspace.isPresent()) {
        tableExists = keyspace.get().getTable(tableName).isPresent();
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
  public RowWriter createWriter(final Index index, final InternalDataAdapter<?> adapter) {
    createTable(index.getName());
    return new CassandraWriter(index.getName(), this);
  }

  private CreateTable addOptions(final CreateTable create) {
    final Iterator<String[]> validOptions =
        options.getTableOptions().entrySet().stream().map(
            e -> new String[] {e.getKey(), e.getValue()}).iterator();
    CreateTable retVal = create;
    boolean addCompaction = true;
    boolean addGcGraceSeconds = true;
    while (validOptions.hasNext()) {
      final String[] option = validOptions.next();
      final String key = option[0].trim();
      final String valueStr = option[1].trim();
      Object value;
      if (valueStr.startsWith("{")) {
        try {
          value = new ObjectMapper().readValue(valueStr, HashMap.class);
        } catch (final IOException e) {
          LOGGER.warn(
              "Unable to convert '" + valueStr + "' to a JSON map for cassandra table creation",
              e);
          value = valueStr;
        }
      } else {
        value = valueStr;
      }

      if ("compaction".equals(key)) {
        addCompaction = false;
        LOGGER.info(
            "Found compaction in general table options, ignoring --compactionStrategy option.");
      } else if ("gc_grace_seconds".equals(key)) {
        addGcGraceSeconds = false;
        LOGGER.info(
            "Found gc_grace_seconds in general table options, ignoring --gcGraceSeconds option.");
      }
      retVal = (CreateTable) retVal.withOption(key, value);
    }
    if (addCompaction) {
      retVal = (CreateTable) retVal.withCompaction(options.getCompactionStrategy());
    }
    if (addGcGraceSeconds) {
      retVal = (CreateTable) retVal.withGcGraceSeconds(options.getGcGraceSeconds());
    }
    return retVal;

  }

  private boolean createTable(final String indexName) {
    synchronized (CREATE_TABLE_MUTEX) {
      try {
        if (!indexExists(indexName)) {
          final String tableName = getCassandraSafeName(indexName);
          CreateTable create =
              addOptions(
                  CassandraField.GW_PARTITION_ID_KEY.addPartitionKey(getCreateTable(tableName)));
          CassandraField[] fields = CassandraField.values();
          if (DataIndexUtils.isDataIndex(tableName)) {
            fields =
                Arrays.stream(fields).filter(f -> f.isDataIndexColumn()).filter(
                    f -> !f.isPartitionKey()).toArray(i -> new CassandraField[i]);
          }
          for (final CassandraField f : fields) {
            create = f.addColumn(create);
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

  public void dropMetadataTable(final MetadataType metadataType) {
    final String tableName = getMetadataTableName(metadataType);
    // this checks for existence prior to drop
    synchronized (CREATE_TABLE_MUTEX) {
      executeDropTable(SchemaBuilder.dropTable(gwNamespace, tableName).ifExists(), tableName);
    }
  }

  private String ensureTableExists(final MetadataType metadataType) {
    final String tableName = getMetadataTableName(metadataType);
    // this checks for existence prior to create
    synchronized (CREATE_TABLE_MUTEX) {
      try {
        if (!indexExists(tableName)) {
          // create table
          CreateTable create =
              addOptions(
                  getCreateTable(tableName).withPartitionKey(
                      CassandraMetadataWriter.PRIMARY_ID_KEY,
                      DataTypes.BLOB));
          if (MetadataType.STATISTICS.equals(metadataType)
              || MetadataType.STATISTIC_VALUES.equals(metadataType)
              || MetadataType.LEGACY_STATISTICS.equals(metadataType)
              || MetadataType.INTERNAL_ADAPTER.equals(metadataType)
              || MetadataType.INDEX_MAPPINGS.equals(metadataType)) {
            create =
                create.withClusteringColumn(
                    CassandraMetadataWriter.SECONDARY_ID_KEY,
                    DataTypes.BLOB).withClusteringColumn(
                        CassandraMetadataWriter.TIMESTAMP_ID_KEY,
                        DataTypes.TIMEUUID);
            if (MetadataType.STATISTIC_VALUES.equals(metadataType)
                || MetadataType.LEGACY_STATISTICS.equals(metadataType)) {
              create = create.withColumn(CassandraMetadataWriter.VISIBILITY_KEY, DataTypes.BLOB);
            }
          }
          executeCreateTable(
              create.withColumn(CassandraMetadataWriter.VALUE_KEY, DataTypes.BLOB),
              tableName);
        }
      } catch (final IOException e) {
        LOGGER.warn("Unable to check if table exists", e);
      }
    }
    return tableName;
  }

  @Override
  public MetadataWriter createMetadataWriter(final MetadataType metadataType) {
    final String tableName = ensureTableExists(metadataType);
    return new CassandraMetadataWriter(this, tableName);
  }

  @Override
  public MetadataReader createMetadataReader(final MetadataType metadataType) {
    ensureTableExists(metadataType);
    return new CassandraMetadataReader(this, metadataType);
  }

  @Override
  public MetadataDeleter createMetadataDeleter(final MetadataType metadataType) {
    ensureTableExists(metadataType);
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
      final byte[] d = r.getByteBuffer(CassandraField.GW_PARTITION_ID_KEY.getFieldName()).array();
      final byte[] v = r.getByteBuffer(CassandraField.GW_VALUE_KEY.getFieldName()).array();

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
        final Select select = getSelect(safeTableName);;
        preparedRead =
            session.prepare(
                select.whereColumn(
                    CassandraRow.CassandraField.GW_PARTITION_ID_KEY.getFieldName()).in(
                        QueryBuilder.bindMarker(
                            CassandraRow.CassandraField.GW_PARTITION_ID_KEY.getBindMarkerName())).whereColumn(
                                CassandraRow.CassandraField.GW_ADAPTER_ID_KEY.getFieldName()).isEqualTo(
                                    QueryBuilder.bindMarker(
                                        CassandraRow.CassandraField.GW_ADAPTER_ID_KEY.getBindMarkerName())).build());
        state.preparedRangeReadsPerTable.put(safeTableName, preparedRead);
      }
    }
    final BoundStatementBuilder statement = preparedRead.boundStatementBuilder();
    final ResultSet results =
        getSession().execute(
            statement.set(
                CassandraField.GW_ADAPTER_ID_KEY.getBindMarkerName(),
                adapterId,
                TypeCodecs.SMALLINT).set(
                    CassandraField.GW_PARTITION_ID_KEY.getBindMarkerName(),
                    Arrays.stream(dataIds).map(d -> ByteBuffer.wrap(d)).collect(
                        Collectors.toList()),
                    TypeCodecs.listOf(TypeCodecs.BLOB)).build());
    final Map<ByteArray, GeoWaveRow> resultsMap = new HashMap<>();
    results.forEach(r -> {
      final byte[] d = r.getByteBuffer(CassandraField.GW_PARTITION_ID_KEY.getFieldName()).array();
      final byte[] v = r.getByteBuffer(CassandraField.GW_VALUE_KEY.getFieldName()).array();
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
    final String tableName = metadataType.id() + "_" + AbstractGeoWavePersistence.METADATA_TABLE;
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
        QueryBuilder.deleteFrom(
            gwNamespace,
            getCassandraSafeName(DataIndexUtils.DATA_ID_INDEX.getName())).whereColumn(
                CassandraField.GW_PARTITION_ID_KEY.getFieldName()).in(
                    Arrays.stream(dataIds).map(
                        d -> QueryBuilder.literal(ByteBuffer.wrap(d))).collect(
                            Collectors.toList())).whereColumn(
                                CassandraField.GW_ADAPTER_ID_KEY.getFieldName()).isEqualTo(
                                    QueryBuilder.literal(adapterId)).build());
  }
}
