/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.datastore.accumulo.operations;

import com.aol.cyclops.util.function.TriFunction;
import com.google.common.collect.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.locationtech.geowave.core.index.*;
import org.locationtech.geowave.core.index.MultiDimensionalCoordinateRangesArray.ArrayOfArrays;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.CloseableIteratorWrapper;
import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.base.dataidx.DataIndexUtils;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowIteratorTransformer;
import org.locationtech.geowave.core.store.metadata.AbstractGeoWavePersistence;
import org.locationtech.geowave.core.store.metadata.DataStatisticsStoreImpl;
import org.locationtech.geowave.core.store.operations.*;
import org.locationtech.geowave.core.store.query.aggregate.CommonIndexAggregation;
import org.locationtech.geowave.core.store.server.BasicOptionProvider;
import org.locationtech.geowave.core.store.server.RowMergingAdapterOptionProvider;
import org.locationtech.geowave.core.store.server.ServerOpConfig.ServerOpScope;
import org.locationtech.geowave.core.store.server.ServerOpHelper;
import org.locationtech.geowave.core.store.server.ServerSideOperations;
import org.locationtech.geowave.core.store.util.DataAdapterAndIndexCache;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import org.locationtech.geowave.datastore.accumulo.AccumuloStoreFactoryFamily;
import org.locationtech.geowave.datastore.accumulo.IteratorConfig;
import org.locationtech.geowave.datastore.accumulo.MergingCombiner;
import org.locationtech.geowave.datastore.accumulo.MergingVisibilityCombiner;
import org.locationtech.geowave.datastore.accumulo.config.AccumuloOptions;
import org.locationtech.geowave.datastore.accumulo.config.AccumuloRequiredOptions;
import org.locationtech.geowave.datastore.accumulo.iterators.*;
import org.locationtech.geowave.datastore.accumulo.mapreduce.AccumuloSplitsProvider;
import org.locationtech.geowave.datastore.accumulo.util.AccumuloUtils;
import org.locationtech.geowave.datastore.accumulo.util.ConnectorPool;
import org.locationtech.geowave.mapreduce.MapReduceDataStoreOperations;
import org.locationtech.geowave.mapreduce.splits.GeoWaveRowRange;
import org.locationtech.geowave.mapreduce.splits.RecordReaderParams;
import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * This class holds all parameters necessary for establishing Accumulo connections and provides
 * basic factory methods for creating a batch scanner and a batch writer
 */
public class AccumuloOperations implements MapReduceDataStoreOperations, ServerSideOperations {
  private static final Logger LOGGER = Logger.getLogger(AccumuloOperations.class);
  private static final int DEFAULT_NUM_THREADS = 16;
  private static final long DEFAULT_TIMEOUT_MILLIS = 1000L; // 1 second
  private static final long DEFAULT_BYTE_BUFFER_SIZE = 1048576L; // 1 MB
  private static final String DEFAULT_AUTHORIZATION = null;
  private static final String DEFAULT_TABLE_NAMESPACE = "";
  private final int numThreads;
  private final long timeoutMillis;
  private final long byteBufferSize;
  private final String authorization;
  private final String tableNamespace;
  protected Connector connector;
  private final Map<String, Long> locGrpCache;
  private long cacheTimeoutMillis;
  private final Map<String, Set<String>> ensuredAuthorizationCache = new HashMap<>();
  private final Map<String, Set<ByteArray>> ensuredPartitionCache = new HashMap<>();
  private final AccumuloOptions options;
  private String passwordOrKeytab;

  /**
   * This is will create an Accumulo connector based on passed in connection information and
   * credentials for convenience convenience. It will also use reasonable defaults for unspecified
   * parameters.
   *
   * @param zookeeperUrl The comma-delimited URLs for all zookeeper servers, this will be directly
   *        used to instantiate a ZookeeperInstance
   * @param instanceName The zookeeper instance name, this will be directly used to instantiate a
   *        ZookeeperInstance
   * @param userName The username for an account to establish an Accumulo connector
   * @param passwordOrKeytab The password for the account to establish an Accumulo connector or path
   *        to keytab file for SASL
   * @param tableNamespace An optional string that is prefixed to any of the table names
   * @param options Options for the Accumulo data store
   * @throws AccumuloException Thrown if a generic exception occurs when establishing a connector
   * @throws AccumuloSecurityException the credentials passed in are invalid
   */
  public AccumuloOperations(
      final String zookeeperUrl,
      final String instanceName,
      final String userName,
      final String passwordOrKeytab,
      final boolean useSasl,
      final String tableNamespace,
      final AccumuloOptions options)
      throws AccumuloException, AccumuloSecurityException, IOException {
    this(null, tableNamespace, options);
    connector =
        ConnectorPool.getInstance().getConnector(
            zookeeperUrl,
            instanceName,
            userName,
            passwordOrKeytab,
            useSasl);
    this.passwordOrKeytab = passwordOrKeytab;
  }

  /**
   * This constructor uses reasonable defaults and only requires an Accumulo connector
   *
   * @param connector The connector to use for all operations
   * @param options Options for the Accumulo data store
   */
  public AccumuloOperations(final Connector connector, final AccumuloOptions options) {
    this(connector, DEFAULT_TABLE_NAMESPACE, options);
  }

  /**
   * This constructor uses reasonable defaults and requires an Accumulo connector and table
   * namespace
   *
   * @param connector The connector to use for all operations
   * @param tableNamespace An optional string that is prefixed to any of the table names
   * @param options Options for the Accumulo data store
   */
  public AccumuloOperations(
      final Connector connector,
      final String tableNamespace,
      final AccumuloOptions options) {
    this(
        DEFAULT_NUM_THREADS,
        DEFAULT_TIMEOUT_MILLIS,
        DEFAULT_BYTE_BUFFER_SIZE,
        DEFAULT_AUTHORIZATION,
        tableNamespace,
        connector,
        options);
  }

  /**
   * This is the full constructor for the operation factory and should be used if any of the
   * defaults are insufficient.
   *
   * @param numThreads The number of threads to use for a batch scanner and batch writer
   * @param timeoutMillis The time out in milliseconds to use for a batch writer
   * @param byteBufferSize The buffer size in bytes to use for a batch writer
   * @param authorization The authorization to use for a batch scanner
   * @param tableNamespace An optional string that is prefixed to any of the table names
   * @param connector The connector to use for all operations
   * @param options Options for the Accumulo data store
   */
  public AccumuloOperations(
      final int numThreads,
      final long timeoutMillis,
      final long byteBufferSize,
      final String authorization,
      final String tableNamespace,
      final Connector connector,
      final AccumuloOptions options) {
    this.numThreads = numThreads;
    this.timeoutMillis = timeoutMillis;
    this.byteBufferSize = byteBufferSize;
    this.authorization = authorization;
    this.tableNamespace = tableNamespace;
    this.connector = connector;
    this.options = options;
    locGrpCache = new HashMap<>();
    cacheTimeoutMillis = TimeUnit.DAYS.toMillis(1);
  }

  public int getNumThreads() {
    return numThreads;
  }

  public long getTimeoutMillis() {
    return timeoutMillis;
  }

  public long getByteBufferSize() {
    return byteBufferSize;
  }

  public Connector getConnector() {
    return connector;
  }

  public static String getUsername(final AccumuloRequiredOptions options)
      throws AccumuloException, AccumuloSecurityException {
    return options.getUser();
  }

  public static String getPassword(final AccumuloRequiredOptions options)
      throws AccumuloException, AccumuloSecurityException {
    return options.getPassword();
  }

  public String getGeoWaveNamespace() {
    return tableNamespace;
  }

  public String getUsername() {
    return connector.whoami();
  }

  public String getPassword() {
    return passwordOrKeytab;
  }

  public Instance getInstance() {
    return connector.getInstance();
  }

  private String[] getAuthorizations(final String... additionalAuthorizations) {
    final String[] safeAdditionalAuthorizations =
        additionalAuthorizations == null ? new String[] {} : additionalAuthorizations;

    return authorization == null ? safeAdditionalAuthorizations
        : (String[]) ArrayUtils.add(safeAdditionalAuthorizations, authorization);
  }

  public boolean createIndex(final Index index) throws IOException {
    return createTable(
        index.getName(),
        options.isServerSideLibraryEnabled(),
        options.isEnableBlockCache());
  }

  public synchronized boolean createTable(
      final String tableName,
      final boolean enableVersioning,
      final boolean enableBlockCache) {
    final String qName = getQualifiedTableName(tableName);

    if (!connector.tableOperations().exists(qName)) {
      try {
        final NewTableConfiguration config = new NewTableConfiguration();

        final Map<String, String> propMap = new HashMap(config.getProperties());

        if (enableBlockCache) {
          propMap.put(Property.TABLE_BLOCKCACHE_ENABLED.getKey(), "true");

          config.setProperties(propMap);
        }

        connector.tableOperations().create(qName, config);

        // Versioning is on by default; only need to detach
        if (!enableVersioning) {
          enableVersioningIterator(tableName, false);
        }
        return true;
      } catch (AccumuloException | AccumuloSecurityException | TableExistsException e) {
        LOGGER.warn("Unable to create table '" + qName + "'", e);
      } catch (final TableNotFoundException e) {
        LOGGER.error("Error disabling version iterator", e);
      }
    }
    return false;
  }

  public long getRowCount(final String tableName, final String... additionalAuthorizations) {
    RowIterator rowIterator;
    try {
      rowIterator =
          new RowIterator(
              connector.createScanner(
                  getQualifiedTableName(tableName),
                  (authorization == null) ? new Authorizations(additionalAuthorizations)
                      : new Authorizations(
                          (String[]) ArrayUtils.add(additionalAuthorizations, authorization))));
      while (rowIterator.hasNext()) {
        rowIterator.next();
      }
      return rowIterator.getKVCount();
    } catch (final TableNotFoundException e) {
      LOGGER.warn("Table '" + tableName + "' not found during count operation", e);
      return 0;
    }
  }

  public boolean deleteTable(final String tableName) {
    final String qName = getQualifiedTableName(tableName);
    try {
      connector.tableOperations().delete(qName);
      return true;
    } catch (final TableNotFoundException e) {
      LOGGER.warn("Unable to delete table, table not found '" + qName + "'", e);
    } catch (AccumuloException | AccumuloSecurityException e) {
      LOGGER.warn("Unable to delete table '" + qName + "'", e);
    }
    return false;
  }

  public String getTableNameSpace() {
    return tableNamespace;
  }

  private String getQualifiedTableName(final String unqualifiedTableName) {
    return AccumuloUtils.getQualifiedTableName(tableNamespace, unqualifiedTableName);
  }

  /** */
  @Override
  public void deleteAll() throws Exception {
    SortedSet<String> tableNames = connector.tableOperations().list();

    if ((tableNamespace != null) && !tableNamespace.isEmpty()) {
      tableNames = tableNames.subSet(tableNamespace, tableNamespace + '\uffff');
    }

    for (final String tableName : tableNames) {
      connector.tableOperations().delete(tableName);
    }
    DataAdapterAndIndexCache.getInstance(
        RowMergingAdapterOptionProvider.ROW_MERGING_ADAPTER_CACHE_ID,
        tableNamespace,
        AccumuloStoreFactoryFamily.TYPE).deleteAll();
    locGrpCache.clear();
    ensuredAuthorizationCache.clear();
    ensuredPartitionCache.clear();
  }

  public boolean delete(
      final String tableName,
      final ByteArray rowId,
      final String columnFamily,
      final byte[] columnQualifier,
      final String... additionalAuthorizations) {
    return this.delete(
        tableName,
        Arrays.asList(rowId),
        columnFamily,
        columnQualifier,
        additionalAuthorizations);
  }

  public boolean deleteAll(
      final String tableName,
      final String columnFamily,
      final String... additionalAuthorizations) {
    BatchDeleter deleter = null;
    try {
      deleter = createBatchDeleter(tableName, additionalAuthorizations);
      deleter.setRanges(Arrays.asList(new Range()));
      deleter.fetchColumnFamily(new Text(columnFamily));
      deleter.delete();
      return true;
    } catch (final TableNotFoundException | MutationsRejectedException e) {
      LOGGER.warn("Unable to delete row from table [" + tableName + "].", e);
      return false;
    } finally {
      if (deleter != null) {
        deleter.close();
      }
    }
  }

  public boolean delete(
      final String tableName,
      final List<ByteArray> rowIds,
      final String columnFamily,
      final byte[] columnQualifier,
      final String... authorizations) {

    boolean success = true;
    BatchDeleter deleter = null;
    try {
      deleter = createBatchDeleter(tableName, authorizations);
      if ((columnFamily != null) && !columnFamily.isEmpty()) {
        if ((columnQualifier != null) && (columnQualifier.length != 0)) {
          deleter.fetchColumn(new Text(columnFamily), new Text(columnQualifier));
        } else {
          deleter.fetchColumnFamily(new Text(columnFamily));
        }
      }
      final Set<ByteArray> removeSet = new HashSet<>();
      final List<Range> rowRanges = new ArrayList<>();
      for (final ByteArray rowId : rowIds) {
        rowRanges.add(Range.exact(new Text(rowId.getBytes())));
        removeSet.add(new ByteArray(rowId.getBytes()));
      }
      deleter.setRanges(rowRanges);

      final Iterator<Map.Entry<Key, Value>> iterator = deleter.iterator();
      while (iterator.hasNext()) {
        final Entry<Key, Value> entry = iterator.next();
        removeSet.remove(new ByteArray(entry.getKey().getRowData().getBackingArray()));
      }

      if (removeSet.isEmpty()) {
        deleter.delete();
      }
    } catch (final TableNotFoundException | MutationsRejectedException e) {
      LOGGER.warn("Unable to delete row from table [" + tableName + "].", e);
      success = false;
    } finally {
      if (deleter != null) {
        deleter.close();
      }
    }

    return success;
  }

  public boolean localityGroupExists(final String tableName, final String typeName)
      throws AccumuloException, TableNotFoundException {
    final String qName = getQualifiedTableName(tableName);
    final String localityGroupStr = qName + typeName;

    // check the cache for our locality group
    if (locGrpCache.containsKey(localityGroupStr)) {
      if ((locGrpCache.get(localityGroupStr) - new Date().getTime()) < cacheTimeoutMillis) {
        return true;
      } else {
        locGrpCache.remove(localityGroupStr);
      }
    }

    // check accumulo to see if locality group exists
    final boolean groupExists =
        connector.tableOperations().exists(qName)
            && connector.tableOperations().getLocalityGroups(qName).keySet().contains(typeName);

    // update the cache
    if (groupExists) {
      locGrpCache.put(localityGroupStr, new Date().getTime());
    }

    return groupExists;
  }

  public void addLocalityGroup(final String tableName, final String typeName, final short adapterId)
      throws AccumuloException, TableNotFoundException, AccumuloSecurityException {
    final String qName = getQualifiedTableName(tableName);
    final String localityGroupStr = qName + typeName;

    // check the cache for our locality group
    if (locGrpCache.containsKey(localityGroupStr)) {
      if ((locGrpCache.get(localityGroupStr) - new Date().getTime()) < cacheTimeoutMillis) {
        return;
      } else {
        locGrpCache.remove(localityGroupStr);
      }
    }

    // add locality group to accumulo and update the cache
    if (connector.tableOperations().exists(qName)) {
      final Map<String, Set<Text>> localityGroups =
          connector.tableOperations().getLocalityGroups(qName);

      final Set<Text> groupSet = new HashSet<>();

      groupSet.add(new Text(ByteArrayUtils.shortToString(adapterId)));

      localityGroups.put(typeName, groupSet);

      connector.tableOperations().setLocalityGroups(qName, localityGroups);

      locGrpCache.put(localityGroupStr, new Date().getTime());
    }
  }

  public ClientSideIteratorScanner createClientScanner(
      final String tableName,
      final String... additionalAuthorizations) throws TableNotFoundException {
    return new ClientSideIteratorScanner(createScanner(tableName, additionalAuthorizations));
  }

  public CloseableIterator<GeoWaveRow> getDataIndexResults(
      final byte[] startRow,
      final byte[] endRow,
      final short adapterId,
      final String... additionalAuthorizations) {
    final byte[] family = StringUtils.stringToBinary(ByteArrayUtils.shortToString(adapterId));

    // to have backwards compatibility before 1.8.0 we can assume BaseScanner is autocloseable
    final Scanner scanner;
    try {
      scanner = createScanner(DataIndexUtils.DATA_ID_INDEX.getName(), additionalAuthorizations);

      scanner.setRange(
          AccumuloUtils.byteArrayRangeToAccumuloRange(new ByteArrayRange(startRow, endRow)));
      scanner.fetchColumnFamily(new Text(family));
      return new CloseableIteratorWrapper(new Closeable() {
        @Override
        public void close() throws IOException {
          scanner.close();
        }
      },
          Streams.stream(scanner.iterator()).map(
              entry -> DataIndexUtils.deserializeDataIndexRow(
                  entry.getKey().getRow().getBytes(),
                  adapterId,
                  entry.getValue().get(),
                  false)).iterator());
    } catch (final TableNotFoundException e) {
      LOGGER.error("unable to find data index table", e);
    }
    return new CloseableIterator.Empty<>();
  }

  public CloseableIterator<GeoWaveRow> getDataIndexResults(
      final short adapterId,
      final String... additionalAuthorizations) {
    final byte[] family = StringUtils.stringToBinary(ByteArrayUtils.shortToString(adapterId));

    // to have backwards compatibility before 1.8.0 we can assume BaseScanner is autocloseable
    final Scanner scanner;
    try {
      scanner = createScanner(DataIndexUtils.DATA_ID_INDEX.getName(), additionalAuthorizations);
      scanner.setRange(new Range());
      scanner.fetchColumnFamily(new Text(family));
      return new CloseableIteratorWrapper(new Closeable() {
        @Override
        public void close() throws IOException {
          scanner.close();
        }
      },
          Streams.stream(scanner).map(
              entry -> DataIndexUtils.deserializeDataIndexRow(
                  entry.getKey().getRow().getBytes(),
                  adapterId,
                  entry.getValue().get(),
                  false)).iterator());
    } catch (final TableNotFoundException e) {
      LOGGER.error("unable to find data index table", e);
    }
    return new CloseableIterator.Empty<>();
  }

  public CloseableIterator<GeoWaveRow> getDataIndexResults(
      final byte[][] rows,
      final short adapterId,
      final String... additionalAuthorizations) {
    if ((rows == null) || (rows.length == 0)) {
      return new CloseableIterator.Empty<>();
    }
    final byte[] family = StringUtils.stringToBinary(ByteArrayUtils.shortToString(adapterId));

    // to have backwards compatibility before 1.8.0 we can assume BaseScanner is autocloseable
    final BatchScanner batchScanner;
    try {
      batchScanner =
          createBatchScanner(DataIndexUtils.DATA_ID_INDEX.getName(), additionalAuthorizations);
      batchScanner.setRanges(
          Arrays.stream(rows).map(r -> Range.exact(new Text(r))).collect(Collectors.toList()));
      batchScanner.fetchColumnFamily(new Text(family));
      final Map<ByteArray, byte[]> results = new HashMap<>();
      batchScanner.iterator().forEachRemaining(
          entry -> results.put(
              new ByteArray(entry.getKey().getRow().getBytes()),
              entry.getValue().get()));
      return new CloseableIteratorWrapper(new Closeable() {
        @Override
        public void close() throws IOException {
          batchScanner.close();
        }
      },
          Arrays.stream(rows).filter(r -> results.containsKey(new ByteArray(r))).map(
              r -> DataIndexUtils.deserializeDataIndexRow(
                  r,
                  adapterId,
                  results.get(new ByteArray(r)),
                  false)).iterator());
    } catch (final TableNotFoundException e) {
      LOGGER.error("unable to find data index table", e);
    }
    return new CloseableIterator.Empty<>();
  }

  @Override
  public RowWriter createDataIndexWriter(final InternalDataAdapter<?> adapter) {
    return internalCreateWriter(
        DataIndexUtils.DATA_ID_INDEX,
        adapter,
        (batchWriter, operations, tableName) -> new AccumuloDataIndexWriter(
            batchWriter,
            operations,
            tableName));
  }

  @Override
  public RowReader<GeoWaveRow> createReader(final DataIndexReaderParams readerParams) {
    if (readerParams.getDataIds() == null) {
      if ((readerParams.getStartInclusiveDataId() != null)
          || (readerParams.getEndInclusiveDataId() != null)) {
        return new RowReaderWrapper<>(
            getDataIndexResults(
                readerParams.getStartInclusiveDataId(),
                readerParams.getEndInclusiveDataId(),
                readerParams.getAdapterId(),
                readerParams.getAdditionalAuthorizations()));
      } else {
        return new RowReaderWrapper<>(
            getDataIndexResults(
                readerParams.getAdapterId(),
                readerParams.getAdditionalAuthorizations()));
      }
    }
    return new RowReaderWrapper<>(
        getDataIndexResults(
            readerParams.getDataIds(),
            readerParams.getAdapterId(),
            readerParams.getAdditionalAuthorizations()));
  }

  public Scanner createScanner(final String tableName, final String... additionalAuthorizations)
      throws TableNotFoundException {
    return connector.createScanner(
        getQualifiedTableName(tableName),
        new Authorizations(getAuthorizations(additionalAuthorizations)));
  }

  public BatchScanner createBatchScanner(
      final String tableName,
      final String... additionalAuthorizations) throws TableNotFoundException {
    return connector.createBatchScanner(
        getQualifiedTableName(tableName),
        new Authorizations(getAuthorizations(additionalAuthorizations)),
        numThreads);
  }

  @Override
  public boolean ensureAuthorizations(final String clientUser, final String... authorizations) {
    String user;
    if (clientUser == null) {
      user = connector.whoami();
    } else {
      user = clientUser;
    }
    final Set<String> unensuredAuths = new HashSet<>();
    Set<String> ensuredAuths = ensuredAuthorizationCache.get(user);
    if (ensuredAuths == null) {
      unensuredAuths.addAll(Arrays.asList(authorizations));
      ensuredAuths = new HashSet<>();
      ensuredAuthorizationCache.put(user, ensuredAuths);
    } else {
      for (final String auth : authorizations) {
        if (!ensuredAuths.contains(auth)) {
          unensuredAuths.add(auth);
        }
      }
    }
    if (!unensuredAuths.isEmpty()) {
      try {
        Authorizations auths = connector.securityOperations().getUserAuthorizations(user);
        final List<byte[]> newSet = new ArrayList<>();
        for (final String auth : unensuredAuths) {
          if (!auths.contains(auth)) {
            newSet.add(auth.getBytes(StringUtils.UTF8_CHARSET));
          }
        }
        if (newSet.size() > 0) {
          newSet.addAll(auths.getAuthorizations());
          connector.securityOperations().changeUserAuthorizations(user, new Authorizations(newSet));
          auths = connector.securityOperations().getUserAuthorizations(user);

          LOGGER.trace(
              clientUser + " has authorizations " + ArrayUtils.toString(auths.getAuthorizations()));
        }
        for (final String auth : unensuredAuths) {
          ensuredAuths.add(auth);
        }
      } catch (AccumuloException | AccumuloSecurityException e) {
        LOGGER.error(
            "Unable to add authorizations '"
                + Arrays.toString(unensuredAuths.toArray(new String[] {}))
                + "'",
            e);
        return false;
      }
    }
    return true;
  }

  public BatchDeleter createBatchDeleter(
      final String tableName,
      final String... additionalAuthorizations) throws TableNotFoundException {
    return connector.createBatchDeleter(
        getQualifiedTableName(tableName),
        new Authorizations(getAuthorizations(additionalAuthorizations)),
        numThreads,
        new BatchWriterConfig().setMaxWriteThreads(numThreads).setMaxMemory(
            byteBufferSize).setTimeout(timeoutMillis, TimeUnit.MILLISECONDS));
  }

  public long getCacheTimeoutMillis() {
    return cacheTimeoutMillis;
  }

  public void setCacheTimeoutMillis(final long cacheTimeoutMillis) {
    this.cacheTimeoutMillis = cacheTimeoutMillis;
  }

  public void ensurePartition(final ByteArray partition, final String tableName) {
    final String qName = getQualifiedTableName(tableName);
    Set<ByteArray> existingPartitions = ensuredPartitionCache.get(qName);
    try {
      synchronized (ensuredPartitionCache) {
        if (existingPartitions == null) {
          Collection<Text> splits;
          splits = connector.tableOperations().listSplits(qName);
          existingPartitions = new HashSet<>();
          for (final Text s : splits) {
            existingPartitions.add(new ByteArray(s.getBytes()));
          }
          ensuredPartitionCache.put(qName, existingPartitions);
        }
        if (!existingPartitions.contains(partition)) {
          final SortedSet<Text> partitionKeys = new TreeSet<>();
          partitionKeys.add(new Text(partition.getBytes()));
          connector.tableOperations().addSplits(qName, partitionKeys);
          existingPartitions.add(partition);
        }
      }
    } catch (TableNotFoundException | AccumuloSecurityException | AccumuloException e) {
      LOGGER.warn(
          "Unable to add partition '" + partition.getHexString() + "' to table '" + qName + "'",
          e);
    }
  }

  public boolean attachIterators(
      final String tableName,
      final boolean createTable,
      final boolean enableVersioning,
      final boolean enableBlockCache,
      final IteratorConfig... iterators) throws TableNotFoundException {
    final String qName = getQualifiedTableName(tableName);
    if (createTable && !connector.tableOperations().exists(qName)) {
      createTable(tableName, enableVersioning, enableBlockCache);
    }
    try {
      if ((iterators != null) && (iterators.length > 0)) {
        final Map<String, EnumSet<IteratorScope>> iteratorScopes =
            connector.tableOperations().listIterators(qName);
        for (final IteratorConfig iteratorConfig : iterators) {
          boolean mustDelete = false;
          boolean exists = false;
          final EnumSet<IteratorScope> existingScopes =
              iteratorScopes.get(iteratorConfig.getIteratorName());
          EnumSet<IteratorScope> configuredScopes;
          if (iteratorConfig.getScopes() == null) {
            configuredScopes = EnumSet.allOf(IteratorScope.class);
          } else {
            configuredScopes = iteratorConfig.getScopes();
          }
          Map<String, String> configuredOptions = null;
          if (existingScopes != null) {
            if (existingScopes.size() == configuredScopes.size()) {
              exists = true;
              for (final IteratorScope s : existingScopes) {
                if (!configuredScopes.contains(s)) {
                  // this iterator exists with the wrong
                  // scope, we will assume we want to remove
                  // it and add the new configuration
                  LOGGER.warn(
                      "found iterator '"
                          + iteratorConfig.getIteratorName()
                          + "' missing scope '"
                          + s.name()
                          + "', removing it and re-attaching");

                  mustDelete = true;
                  break;
                }
              }
            }
            if (existingScopes.size() > 0) {
              // see if the options are the same, if they are not
              // the same, apply a merge with the existing options
              // and the configured options
              final Iterator<IteratorScope> it = existingScopes.iterator();
              while (it.hasNext()) {
                final IteratorScope scope = it.next();
                final IteratorSetting setting =
                    connector.tableOperations().getIteratorSetting(
                        qName,
                        iteratorConfig.getIteratorName(),
                        scope);
                if (setting != null) {
                  final Map<String, String> existingOptions = setting.getOptions();
                  configuredOptions = iteratorConfig.getOptions(existingOptions);
                  if (existingOptions == null) {
                    mustDelete = (configuredOptions == null);
                  } else if (configuredOptions == null) {
                    mustDelete = true;
                  } else {
                    // neither are null, compare the size of
                    // the entry sets and check that they
                    // are equivalent
                    final Set<Entry<String, String>> existingEntries = existingOptions.entrySet();
                    final Set<Entry<String, String>> configuredEntries =
                        configuredOptions.entrySet();
                    if (existingEntries.size() != configuredEntries.size()) {
                      mustDelete = true;
                    } else {
                      mustDelete = (!existingEntries.containsAll(configuredEntries));
                    }
                  }
                  // we found the setting existing in one
                  // scope, assume the options are the same
                  // for each scope
                  break;
                }
              }
            }
          }
          if (mustDelete) {
            connector.tableOperations().removeIterator(
                qName,
                iteratorConfig.getIteratorName(),
                existingScopes);
            exists = false;
          }
          if (!exists) {
            if (configuredOptions == null) {
              configuredOptions = iteratorConfig.getOptions(new HashMap<>());
            }
            connector.tableOperations().attachIterator(
                qName,
                new IteratorSetting(
                    iteratorConfig.getIteratorPriority(),
                    iteratorConfig.getIteratorName(),
                    iteratorConfig.getIteratorClass(),
                    configuredOptions),
                configuredScopes);
          }
        }
      }
    } catch (AccumuloException | AccumuloSecurityException e) {
      LOGGER.warn("Unable to create table '" + qName + "'", e);
    }
    return true;
  }

  public static AccumuloOperations createOperations(final AccumuloRequiredOptions options)
      throws AccumuloException, AccumuloSecurityException, IOException {
    return new AccumuloOperations(
        options.getZookeeper(),
        options.getInstance(),
        options.getUser(),
        options.getPasswordOrKeytab(),
        options.isUseSasl(),
        options.getGeoWaveNamespace(),
        (AccumuloOptions) options.getStoreOptions());
  }

  @Override
  public boolean indexExists(final String indexName) throws IOException {
    final String qName = getQualifiedTableName(indexName);
    return connector.tableOperations().exists(qName);
  }

  @Override
  public boolean deleteAll(
      final String indexName,
      final String typeName,
      final Short adapterId,
      final String... additionalAuthorizations) {
    BatchDeleter deleter = null;
    try {
      deleter = createBatchDeleter(indexName, additionalAuthorizations);

      deleter.setRanges(Arrays.asList(new Range()));
      deleter.fetchColumnFamily(new Text(ByteArrayUtils.shortToString(adapterId)));
      deleter.delete();
      return true;
    } catch (final TableNotFoundException | MutationsRejectedException e) {
      LOGGER.warn("Unable to delete row from table [" + indexName + "].", e);
      return false;
    } finally {
      if (deleter != null) {
        deleter.close();
      }
    }
  }

  protected <T> ScannerBase getScanner(final ReaderParams<T> params, final boolean delete) {
    final List<ByteArrayRange> ranges = params.getQueryRanges().getCompositeQueryRanges();
    final String tableName = params.getIndex().getName();
    ScannerBase scanner;
    try {
      if (!params.isAggregation() && (ranges != null) && (ranges.size() == 1) && !delete) {
        if (!options.isServerSideLibraryEnabled()) {
          scanner = createClientScanner(tableName, params.getAdditionalAuthorizations());
        } else {
          scanner = createScanner(tableName, params.getAdditionalAuthorizations());
        }
        final ByteArrayRange r = ranges.get(0);
        if (r.isSingleValue()) {
          ((Scanner) scanner).setRange(Range.exact(new Text(r.getStart())));
        } else {
          ((Scanner) scanner).setRange(AccumuloUtils.byteArrayRangeToAccumuloRange(r));
        }
        if ((params.getLimit() != null)
            && (params.getLimit() > 0)
            && (params.getLimit() < ((Scanner) scanner).getBatchSize())) {
          // do allow the limit to be set to some enormous size.
          ((Scanner) scanner).setBatchSize(Math.min(1024, params.getLimit()));
        }
      } else {
        if (options.isServerSideLibraryEnabled()) {
          if (delete) {
            scanner = createBatchDeleter(tableName, params.getAdditionalAuthorizations());
            ((BatchDeleter) scanner).setRanges(
                AccumuloUtils.byteArrayRangesToAccumuloRanges(ranges));
          } else {
            scanner = createBatchScanner(tableName, params.getAdditionalAuthorizations());
            ((BatchScanner) scanner).setRanges(
                AccumuloUtils.byteArrayRangesToAccumuloRanges(ranges));
          }
        } else {
          scanner = createClientScanner(tableName, params.getAdditionalAuthorizations());
          if (ranges != null) {
            ((Scanner) scanner).setRange(
                AccumuloUtils.byteArrayRangeToAccumuloRange(ByteArrayUtils.getSingleRange(ranges)));

          }
        }
      }
      if (params.getMaxResolutionSubsamplingPerDimension() != null) {
        if (params.getMaxResolutionSubsamplingPerDimension().length != params.getIndex().getIndexStrategy().getOrderedDimensionDefinitions().length) {
          LOGGER.warn(
              "Unable to subsample for table '"
                  + tableName
                  + "'. Subsample dimensions = "
                  + params.getMaxResolutionSubsamplingPerDimension().length
                  + " when indexed dimensions = "
                  + params.getIndex().getIndexStrategy().getOrderedDimensionDefinitions().length);
        } else {

          final int cardinalityToSubsample =
              (int) Math.round(
                  IndexUtils.getDimensionalBitsUsed(
                      params.getIndex().getIndexStrategy(),
                      params.getMaxResolutionSubsamplingPerDimension())
                      + (8 * params.getIndex().getIndexStrategy().getPartitionKeyLength()));

          final IteratorSetting iteratorSettings =
              new IteratorSetting(
                  FixedCardinalitySkippingIterator.CARDINALITY_SKIPPING_ITERATOR_PRIORITY,
                  FixedCardinalitySkippingIterator.CARDINALITY_SKIPPING_ITERATOR_NAME,
                  FixedCardinalitySkippingIterator.class);
          iteratorSettings.addOption(
              FixedCardinalitySkippingIterator.CARDINALITY_SKIP_INTERVAL,
              Integer.toString(cardinalityToSubsample));
          scanner.addScanIterator(iteratorSettings);
        }
      }
    } catch (final TableNotFoundException e) {
      LOGGER.warn("Unable to query table '" + tableName + "'.  Table does not exist.", e);
      return null;
    }
    if ((params.getAdapterIds() != null) && (params.getAdapterIds().length > 0)) {
      for (final short adapterId : params.getAdapterIds()) {
        scanner.fetchColumnFamily(new Text(ByteArrayUtils.shortToString(adapterId)));
      }
    }
    return scanner;
  }

  protected <T> void addConstraintsScanIteratorSettings(
      final RecordReaderParams params,
      final ScannerBase scanner,
      final DataStoreOptions options) {
    addFieldSubsettingToIterator(params, scanner);
    if (params.isMixedVisibility()) {
      // we have to at least use a whole row iterator
      final IteratorSetting iteratorSettings =
          new IteratorSetting(
              QueryFilterIterator.QUERY_ITERATOR_PRIORITY,
              QueryFilterIterator.QUERY_ITERATOR_NAME,
              WholeRowIterator.class);
      scanner.addScanIterator(iteratorSettings);
    }
  }

  protected <T> void addConstraintsScanIteratorSettings(
      final ReaderParams<T> params,
      final ScannerBase scanner,
      final DataStoreOptions options) {
    addFieldSubsettingToIterator(params, scanner);
    IteratorSetting iteratorSettings = null;
    if (params.isServersideAggregation()) {
      if (params.isMixedVisibility()) {
        iteratorSettings =
            new IteratorSetting(
                QueryFilterIterator.QUERY_ITERATOR_PRIORITY,
                QueryFilterIterator.QUERY_ITERATOR_NAME,
                WholeRowAggregationIterator.class);
      } else {
        iteratorSettings =
            new IteratorSetting(
                QueryFilterIterator.QUERY_ITERATOR_PRIORITY,
                QueryFilterIterator.QUERY_ITERATOR_NAME,
                AggregationIterator.class);
      }
      if (!(params.getAggregation().getRight() instanceof CommonIndexAggregation)
          && (params.getAggregation().getLeft() != null)) {
        iteratorSettings.addOption(
            AggregationIterator.ADAPTER_OPTION_NAME,
            ByteArrayUtils.byteArrayToString(
                PersistenceUtils.toBinary(params.getAggregation().getLeft())));
      }
      final Aggregation aggr = params.getAggregation().getRight();
      iteratorSettings.addOption(
          AggregationIterator.AGGREGATION_OPTION_NAME,
          ByteArrayUtils.byteArrayToString(PersistenceUtils.toClassId(aggr)));
      if (aggr.getParameters() != null) { // sets the parameters
        iteratorSettings.addOption(
            AggregationIterator.PARAMETER_OPTION_NAME,
            ByteArrayUtils.byteArrayToString((PersistenceUtils.toBinary(aggr.getParameters()))));
      }
    }

    boolean usingDistributableFilter = false;

    if ((params.getFilter() != null) && !options.isSecondaryIndexing()) {
      usingDistributableFilter = true;
      if (iteratorSettings == null) {
        if (params.isMixedVisibility()) {
          iteratorSettings =
              new IteratorSetting(
                  QueryFilterIterator.QUERY_ITERATOR_PRIORITY,
                  QueryFilterIterator.QUERY_ITERATOR_NAME,
                  WholeRowQueryFilterIterator.class);
        } else {
          iteratorSettings =
              new IteratorSetting(
                  QueryFilterIterator.QUERY_ITERATOR_PRIORITY,
                  QueryFilterIterator.QUERY_ITERATOR_NAME,
                  QueryFilterIterator.class);
        }
      }
      iteratorSettings.addOption(
          QueryFilterIterator.FILTER,
          ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(params.getFilter())));
      if (!iteratorSettings.getOptions().containsKey(QueryFilterIterator.MODEL)) {
        // it may already be added as an option if its an aggregation
        iteratorSettings.addOption(
            QueryFilterIterator.MODEL,
            ByteArrayUtils.byteArrayToString(
                PersistenceUtils.toBinary(params.getIndex().getIndexModel())));
        iteratorSettings.addOption(
            QueryFilterIterator.PARTITION_KEY_LENGTH,
            Integer.toString(params.getIndex().getIndexStrategy().getPartitionKeyLength()));
      }
    } else if ((iteratorSettings == null) && params.isMixedVisibility()) {
      // we have to at least use a whole row iterator
      iteratorSettings =
          new IteratorSetting(
              QueryFilterIterator.QUERY_ITERATOR_PRIORITY,
              QueryFilterIterator.QUERY_ITERATOR_NAME,
              WholeRowIterator.class);
    }
    if (!usingDistributableFilter && (!options.isSecondaryIndexing())) {
      // it ends up being duplicative and slower to add both a
      // distributable query and the index constraints, but one of the two
      // is important to limit client-side filtering
      addIndexFilterToIterator(params, scanner);
    }
    if (iteratorSettings != null) {
      scanner.addScanIterator(iteratorSettings);
    }
  }

  protected <T> void addIndexFilterToIterator(
      final ReaderParams<T> params,
      final ScannerBase scanner) {
    final List<MultiDimensionalCoordinateRangesArray> coords = params.getCoordinateRanges();
    if ((coords != null) && !coords.isEmpty()) {
      final IteratorSetting iteratorSetting =
          new IteratorSetting(
              NumericIndexStrategyFilterIterator.IDX_FILTER_ITERATOR_PRIORITY,
              NumericIndexStrategyFilterIterator.IDX_FILTER_ITERATOR_NAME,
              NumericIndexStrategyFilterIterator.class);

      iteratorSetting.addOption(
          NumericIndexStrategyFilterIterator.INDEX_STRATEGY_KEY,
          ByteArrayUtils.byteArrayToString(
              PersistenceUtils.toBinary(params.getIndex().getIndexStrategy())));

      iteratorSetting.addOption(
          NumericIndexStrategyFilterIterator.COORDINATE_RANGE_KEY,
          ByteArrayUtils.byteArrayToString(
              new ArrayOfArrays(
                  coords.toArray(new MultiDimensionalCoordinateRangesArray[] {})).toBinary()));
      scanner.addScanIterator(iteratorSetting);
    }
  }

  protected <T> void addFieldSubsettingToIterator(
      final RangeReaderParams<T> params,
      final ScannerBase scanner) {
    if ((params.getFieldSubsets() != null) && !params.isAggregation()) {
      final String[] fieldNames = params.getFieldSubsets().getLeft();
      final DataTypeAdapter<?> associatedAdapter = params.getFieldSubsets().getRight();
      if ((fieldNames != null) && (fieldNames.length > 0) && (associatedAdapter != null)) {
        final IteratorSetting iteratorSetting = AttributeSubsettingIterator.getIteratorSetting();

        AttributeSubsettingIterator.setFieldNames(
            iteratorSetting,
            associatedAdapter,
            fieldNames,
            params.getIndex().getIndexModel());

        iteratorSetting.addOption(
            AttributeSubsettingIterator.WHOLE_ROW_ENCODED_KEY,
            Boolean.toString(params.isMixedVisibility()));
        scanner.addScanIterator(iteratorSetting);
      }
    }
  }

  protected <T> void addRowScanIteratorSettings(
      final ReaderParams<T> params,
      final ScannerBase scanner) {
    addFieldSubsettingToIterator(params, scanner);
    if (params.isMixedVisibility()) {
      // we have to at least use a whole row iterator
      final IteratorSetting iteratorSettings =
          new IteratorSetting(
              QueryFilterIterator.QUERY_ITERATOR_PRIORITY,
              QueryFilterIterator.QUERY_ITERATOR_NAME,
              WholeRowIterator.class);
      scanner.addScanIterator(iteratorSettings);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> RowReader<T> createReader(final ReaderParams<T> params) {
    final ScannerBase scanner = getScanner(params, false);

    addConstraintsScanIteratorSettings(params, scanner, options);

    return new AccumuloReader<>(
        scanner,
        params.getRowTransformer(),
        params.getIndex().getIndexStrategy().getPartitionKeyLength(),
        params.isMixedVisibility() && !params.isServersideAggregation(),
        params.isClientsideRowMerging(),
        true);
  }

  protected <T> Scanner getScanner(final RecordReaderParams params) {
    final GeoWaveRowRange range = params.getRowRange();
    final String tableName = params.getIndex().getName();
    Scanner scanner;
    try {
      scanner = createScanner(tableName, params.getAdditionalAuthorizations());
      if (range == null) {
        scanner.setRange(new Range());
      } else {
        scanner.setRange(
            AccumuloSplitsProvider.toAccumuloRange(
                range,
                params.getIndex().getIndexStrategy().getPartitionKeyLength()));
      }
      if ((params.getLimit() != null)
          && (params.getLimit() > 0)
          && (params.getLimit() < scanner.getBatchSize())) {
        // do allow the limit to be set to some enormous size.
        scanner.setBatchSize(Math.min(1024, params.getLimit()));
      }
      if (params.getMaxResolutionSubsamplingPerDimension() != null) {
        if (params.getMaxResolutionSubsamplingPerDimension().length != params.getIndex().getIndexStrategy().getOrderedDimensionDefinitions().length) {
          LOGGER.warn(
              "Unable to subsample for table '"
                  + tableName
                  + "'. Subsample dimensions = "
                  + params.getMaxResolutionSubsamplingPerDimension().length
                  + " when indexed dimensions = "
                  + params.getIndex().getIndexStrategy().getOrderedDimensionDefinitions().length);
        } else {

          final int cardinalityToSubsample =
              (int) Math.round(
                  IndexUtils.getDimensionalBitsUsed(
                      params.getIndex().getIndexStrategy(),
                      params.getMaxResolutionSubsamplingPerDimension())
                      + (8 * params.getIndex().getIndexStrategy().getPartitionKeyLength()));

          final IteratorSetting iteratorSettings =
              new IteratorSetting(
                  FixedCardinalitySkippingIterator.CARDINALITY_SKIPPING_ITERATOR_PRIORITY,
                  FixedCardinalitySkippingIterator.CARDINALITY_SKIPPING_ITERATOR_NAME,
                  FixedCardinalitySkippingIterator.class);
          iteratorSettings.addOption(
              FixedCardinalitySkippingIterator.CARDINALITY_SKIP_INTERVAL,
              Integer.toString(cardinalityToSubsample));
          scanner.addScanIterator(iteratorSettings);
        }
      }
    } catch (final TableNotFoundException e) {
      LOGGER.warn("Unable to query table '" + tableName + "'.  Table does not exist.", e);
      return null;
    }
    if ((params.getAdapterIds() != null) && (params.getAdapterIds().length > 0)) {
      for (final Short adapterId : params.getAdapterIds()) {
        scanner.fetchColumnFamily(new Text(ByteArrayUtils.shortToString(adapterId)));
      }
    }
    return scanner;
  }

  @SuppressWarnings("unchecked")
  @Override
  public RowReader<GeoWaveRow> createReader(final RecordReaderParams readerParams) {
    final ScannerBase scanner = getScanner(readerParams);
    addConstraintsScanIteratorSettings(readerParams, scanner, options);
    return new AccumuloReader<>(
        scanner,
        GeoWaveRowIteratorTransformer.NO_OP_TRANSFORMER,
        readerParams.getIndex().getIndexStrategy().getPartitionKeyLength(),
        readerParams.isMixedVisibility(),
        readerParams.isClientsideRowMerging(),
        false);
  }

  @Override
  public RowDeleter createRowDeleter(
      final String indexName,
      final PersistentAdapterStore adapterStore,
      final InternalAdapterStore internalAdapterStore,
      final String... authorizations) {
    try {
      return new AccumuloRowDeleter(createBatchDeleter(indexName, authorizations));
    } catch (final TableNotFoundException e) {
      LOGGER.error("Unable to create deleter", e);
      return null;
    }
  }

  @Override
  public RowWriter createWriter(final Index index, final InternalDataAdapter<?> adapter) {
    return internalCreateWriter(
        index,
        adapter,
        (batchWriter, operations, tableName) -> new AccumuloWriter(
            batchWriter,
            operations,
            tableName));
  }

  public RowWriter internalCreateWriter(
      final Index index,
      final InternalDataAdapter<?> adapter,
      final TriFunction<BatchWriter, AccumuloOperations, String, RowWriter> rowWriterSupplier) {
    final String tableName = index.getName();
    if (createTable(
        tableName,
        options.isServerSideLibraryEnabled(),
        options.isEnableBlockCache())) {
      try {
        if (options.isUseLocalityGroups()
            && !localityGroupExists(tableName, adapter.getTypeName())) {
          addLocalityGroup(tableName, adapter.getTypeName(), adapter.getAdapterId());
        }
      } catch (AccumuloException | TableNotFoundException | AccumuloSecurityException e) {
        LOGGER.error("unexpected error while looking up locality group", e);
      }
    }

    try {
      return rowWriterSupplier.apply(createBatchWriter(tableName), this, tableName);
    } catch (final TableNotFoundException e) {
      LOGGER.error("Table does not exist", e);
    }
    return null;
  }

  public BatchWriter createBatchWriter(final String tableName) throws TableNotFoundException {
    final String qName = getQualifiedTableName(tableName);
    final BatchWriterConfig config = new BatchWriterConfig();
    config.setMaxMemory(byteBufferSize);
    config.setMaxLatency(timeoutMillis, TimeUnit.MILLISECONDS);
    config.setMaxWriteThreads(numThreads);
    return connector.createBatchWriter(qName, config);
  }

  private boolean iteratorsAttached = false;

  @Override
  public MetadataWriter createMetadataWriter(final MetadataType metadataType) {
    // this checks for existence prior to create
    createTable(AbstractGeoWavePersistence.METADATA_TABLE, false, options.isEnableBlockCache());
    if (MetadataType.STATS.equals(metadataType) && options.isServerSideLibraryEnabled()) {
      synchronized (this) {
        if (!iteratorsAttached) {
          iteratorsAttached = true;

          final BasicOptionProvider optionProvider = new BasicOptionProvider(new HashMap<>());
          ServerOpHelper.addServerSideMerging(
              this,
              DataStatisticsStoreImpl.STATISTICS_COMBINER_NAME,
              DataStatisticsStoreImpl.STATS_COMBINER_PRIORITY,
              MergingCombiner.class.getName(),
              MergingVisibilityCombiner.class.getName(),
              optionProvider,
              AbstractGeoWavePersistence.METADATA_TABLE);
        }
      }
    }
    try {
      return new AccumuloMetadataWriter(
          createBatchWriter(AbstractGeoWavePersistence.METADATA_TABLE),
          metadataType);
    } catch (final TableNotFoundException e) {
      LOGGER.error("Unable to create metadata writer", e);
    }
    return null;
  }

  @Override
  public MetadataReader createMetadataReader(final MetadataType metadataType) {
    return new AccumuloMetadataReader(this, options, metadataType);
  }

  @Override
  public MetadataDeleter createMetadataDeleter(final MetadataType metadataType) {
    return new AccumuloMetadataDeleter(this, metadataType);
  }

  @Override
  public boolean mergeData(
      final Index index,
      final PersistentAdapterStore adapterStore,
      final InternalAdapterStore internalAdapterStore,
      final AdapterIndexMappingStore adapterIndexMappingStore,
      final Integer maxRangeDecomposition) {
    if (options.isServerSideLibraryEnabled()) {
      return compactTable(index.getName());
    } else {
      return DataStoreUtils.mergeData(
          this,
          maxRangeDecomposition,
          index,
          adapterStore,
          internalAdapterStore,
          adapterIndexMappingStore);
    }
  }

  @Override
  public boolean mergeStats(
      final DataStatisticsStore statsStore,
      final InternalAdapterStore internalAdapterStore) {
    if (options.isServerSideLibraryEnabled()) {
      return compactTable(AbstractGeoWavePersistence.METADATA_TABLE);
    } else {
      return DataStoreUtils.mergeStats(statsStore, internalAdapterStore);
    }
  }

  public boolean compactTable(final String unqualifiedTableName) {
    final String tableName = getQualifiedTableName(unqualifiedTableName);
    try {
      LOGGER.info("Compacting table '" + tableName + "'");
      connector.tableOperations().compact(tableName, null, null, true, true);
      LOGGER.info("Successfully compacted table '" + tableName + "'");
    } catch (AccumuloSecurityException | TableNotFoundException | AccumuloException e) {
      LOGGER.error("Unable to merge data by compacting table '" + tableName + "'", e);
      return false;
    }
    return true;
  }

  public void enableVersioningIterator(final String tableName, final boolean enable)
      throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
    synchronized (this) {
      final String qName = getQualifiedTableName(tableName);

      if (enable) {
        connector.tableOperations().attachIterator(
            qName,
            new IteratorSetting(20, "vers", VersioningIterator.class.getName()),
            EnumSet.allOf(IteratorScope.class));
      } else {
        connector.tableOperations().removeIterator(
            qName,
            "vers",
            EnumSet.allOf(IteratorScope.class));
      }
    }
  }

  public void setMaxVersions(final String tableName, final int maxVersions)
      throws AccumuloException, TableNotFoundException, AccumuloSecurityException {
    for (final IteratorScope iterScope : IteratorScope.values()) {
      connector.tableOperations().setProperty(
          getQualifiedTableName(tableName),
          Property.TABLE_ITERATOR_PREFIX + iterScope.name() + ".vers.opt.maxVersions",
          Integer.toString(maxVersions));
    }
  }

  @Override
  public Map<String, ImmutableSet<ServerOpScope>> listServerOps(final String index) {
    try {
      return Maps.transformValues(
          connector.tableOperations().listIterators(getQualifiedTableName(index)),
          input -> Sets.immutableEnumSet(
              (Iterable) Iterables.transform(input, i -> fromAccumulo(i))));
    } catch (AccumuloSecurityException | AccumuloException | TableNotFoundException e) {
      LOGGER.error("Unable to list iterators for table '" + index + "'", e);
    }
    return null;
  }

  private static IteratorScope toAccumulo(final ServerOpScope scope) {
    switch (scope) {
      case MAJOR_COMPACTION:
        return IteratorScope.majc;
      case MINOR_COMPACTION:
        return IteratorScope.minc;
      case SCAN:
        return IteratorScope.scan;
    }
    return null;
  }

  private static ServerOpScope fromAccumulo(final IteratorScope scope) {
    switch (scope) {
      case majc:
        return ServerOpScope.MAJOR_COMPACTION;
      case minc:
        return ServerOpScope.MINOR_COMPACTION;
      case scan:
        return ServerOpScope.SCAN;
    }
    return null;
  }

  private static EnumSet<IteratorScope> toEnumSet(final ImmutableSet<ServerOpScope> scopes) {
    final Collection<IteratorScope> c = Collections2.transform(scopes, scope -> toAccumulo(scope));
    EnumSet<IteratorScope> itSet;
    if (!c.isEmpty()) {
      final Iterator<IteratorScope> it = c.iterator();
      final IteratorScope first = it.next();
      final IteratorScope[] rest = new IteratorScope[c.size() - 1];
      int i = 0;
      while (it.hasNext()) {
        rest[i++] = it.next();
      }
      itSet = EnumSet.of(first, rest);
    } else {
      itSet = EnumSet.noneOf(IteratorScope.class);
    }
    return itSet;
  }

  @Override
  public Map<String, String> getServerOpOptions(
      final String index,
      final String serverOpName,
      final ServerOpScope scope) {
    try {
      final IteratorSetting setting =
          connector.tableOperations().getIteratorSetting(
              getQualifiedTableName(index),
              serverOpName,
              toAccumulo(scope));
      if (setting != null) {
        return setting.getOptions();
      }
    } catch (AccumuloSecurityException | AccumuloException | TableNotFoundException e) {
      LOGGER.error("Unable to get iterator options for table '" + index + "'", e);
    }
    return Collections.emptyMap();
  }

  @Override
  public void removeServerOp(
      final String index,
      final String serverOpName,
      final ImmutableSet<ServerOpScope> scopes) {

    try {
      connector.tableOperations().removeIterator(
          getQualifiedTableName(index),
          serverOpName,
          toEnumSet(scopes));
    } catch (AccumuloSecurityException | AccumuloException | TableNotFoundException e) {
      LOGGER.error("Unable to remove iterator", e);
    }
  }

  @Override
  public void addServerOp(
      final String index,
      final int priority,
      final String name,
      final String operationClass,
      final Map<String, String> properties,
      final ImmutableSet<ServerOpScope> configuredScopes) {
    try {
      connector.tableOperations().attachIterator(
          getQualifiedTableName(index),
          new IteratorSetting(priority, name, operationClass, properties),
          toEnumSet(configuredScopes));
    } catch (AccumuloSecurityException | AccumuloException | TableNotFoundException e) {
      LOGGER.error("Unable to attach iterator", e);
    }
  }

  @Override
  public void updateServerOp(
      final String index,
      final int priority,
      final String name,
      final String operationClass,
      final Map<String, String> properties,
      final ImmutableSet<ServerOpScope> currentScopes,
      final ImmutableSet<ServerOpScope> newScopes) {
    removeServerOp(index, name, currentScopes);
    addServerOp(index, priority, name, operationClass, properties, newScopes);
  }

  public boolean isRowMergingEnabled(final short internalAdapterId, final String indexId) {
    return DataAdapterAndIndexCache.getInstance(
        RowMergingAdapterOptionProvider.ROW_MERGING_ADAPTER_CACHE_ID,
        tableNamespace,
        AccumuloStoreFactoryFamily.TYPE).add(internalAdapterId, indexId);
  }

  @Override
  public boolean metadataExists(final MetadataType type) throws IOException {
    final String qName = getQualifiedTableName(AbstractGeoWavePersistence.METADATA_TABLE);
    return connector.tableOperations().exists(qName);
  }

  @Override
  public String getVersion() {
    // this just creates it if it doesn't exist
    createTable(AbstractGeoWavePersistence.METADATA_TABLE, true, true);
    try {
      final Scanner scanner = createScanner(AbstractGeoWavePersistence.METADATA_TABLE);
      scanner.addScanIterator(new IteratorSetting(25, VersionIterator.class));
      return StringUtils.stringFromBinary(scanner.iterator().next().getValue().get());
    } catch (final TableNotFoundException e) {
      LOGGER.error("Unable to get GeoWave version from Accumulo", e);
    }
    return null;
  }

  @Override
  public <T> Deleter<T> createDeleter(final ReaderParams<T> readerParams) {

    final ScannerBase scanner = getScanner(readerParams, true);
    if (readerParams.isMixedVisibility()
        || (scanner == null)
        || !options.isServerSideLibraryEnabled()) {
      // currently scanner shouldn't be null, but in the future this could
      // be used to imply that range or bulk delete is unnecessary and we
      // instead simply delete by row ID

      // however it has been discovered the batch deletion doesn't work
      // with Accumulo's WholeRowIterator so if there are mixed
      // visibilities, meaning a single row with varying visibilities for
      // different fields we would not be assured we are properly
      // combining the visibilities of a single row without
      // WholeRowIterator so therefore we need to backup to using the
      // slower delete by row technique
      final RowDeleter rowDeleter =
          createRowDeleter(
              readerParams.getIndex().getName(),
              readerParams.getAdapterStore(),
              readerParams.getInternalAdapterStore(),
              readerParams.getAdditionalAuthorizations());
      if (rowDeleter != null) {
        return new QueryAndDeleteByRow<>(rowDeleter, createReader(readerParams));
      }
      return new QueryAndDeleteByRow<>();
    }

    addConstraintsScanIteratorSettings(readerParams, scanner, options);
    // removing the "novalue" iterator means the batch deleter will return
    // values which is essential to maintaining stats

    // this is applicable to accumulo versions < 1.9
    scanner.removeScanIterator(BatchDeleter.class.getName() + ".NOVALUE");
    // this is applicable to accumulo versions >= 1.9
    scanner.removeScanIterator(BatchDeleter.class.getName().replaceAll("[.]", "_") + "_NOVALUE");
    return new AccumuloDeleter<>(
        (BatchDeleter) scanner,
        readerParams.getRowTransformer(),
        readerParams.getIndex().getIndexStrategy().getPartitionKeyLength(),
        readerParams.isMixedVisibility() && !readerParams.isServersideAggregation(),
        readerParams.isClientsideRowMerging(),
        true);
  }

  @Override
  public void delete(final DataIndexReaderParams readerParams) {
    deleteRowsFromDataIndex(readerParams.getDataIds(), readerParams.getAdapterId());
  }

  public void deleteRowsFromDataIndex(final byte[][] rows, final short adapterId) {
    // to have backwards compatibility before 1.8.0 we can assume BaseScanner is autocloseable
    BatchDeleter deleter = null;
    try {
      deleter = createBatchDeleter(DataIndexUtils.DATA_ID_INDEX.getName());
      deleter.fetchColumnFamily(new Text(ByteArrayUtils.shortToString(adapterId)));
      deleter.setRanges(
          Arrays.stream(rows).map(r -> Range.exact(new Text(r))).collect(Collectors.toList()));

      deleter.delete();
    } catch (final TableNotFoundException | MutationsRejectedException e) {
      LOGGER.warn("Unable to delete from data index", e);
    } finally {
      if (deleter != null) {
        deleter.close();
      }
    }
  }
}
