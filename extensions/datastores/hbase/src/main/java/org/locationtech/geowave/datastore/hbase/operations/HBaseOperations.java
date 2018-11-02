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
package org.locationtech.geowave.datastore.hbase.operations;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.RegionException;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoResponse.CompactionState;
import org.apache.hadoop.hbase.shaded.com.google.protobuf.ByteString;
import org.locationtech.geowave.core.cli.VersionUtils;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.ByteArrayRange;
import org.locationtech.geowave.core.index.ByteArrayUtils;
import org.locationtech.geowave.core.index.MultiDimensionalCoordinateRangesArray;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.AdapterIndexMappingStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalDataAdapter;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.adapter.statistics.DataStatisticsStore;
import org.locationtech.geowave.core.store.api.Aggregation;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.entities.GeoWaveMetadata;
import org.locationtech.geowave.core.store.entities.GeoWaveRow;
import org.locationtech.geowave.core.store.entities.GeoWaveRowImpl;
import org.locationtech.geowave.core.store.entities.GeoWaveValue;
import org.locationtech.geowave.core.store.entities.GeoWaveValueImpl;
import org.locationtech.geowave.core.store.metadata.AbstractGeoWavePersistence;
import org.locationtech.geowave.core.store.metadata.DataStatisticsStoreImpl;
import org.locationtech.geowave.core.store.operations.Deleter;
import org.locationtech.geowave.core.store.operations.MetadataDeleter;
import org.locationtech.geowave.core.store.operations.MetadataQuery;
import org.locationtech.geowave.core.store.operations.MetadataReader;
import org.locationtech.geowave.core.store.operations.MetadataType;
import org.locationtech.geowave.core.store.operations.MetadataWriter;
import org.locationtech.geowave.core.store.operations.QueryAndDeleteByRow;
import org.locationtech.geowave.core.store.operations.ReaderParams;
import org.locationtech.geowave.core.store.operations.RowDeleter;
import org.locationtech.geowave.core.store.operations.RowReader;
import org.locationtech.geowave.core.store.operations.RowWriter;
import org.locationtech.geowave.core.store.query.aggregate.CommonIndexAggregation;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;
import org.locationtech.geowave.core.store.server.BasicOptionProvider;
import org.locationtech.geowave.core.store.server.RowMergingAdapterOptionProvider;
import org.locationtech.geowave.core.store.server.ServerOpConfig.ServerOpScope;
import org.locationtech.geowave.core.store.server.ServerOpHelper;
import org.locationtech.geowave.core.store.server.ServerSideOperations;
import org.locationtech.geowave.core.store.util.DataAdapterAndIndexCache;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import org.locationtech.geowave.datastore.hbase.HBaseRow;
import org.locationtech.geowave.datastore.hbase.HBaseStoreFactoryFamily;
import org.locationtech.geowave.datastore.hbase.cli.config.HBaseOptions;
import org.locationtech.geowave.datastore.hbase.cli.config.HBaseRequiredOptions;
import org.locationtech.geowave.datastore.hbase.coprocessors.protobuf.AggregationProtosClient;
import org.locationtech.geowave.datastore.hbase.coprocessors.protobuf.HBaseBulkDeleteProtosClient;
import org.locationtech.geowave.datastore.hbase.coprocessors.protobuf.HBaseBulkDeleteProtosClient.BulkDeleteResponse;
import org.locationtech.geowave.datastore.hbase.filters.HBaseNumericIndexStrategyFilter;
import org.locationtech.geowave.datastore.hbase.operations.GeoWaveColumnFamily.GeoWaveColumnFamilyFactory;
import org.locationtech.geowave.datastore.hbase.operations.GeoWaveColumnFamily.StringColumnFamily;
import org.locationtech.geowave.datastore.hbase.operations.GeoWaveColumnFamily.StringColumnFamilyFactory;
import org.locationtech.geowave.datastore.hbase.query.protobuf.VersionProtosClient;
import org.locationtech.geowave.datastore.hbase.query.protobuf.VersionProtosClient.VersionRequest;
import org.locationtech.geowave.datastore.hbase.server.MergingServerOp;
import org.locationtech.geowave.datastore.hbase.server.MergingVisibilityServerOp;
import org.locationtech.geowave.datastore.hbase.server.ServerSideOperationUtils;
import org.locationtech.geowave.datastore.hbase.util.ConnectionPool;
import org.locationtech.geowave.datastore.hbase.util.HBaseUtils;
import org.locationtech.geowave.mapreduce.MapReduceDataStoreOperations;
import org.locationtech.geowave.mapreduce.URLClassloaderUtils;
import org.locationtech.geowave.mapreduce.splits.RecordReaderParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

public class HBaseOperations implements
		MapReduceDataStoreOperations,
		ServerSideOperations
{
	private final static Logger LOGGER = LoggerFactory.getLogger(HBaseOperations.class);
	private boolean iteratorsAttached;
	protected static final String DEFAULT_TABLE_NAMESPACE = "";
	public static final Object ADMIN_MUTEX = new Object();
	private static final long SLEEP_INTERVAL = HConstants.DEFAULT_HBASE_SERVER_PAUSE;
	private static final String SPLIT_STRING = Pattern.quote(".");
	private static final int MAX_AGGREGATE_RETRIES = 3;
	private static final int DELETE_BATCH_SIZE = 1000000;
	private static final HBaseBulkDeleteProtosClient.BulkDeleteRequest.BulkDeleteType DELETE_TYPE = HBaseBulkDeleteProtosClient.BulkDeleteRequest.BulkDeleteType.ROW;

	protected final Connection conn;

	private final String tableNamespace;
	private final boolean schemaUpdateEnabled;
	private final HashMap<String, List<String>> coprocessorCache = new HashMap<>();
	private final Map<TableName, Set<ByteArray>> partitionCache = new HashMap<>();
	private final HashMap<TableName, Set<GeoWaveColumnFamily>> cfCache = new HashMap<>();

	private final HBaseOptions options;

	public static final Pair<GeoWaveColumnFamily, Boolean>[] METADATA_CFS_VERSIONING = new Pair[] {
		ImmutablePair.of(
				new StringColumnFamily(
						MetadataType.AIM.name()),
				true),
		ImmutablePair.of(
				new StringColumnFamily(
						MetadataType.ADAPTER.name()),
				true),
		ImmutablePair.of(
				new StringColumnFamily(
						MetadataType.STATS.name()),
				false),
		ImmutablePair.of(
				new StringColumnFamily(
						MetadataType.INDEX.name()),
				true),
		ImmutablePair.of(
				new StringColumnFamily(
						MetadataType.INTERNAL_ADAPTER.name()),
				true),
	};

	public static final int MERGING_MAX_VERSIONS = HConstants.ALL_VERSIONS;
	public static final int DEFAULT_MAX_VERSIONS = 1;

	public HBaseOperations(
			final String zookeeperInstances,
			final String geowaveNamespace,
			final HBaseOptions options )
			throws IOException {
		conn = ConnectionPool.getInstance().getConnection(
				zookeeperInstances);
		tableNamespace = geowaveNamespace;

		schemaUpdateEnabled = conn.getConfiguration().getBoolean(
				"hbase.online.schema.update.enable",
				true);

		this.options = options;
	}

	public HBaseOperations(
			final Connection connection,
			final String geowaveNamespace,
			final HBaseOptions options ) {
		conn = connection;

		tableNamespace = geowaveNamespace;

		schemaUpdateEnabled = conn.getConfiguration().getBoolean(
				"hbase.online.schema.update.enable",
				false);

		this.options = options;
	}

	public static HBaseOperations createOperations(
			final HBaseRequiredOptions options )
			throws IOException {
		return new HBaseOperations(
				options.getZookeeper(),
				options.getGeowaveNamespace(),
				(HBaseOptions) options.getStoreOptions());
	}

	public Connection getConnection() {
		return conn;
	}

	public boolean isSchemaUpdateEnabled() {
		return schemaUpdateEnabled;
	}

	public boolean isServerSideLibraryEnabled() {
		if (options != null) {
			return options.isServerSideLibraryEnabled();
		}

		return true;
	}

	public int getScanCacheSize() {
		if (options != null) {
			if (options.getScanCacheSize() != HConstants.DEFAULT_HBASE_CLIENT_SCANNER_CACHING) {
				return options.getScanCacheSize();
			}
		}

		// Need to get default from config.
		return 10000;
	}

	public boolean isEnableBlockCache() {
		if (options != null) {
			return options.isEnableBlockCache();
		}

		return true;
	}

	public TableName getTableName(
			final String tableName ) {
		return TableName.valueOf(getQualifiedTableName(tableName));
	}

	protected void createTable(
			final Set<ByteArray> preSplits,
			final Pair<GeoWaveColumnFamily, Boolean>[] columnFamiliesAndVersioningPairs,
			final GeoWaveColumnFamilyFactory columnFamilyFactory,
			final TableName tableName )
			throws IOException {
		synchronized (ADMIN_MUTEX) {
			try (Admin admin = conn.getAdmin()) {
				if (!admin
						.tableExists(
								tableName)) {
					final HTableDescriptor desc = new HTableDescriptor(
							tableName);

					final HashSet<GeoWaveColumnFamily> cfSet = new HashSet<>();

					for (final Pair<GeoWaveColumnFamily, Boolean> columnFamilyAndVersioning : columnFamiliesAndVersioningPairs) {
						final HColumnDescriptor column = columnFamilyAndVersioning.getLeft().toColumnDescriptor();
						if (!columnFamilyAndVersioning.getRight()) {
							column
									.setMaxVersions(
											Integer.MAX_VALUE);
						}
						desc
								.addFamily(
										column);

						cfSet
								.add(
										columnFamilyAndVersioning.getLeft());
					}

					cfCache
							.put(
									tableName,
									cfSet);

					try {
						if (!preSplits.isEmpty()) {
							admin
									.createTable(
											desc,
											preSplits
													.stream()
													.map(
															id -> id.getBytes())
													.toArray(
															i -> new byte[i][]));
						}
						else {
							admin
									.createTable(
											desc);
						}
					}
					catch (final Exception e) {
						// We can ignore TableExists on create
						if (!(e instanceof TableExistsException)) {
							throw (e);
						}
					}
				}
			}
		}
	}

	protected void createTable(
			final Set<ByteArray> preSplits,
			final GeoWaveColumnFamily[] columnFamilies,
			final GeoWaveColumnFamilyFactory columnFamilyFactory,
			final boolean enableVersioning,
			final TableName tableName )
			throws IOException {
		createTable(
				preSplits,
				Arrays
						.stream(
								columnFamilies)
						.map(
								cf -> ImmutablePair
										.of(
												cf,
												enableVersioning))
						.toArray(
								Pair[]::new),
				columnFamilyFactory,
				tableName);
	}

	public boolean verifyColumnFamily(
			final short columnFamily,
			final boolean enableVersioning,
			final String tableNameStr,
			final boolean addIfNotExist ) {
		final TableName tableName = getTableName(tableNameStr);

		final GeoWaveColumnFamily[] columnFamilies = new GeoWaveColumnFamily[1];
		columnFamilies[0] = new StringColumnFamily(
				ByteArrayUtils.shortToString(columnFamily));

		try {
			return verifyColumnFamilies(
					columnFamilies,
					StringColumnFamilyFactory.getSingletonInstance(),
					enableVersioning,
					tableName,
					addIfNotExist);
		}
		catch (final IOException e) {
			LOGGER.error(
					"Error verifying column family " + columnFamily + " on table " + tableNameStr,
					e);
		}

		return false;
	}

	protected boolean verifyColumnFamilies(
			final GeoWaveColumnFamily[] columnFamilies,
			final GeoWaveColumnFamilyFactory columnFamilyFactory,
			final boolean enableVersioning,
			final TableName tableName,
			final boolean addIfNotExist )
			throws IOException {
		// Check the cache first and create the update list
		Set<GeoWaveColumnFamily> cfCacheSet = cfCache.get(tableName);

		if (cfCacheSet == null) {
			cfCacheSet = new HashSet<>();
			cfCache.put(
					tableName,
					cfCacheSet);
		}

		final HashSet<GeoWaveColumnFamily> newCFs = new HashSet<>();
		for (final GeoWaveColumnFamily columnFamily : columnFamilies) {
			if (!cfCacheSet.contains(columnFamily)) {
				newCFs.add(columnFamily);
			}
		}
		// Nothing to add
		if (newCFs.isEmpty()) {
			return true;
		}

		final List<GeoWaveColumnFamily> existingColumnFamilies = new ArrayList<>();
		final List<GeoWaveColumnFamily> newColumnFamilies = new ArrayList<>();
		synchronized (ADMIN_MUTEX) {
			try (Admin admin = conn.getAdmin()) {
				if (admin.tableExists(tableName)) {
					final HTableDescriptor existingTableDescriptor = admin.getTableDescriptor(tableName);
					final HColumnDescriptor[] existingColumnDescriptors = existingTableDescriptor.getColumnFamilies();
					for (final HColumnDescriptor hColumnDescriptor : existingColumnDescriptors) {
						existingColumnFamilies.add(columnFamilyFactory.fromColumnDescriptor(hColumnDescriptor));
					}
					for (final GeoWaveColumnFamily columnFamily : newCFs) {
						if (!existingColumnFamilies.contains(columnFamily)) {
							newColumnFamilies.add(columnFamily);
						}
					}

					if (!newColumnFamilies.isEmpty()) {
						if (!addIfNotExist) {
							return false;
						}
						disableTable(
								admin,
								tableName);
						for (final GeoWaveColumnFamily newColumnFamily : newColumnFamilies) {
							final HColumnDescriptor column = newColumnFamily.toColumnDescriptor();
							if (!enableVersioning) {
								column.setMaxVersions(Integer.MAX_VALUE);
							}
							admin.addColumn(
									tableName,
									column);
							cfCacheSet.add(newColumnFamily);
						}

						enableTable(
								admin,
								tableName);
						waitForUpdate(
								admin,
								tableName,
								SLEEP_INTERVAL);
					}
					else {
						return true;
					}
				}
			}
		}

		return true;
	}

	private void enableTable(
			final Admin admin,
			final TableName tableName )
			throws IOException {
		admin.enableTableAsync(tableName);
		while (!admin.isTableEnabled(tableName)) {
			try {
				Thread.sleep(10);
			}
			catch (final InterruptedException e) {
				// Do nothing
			}
		}
	}

	private void disableTable(
			final Admin admin,
			final TableName tableName )
			throws IOException {
		admin.disableTableAsync(tableName);
		while (!admin.isTableDisabled(tableName)) {
			try {
				Thread.sleep(10);
			}
			catch (final InterruptedException e) {
				// Do nothing
			}
		}
	}

	private void waitForUpdate(
			final Admin admin,
			final TableName tableName,
			final long sleepTimeMs ) {
		try {
			while (admin.getAlterStatus(
					tableName).getFirst() > 0) {

				Thread.sleep(sleepTimeMs);
			}
		}
		catch (final Exception e) {
			LOGGER.error(
					"Error waiting for table update",
					e);
		}
	}

	public String getQualifiedTableName(
			final String unqualifiedTableName ) {
		return HBaseUtils.getQualifiedTableName(
				tableNamespace,
				unqualifiedTableName);
	}

	@Override
	public void deleteAll()
			throws IOException {
		try (Admin admin = conn.getAdmin()) {
			final TableName[] tableNamesArr = admin.listTableNames();
			for (final TableName tableName : tableNamesArr) {
				if ((tableNamespace == null) || tableName.getNameAsString().startsWith(
						tableNamespace)) {
					synchronized (ADMIN_MUTEX) {
						if (admin.tableExists(tableName)) {
							disableTable(
									admin,
									tableName);
							admin.deleteTable(tableName);
						}
					}
				}
			}
			synchronized (this) {
				iteratorsAttached = false;
			}
			cfCache.clear();
			partitionCache.clear();
			coprocessorCache.clear();
			DataAdapterAndIndexCache.getInstance(
					RowMergingAdapterOptionProvider.ROW_MERGING_ADAPTER_CACHE_ID,
					tableNamespace,
					HBaseStoreFactoryFamily.TYPE).deleteAll();
		}
	}

	protected String getIndexId(
			final TableName tableName ) {
		final String name = tableName.getNameAsString();
		if ((tableNamespace == null) || tableNamespace.isEmpty()) {
			return name;
		}
		return name.substring(tableNamespace.length() + 1);
	}

	public boolean isRowMergingEnabled(
			final short internalAdapterId,
			final String indexId ) {
		return DataAdapterAndIndexCache.getInstance(
				RowMergingAdapterOptionProvider.ROW_MERGING_ADAPTER_CACHE_ID,
				tableNamespace,
				HBaseStoreFactoryFamily.TYPE).add(
				internalAdapterId,
				indexId);
	}

	@Override
	public boolean deleteAll(
			final String indexName,
			final String typeName,
			final Short adapterId,
			final String... additionalAuthorizations ) {
		RowDeleter deleter = null;
		Iterable<Result> scanner = null;
		try {
			deleter = createRowDeleter(
					indexName,
					// these params aren't needed for hbase
					null,
					null,
					additionalAuthorizations);
			Index index = null;
			try (final CloseableIterator<GeoWaveMetadata> it = createMetadataReader(
					MetadataType.INDEX).query(
					new MetadataQuery(
							StringUtils.stringToBinary(indexName),
							null,
							additionalAuthorizations))) {
				if (!it.hasNext()) {
					LOGGER.warn("Unable to find index to delete");
					return false;
				}
				final GeoWaveMetadata indexMd = it.next();
				index = (Index) URLClassloaderUtils.fromBinary(indexMd.getValue());
			}
			final Scan scan = new Scan();
			scan.addFamily(ByteArrayUtils.shortToByteArray(adapterId));
			scanner = getScannedResults(
					scan,
					indexName);
			for (final Result result : scanner) {
				deleter.delete(new HBaseRow(
						result,
						index.getIndexStrategy().getPartitionKeyLength()));
			}
			return true;
		}
		catch (final Exception e) {
			LOGGER.warn(
					"Unable to close deleter",
					e);
		}
		finally {
			if ((scanner != null) && (scanner instanceof ResultScanner)) {
				((ResultScanner) scanner).close();
			}
			if (deleter != null) {
				try {
					deleter.close();
				}
				catch (final Exception e) {
					LOGGER.warn(
							"Unable to close deleter",
							e);
				}
			}
		}
		return false;
	}

	public Iterable<Result> getScannedResults(
			final Scan scanner,
			final String tableName )
			throws IOException {
		final Table table = conn.getTable(getTableName(tableName));

		final ResultScanner results = table.getScanner(scanner);

		table.close();

		return results;
	}

	public <T> void startParallelScan(
			final HBaseParallelDecoder<T> scanner,
			final String tableName )
			throws Exception {
		scanner.setTableName(getTableName(tableName));
		scanner.startDecode();
	}

	public RegionLocator getRegionLocator(
			final String tableName )
			throws IOException {
		return getRegionLocator(getTableName(tableName));
	}

	public RegionLocator getRegionLocator(
			final TableName tableName )
			throws IOException {
		return conn.getRegionLocator(tableName);
	}

	public boolean parallelDecodeEnabled() {
		return true;
	}

	public Table getTable(
			final String tableName )
			throws IOException {
		return conn.getTable(getTableName(tableName));
	}

	public boolean verifyCoprocessor(
			final String tableNameStr,
			final String coprocessorName,
			final String coprocessorJar ) {
		try {
			// Check the cache first
			final List<String> checkList = coprocessorCache.get(tableNameStr);
			if (checkList != null) {
				if (checkList.contains(coprocessorName)) {
					return true;
				}
			}
			else {
				coprocessorCache.put(
						tableNameStr,
						new ArrayList<String>());
			}

			synchronized (ADMIN_MUTEX) {
				try (Admin admin = conn.getAdmin()) {
					final TableName tableName = getTableName(tableNameStr);
					final HTableDescriptor td = admin.getTableDescriptor(tableName);

					if (!td.hasCoprocessor(coprocessorName)) {
						LOGGER.debug(tableNameStr + " does not have coprocessor. Adding " + coprocessorName);

						LOGGER.debug("- disable table...");
						disableTable(
								admin,
								tableName);

						LOGGER.debug("- add coprocessor...");

						// Retrieve coprocessor jar path from config
						Path hdfsJarPath = null;
						if (coprocessorJar == null) {
							try {
								hdfsJarPath = getGeoWaveJarOnPath();
							}
							catch (final Exception e) {
								LOGGER.warn(
										"Unable to infer coprocessor library",
										e);
							}
						}
						else {
							hdfsJarPath = new Path(
									coprocessorJar);
						}

						if (hdfsJarPath == null) {
							td.addCoprocessor(coprocessorName);
						}
						else {
							LOGGER.debug("Coprocessor jar path: " + hdfsJarPath.toString());

							td.addCoprocessor(
									coprocessorName,
									hdfsJarPath,
									Coprocessor.PRIORITY_USER,
									null);
						}
						LOGGER.debug("- modify table...");
						admin.modifyTable(
								tableName,
								td);

						LOGGER.debug("- enable table...");
						enableTable(
								admin,
								tableName);

						waitForUpdate(
								admin,
								tableName,
								SLEEP_INTERVAL);
					}

					LOGGER.debug("Successfully added coprocessor");

					coprocessorCache.get(
							tableNameStr).add(
							coprocessorName);

					coprocessorCache.get(
							tableNameStr).add(
							coprocessorName);
				}
			}
		}
		catch (

		final IOException e) {
			LOGGER.error(
					"Error verifying/adding coprocessor.",
					e);

			return false;
		}

		return true;
	}

	private static Path getGeoWaveJarOnPath()
			throws IOException {
		final Configuration conf = HBaseConfiguration.create();
		final File f = new File(
				"/etc/hbase/conf/hbase-site.xml");
		if (f.exists()) {
			conf.addResource(f.toURI().toURL());
		}
		final String remotePath = conf.get("hbase.dynamic.jars.dir");
		if (remotePath == null) {
			return null;
		}
		final Path remoteDir = new Path(
				remotePath);

		final FileSystem remoteDirFs = remoteDir.getFileSystem(conf);
		if (!remoteDirFs.exists(remoteDir)) {
			return null;
		}
		final FileStatus[] statuses = remoteDirFs.listStatus(remoteDir);
		if ((statuses == null) || (statuses.length == 0)) {
			return null; // no remote files at all
		}
		Path retVal = null;
		for (final FileStatus status : statuses) {
			if (status.isDirectory()) {
				continue; // No recursive lookup
			}
			final Path path = status.getPath();
			final String fileName = path.getName();

			if (fileName.endsWith(".jar")) {
				if (fileName.contains("geowave") && fileName.contains("hbase")) {
					LOGGER.info("inferring " + status.getPath().toString() + " as the library for this coprocesor");
					// this is the best guess at the right jar if there are
					// multiple
					return status.getPath();
				}
				retVal = status.getPath();
			}
		}
		if (retVal != null) {
			LOGGER.info("inferring " + retVal.toString() + " as the library for this coprocesor");
		}
		return retVal;
	}

	@Override
	public boolean indexExists(
			final String indexName )
			throws IOException {
		synchronized (ADMIN_MUTEX) {
			try (Admin admin = conn.getAdmin()) {
				final TableName tableName = getTableName(indexName);
				return admin.tableExists(tableName);
			}
		}
	}

	@Override
	public boolean mergeData(
			final Index index,
			final PersistentAdapterStore adapterStore,
			final InternalAdapterStore internalAdapterStore,
			final AdapterIndexMappingStore adapterIndexMappingStore ) {
		if (options.isServerSideLibraryEnabled()) {
			final TableName tableName = getTableName(index.getName());
			try (Admin admin = conn.getAdmin()) {
				admin.compact(tableName);
				// wait for table compaction to finish
				while (!admin.getCompactionState(
						tableName).equals(
						CompactionState.NONE)) {
					Thread.sleep(100);
				}
			}
			catch (final Exception e) {
				LOGGER.error(
						"Cannot compact table '" + index.getName() + "'",
						e);
				return false;
			}
		}
		else {
			return DataStoreUtils.mergeData(
					this,
					options,
					index,
					adapterStore,
					internalAdapterStore,
					adapterIndexMappingStore);
		}
		return true;
	}

	@Override
	public boolean mergeStats(
			final DataStatisticsStore store,
			final InternalAdapterStore internalAdapterStore ) {
		if (options.isServerSideLibraryEnabled()) {
			try (Admin admin = conn.getAdmin()) {
				admin.compact(getTableName(AbstractGeoWavePersistence.METADATA_TABLE));
			}
			catch (final IOException e) {
				LOGGER.error(
						"Cannot compact table '" + AbstractGeoWavePersistence.METADATA_TABLE + "'",
						e);
				return false;
			}
		}
		else {

		}
		return true;
	}

	@Override
	public boolean ensureAuthorizations(
			final String clientUser,
			final String... authorizations ) {
		return true;
	}

	public void ensureServerSideOperationsObserverAttached(
			final String indexName ) {
		// Use the server-side operations observer
		verifyCoprocessor(
				indexName,
				"org.locationtech.geowave.datastore.hbase.coprocessors.ServerSideOperationsObserver",
				options.getCoprocessorJar());
	}

	public void createTable(
			final Set<ByteArray> preSplits,
			final String indexName,
			final boolean enableVersioning,
			final short internalAdapterId ) {
		final TableName tableName = getTableName(indexName);

		final GeoWaveColumnFamily[] columnFamilies = new GeoWaveColumnFamily[1];
		columnFamilies[0] = new StringColumnFamily(
				ByteArrayUtils.shortToString(internalAdapterId));
		try {
			createTable(
					preSplits,
					columnFamilies,
					StringColumnFamilyFactory.getSingletonInstance(),
					enableVersioning,
					tableName);
		}
		catch (final IOException e) {
			LOGGER.error(
					"Error creating table: " + indexName,
					e);
		}
	}

	protected String getMetadataTableName(
			final MetadataType type ) {
		return AbstractGeoWavePersistence.METADATA_TABLE;
	}

	@Override
	public RowWriter createWriter(
			final Index index,
			final InternalDataAdapter<?> adapter ) {
		final TableName tableName = getTableName(index.getName());
		try {
			final GeoWaveColumnFamily[] columnFamilies = new GeoWaveColumnFamily[1];
			columnFamilies[0] = new StringColumnFamily(
					ByteArrayUtils.shortToString(adapter.getAdapterId()));

			createTable(
					index.getIndexStrategy().getPredefinedSplits(),
					columnFamilies,
					StringColumnFamilyFactory.getSingletonInstance(),
					options.isServerSideLibraryEnabled(),
					tableName);

			verifyColumnFamilies(
					columnFamilies,
					StringColumnFamilyFactory.getSingletonInstance(),
					true,
					tableName,
					true);

			return new HBaseWriter(
					getBufferedMutator(tableName));
		}
		catch (final TableNotFoundException e) {
			LOGGER.error(
					"Table does not exist",
					e);
		}
		catch (final IOException e) {
			LOGGER.error(
					"Error creating table: " + index.getName(),
					e);
		}

		return null;
	}

	@Override
	public MetadataWriter createMetadataWriter(
			final MetadataType metadataType ) {
		final TableName tableName = getTableName(getMetadataTableName(metadataType));
		try {
			createTable(
					Collections.EMPTY_SET,
					METADATA_CFS_VERSIONING,
					StringColumnFamilyFactory.getSingletonInstance(),
					tableName);
			if (MetadataType.STATS.equals(metadataType) && options.isServerSideLibraryEnabled()) {
				synchronized (this) {
					if (!iteratorsAttached) {
						iteratorsAttached = true;

						final BasicOptionProvider optionProvider = new BasicOptionProvider(
								new HashMap<>());
						ensureServerSideOperationsObserverAttached(getMetadataTableName(metadataType));
						ServerOpHelper.addServerSideMerging(
								this,
								DataStatisticsStoreImpl.STATISTICS_COMBINER_NAME,
								DataStatisticsStoreImpl.STATS_COMBINER_PRIORITY,
								MergingServerOp.class.getName(),
								MergingVisibilityServerOp.class.getName(),
								optionProvider,
								getMetadataTableName(metadataType));
					}
				}
			}
			return new HBaseMetadataWriter(
					getBufferedMutator(tableName),
					metadataType);
		}
		catch (final IOException e) {
			LOGGER.error(
					"Error creating metadata table: " + getMetadataTableName(metadataType),
					e);
		}

		return null;
	}

	@Override
	public MetadataReader createMetadataReader(
			final MetadataType metadataType ) {
		return new HBaseMetadataReader(
				this,
				options,
				metadataType);
	}

	@Override
	public MetadataDeleter createMetadataDeleter(
			final MetadataType metadataType ) {
		return new HBaseMetadataDeleter(
				this,
				metadataType);
	}

	@Override
	public <T> RowReader<T> createReader(
			final ReaderParams<T> readerParams ) {
		final HBaseReader<T> hbaseReader = new HBaseReader<>(
				readerParams,
				this);

		return hbaseReader;
	}

	@Override
	public <T> RowReader<T> createReader(
			final RecordReaderParams<T> recordReaderParams ) {
		return new HBaseReader<>(
				recordReaderParams,
				this);
	}

	@Override
	public RowDeleter createRowDeleter(
			final String indexName,
			final PersistentAdapterStore adapterStore,
			final InternalAdapterStore internalAdapterStore,
			final String... authorizations ) {
		try {
			final TableName tableName = getTableName(indexName);
			return new HBaseRowDeleter(
					getBufferedMutator(tableName));
		}
		catch (final IOException ioe) {
			LOGGER.error(
					"Error creating deleter",
					ioe);
		}
		return null;
	}

	public BufferedMutator getBufferedMutator(
			final TableName tableName )
			throws IOException {
		final BufferedMutatorParams params = new BufferedMutatorParams(
				tableName);

		return conn.getBufferedMutator(params);
	}

	public MultiRowRangeFilter getMultiRowRangeFilter(
			final List<ByteArrayRange> ranges ) {
		// create the multi-row filter
		final List<RowRange> rowRanges = new ArrayList<>();
		if ((ranges == null) || ranges.isEmpty()) {
			rowRanges.add(new RowRange(
					HConstants.EMPTY_BYTE_ARRAY,
					true,
					HConstants.EMPTY_BYTE_ARRAY,
					false));
		}
		else {
			for (final ByteArrayRange range : ranges) {
				if (range.getStart() != null) {
					final byte[] startRow = range.getStart().getBytes();
					byte[] stopRow;
					if (!range.isSingleValue()) {
						stopRow = range.getEndAsNextPrefix().getBytes();
					}
					else {
						stopRow = range.getStart().getNextPrefix();
					}

					final RowRange rowRange = new RowRange(
							startRow,
							true,
							stopRow,
							false);

					rowRanges.add(rowRange);
				}
			}
		}

		// Create the multi-range filter
		try {
			return new MultiRowRangeFilter(
					rowRanges);
		}
		catch (final IOException e) {
			LOGGER.error(
					"Error creating range filter.",
					e);
		}
		return null;
	}

	public <T> Iterator<GeoWaveRow> aggregateServerSide(
			final ReaderParams<T> readerParams ) {
		final String tableName = readerParams.getIndex().getName();

		try {
			// Use the row count coprocessor
			if (options.isVerifyCoprocessors()) {
				verifyCoprocessor(
						tableName,
						"org.locationtech.geowave.datastore.hbase.coprocessors.AggregationEndpoint",
						options.getCoprocessorJar());
			}

			final Aggregation aggregation = readerParams.getAggregation().getRight();

			final AggregationProtosClient.AggregationType.Builder aggregationBuilder = AggregationProtosClient.AggregationType
					.newBuilder();
			aggregationBuilder
					.setClassId(
							ByteString
									.copyFrom(
											URLClassloaderUtils
													.toClassId(
															aggregation)));

			if (aggregation.getParameters() != null) {
				final byte[] paramBytes = URLClassloaderUtils
						.toBinary(
								aggregation.getParameters());
				aggregationBuilder
						.setParams(
								ByteString
										.copyFrom(
												paramBytes));
			}

			final AggregationProtosClient.AggregationRequest.Builder requestBuilder = AggregationProtosClient.AggregationRequest
					.newBuilder();
			requestBuilder
					.setAggregation(
							aggregationBuilder.build());
			if (readerParams.getFilter() != null) {
				final List<QueryFilter> distFilters = new ArrayList();
				distFilters
						.add(
								readerParams.getFilter());

				final byte[] filterBytes = URLClassloaderUtils
						.toBinary(
								distFilters);
				final ByteString filterByteString = ByteString
						.copyFrom(
								filterBytes);
				requestBuilder
						.setFilter(
								filterByteString);
			}
			else {
				final List<MultiDimensionalCoordinateRangesArray> coords = readerParams.getCoordinateRanges();
				if (!coords.isEmpty()) {
					final byte[] filterBytes = new HBaseNumericIndexStrategyFilter(
							readerParams.getIndex().getIndexStrategy(),
							coords
									.toArray(
											new MultiDimensionalCoordinateRangesArray[] {})).toByteArray();
					final ByteString filterByteString = ByteString
							.copyFrom(
									new byte[] {
										0
									})
							.concat(
									ByteString
											.copyFrom(
													filterBytes));

					requestBuilder
							.setNumericIndexStrategyFilter(
									filterByteString);
				}
			}
			requestBuilder
					.setModel(
							ByteString
									.copyFrom(
											URLClassloaderUtils
													.toBinary(
															readerParams.getIndex().getIndexModel())));

			final int maxRangeDecomposition = readerParams.getMaxRangeDecomposition() == null
					? options.getAggregationMaxRangeDecomposition()
					: readerParams.getMaxRangeDecomposition();
			final MultiRowRangeFilter multiFilter = getMultiRowRangeFilter(
					DataStoreUtils
							.constraintsToQueryRanges(
									readerParams.getConstraints(),
									readerParams.getIndex().getIndexStrategy(),
									null,
									maxRangeDecomposition)
							.getCompositeQueryRanges());
			if (multiFilter != null) {
				requestBuilder
						.setRangeFilter(
								ByteString
										.copyFrom(
												multiFilter.toByteArray()));
			}
			if (readerParams.getAggregation().getLeft() != null) {
				if (readerParams.getAggregation().getRight() instanceof CommonIndexAggregation) {
					requestBuilder
							.setInternalAdapterId(
									ByteString
											.copyFrom(
													ByteArrayUtils
															.shortToByteArray(
																	readerParams
																			.getAggregation()
																			.getLeft()
																			.getAdapterId())));
				}
				else {
					requestBuilder
							.setAdapter(
									ByteString
											.copyFrom(
													URLClassloaderUtils
															.toBinary(
																	readerParams.getAggregation().getLeft())));
				}
			}

			if ((readerParams.getAdditionalAuthorizations() != null)
					&& (readerParams.getAdditionalAuthorizations().length > 0)) {
				requestBuilder
						.setVisLabels(
								ByteString
										.copyFrom(
												StringUtils
														.stringsToBinary(
																readerParams.getAdditionalAuthorizations())));
			}

			if (readerParams.isMixedVisibility()) {
				requestBuilder
						.setWholeRowFilter(
								true);
			}

			requestBuilder
					.setPartitionKeyLength(
							readerParams.getIndex().getIndexStrategy().getPartitionKeyLength());

			final AggregationProtosClient.AggregationRequest request = requestBuilder.build();

			byte[] startRow = null;
			byte[] endRow = null;

			final List<ByteArrayRange> ranges = readerParams.getQueryRanges().getCompositeQueryRanges();
			if ((ranges != null) && !ranges.isEmpty()) {
				final ByteArrayRange aggRange = getSingleRange(
						ranges);
				startRow = aggRange.getStart().getBytes();
				endRow = aggRange.getEnd().getBytes();
			}

			Map<byte[], ByteString> results = null;
			boolean shouldRetry;
			int retries = 0;
			do {
				shouldRetry = false;

				try (final Table table = getTable(
						tableName)) {
					results = table
							.coprocessorService(
									AggregationProtosClient.AggregationService.class,
									startRow,
									endRow,
									new Batch.Call<AggregationProtosClient.AggregationService, ByteString>() {
										@Override
										public ByteString call(
												final AggregationProtosClient.AggregationService counter )
												throws IOException {
											final BlockingRpcCallback<AggregationProtosClient.AggregationResponse> rpcCallback = new BlockingRpcCallback<>();
											counter
													.aggregate(
															null,
															request,
															rpcCallback);
											final AggregationProtosClient.AggregationResponse response = rpcCallback
													.get();
											if (response == null) {
												// Region returned no response
												throw new RegionException();
											}
											return response.hasValue() ? response.getValue() : null;
										}
									});
					break;
				}
				catch (final RegionException e) {
					retries++;
					if (retries <= MAX_AGGREGATE_RETRIES) {
						LOGGER
								.warn(
										"Aggregate timed out due to unavailable region. Retrying (" + retries + " of "
												+ MAX_AGGREGATE_RETRIES + ")");
						shouldRetry = true;
					}
				}
			}
			while (shouldRetry);

			if (results == null) {
				LOGGER
						.error(
								"Aggregate timed out and exceeded max retries.");
				return null;
			}

			return Iterators
					.transform(
							results.values().iterator(),
							b -> new GeoWaveRowImpl(
									null,
									new GeoWaveValue[] {
										new GeoWaveValueImpl(
												null,
												null,
												b.toByteArray())
									}));
		}
		catch (final Exception e) {
			LOGGER
					.error(
							"Error during aggregation.",
							e);
		}
		catch (final Throwable e) {
			LOGGER
					.error(
							"Error during aggregation.",
							e);
		}

		return null;
	}

	public void bulkDelete(
			final ReaderParams readerParams ) {
		final String tableName = readerParams.getIndex().getName();
		final short[] adapterIds = readerParams.getAdapterIds();
		Long total = 0L;

		try {
			// Use the row count coprocessor
			if (options.isVerifyCoprocessors()) {
				verifyCoprocessor(
						tableName,
						"org.locationtech.geowave.datastore.hbase.coprocessors.HBaseBulkDeleteEndpoint",
						options.getCoprocessorJar());
			}

			final HBaseBulkDeleteProtosClient.BulkDeleteRequest.Builder requestBuilder = HBaseBulkDeleteProtosClient.BulkDeleteRequest
					.newBuilder();

			requestBuilder.setDeleteType(DELETE_TYPE);
			requestBuilder.setRowBatchSize(DELETE_BATCH_SIZE);

			if (readerParams.getFilter() != null) {
				final List<QueryFilter> distFilters = new ArrayList();
				distFilters.add(readerParams.getFilter());

				final byte[] filterBytes = PersistenceUtils.toBinary(distFilters);
				final ByteString filterByteString = ByteString.copyFrom(filterBytes);
				requestBuilder.setFilter(filterByteString);
			}
			else {
				final List<MultiDimensionalCoordinateRangesArray> coords = readerParams.getCoordinateRanges();
				if (!coords.isEmpty()) {
					final byte[] filterBytes = new HBaseNumericIndexStrategyFilter(
							readerParams.getIndex().getIndexStrategy(),
							coords.toArray(new MultiDimensionalCoordinateRangesArray[] {})).toByteArray();
					final ByteString filterByteString = ByteString.copyFrom(
							new byte[] {
								0
							}).concat(
							ByteString.copyFrom(filterBytes));

					requestBuilder.setNumericIndexStrategyFilter(filterByteString);
				}
			}
			requestBuilder.setModel(ByteString.copyFrom(PersistenceUtils.toBinary(readerParams
					.getIndex()
					.getIndexModel())));

			final int maxRangeDecomposition = readerParams.getMaxRangeDecomposition() == null ? options
					.getAggregationMaxRangeDecomposition() : readerParams.getMaxRangeDecomposition();
			final MultiRowRangeFilter multiFilter = getMultiRowRangeFilter(DataStoreUtils.constraintsToQueryRanges(
					readerParams.getConstraints(),
					readerParams.getIndex().getIndexStrategy(),
					null,
					maxRangeDecomposition).getCompositeQueryRanges());
			if (multiFilter != null) {
				requestBuilder.setRangeFilter(ByteString.copyFrom(multiFilter.toByteArray()));
			}
			if ((adapterIds != null) && (adapterIds.length > 0)) {
				final ByteBuffer buf = ByteBuffer.allocate(2 * adapterIds.length);
				for (final Short a : adapterIds) {
					buf.putShort(a);
				}
				requestBuilder.setAdapterIds(ByteString.copyFrom(buf.array()));
			}

			final Table table = getTable(tableName);
			final HBaseBulkDeleteProtosClient.BulkDeleteRequest request = requestBuilder.build();

			byte[] startRow = null;
			byte[] endRow = null;

			final List<ByteArrayRange> ranges = readerParams.getQueryRanges().getCompositeQueryRanges();
			if ((ranges != null) && !ranges.isEmpty()) {
				final ByteArrayRange aggRange = getSingleRange(ranges);
				startRow = aggRange.getStart().getBytes();
				endRow = aggRange.getEnd().getBytes();
			}
			final Map<byte[], Long> results = table.coprocessorService(
					HBaseBulkDeleteProtosClient.BulkDeleteService.class,
					startRow,
					endRow,
					new Batch.Call<HBaseBulkDeleteProtosClient.BulkDeleteService, Long>() {
						@Override
						public Long call(
								final HBaseBulkDeleteProtosClient.BulkDeleteService counter )
								throws IOException {
							final BlockingRpcCallback<HBaseBulkDeleteProtosClient.BulkDeleteResponse> rpcCallback = new BlockingRpcCallback<>();
							counter.delete(
									null,
									request,
									rpcCallback);
							final BulkDeleteResponse response = rpcCallback.get();
							return response.hasRowsDeleted() ? response.getRowsDeleted() : null;
						}
					});

			int regionCount = 0;
			for (final Map.Entry<byte[], Long> entry : results.entrySet()) {
				regionCount++;

				final Long value = entry.getValue();
				if (value != null) {
					LOGGER.debug("Value from region " + regionCount + " is " + value);
					total += value;
				}
				else {
					LOGGER.debug("Empty response for region " + regionCount);
				}
			}
		}
		catch (final Exception e) {
			LOGGER.error(
					"Error during bulk delete.",
					e);
		}
		catch (final Throwable e) {
			LOGGER.error(
					"Error during bulkdelete.",
					e);
		}
	}

	private ByteArrayRange getSingleRange(
			final List<ByteArrayRange> ranges ) {
		ByteArray start = null;
		ByteArray end = null;

		for (final ByteArrayRange range : ranges) {
			if ((start == null) || (range.getStart().compareTo(
					start) < 0)) {
				start = range.getStart();
			}
			if ((end == null) || (range.getEnd().compareTo(
					end) > 0)) {
				end = range.getEnd();
			}
		}
		return new ByteArrayRange(
				start,
				end);
	}

	public List<ByteArray> getTableRegions(
			final String tableNameStr ) {
		final ArrayList<ByteArray> regionIdList = Lists.newArrayList();

		try (final RegionLocator locator = getRegionLocator(tableNameStr)) {
			for (final HRegionLocation regionLocation : locator.getAllRegionLocations()) {
				regionIdList.add(new ByteArray(
						regionLocation.getRegionInfo().getRegionName()));
			}
		}
		catch (final IOException e) {
			LOGGER.error(
					"Error accessing region locator for " + tableNameStr,
					e);
		}

		return regionIdList;
	}

	@Override
	public Map<String, ImmutableSet<ServerOpScope>> listServerOps(
			final String index ) {
		final Map<String, ImmutableSet<ServerOpScope>> map = new HashMap<>();
		try (Admin admin = conn.getAdmin()) {
			final TableName tableName = getTableName(index);
			final String namespace = HBaseUtils.writeTableNameAsConfigSafe(tableName.getNamespaceAsString());
			final String qualifier = HBaseUtils.writeTableNameAsConfigSafe(tableName.getQualifierAsString());
			final HTableDescriptor desc = admin.getTableDescriptor(tableName);
			final Map<String, String> config = desc.getConfiguration();

			for (final Entry<String, String> e : config.entrySet()) {
				if (e.getKey().startsWith(
						ServerSideOperationUtils.SERVER_OP_PREFIX)) {
					final String[] parts = e.getKey().split(
							SPLIT_STRING);
					if ((parts.length == 5) && parts[1].equals(namespace) && parts[2].equals(qualifier)
							&& parts[4].equals(ServerSideOperationUtils.SERVER_OP_SCOPES_KEY)) {
						map.put(
								parts[3],
								HBaseUtils.stringToScopes(e.getValue()));
					}
				}
			}
		}
		catch (final IOException e) {
			LOGGER.error(
					"Unable to get table descriptor",
					e);
		}
		return map;
	}

	@Override
	public Map<String, String> getServerOpOptions(
			final String index,
			final String serverOpName,
			final ServerOpScope scope ) {
		final Map<String, String> map = new HashMap<>();
		try (Admin admin = conn.getAdmin()) {
			final TableName tableName = getTableName(index);
			final String namespace = HBaseUtils.writeTableNameAsConfigSafe(tableName.getNamespaceAsString());
			final String qualifier = HBaseUtils.writeTableNameAsConfigSafe(tableName.getQualifierAsString());
			final HTableDescriptor desc = admin.getTableDescriptor(tableName);
			final Map<String, String> config = desc.getConfiguration();

			for (final Entry<String, String> e : config.entrySet()) {
				if (e.getKey().startsWith(
						ServerSideOperationUtils.SERVER_OP_PREFIX)) {
					final String[] parts = e.getKey().split(
							SPLIT_STRING);
					if ((parts.length == 6) && parts[1].equals(namespace) && parts[2].equals(qualifier)
							&& parts[3].equals(serverOpName)
							&& parts[4].equals(ServerSideOperationUtils.SERVER_OP_OPTIONS_PREFIX)) {
						map.put(
								parts[5],
								e.getValue());
					}
				}
			}
		}
		catch (final IOException e) {
			LOGGER.error(
					"Unable to get table descriptor",
					e);
		}
		return map;
	}

	@Override
	public void removeServerOp(
			final String index,
			final String serverOpName,
			final ImmutableSet<ServerOpScope> scopes ) {
		final TableName table = getTableName(index);
		try (Admin admin = conn.getAdmin()) {
			final HTableDescriptor desc = admin.getTableDescriptor(table);

			if (removeConfig(
					desc,
					HBaseUtils.writeTableNameAsConfigSafe(table.getNamespaceAsString()),
					HBaseUtils.writeTableNameAsConfigSafe(table.getQualifierAsString()),
					serverOpName)) {
				admin.modifyTable(
						table,
						desc);
				waitForUpdate(
						admin,
						table,
						SLEEP_INTERVAL);
			}
		}
		catch (final IOException e) {
			LOGGER.error(
					"Unable to remove server operation",
					e);
		}
	}

	private static boolean removeConfig(
			final HTableDescriptor desc,
			final String namespace,
			final String qualifier,
			final String serverOpName ) {
		final Map<String, String> config = new HashMap<>(
				desc.getConfiguration());
		boolean changed = false;
		for (final Entry<String, String> e : config.entrySet()) {
			if (e.getKey().startsWith(
					ServerSideOperationUtils.SERVER_OP_PREFIX)) {
				final String[] parts = e.getKey().split(
						SPLIT_STRING);
				if ((parts.length >= 5) && parts[1].equals(namespace) && parts[2].equals(qualifier)
						&& parts[3].equals(serverOpName)) {
					changed = true;
					desc.removeConfiguration(e.getKey());
				}
			}
		}
		return changed;
	}

	private static void addConfig(
			final HTableDescriptor desc,
			final String namespace,
			final String qualifier,
			final int priority,
			final String serverOpName,
			final String operationClassName,
			final ImmutableSet<ServerOpScope> scopes,
			final Map<String, String> properties ) {
		final String basePrefix = new StringBuilder(
				ServerSideOperationUtils.SERVER_OP_PREFIX)
						.append(
								".")
						.append(
								HBaseUtils
										.writeTableNameAsConfigSafe(
												namespace))
						.append(
								".")
						.append(
								HBaseUtils
										.writeTableNameAsConfigSafe(
												qualifier))
						.append(
								".")
						.append(
								serverOpName)
						.append(
								".")
						.toString();

		desc
				.setConfiguration(
						basePrefix + ServerSideOperationUtils.SERVER_OP_CLASS_KEY,
						ByteArrayUtils
								.byteArrayToString(
										URLClassloaderUtils
												.toClassId(
														operationClassName)));
		desc
				.setConfiguration(
						basePrefix + ServerSideOperationUtils.SERVER_OP_PRIORITY_KEY,
						Integer
								.toString(
										priority));

		desc
				.setConfiguration(
						basePrefix + ServerSideOperationUtils.SERVER_OP_SCOPES_KEY,
						scopes
								.stream()
								.map(
										ServerOpScope::name)
								.collect(
										Collectors
												.joining(
														",")));
		final String optionsPrefix = String
				.format(
						basePrefix + ServerSideOperationUtils.SERVER_OP_OPTIONS_PREFIX + ".");
		for (final Entry<String, String> e : properties.entrySet()) {
			desc
					.setConfiguration(
							optionsPrefix + e.getKey(),
							e.getValue());
		}
	}

	@Override
	public void addServerOp(
			final String index,
			final int priority,
			final String name,
			final String operationClass,
			final Map<String, String> properties,
			final ImmutableSet<ServerOpScope> configuredScopes ) {
		final TableName table = getTableName(index);
		try (Admin admin = conn.getAdmin()) {
			final HTableDescriptor desc = admin.getTableDescriptor(table);

			addConfig(
					desc,
					table.getNamespaceAsString(),
					table.getQualifierAsString(),
					priority,
					name,
					operationClass,
					configuredScopes,
					properties);
			admin.modifyTable(
					table,
					desc);
			waitForUpdate(
					admin,
					table,
					SLEEP_INTERVAL);
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Cannot add server op",
					e);
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
			final ImmutableSet<ServerOpScope> newScopes ) {
		final TableName table = getTableName(index);
		try (Admin admin = conn.getAdmin()) {
			final HTableDescriptor desc = admin.getTableDescriptor(table);

			final String namespace = HBaseUtils.writeTableNameAsConfigSafe(table.getNamespaceAsString());
			final String qualifier = HBaseUtils.writeTableNameAsConfigSafe(table.getQualifierAsString());
			removeConfig(
					desc,
					namespace,
					qualifier,
					name);
			addConfig(
					desc,
					namespace,
					qualifier,
					priority,
					name,
					operationClass,
					newScopes,
					properties);
			admin.modifyTable(
					table,
					desc);
			waitForUpdate(
					admin,
					table,
					SLEEP_INTERVAL);
		}
		catch (final IOException e) {
			LOGGER.error(
					"Unable to update server operation",
					e);
		}
	}

	@Override
	public boolean metadataExists(
			final MetadataType type )
			throws IOException {
		synchronized (ADMIN_MUTEX) {
			try (Admin admin = conn.getAdmin()) {
				return admin.tableExists(getTableName(getMetadataTableName(type)));
			}
		}
	}

	@Override
	public String getVersion() {
		String version = null;

		if ((options == null) || !options.isServerSideLibraryEnabled()) {
			LOGGER.warn("Serverside library not enabled, serverside version is irrelevant");
			return null;
		}
		try {
			// use Index as the type to check for version (for hbase type
			// doesn't matter anyways)
			final MetadataType type = MetadataType.INDEX;
			final String tableName = getMetadataTableName(type);
			if (!indexExists(tableName)) {
				createTable(
						Collections.EMPTY_SET,
						HBaseOperations.METADATA_CFS_VERSIONING,
						StringColumnFamilyFactory.getSingletonInstance(),
						getTableName(getQualifiedTableName(tableName)));
			}

			// Use the row count coprocessor
			if (options.isVerifyCoprocessors()) {
				verifyCoprocessor(
						tableName,
						"org.locationtech.geowave.datastore.hbase.coprocessors.VersionEndpoint",
						options.getCoprocessorJar());
			}
			final Table table = getTable(tableName);
			final Map<byte[], List<String>> versionInfoResponse = table.coprocessorService(
					VersionProtosClient.VersionService.class,
					null,
					null,
					new Batch.Call<VersionProtosClient.VersionService, List<String>>() {
						@Override
						public List<String> call(
								final VersionProtosClient.VersionService versionService )
								throws IOException {
							final BlockingRpcCallback<VersionProtosClient.VersionResponse> rpcCallback = new BlockingRpcCallback<>();
							versionService.version(
									null,
									VersionRequest.getDefaultInstance(),
									rpcCallback);
							final VersionProtosClient.VersionResponse response = rpcCallback.get();
							return response.getVersionInfoList();
						}
					});
			table.close();
			if ((versionInfoResponse == null) || versionInfoResponse.isEmpty()) {
				LOGGER.error("No response from version coprocessor");
			}
			else {
				final Iterator<List<String>> values = versionInfoResponse.values().iterator();

				final List<String> value = values.next();
				while (values.hasNext()) {
					final List<String> newValue = values.next();
					if (!value.equals(newValue)) {
						LOGGER
								.error("Version Info '"
										+ Arrays.toString(value.toArray())
										+ "' and '"
										+ Arrays.toString(newValue.toArray())
										+ "' differ.  This may mean that different regions are using different versions of GeoWave.");
					}
				}
				version = VersionUtils.asLineDelimitedString(value);
			}
		}
		catch (final Throwable e) {
			LOGGER.warn(
					"Unable to check metadata table for version",
					e);
		}
		return version;
	}

	@Override
	public boolean createIndex(
			final Index index )
			throws IOException {
		createTable(
				index.getIndexStrategy().getPredefinedSplits(),
				new GeoWaveColumnFamily[0],
				StringColumnFamilyFactory.getSingletonInstance(),
				options.isServerSideLibraryEnabled(),
				getTableName(index.getName()));
		return true;
	}

	@Override
	public <T> Deleter<T> createDeleter(
			final ReaderParams<T> readerParams ) {
		if (isServerSideLibraryEnabled()) {
			return new HBaseDeleter(
					readerParams,
					this);
		}
		else {
			final RowDeleter rowDeleter = createRowDeleter(
					readerParams.getIndex().getName(),
					readerParams.getAdapterStore(),
					readerParams.getInternalAdapterStore(),
					readerParams.getAdditionalAuthorizations());
			if (rowDeleter != null) {
				return new QueryAndDeleteByRow<>(
						rowDeleter,
						createReader(readerParams));
			}
			return new QueryAndDeleteByRow<>();
		}
	}
}
