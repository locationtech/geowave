/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.datastore.hbase.operations;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.hadoop.hbase.security.visibility.Authorizations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;

import mil.nga.giat.geowave.core.cli.VersionUtils;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.index.MultiDimensionalCoordinateRangesArray;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.PersistentAdapterStore;
import mil.nga.giat.geowave.core.store.base.BaseDataStoreUtils;
import mil.nga.giat.geowave.core.store.entities.GeoWaveMetadata;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.metadata.AbstractGeoWavePersistence;
import mil.nga.giat.geowave.core.store.metadata.DataStatisticsStoreImpl;
import mil.nga.giat.geowave.core.store.operations.Deleter;
import mil.nga.giat.geowave.core.store.operations.MetadataDeleter;
import mil.nga.giat.geowave.core.store.operations.MetadataQuery;
import mil.nga.giat.geowave.core.store.operations.MetadataReader;
import mil.nga.giat.geowave.core.store.operations.MetadataType;
import mil.nga.giat.geowave.core.store.operations.MetadataWriter;
import mil.nga.giat.geowave.core.store.operations.Reader;
import mil.nga.giat.geowave.core.store.operations.ReaderParams;
import mil.nga.giat.geowave.core.store.operations.Writer;
import mil.nga.giat.geowave.core.store.query.aggregate.Aggregation;
import mil.nga.giat.geowave.core.store.query.aggregate.CommonIndexAggregation;
import mil.nga.giat.geowave.core.store.server.BasicOptionProvider;
import mil.nga.giat.geowave.core.store.server.RowMergingAdapterOptionProvider;
import mil.nga.giat.geowave.core.store.server.ServerOpConfig.ServerOpScope;
import mil.nga.giat.geowave.core.store.server.ServerOpHelper;
import mil.nga.giat.geowave.core.store.server.ServerSideOperations;
import mil.nga.giat.geowave.core.store.util.DataAdapterAndIndexCache;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;
import mil.nga.giat.geowave.datastore.hbase.HBaseRow;
import mil.nga.giat.geowave.datastore.hbase.HBaseStoreFactoryFamily;
import mil.nga.giat.geowave.datastore.hbase.cli.config.HBaseOptions;
import mil.nga.giat.geowave.datastore.hbase.cli.config.HBaseRequiredOptions;
import mil.nga.giat.geowave.datastore.hbase.coprocessors.AggregationEndpoint;
import mil.nga.giat.geowave.datastore.hbase.coprocessors.ServerSideOperationsObserver;
import mil.nga.giat.geowave.datastore.hbase.coprocessors.VersionEndpoint;
import mil.nga.giat.geowave.datastore.hbase.coprocessors.protobuf.AggregationProtos;
import mil.nga.giat.geowave.datastore.hbase.filters.HBaseNumericIndexStrategyFilter;
import mil.nga.giat.geowave.datastore.hbase.operations.GeoWaveColumnFamily.ByteArrayColumnFamily;
import mil.nga.giat.geowave.datastore.hbase.operations.GeoWaveColumnFamily.ByteArrayColumnFamilyFactory;
import mil.nga.giat.geowave.datastore.hbase.operations.GeoWaveColumnFamily.GeoWaveColumnFamilyFactory;
import mil.nga.giat.geowave.datastore.hbase.operations.GeoWaveColumnFamily.StringColumnFamily;
import mil.nga.giat.geowave.datastore.hbase.operations.GeoWaveColumnFamily.StringColumnFamilyFactory;
import mil.nga.giat.geowave.datastore.hbase.query.protobuf.VersionProtos;
import mil.nga.giat.geowave.datastore.hbase.query.protobuf.VersionProtos.VersionRequest;
import mil.nga.giat.geowave.datastore.hbase.server.MergingServerOp;
import mil.nga.giat.geowave.datastore.hbase.server.MergingVisibilityServerOp;
import mil.nga.giat.geowave.datastore.hbase.util.ConnectionPool;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseUtils;
import mil.nga.giat.geowave.mapreduce.MapReduceDataStoreOperations;
import mil.nga.giat.geowave.mapreduce.URLClassloaderUtils;
import mil.nga.giat.geowave.mapreduce.splits.RecordReaderParams;

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

	protected final Connection conn;

	private final String tableNamespace;
	private final boolean schemaUpdateEnabled;
	private final HashMap<ByteArrayId, Boolean> tableAvailableCache = new HashMap<>();
	private final HashMap<String, List<String>> coprocessorCache = new HashMap<>();
	private final Map<TableName, Set<ByteArrayId>> partitionCache = new HashMap<>();
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
			final Pair<GeoWaveColumnFamily, Boolean>[] columnFamiliesAndVersioningPairs,
			GeoWaveColumnFamilyFactory columnFamilyFactory,
			final TableName tableName )
			throws IOException {
		synchronized (ADMIN_MUTEX) {
			try (Admin admin = conn.getAdmin()) {
				if (!admin.isTableAvailable(tableName)) {
					final HTableDescriptor desc = new HTableDescriptor(
							tableName);

					final HashSet<GeoWaveColumnFamily> cfSet = new HashSet<>();

					for (final Pair<GeoWaveColumnFamily, Boolean> columnFamilyAndVersioning : columnFamiliesAndVersioningPairs) {
						final HColumnDescriptor column = columnFamilyAndVersioning.getLeft().toColumnDescriptor();
						if (!columnFamilyAndVersioning.getRight()) {
							column.setMaxVersions(Integer.MAX_VALUE);
						}
						desc.addFamily(column);

						cfSet.add(columnFamilyAndVersioning.getLeft());
					}

					cfCache.put(
							tableName,
							cfSet);

					try {
						admin.createTable(desc);
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
			final GeoWaveColumnFamily[] columnFamilies,
			final GeoWaveColumnFamilyFactory columnFamilyFactory,
			final boolean enableVersioning,
			final TableName tableName )
			throws IOException {
		createTable(
				Arrays
						.stream(
								columnFamilies)
						.map(
								cf -> ImmutablePair.of(
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
				if (admin.isTableAvailable(tableName)) {
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
						admin.disableTable(tableName);
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

						admin.enableTable(tableName);
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
						if (admin.isTableAvailable(tableName)) {
							admin.disableTable(tableName);
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
			final ByteArrayId indexId,
			final Short adapterId,
			final String... additionalAuthorizations ) {
		Deleter deleter = null;
		Iterable<Result> scanner = null;
		try {
			deleter = createDeleter(
					indexId,
					additionalAuthorizations);
			DataAdapter<?> adapter = null;
			PrimaryIndex index = null;
			try (final CloseableIterator<GeoWaveMetadata> it = createMetadataReader(
					MetadataType.ADAPTER).query(
					new MetadataQuery(
							ByteArrayUtils.shortToByteArray(adapterId),
							null,
							additionalAuthorizations))) {
				if (!it.hasNext()) {
					LOGGER.warn("Unable to find adapter to delete");
					return false;
				}
				final GeoWaveMetadata adapterMd = it.next();
				adapter = (DataAdapter<?>) URLClassloaderUtils.fromBinary(adapterMd.getValue());
			}
			try (final CloseableIterator<GeoWaveMetadata> it = createMetadataReader(
					MetadataType.INDEX).query(
					new MetadataQuery(
							indexId.getBytes(),
							null,
							additionalAuthorizations))) {
				if (!it.hasNext()) {
					LOGGER.warn("Unable to find index to delete");
					return false;
				}
				final GeoWaveMetadata indexMd = it.next();
				index = (PrimaryIndex) URLClassloaderUtils.fromBinary(indexMd.getValue());
			}
			final Scan scan = new Scan();
			scan.addFamily(ByteArrayUtils.shortToByteArray(adapterId));
			scanner = getScannedResults(
					scan,
					indexId.getString(),
					additionalAuthorizations);
			for (final Result result : scanner) {
				deleter.delete(
						new HBaseRow(
								result,
								index.getIndexStrategy().getPartitionKeyLength()),
						adapter);
			}
			return true;
		}
		catch (final Exception e) {
			LOGGER.warn(
					"Unable to close deleter",
					e);
		}
		finally {
			if (scanner != null && scanner instanceof ResultScanner) {
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
			final String tableName,
			final String... authorizations )
			throws IOException {
		if ((authorizations != null) && (authorizations.length > 0)) {
			scanner.setAuthorizations(new Authorizations(
					authorizations));
		}

		final Table table = conn.getTable(getTableName(tableName));

		final ResultScanner results = table.getScanner(scanner);

		table.close();

		return results;
	}

	public RegionLocator getRegionLocator(
			final String tableName )
			throws IOException {
		return conn.getRegionLocator(getTableName(tableName));
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
						admin.disableTable(tableName);

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
						admin.enableTable(tableName);

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
			final ByteArrayId indexId )
			throws IOException {
		Boolean tableAvailable = tableAvailableCache.get(indexId);
		if (tableAvailable == null) {
			synchronized (ADMIN_MUTEX) {
				try (Admin admin = conn.getAdmin()) {
					TableName tableName = getTableName(indexId.getString());
					tableAvailable = admin.isTableAvailable(tableName);
					if (!tableAvailable) {
						waitForUpdate(
								admin,
								tableName,
								SLEEP_INTERVAL);
						tableAvailable = admin.isTableAvailable(tableName);
					}
				}
				tableAvailableCache.put(
						indexId,
						tableAvailable);
			}
		}
		return tableAvailable;
	}

	@Override
	public boolean mergeData(
			final PrimaryIndex index,
			final PersistentAdapterStore adapterStore,
			final AdapterIndexMappingStore adapterIndexMappingStore ) {
		// simply compact the table,
		// NOTE: this is an asynchronous operations and this does not block and
		// wait for the table to be fully compacted, which may be a long-running
		// distributed process, but this is primarily used for efficiency not
		// correctness so it seems ok to just let it compact in the background
		// but we can consider blocking and waiting for completion
		try {
			conn.getAdmin().compact(
					getTableName(index.getId().getString()));
		}
		catch (final IOException e) {
			LOGGER.error(
					"Cannot compact table '" + index.getId().getString() + "'",
					e);
			return false;
		}
		return true;
	}

	public void insurePartition(
			final ByteArrayId partition,
			final String tableNameStr ) {
		final TableName tableName = getTableName(tableNameStr);
		Set<ByteArrayId> existingPartitions = partitionCache.get(tableName);

		try {
			synchronized (partitionCache) {
				if (existingPartitions == null) {
					try (RegionLocator regionLocator = getRegionLocator(tableNameStr)) {
						existingPartitions = new HashSet<>();

						for (final byte[] startKey : regionLocator.getStartKeys()) {
							if (startKey.length > 0) {
								existingPartitions.add(new ByteArrayId(
										startKey));
							}
						}
					}

					partitionCache.put(
							tableName,
							existingPartitions);
				}

				if (!existingPartitions.contains(partition)) {
					existingPartitions.add(partition);

					LOGGER.debug("> Splitting: " + partition.getHexString());

					try (Admin admin = conn.getAdmin()) {
						admin.split(
								tableName,
								partition.getBytes());

						// waitForUpdate(
						// admin,
						// tableName,
						// 100L);
					}

					LOGGER.debug("> Split complete: " + partition.getHexString());
				}
			}
		}
		catch (final IOException e) {
			LOGGER.error("Error accessing region info: " + e.getMessage());
		}
	}

	@Override
	public boolean insureAuthorizations(
			final String clientUser,
			final String... authorizations ) {
		return true;
	}

	public void ensureServerSideOperationsObserverAttached(
			final ByteArrayId indexId ) {
		// Use the server-side operations observer
		verifyCoprocessor(
				indexId.getString(),
				ServerSideOperationsObserver.class.getName(),
				options.getCoprocessorJar());
	}

	public void createTable(
			final ByteArrayId indexId,
			final boolean enableVersioning,
			final short internalAdapterId ) {
		final TableName tableName = getTableName(indexId.getString());

		final GeoWaveColumnFamily[] columnFamilies = new GeoWaveColumnFamily[1];
		columnFamilies[0] = new StringColumnFamily(
				ByteArrayUtils.shortToString(internalAdapterId));
		try {
			createTable(
					columnFamilies,
					StringColumnFamilyFactory.getSingletonInstance(),
					enableVersioning,
					tableName);
		}
		catch (final IOException e) {
			LOGGER.error(
					"Error creating table: " + indexId.getString(),
					e);
		}
	}

	@Override
	public Writer createWriter(
			final ByteArrayId indexId,
			final short internalAdapterId ) {
		final TableName tableName = getTableName(indexId.getString());
		try {
			final GeoWaveColumnFamily[] columnFamilies = new GeoWaveColumnFamily[1];
			columnFamilies[0] = new StringColumnFamily(
					ByteArrayUtils.shortToString(internalAdapterId));

			if (options.isCreateTable()) {
				createTable(
						columnFamilies,
						StringColumnFamilyFactory.getSingletonInstance(),
						options.isServerSideLibraryEnabled(),
						tableName);
			}

			verifyColumnFamilies(
					columnFamilies,
					StringColumnFamilyFactory.getSingletonInstance(),
					true,
					tableName,
					true);

			return new HBaseWriter(
					getBufferedMutator(tableName),
					this,
					indexId.getString());
		}
		catch (final TableNotFoundException e) {
			LOGGER.error(
					"Table does not exist",
					e);
		}
		catch (final IOException e) {
			LOGGER.error(
					"Error creating table: " + indexId.getString(),
					e);
		}

		return null;
	}

	@Override
	public MetadataWriter createMetadataWriter(
			final MetadataType metadataType ) {
		final TableName tableName = getTableName(AbstractGeoWavePersistence.METADATA_TABLE);
		try {
			if (options.isCreateTable()) {
				createTable(
						METADATA_CFS_VERSIONING,
						StringColumnFamilyFactory.getSingletonInstance(),
						tableName);
			}
			if (MetadataType.STATS.equals(metadataType) && options.isServerSideLibraryEnabled()) {
				synchronized (this) {
					if (!iteratorsAttached) {
						iteratorsAttached = true;

						final BasicOptionProvider optionProvider = new BasicOptionProvider(
								new HashMap<>());
						ensureServerSideOperationsObserverAttached(new ByteArrayId(
								AbstractGeoWavePersistence.METADATA_TABLE));
						ServerOpHelper.addServerSideMerging(
								this,
								DataStatisticsStoreImpl.STATISTICS_COMBINER_NAME,
								DataStatisticsStoreImpl.STATS_COMBINER_PRIORITY,
								MergingServerOp.class.getName(),
								MergingVisibilityServerOp.class.getName(),
								optionProvider,
								AbstractGeoWavePersistence.METADATA_TABLE);
					}
				}
			}
			return new HBaseMetadataWriter(
					getBufferedMutator(tableName),
					metadataType);
		}
		catch (final IOException e) {
			LOGGER.error(
					"Error creating metadata table: " + AbstractGeoWavePersistence.METADATA_TABLE,
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
	public Reader createReader(
			final ReaderParams readerParams ) {
		final HBaseReader hbaseReader = new HBaseReader(
				readerParams,
				this);

		return hbaseReader;
	}

	@Override
	public Reader createReader(
			final RecordReaderParams recordReaderParams ) {
		return new HBaseReader(
				recordReaderParams,
				this);
	}

	@Override
	public Deleter createDeleter(
			final ByteArrayId indexId,
			final String... authorizations )
			throws Exception {
		final TableName tableName = getTableName(indexId.getString());

		return new HBaseDeleter(
				getBufferedMutator(tableName));
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
		final List<RowRange> rowRanges = new ArrayList<RowRange>();
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

	public Mergeable aggregateServerSide(
			final ReaderParams readerParams ) {
		final String tableName = readerParams.getIndex().getId().getString();

		try {
			// Use the row count coprocessor
			if (options.isVerifyCoprocessors()) {
				verifyCoprocessor(
						tableName,
						AggregationEndpoint.class.getName(),
						options.getCoprocessorJar());
			}

			final Aggregation aggregation = readerParams.getAggregation().getRight();

			final AggregationProtos.AggregationType.Builder aggregationBuilder = AggregationProtos.AggregationType
					.newBuilder();
			aggregationBuilder.setClassId(ByteString.copyFrom(URLClassloaderUtils.toClassId(aggregation)));

			if (aggregation.getParameters() != null) {
				final byte[] paramBytes = URLClassloaderUtils.toBinary(aggregation.getParameters());
				aggregationBuilder.setParams(ByteString.copyFrom(paramBytes));
			}

			final AggregationProtos.AggregationRequest.Builder requestBuilder = AggregationProtos.AggregationRequest
					.newBuilder();
			requestBuilder.setAggregation(aggregationBuilder.build());
			if (readerParams.getFilter() != null) {
				final List<DistributableQueryFilter> distFilters = new ArrayList();
				distFilters.add(readerParams.getFilter());

				final byte[] filterBytes = URLClassloaderUtils.toBinary(distFilters);
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
			requestBuilder.setModel(ByteString.copyFrom(URLClassloaderUtils.toBinary(readerParams
					.getIndex()
					.getIndexModel())));

			final MultiRowRangeFilter multiFilter = getMultiRowRangeFilter(DataStoreUtils.constraintsToQueryRanges(
					readerParams.getConstraints(),
					readerParams.getIndex().getIndexStrategy(),
					BaseDataStoreUtils.MAX_RANGE_DECOMPOSITION).getCompositeQueryRanges());
			if (multiFilter != null) {
				requestBuilder.setRangeFilter(ByteString.copyFrom(multiFilter.toByteArray()));
			}
			if (readerParams.getAggregation().getLeft() != null) {
				if (readerParams.getAggregation().getRight() instanceof CommonIndexAggregation) {
					requestBuilder.setInternalAdapterId(ByteString.copyFrom(ByteArrayUtils
							.shortToByteArray(readerParams.getAggregation().getLeft().getInternalAdapterId())));
				}
				else {
					requestBuilder.setAdapter(ByteString.copyFrom(URLClassloaderUtils.toBinary(readerParams
							.getAggregation()
							.getLeft())));
				}
			}

			if ((readerParams.getAdditionalAuthorizations() != null)
					&& (readerParams.getAdditionalAuthorizations().length > 0)) {
				requestBuilder.setVisLabels(ByteString.copyFrom(StringUtils.stringsToBinary(readerParams
						.getAdditionalAuthorizations())));
			}

			if (readerParams.isMixedVisibility()) {
				requestBuilder.setWholeRowFilter(true);
			}

			requestBuilder.setPartitionKeyLength(readerParams.getIndex().getIndexStrategy().getPartitionKeyLength());

			final AggregationProtos.AggregationRequest request = requestBuilder.build();

			final Table table = getTable(tableName);

			byte[] startRow = null;
			byte[] endRow = null;

			final List<ByteArrayRange> ranges = readerParams.getQueryRanges().getCompositeQueryRanges();
			if ((ranges != null) && !ranges.isEmpty()) {
				final ByteArrayRange aggRange = getSingleRange(ranges);
				startRow = aggRange.getStart().getBytes();
				endRow = aggRange.getEnd().getBytes();
			}

			final Map<byte[], ByteString> results = table.coprocessorService(
					AggregationProtos.AggregationService.class,
					startRow,
					endRow,
					new Batch.Call<AggregationProtos.AggregationService, ByteString>() {
						@Override
						public ByteString call(
								final AggregationProtos.AggregationService counter )
								throws IOException {
							final BlockingRpcCallback<AggregationProtos.AggregationResponse> rpcCallback = new BlockingRpcCallback<AggregationProtos.AggregationResponse>();
							counter.aggregate(
									null,
									request,
									rpcCallback);
							final AggregationProtos.AggregationResponse response = rpcCallback.get();
							return response.hasValue() ? response.getValue() : null;
						}
					});

			Mergeable total = null;

			int regionCount = 0;
			for (final Map.Entry<byte[], ByteString> entry : results.entrySet()) {
				regionCount++;

				final ByteString value = entry.getValue();
				if ((value != null) && !value.isEmpty()) {
					final byte[] bvalue = value.toByteArray();
					final Mergeable mvalue = (Mergeable) URLClassloaderUtils.fromBinary(bvalue);

					LOGGER.debug("Value from region " + regionCount + " is " + mvalue);

					if (total == null) {
						total = mvalue;
					}
					else {
						total.merge(mvalue);
					}
				}
				else {
					LOGGER.debug("Empty response for region " + regionCount);
				}
			}

			return total;
		}
		catch (final Exception e) {
			LOGGER.error(
					"Error during aggregation.",
					e);
		}
		catch (final Throwable e) {
			LOGGER.error(
					"Error during aggregation.",
					e);
		}

		return null;
	}

	private ByteArrayRange getSingleRange(
			final List<ByteArrayRange> ranges ) {
		ByteArrayId start = null;
		ByteArrayId end = null;

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

	public List<ByteArrayId> getTableRegions(
			final String tableNameStr ) {
		final ArrayList<ByteArrayId> regionIdList = new ArrayList();

		try {
			final RegionLocator locator = getRegionLocator(tableNameStr);
			for (final HRegionLocation regionLocation : locator.getAllRegionLocations()) {
				regionIdList.add(new ByteArrayId(
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
		try {
			final TableName tableName = getTableName(index);
			final String namespace = tableName.getNamespaceAsString();
			final String qualifier = tableName.getQualifierAsString();
			final HTableDescriptor desc = conn.getAdmin().getTableDescriptor(
					tableName);
			final Map<String, String> config = desc.getConfiguration();

			for (final Entry<String, String> e : config.entrySet()) {
				if (e.getKey().startsWith(
						ServerSideOperationsObserver.SERVER_OP_PREFIX)) {
					final String[] parts = e.getKey().split(
							SPLIT_STRING);
					if ((parts.length == 5) && parts[1].equals(namespace) && parts[2].equals(qualifier)
							&& parts[4].equals(ServerSideOperationsObserver.SERVER_OP_SCOPES_KEY)) {
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
		try {
			final TableName tableName = getTableName(index);
			final String namespace = tableName.getNamespaceAsString();
			final String qualifier = tableName.getQualifierAsString();
			final HTableDescriptor desc = conn.getAdmin().getTableDescriptor(
					tableName);
			final Map<String, String> config = desc.getConfiguration();

			for (final Entry<String, String> e : config.entrySet()) {
				if (e.getKey().startsWith(
						ServerSideOperationsObserver.SERVER_OP_PREFIX)) {
					final String[] parts = e.getKey().split(
							SPLIT_STRING);
					if ((parts.length == 6) && parts[1].equals(namespace) && parts[2].equals(qualifier)
							&& parts[3].equals(serverOpName)
							&& parts[4].equals(ServerSideOperationsObserver.SERVER_OP_OPTIONS_PREFIX)) {
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
		try {
			final HTableDescriptor desc = conn.getAdmin().getTableDescriptor(
					table);

			if (removeConfig(
					desc,
					table.getNamespaceAsString(),
					table.getQualifierAsString(),
					serverOpName)) {
				conn.getAdmin().modifyTable(
						table,
						desc);
				waitForUpdate(
						conn.getAdmin(),
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
					ServerSideOperationsObserver.SERVER_OP_PREFIX)) {
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
				ServerSideOperationsObserver.SERVER_OP_PREFIX)
						.append(
								".")
						.append(
								namespace)
						.append(
								".")
						.append(
								qualifier)
						.append(
								".")
						.append(
								serverOpName)
						.append(
								".")
						.toString();

		desc.setConfiguration(
				basePrefix + ServerSideOperationsObserver.SERVER_OP_CLASS_KEY,
				ByteArrayUtils.byteArrayToString(
						URLClassloaderUtils.toClassId(
								operationClassName)));
		desc.setConfiguration(
				basePrefix + ServerSideOperationsObserver.SERVER_OP_PRIORITY_KEY,
				Integer.toString(
						priority));

		desc.setConfiguration(
				basePrefix + ServerSideOperationsObserver.SERVER_OP_SCOPES_KEY,
				scopes.stream().map(
						ServerOpScope::name).collect(
								Collectors.joining(
										",")));
		final String optionsPrefix = String.format(
				basePrefix + ServerSideOperationsObserver.SERVER_OP_OPTIONS_PREFIX + ".");
		for (final Entry<String, String> e : properties.entrySet()) {
			desc.setConfiguration(
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
		try {
			final HTableDescriptor desc = conn.getAdmin().getTableDescriptor(
					table);

			addConfig(
					desc,
					table.getNamespaceAsString(),
					table.getQualifierAsString(),
					priority,
					name,
					operationClass,
					configuredScopes,
					properties);
			conn.getAdmin().modifyTable(
					table,
					desc);
			waitForUpdate(
					conn.getAdmin(),
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
		try {
			final HTableDescriptor desc = conn.getAdmin().getTableDescriptor(
					table);

			final String namespace = table.getNamespaceAsString();
			final String qualifier = table.getQualifierAsString();
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
			conn.getAdmin().modifyTable(
					table,
					desc);
			waitForUpdate(
					conn.getAdmin(),
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
				return admin.isTableAvailable(getTableName(AbstractGeoWavePersistence.METADATA_TABLE));
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
			if (!indexExists(AbstractGeoWavePersistence.METADATA_INDEX_ID)) {
				createTable(
						HBaseOperations.METADATA_CFS_VERSIONING,
						StringColumnFamilyFactory.getSingletonInstance(),
						getTableName(getQualifiedTableName(AbstractGeoWavePersistence.METADATA_TABLE)));
			}

			// Use the row count coprocessor
			if (options.isVerifyCoprocessors()) {
				verifyCoprocessor(
						AbstractGeoWavePersistence.METADATA_TABLE,
						VersionEndpoint.class.getName(),
						options.getCoprocessorJar());
			}
			final Table table = getTable(AbstractGeoWavePersistence.METADATA_TABLE);
			final Map<byte[], List<String>> versionInfoResponse = table.coprocessorService(
					VersionProtos.VersionService.class,
					null,
					null,
					new Batch.Call<VersionProtos.VersionService, List<String>>() {
						@Override
						public List<String> call(
								final VersionProtos.VersionService versionService )
								throws IOException {
							final BlockingRpcCallback<VersionProtos.VersionResponse> rpcCallback = new BlockingRpcCallback<VersionProtos.VersionResponse>();
							versionService.version(
									null,
									VersionRequest.getDefaultInstance(),
									rpcCallback);
							final VersionProtos.VersionResponse response = rpcCallback.get();
							return response.getVersionInfoList();
						}
					});
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
}
