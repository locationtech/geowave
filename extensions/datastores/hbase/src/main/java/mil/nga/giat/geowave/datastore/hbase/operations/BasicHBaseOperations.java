package mil.nga.giat.geowave.datastore.hbase.operations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.security.visibility.Authorizations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStoreOperations;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.RowMergingDataAdapter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.datastore.hbase.io.HBaseWriter;
import mil.nga.giat.geowave.datastore.hbase.operations.config.HBaseRequiredOptions;
import mil.nga.giat.geowave.datastore.hbase.util.ConnectionPool;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseUtils;
import mil.nga.giat.geowave.datastore.hbase.util.RewritingMergingEntryIterator;

public class BasicHBaseOperations implements
		DataStoreOperations
{
	private final static Logger LOGGER = LoggerFactory.getLogger(BasicHBaseOperations.class);
	protected static final String DEFAULT_TABLE_NAMESPACE = "";
	public static final Object ADMIN_MUTEX = new Object();
	private static final long SLEEP_INTERVAL = 10000L;

	private final Connection conn;
	private final String tableNamespace;
	private final boolean schemaUpdateEnabled;

	public BasicHBaseOperations(
			final Connection connection,
			final String geowaveNamespace )
			throws IOException {
		conn = connection;
		tableNamespace = geowaveNamespace;

		schemaUpdateEnabled = conn.getConfiguration().getBoolean(
				"hbase.online.schema.update.enable",
				false);
	}

	public BasicHBaseOperations(
			final String zookeeperInstances,
			final String geowaveNamespace )
			throws IOException {
		conn = ConnectionPool.getInstance().getConnection(
				zookeeperInstances);
		tableNamespace = geowaveNamespace;

		schemaUpdateEnabled = conn.getConfiguration().getBoolean(
				"hbase.online.schema.update.enable",
				false);
	}

	public static BasicHBaseOperations createOperations(
			final HBaseRequiredOptions options )
			throws IOException {
		return new BasicHBaseOperations(
				options.getZookeeper(),
				options.getGeowaveNamespace());
	}

	public Configuration getConfig() {
		return conn.getConfiguration();
	}

	public boolean isSchemaUpdateEnabled() {
		return schemaUpdateEnabled;
	}

	public static TableName getTableName(
			final String tableName ) {
		return TableName.valueOf(tableName);
	}

	public HBaseWriter createWriter(
			final String sTableName,
			final String[] columnFamilies,
			final boolean createTable )
			throws IOException {
		return createWriter(
				sTableName,
				columnFamilies,
				createTable,
				null);
	}

	public HBaseWriter createWriter(
			final String sTableName,
			final String[] columnFamilies,
			final boolean createTable,
			final Set<ByteArrayId> splits )
			throws IOException {
		final String qTableName = getQualifiedTableName(sTableName);

		if (createTable) {
			createTable(
					columnFamilies,
					getTableName(qTableName),
					splits);
		}

		return new HBaseWriter(
				conn.getAdmin(),
				qTableName);
	}

	public void createTable(
			final String[] columnFamilies,
			final TableName name,
			final Set<ByteArrayId> splits )
			throws IOException {
		synchronized (ADMIN_MUTEX) {
			if (!conn.getAdmin().isTableAvailable(
					name)) {
				final HTableDescriptor desc = new HTableDescriptor(
						name);
				for (final String columnFamily : columnFamilies) {
					desc.addFamily(new HColumnDescriptor(
							columnFamily));
				}

				try {
					if ((splits != null) && !splits.isEmpty()) {
						final byte[][] splitKeys = new byte[splits.size()][];
						int i = 0;
						for (final ByteArrayId split : splits) {
							splitKeys[i++] = split.getBytes();
						}
						conn.getAdmin().createTable(
								desc,
								splitKeys);
					}
					else {
						conn.getAdmin().createTable(
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

	public void addColumnFamiles(
			final String[] columnFamilies,
			final String tableName )
			throws IOException {
		final TableName table = getTableName(tableName);
		final List<String> existingColumnFamilies = new ArrayList<>();
		final List<String> newColumnFamilies = new ArrayList<>();
		synchronized (ADMIN_MUTEX) {
			if (conn.getAdmin().isTableAvailable(
					table)) {
				final HTableDescriptor existingTableDescriptor = conn.getAdmin().getTableDescriptor(
						table);
				final HColumnDescriptor[] existingColumnDescriptors = existingTableDescriptor.getColumnFamilies();
				for (final HColumnDescriptor hColumnDescriptor : existingColumnDescriptors) {
					existingColumnFamilies.add(hColumnDescriptor.getNameAsString());
				}
				for (final String columnFamily : columnFamilies) {
					if (!existingColumnFamilies.contains(columnFamily)) {
						newColumnFamilies.add(columnFamily);
					}
				}
				for (final String newColumnFamily : newColumnFamilies) {
					existingTableDescriptor.addFamily(new HColumnDescriptor(
							newColumnFamily));
				}
				conn.getAdmin().modifyTable(
						table,
						existingTableDescriptor);
			}
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
		final TableName[] tableNamesArr = conn.getAdmin().listTableNames();
		for (final TableName tableName : tableNamesArr) {
			if ((tableNamespace == null) || tableName.getNameAsString().startsWith(
					tableNamespace)) {
				synchronized (ADMIN_MUTEX) {
					if (conn.getAdmin().isTableAvailable(
							tableName)) {
						conn.getAdmin().disableTable(
								tableName);
						conn.getAdmin().deleteTable(
								tableName);
					}
				}
			}
		}
	}

	@Override
	public boolean tableExists(
			final String tableName )
			throws IOException {
		final String qName = getQualifiedTableName(tableName);
		synchronized (ADMIN_MUTEX) {
			return conn.getAdmin().isTableAvailable(
					getTableName(qName));
		}

	}

	public boolean columnFamilyExists(
			final String tableName,
			final String columnFamily )
			throws IOException {
		final String qName = getQualifiedTableName(tableName);
		synchronized (ADMIN_MUTEX) {
			final HTableDescriptor descriptor = conn.getAdmin().getTableDescriptor(
					getTableName(qName));

			if (descriptor != null) {
				for (final HColumnDescriptor hColumnDescriptor : descriptor.getColumnFamilies()) {
					if (hColumnDescriptor.getNameAsString().equalsIgnoreCase(
							columnFamily)) {
						return true;
					}
				}
			}
		}
		return false;
	}

	public ResultScanner getScannedResults(
			final Scan scanner,
			final String tableName,
			final String... authorizations )
			throws IOException {
		if (authorizations != null) {
			scanner.setAuthorizations(new Authorizations(
					authorizations));
		}

		final Table table = conn.getTable(getTableName(getQualifiedTableName(tableName)));

		final ResultScanner results = table.getScanner(scanner);

		table.close();

		return results;
	}

	public boolean deleteTable(
			final String tableName ) {
		final String qName = getQualifiedTableName(tableName);
		try {
			conn.getAdmin().deleteTable(
					getTableName(qName));
			return true;
		}
		catch (final IOException ex) {
			LOGGER.warn(
					"Unable to delete table '" + qName + "'",
					ex);
		}
		return false;

	}

	public RegionLocator getRegionLocator(
			final String tableName )
			throws IOException {
		return conn.getRegionLocator(getTableName(getQualifiedTableName(tableName)));
	}

	@Override
	public String getTableNameSpace() {
		return tableNamespace;
	}

	public Table getTable(
			final String tableName )
			throws IOException {
		return conn.getTable(getTableName(getQualifiedTableName(tableName)));
	}

	public void verifyCoprocessor(
			final String tableNameStr,
			final String coprocessorName,
			final String coprocessorJar ) {
		try {
			final Admin admin = conn.getAdmin();
			final TableName tableName = getTableName(getQualifiedTableName(tableNameStr));
			final HTableDescriptor td = admin.getTableDescriptor(tableName);

			if (!td.hasCoprocessor(coprocessorName)) {
				LOGGER.debug(tableNameStr + " does not have coprocessor. Adding " + coprocessorName);

				// if (!schemaUpdateEnabled &&
				// !admin.isTableDisabled(tableName)) {
				LOGGER.debug("- disable table...");
				admin.disableTable(tableName);
				// }

				LOGGER.debug("- add coprocessor...");

				// Retrieve coprocessor jar path from config
				if (coprocessorJar == null) {
					td.addCoprocessor(coprocessorName);
				}
				else {
					final Path hdfsJarPath = new Path(
							coprocessorJar);
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

				// if (!schemaUpdateEnabled) {
				LOGGER.debug("- enable table...");
				admin.enableTable(tableName);
			}
			// }

			// if (schemaUpdateEnabled) {
			int regionsLeft;

			do {
				regionsLeft = admin.getAlterStatus(
						tableName).getFirst();
				LOGGER.debug(regionsLeft + " regions remaining in table modify");

				try {
					Thread.sleep(SLEEP_INTERVAL);
				}
				catch (final InterruptedException e) {
					LOGGER.warn(
							"Sleeping while coprocessor add interrupted",
							e);
				}
			}
			while (regionsLeft > 0);
			// }

			LOGGER.debug("Successfully added coprocessor");
		}
		catch (

		final IOException e) {
			LOGGER.error(
					"Error verifying/adding coprocessor.",
					e);
		}
	}

	@Override
	public boolean mergeData(
			final PrimaryIndex index,
			final AdapterStore adapterStore,
			final AdapterIndexMappingStore adapterIndexMappingStore ) {
		// loop through all adapters and find row merging adapters
		final Map<ByteArrayId, RowMergingDataAdapter> map = new HashMap<>();
		final List<String> columnFamilies = new ArrayList<>();
		try (CloseableIterator<DataAdapter<?>> it = adapterStore.getAdapters()) {
			while (it.hasNext()) {
				final DataAdapter a = it.next();
				if (a instanceof RowMergingDataAdapter) {
					if (adapterIndexMappingStore.getIndicesForAdapter(
							a.getAdapterId()).contains(
							index.getId())) {
						map.put(
								a.getAdapterId(),
								(RowMergingDataAdapter) a);
						columnFamilies.add(a.getAdapterId().getString());
					}
				}
			}
		}
		catch (final IOException e) {
			LOGGER.error(
					"Cannot lookup data adapters",
					e);
			return false;
		}
		if (columnFamilies.isEmpty()) {
			LOGGER.warn("There is no mergeable data found in datastore");
			return false;
		}
		final String table = index.getId().getString();
		try (HBaseWriter writer = createWriter(
				table,
				columnFamilies.toArray(new String[] {}),
				false)) {
			final Scan scanner = new Scan();
			for (final String cf : columnFamilies) {
				scanner.addFamily(new ByteArrayId(
						cf).getBytes());
			}
			final ResultScanner rs = getScannedResults(
					scanner,
					table);
			final RewritingMergingEntryIterator iterator = new RewritingMergingEntryIterator<>(
					adapterStore,
					index,
					rs.iterator(),
					map,
					writer);
			while (iterator.hasNext()) {
				iterator.next();
			}
			return true;
		}
		catch (final IOException e) {
			LOGGER.error(
					"Cannot create writer for table '" + index.getId().getString() + "'",
					e);
		}
		return false;
	}
}
