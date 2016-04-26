package mil.nga.giat.geowave.datastore.hbase.operations;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.datastore.hbase.io.HBaseWriter;
import mil.nga.giat.geowave.datastore.hbase.operations.config.HBaseRequiredOptions;
import mil.nga.giat.geowave.datastore.hbase.util.ConnectionPool;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseUtils;

public class BasicHBaseOperations
{

	private final static Logger LOGGER = Logger.getLogger(BasicHBaseOperations.class);
	private static final String DEFAULT_TABLE_NAMESPACE = "";
	public static final Object ADMIN_MUTEX = new Object();

	private final Connection conn;
	private final String tableNamespace;

	public BasicHBaseOperations(
			final String zookeeperInstances,
			final String geowaveNamespace )
			throws IOException {
		conn = ConnectionPool.getInstance().getConnection(
				zookeeperInstances);
		tableNamespace = geowaveNamespace;
	}

	public BasicHBaseOperations(
			final String zookeeperInstances )
			throws IOException {
		this(
				zookeeperInstances,
				DEFAULT_TABLE_NAMESPACE);
	}

	public BasicHBaseOperations(
			final Connection connector ) {
		this(
				DEFAULT_TABLE_NAMESPACE,
				connector);
	}

	public BasicHBaseOperations(
			final String tableNamespace,
			final Connection connector ) {
		this.tableNamespace = tableNamespace;
		conn = connector;
	}

	public static BasicHBaseOperations createOperations(
			final HBaseRequiredOptions options )
			throws IOException {
		return new BasicHBaseOperations(
				options.getZookeeper(),
				options.getGeowaveNamespace());
	}

	public HBaseWriter createWriter(
			final String tableName,
			final String columnFamily )
			throws IOException {
		return createWriter(
				tableName,
				columnFamily,
				true);
	}

	private TableName getTableName(
			final String tableName ) {
		return TableName.valueOf(tableName);
	}

	public HBaseWriter createWriter(
			final String sTableName,
			final String columnFamily,
			final boolean createTable )
			throws IOException {
		final TableName tName = getTableName(getQualifiedTableName(sTableName));
		Table table = null;
		table = getTable(
				createTable,
				columnFamily,
				tName);
		return new HBaseWriter(
				conn.getAdmin(),
				table);
	}

	/*
	 * private Table getTable( final boolean create, TableName name ) throws
	 * IOException { return getTable( create, DEFAULT_COLUMN_FAMILY, name); }
	 */

	private Table getTable(
			final boolean create,
			final String columnFamily,
			final TableName name )
			throws IOException {
		Table table;
		synchronized (ADMIN_MUTEX) {
			if (create && !conn.getAdmin().isTableAvailable(
					name)) {
				final HTableDescriptor desc = new HTableDescriptor(
						name);
				desc.addFamily(new HColumnDescriptor(
						columnFamily));
				conn.getAdmin().createTable(
						desc);
			}
		}
		table = conn.getTable(name);
		return table;
	}

	public String getQualifiedTableName(
			final String unqualifiedTableName ) {
		return HBaseUtils.getQualifiedTableName(
				tableNamespace,
				unqualifiedTableName);
	}

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

	public boolean tableExists(
			final String tableName )
			throws IOException {
		final String qName = getQualifiedTableName(tableName);
		synchronized (ADMIN_MUTEX) {
			return conn.getAdmin().isTableAvailable(
					getTableName(qName));
		}

	}

	public ResultScanner getScannedResults(
			final Scan scanner,
			final String tableName )
			throws IOException {
		return conn.getTable(
				getTableName(getQualifiedTableName(tableName))).getScanner(
				scanner);
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
}
