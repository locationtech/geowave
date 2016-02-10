/**
 *
 */
package mil.nga.giat.geowave.datastore.hbase.operations;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.store.config.AbstractConfigOption;
import mil.nga.giat.geowave.core.store.config.StringConfigOption;
import mil.nga.giat.geowave.datastore.hbase.io.HBaseWriter;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseUtils;

/**
 * @author viggy Functionality similar to <code> BasicAccumuloOperations </code>
 *         . It is currently not extending any interface like AccumuloOperations
 *         to avoid replication.
 */
public class BasicHBaseOperations
{

	private final static Logger LOGGER = Logger.getLogger(BasicHBaseOperations.class);
	private static final String HBASE_CONFIGURATION_TIMEOUT = "timeout";
	private static final String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
	private static final String DEFAULT_TABLE_NAMESPACE = "";

	public static final String ZOOKEEPER_INSTANCES_NAME = "zookeeper";
	private static final AbstractConfigOption<?>[] CONFIG_OPTIONS = new AbstractConfigOption[] {
		new StringConfigOption(
				ZOOKEEPER_INSTANCES_NAME,
				"A comma-separated list of zookeeper servers that an HBase instance is using")
	};

	private final Connection conn;
	private final String tableNamespace;

	public BasicHBaseOperations(
			final String zookeeperInstances,
			final String geowaveNamespace )
			throws IOException {
		final Configuration hConf = HBaseConfiguration.create();
		hConf.set(
				HBASE_CONFIGURATION_ZOOKEEPER_QUORUM,
				zookeeperInstances);
		hConf.setInt(
				HBASE_CONFIGURATION_TIMEOUT,
				120000);
		conn = ConnectionFactory.createConnection(hConf);
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
			final Map<String, Object> configOptions,
			final String namespace ) 
			throws IOException {
		return new BasicHBaseOperations(
				configOptions.get(
						ZOOKEEPER_INSTANCES_NAME).toString(),
				namespace);
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
		if (create && !conn.getAdmin().isTableAvailable(
				name)) {
			final HTableDescriptor desc = new HTableDescriptor(
					name);
			desc.addFamily(new HColumnDescriptor(
					columnFamily));
			conn.getAdmin().createTable(
					desc);
		}
		table = conn.getTable(name);
		return table;
	}

	private String getQualifiedTableName(
			final String unqualifiedTableName ) {
		return HBaseUtils.getQualifiedTableName(
				tableNamespace,
				unqualifiedTableName);
	}

	public void deleteAll()
			throws IOException {
		final TableName[] tableNamesArr = conn.getAdmin().listTableNames();
		final SortedSet<TableName> tableNames = new TreeSet<TableName>();
		Collections.addAll(
				tableNames,
				tableNamesArr);
		for (final TableName tableName : tableNames) {
			if (conn.getAdmin().isTableAvailable(
					tableName)) {
				conn.getAdmin().disableTable(
						tableName);
				conn.getAdmin().deleteTable(
						tableName);
			}
		}
	}

	public boolean tableExists(
			final String tableName )
			throws IOException {
		final String qName = getQualifiedTableName(tableName);
		return conn.getAdmin().isTableAvailable(
				getTableName(qName));

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

	public static AbstractConfigOption<?>[] getOptions() {
		return CONFIG_OPTIONS;
	}

}
