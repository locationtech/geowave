/**
 * 
 */
package mil.nga.giat.geowave.datastore.hbase.io;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.Logger;

/**
 * @author viggy Functionality similar to <code> BatchWriterWrapper </code>
 * 
 *         TODO #406 This class directly writes to the HBase table instead of
 *         using any existing Writer API provided by HBase.
 * 
 */
public class HBaseWriter
{

	private final static Logger LOGGER = Logger.getLogger(HBaseWriter.class);
	private Table table;
	private Admin admin;

	public HBaseWriter(
			Admin admin,
			Table table ) {
		this.admin = admin;
		this.table = table;
	}

	private void write(
			RowMutations rowMutation )
			throws IOException {
		table.mutateRow(rowMutation);
	}

	public void close() {}

	public void write(
			Iterable<RowMutations> iterable,
			String columnFamily )
			throws IOException {
		addColumnFamilyToTable(
				table.getName(),
				columnFamily);
		for (RowMutations rowMutation : iterable) {
			write(rowMutation);
		}
	}

	/*
	 * private boolean columnFamilyExists( String columnFamily ) throws
	 * IOException { for (HColumnDescriptor cDesc :
	 * table.getTableDescriptor().getColumnFamilies()) { if
	 * (cDesc.getNameAsString().matches( columnFamily)) return true; } return
	 * false; }
	 */

	public void write(
			RowMutations mutation,
			String columnFamily ) {
		try {
			addColumnFamilyToTable(
					table.getName(),
					columnFamily);
			write(mutation);
		}
		catch (IOException e) {
			LOGGER.warn(
					"Unable to add column family " + columnFamily,
					e);
		}
	}

	private void addColumnFamilyToTable(
			TableName name,
			String columnFamilyName )
			throws IOException {
		HColumnDescriptor cfDesciptor = new HColumnDescriptor(
				columnFamilyName);
		if (admin.tableExists(name)) {
			// Before any modification to table schema, it's necessary to
			// disable it
			if (!admin.isTableEnabled(name)) {
				admin.enableTable(name);
			}
			HTableDescriptor descriptor = admin.getTableDescriptor(name);
			boolean found = false;
			for (HColumnDescriptor hColumnDescriptor : descriptor.getColumnFamilies()) {
				if (hColumnDescriptor.getNameAsString().equalsIgnoreCase(
						columnFamilyName)) found = true;
			}
			if (!found) {
				if (admin.isTableEnabled(name)) {
					admin.disableTable(name);
				}
				admin.addColumn(
						name,
						cfDesciptor);
				// Enable table once done
				admin.enableTable(name);
			}
		}
		else {
			LOGGER.warn("Table " + name.getNameAsString() + " doesn't exist, so no question of adding column family " + columnFamilyName + " to it!");
		}
	}

	public void delete(
			Iterable<RowMutations> iterable )
			throws IOException {
		for (RowMutations rowMutation : iterable) {
			write(rowMutation);
		}
	}

	public void delete(
			Delete delete )
			throws IOException {
		table.delete(delete);
	}

}
