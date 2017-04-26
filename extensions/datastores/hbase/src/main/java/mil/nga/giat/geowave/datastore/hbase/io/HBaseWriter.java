package mil.nga.giat.geowave.datastore.hbase.io;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.base.Writer;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.BufferedMutator.ExceptionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Functionality similar to <code> BatchWriterWrapper </code>
 *
 * This class directly writes to the HBase table instead of using any existing
 * Writer API provided by HBase.
 *
 */
public class HBaseWriter implements
		Writer<RowMutations>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(HBaseWriter.class);
	private final TableName tableName;
	private final Admin admin;
	private static final long SLEEP_INTERVAL_FOR_CF_VERIFY = 100L;

	private final HashMap<String, Boolean> cfMap;
	private BufferedMutator mutator;

	private final boolean schemaUpdateEnabled;

	public HBaseWriter(
			final Admin admin,
			final String tableName ) {
		this.admin = admin;
		this.tableName = TableName.valueOf(tableName);

		cfMap = new HashMap<String, Boolean>();

		schemaUpdateEnabled = admin.getConfiguration().getBoolean(
				"hbase.online.schema.update.enable",
				false);

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug("Schema Update Enabled = " + schemaUpdateEnabled);

			final String check = admin.getConfiguration().get(
					"hbase.online.schema.update.enable");
			if (check == null) {
				LOGGER.debug("'hbase.online.schema.update.enable' property should be true for best performance");
			}
		}
	}

	public BufferedMutator getBufferedMutator()
			throws IOException {
		if (mutator == null) {
			final BufferedMutatorParams params = new BufferedMutatorParams(
					tableName);

			params.listener(new ExceptionListener() {
				@Override
				public void onException(
						final RetriesExhaustedWithDetailsException exception,
						final BufferedMutator mutator )
						throws RetriesExhaustedWithDetailsException {
					LOGGER.error(
							"Error in buffered mutator",
							exception);
					// Get details
					for (Throwable cause : exception.getCauses()) {
						cause.printStackTrace();
					}
				}
			});
			mutator = admin.getConnection().getBufferedMutator(
					params);

		}
		return mutator;
	}

	@Override
	public void write(
			final RowMutations rowMutation ) {
		try {
			getBufferedMutator().mutate(
					rowMutation.getMutations());
		}
		catch (final IOException e) {
			LOGGER.error(
					"Unable to write mutation.",
					e);
		}
	}

	@Override
	public void write(
			final Iterable<RowMutations> mutations ) {
		for (final RowMutations rowMutation : mutations) {
			write(rowMutation);
		}
	}

	@Override
	public void close() {
		try {
			if (mutator != null) {
				mutator.close();
				mutator = null;
			}
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to close BufferedMutator",
					e);
		}
	}

	public void write(
			final Iterable<RowMutations> iterable,
			final String columnFamily )
			throws IOException {
		addColumnFamilyIfNotExist(columnFamily);

		for (final RowMutations rowMutation : iterable) {
			write(rowMutation);
		}
	}

	public void write(
			final List<Put> puts,
			final String columnFamily )
			throws IOException {
		addColumnFamilyIfNotExist(columnFamily);

		getBufferedMutator().mutate(
				puts);
	}

	private void addColumnFamilyIfNotExist(
			final String columnFamily )
			throws IOException {
		synchronized (BasicHBaseOperations.ADMIN_MUTEX) {
			if (!columnFamilyExists(columnFamily)) {
				addColumnFamilyToTable(
						tableName,
						columnFamily);
			}
		}
	}

	public void write(
			final RowMutations mutation,
			final String columnFamily ) {
		try {
			addColumnFamilyIfNotExist(columnFamily);
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to add column family " + columnFamily,
					e);
		}

		write(mutation);
	}

	private boolean columnFamilyExists(
			final String columnFamily ) {
		Boolean found = false;

		try {
			found = cfMap.get(columnFamily);

			if (found == null) {
				found = Boolean.FALSE;
			}

			if (!found) {
				if (!schemaUpdateEnabled && !admin.isTableEnabled(tableName)) {
					admin.enableTable(tableName);
				}

				// update the table descriptor
				HTableDescriptor tableDescriptor = admin.getTableDescriptor(tableName);

				found = tableDescriptor.hasFamily(columnFamily.getBytes(StringUtils.UTF8_CHAR_SET));

				cfMap.put(
						columnFamily,
						found);
			}
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to check existence of column family " + columnFamily,
					e);
		}

		return found;
	}

	private void addColumnFamilyToTable(
			final TableName tableName,
			final String columnFamilyName )
			throws IOException {
		LOGGER.debug("Creating column family: " + columnFamilyName);

		final HColumnDescriptor cfDescriptor = new HColumnDescriptor(
				columnFamilyName);

		if (!schemaUpdateEnabled && !admin.isTableDisabled(tableName)) {
			admin.disableTable(tableName);
		}

		// Try adding column family to the table descriptor instead
		admin.addColumn(
				tableName,
				cfDescriptor);

		if (schemaUpdateEnabled) {
			do {
				try {
					Thread.sleep(SLEEP_INTERVAL_FOR_CF_VERIFY);
				}
				catch (final InterruptedException e) {
					LOGGER.warn(
							"Sleeping while column family added interrupted",
							e);
				}
			}
			while (admin.getAlterStatus(
					tableName).getFirst() > 0);
		}
		cfMap.put(
				columnFamilyName,
				Boolean.TRUE);

		// Enable table once done
		if (!schemaUpdateEnabled) {
			admin.enableTable(tableName);
		}
	}

	public void delete(
			final Iterable<RowMutations> iterable )
			throws IOException {
		for (final RowMutations rowMutation : iterable) {
			write(rowMutation);
		}
	}

	public void delete(
			final Delete delete )
			throws IOException {
		getBufferedMutator().mutate(
				delete);
	}

	public void delete(
			final List<Delete> deletes )
			throws IOException {
		getBufferedMutator().mutate(
				deletes);
	}

	@Override
	public void flush() {
		try {
			if (mutator != null) {
				mutator.flush();
			}
		}
		catch (final IOException e) {
			LOGGER.warn(
					"Unable to flush BufferedMutator",
					e);
		}
	}

}
