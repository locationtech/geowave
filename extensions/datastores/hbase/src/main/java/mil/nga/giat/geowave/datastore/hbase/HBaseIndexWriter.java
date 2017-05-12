/**
 *
 */
package mil.nga.giat.geowave.datastore.hbase;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RowMutations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.callback.IngestCallback;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.index.DataStoreIndexWriter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataAdapter;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexUtils;
import mil.nga.giat.geowave.datastore.hbase.io.HBaseWriter;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;
import mil.nga.giat.geowave.datastore.hbase.operations.config.HBaseOptions;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseUtils;

public class HBaseIndexWriter<T> extends
		DataStoreIndexWriter<T, RowMutations>
{

	private final static Logger LOGGER = LoggerFactory.getLogger(HBaseIndexWriter.class);
	private final BasicHBaseOperations operations;
	protected final HBaseOptions options;

	public HBaseIndexWriter(
			final DataAdapter<T> adapter,
			final PrimaryIndex index,
			final BasicHBaseOperations operations,
			final HBaseOptions options,
			final IngestCallback<T> callback,
			final Closeable closable ) {
		super(
				adapter,
				index,
				operations,
				options,
				callback,
				closable);
		this.operations = operations;
		this.options = options;
		initializeSecondaryIndexTables();
	}

	@Override
	protected DataStoreEntryInfo getEntryInfo(
			T entry,
			VisibilityWriter<T> visibilityWriter ) {
		return HBaseUtils.write(
				(WritableDataAdapter<T>) adapter,
				index,
				entry,
				(HBaseWriter) writer,
				visibilityWriter);
	}

	@Override
	protected synchronized void closeInternal() {
		if (writer != null) {
			try {
				writer.close();
				writer = null;
			}
			catch (IOException e) {
				LOGGER.warn(
						"Unable to close HBase writer",
						e);
			}
		}
	}

	protected synchronized void ensureOpen()
			throws IOException {
		if (writer == null) {
			try {
				writer = operations.createWriter(
						StringUtils.stringFromBinary(index.getId().getBytes()),
						new String[] {
							adapter.getAdapterId().getString()
						},
						options.isCreateTable(),
						index.getIndexStrategy().getNaturalSplits());

				if (writer == null) {
					throw new IOException(
							"Create writer failed without an exception");
				}
			}
			catch (final IOException e) {
				LOGGER.error(
						"Unable to open writer",
						e);
				throw (e);
			}
		}
	}

	private void initializeSecondaryIndexTables() {
		if (adapter instanceof SecondaryIndexDataAdapter<?>) {
			final Map<String, List<ByteArrayId>> tableToFieldMap = new HashMap<>();
			final List<SecondaryIndex<T>> secondaryIndices = ((SecondaryIndexDataAdapter<T>) adapter)
					.getSupportedSecondaryIndices();
			// aggregate fields for each unique table
			for (final SecondaryIndex<T> secondaryIndex : secondaryIndices) {
				final String tableName = operations.getQualifiedTableName(secondaryIndex.getId().getString());
				if (tableToFieldMap.containsKey(tableName)) {
					final List<ByteArrayId> fieldIds = tableToFieldMap.get(tableName);
					fieldIds.add(secondaryIndex.getFieldId());
				}
				else {
					final List<ByteArrayId> fieldIds = new ArrayList<>();
					fieldIds.add(secondaryIndex.getFieldId());
					tableToFieldMap.put(
							tableName,
							fieldIds);
				}
			}
			// ensure each table is configured for the appropriate column
			// families
			for (final Entry<String, List<ByteArrayId>> entry : tableToFieldMap.entrySet()) {
				final String table = entry.getKey();
				final List<ByteArrayId> fieldIds = entry.getValue();
				final String[] columnFamilies = new String[fieldIds.size()];
				int idx = 0;
				for (final ByteArrayId fieldId : fieldIds) {
					final byte[] cfBytes = SecondaryIndexUtils.constructColumnFamily(
							adapter.getAdapterId(),
							fieldId);
					columnFamilies[idx++] = new ByteArrayId(
							cfBytes).getString();
				}
				try {
					if (operations.tableExists(table)) {
						operations.addColumnFamiles(
								columnFamilies,
								table);
					}
					else {
						operations.createTable(
								columnFamilies,
								TableName.valueOf(table),
								null);
					}
				}
				catch (final IOException e) {
					LOGGER.error(
							"Unable to check if table " + table + " exists",
							e);
				}
			}
		}
	}

}