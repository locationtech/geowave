/**
 *
 */
package mil.nga.giat.geowave.datastore.hbase;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.callback.IngestCallback;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.index.DataStoreIndexWriter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.datastore.hbase.io.HBaseWriter;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;
import mil.nga.giat.geowave.datastore.hbase.operations.config.HBaseOptions;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseUtils;

public class HBaseIndexWriter<T> extends
		DataStoreIndexWriter<T, RowMutations>
{

	private final static Logger LOGGER = Logger.getLogger(HBaseIndexWriter.class);
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

	protected synchronized void ensureOpen() {
		if (writer == null) {
			try {
				writer = operations.createWriter(
						StringUtils.stringFromBinary(index.getId().getBytes()),
						new String[] {
							adapter.getAdapterId().getString()
						},
						options.isCreateTable(),
						index.getIndexStrategy().getNaturalSplits());
			}
			catch (final IOException e) {
				LOGGER.error(
						"Unable to open writer",
						e);
			}
		}
	}

}