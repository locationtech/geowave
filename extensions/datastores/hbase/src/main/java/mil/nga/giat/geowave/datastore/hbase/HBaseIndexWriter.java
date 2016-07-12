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
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.IngestCallback;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
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
	public List<ByteArrayId> writeInternal(
			final T entry,
			final VisibilityWriter<T> visibilityWriter ) {

		DataStoreEntryInfo entryInfo;
		synchronized (this) {

			ensureOpen();
			if (writer == null) {
				return Collections.emptyList();
			}
			entryInfo = HBaseUtils.write(
					(WritableDataAdapter<T>) adapter,
					index,
					entry,
					(HBaseWriter) writer,
					visibilityWriter);

			callback.entryIngested(
					entryInfo,
					entry);
		}
		return entryInfo.getRowIds();
	}

	@Override
	protected synchronized void closeInternal() {
		if (writer != null) {
			writer.close();
			writer = null;
		}
	}

	private synchronized void ensureOpen() {
		if (writer == null) {
			try {
				writer = operations.createWriter(
						StringUtils.stringFromBinary(index.getId().getBytes()),
						adapter.getAdapterId().getString(),
						options.isCreateTable());
			}
			catch (final IOException e) {
				LOGGER.error(
						"Unable to open writer",
						e);
			}
		}
	}

}
