/**
 *
 */
package mil.nga.giat.geowave.datastore.hbase;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.IngestCallback;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.datastore.hbase.io.HBaseWriter;
import mil.nga.giat.geowave.datastore.hbase.operations.BasicHBaseOperations;
import mil.nga.giat.geowave.datastore.hbase.util.HBaseUtils;

public class HBaseIndexWriter<T> implements
		IndexWriter<T>
{

	private final static Logger LOGGER = Logger.getLogger(HBaseIndexWriter.class);
	private final PrimaryIndex index;
	private final BasicHBaseOperations operations;
	protected boolean persistStats;
	protected final IngestCallback<T> callback;
	protected HBaseWriter writer;
	protected final HBaseOptions options;

	protected final DataAdapter<T> adapter;
	protected final byte[] adapterId;
	final Closeable closable;

	protected final VisibilityWriter<?> customFieldVisibilityWriter;

	public HBaseIndexWriter(
			final DataAdapter<T> adapter,
			final PrimaryIndex index,
			final BasicHBaseOperations operations,
			final IngestCallback<T> callback,
			final Closeable closable,
			final VisibilityWriter<?> customFieldVisibilityWriter ) {
		this(
				adapter,
				index,
				operations,
				new HBaseOptions(),
				callback,
				closable,
				customFieldVisibilityWriter);
	}

	public HBaseIndexWriter(
			final DataAdapter<T> adapter,
			final PrimaryIndex index,
			final BasicHBaseOperations operations,
			final HBaseOptions options,
			final IngestCallback<T> callback,
			final Closeable closable,
			final VisibilityWriter<?> customFieldVisibilityWriter ) {
		this.adapter = adapter;
		this.index = index;
		this.operations = operations;
		this.options = options;
		this.closable = closable;
		this.callback = callback;
		this.adapterId = adapter.getAdapterId().getBytes();
		this.customFieldVisibilityWriter = customFieldVisibilityWriter;
	}

	@Override
	public void close()
			throws IOException {
		try {
			closable.close();
		}
		catch (final IOException e) {
			LOGGER.error(
					"Cannot close callbacks",
					e);
		}
		// thread safe close
		closeInternal();
	}

	private synchronized void closeInternal() {
		if (writer != null) {
			writer.close();
			writer = null;
		}
	}

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
					writer,
					visibilityWriter);

			callback.entryIngested(
					entryInfo,
					entry);
		}
		return entryInfo.getRowIds();
	}

	private synchronized void ensureOpen() {
		if (writer == null) {
			try {
				writer = operations.createWriter(
						StringUtils.stringFromBinary(index.getId().getBytes()),
						adapter.getAdapterId().getString(),
						options.createTable);
			}
			catch (final IOException e) {
				LOGGER.error(
						"Unable to open writer",
						e);
			}
		}
	}

	@Override
	public synchronized void flush() {
		// HBase writer does not require/support flush
		if (this.callback instanceof Flushable) {
			try {
				((Flushable) callback).flush();
			}
			catch (final IOException e) {
				LOGGER.error(
						"Cannot flush callbacks",
						e);
			}
		}
	}

	@Override
	public List<ByteArrayId> write(
			final T entry ) {
		return write(
				entry,
				(VisibilityWriter<T>) customFieldVisibilityWriter);
	}

	@Override
	public List<ByteArrayId> write(
			final T entry,
			final VisibilityWriter<T> feldVisibilityWriter ) {
		return writeInternal(
				entry,
				feldVisibilityWriter);
	}

	@Override
	public PrimaryIndex[] getIndices() {
		return new PrimaryIndex[] {
			index
		};
	}

}
