package mil.nga.giat.geowave.core.store.index;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.DataStoreOperations;
import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.IngestCallback;
import mil.nga.giat.geowave.core.store.Writer;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.memory.DataStoreUtils;

/**
 * This class can write many entries for a single index by retaining a single
 * open writer. The first entry that is written will open a writer and it is the
 * responsibility of the caller to close this writer when complete.
 *
 */
public abstract class DataStoreIndexWriter<T, MutationType> implements
		IndexWriter<T>
{
	private final static Logger LOGGER = Logger.getLogger(DataStoreIndexWriter.class);
	protected final PrimaryIndex index;
	protected final DataStoreOperations operations;
	protected final DataStoreOptions options;
	protected final IngestCallback<T> callback;
	protected Writer<MutationType> writer;

	protected final DataAdapter<T> adapter;
	protected final byte[] adapterId;
	final Closeable closable;

	public DataStoreIndexWriter(
			final DataAdapter<T> adapter,
			final PrimaryIndex index,
			final DataStoreOperations operations,
			final DataStoreOptions options,
			final IngestCallback<T> callback,
			final Closeable closable ) {
		this.index = index;
		this.operations = operations;
		this.options = options;
		this.callback = callback;
		this.adapter = adapter;
		this.adapterId = adapter.getAdapterId().getBytes();
		this.closable = closable;
	}

	private synchronized void closeInternal() {
		if (writer != null) {
			writer.close();
			writer = null;
		}
	}

	@Override
	public PrimaryIndex[] getIndices() {
		return new PrimaryIndex[] {
			index
		};
	}

	@Override
	public List<ByteArrayId> write(
			final T entry ) {
		return write(
				entry,
				DataStoreUtils.UNCONSTRAINED_VISIBILITY);
	}

	@Override
	public List<ByteArrayId> write(
			final T entry,
			final VisibilityWriter<T> feldVisibilityWriter ) {
		return writeInternal(
				entry,
				feldVisibilityWriter);
	}

	protected abstract List<ByteArrayId> writeInternal(
			final T entry,
			final VisibilityWriter<T> visibilityWriter );

	@Override
	public void close() {
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

	@Override
	public synchronized void flush() {
		// thread safe flush of the writers
		if (writer != null) {
			writer.flush();
		}
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
}
