package mil.nga.giat.geowave.core.store.index;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.DataStoreOperations;
import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.base.Writer;
import mil.nga.giat.geowave.core.store.callback.IngestCallback;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;

/**
 * This class can write many entries for a single index by retaining a single
 * open writer. The first entry that is written will open a writer and it is the
 * responsibility of the caller to close this writer when complete.
 *
 */
public abstract class DataStoreIndexWriter<T, MutationType> implements
		IndexWriter<T>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(DataStoreIndexWriter.class);
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

	protected synchronized void closeInternal() {
		if (writer != null) {
			try {
				writer.close();
				writer = null;
			}
			catch (IOException e) {
				LOGGER.warn(
						"Unable to close writer",
						e);
			}
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
			final T entry )
			throws IOException {
		return write(
				entry,
				DataStoreUtils.UNCONSTRAINED_VISIBILITY);
	}

	@Override
	public List<ByteArrayId> write(
			final T entry,
			final VisibilityWriter<T> fieldVisibilityWriter )
			throws IOException {

		DataStoreEntryInfo entryInfo;
		synchronized (this) {

			ensureOpen();

			if (writer == null) {
				LOGGER.error("Null writer - empty list returned");
				return Collections.emptyList();
			}
			entryInfo = getEntryInfo(
					entry,
					fieldVisibilityWriter);
			if (entryInfo == null) {
				LOGGER.error("Null EntryInfo - empty list returned");
				return Collections.emptyList();
			}
			callback.entryIngested(
					entryInfo,
					entry);
		}

		return entryInfo.getRowIds();
	}

	protected abstract void ensureOpen()
			throws IOException;

	protected abstract DataStoreEntryInfo getEntryInfo(
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