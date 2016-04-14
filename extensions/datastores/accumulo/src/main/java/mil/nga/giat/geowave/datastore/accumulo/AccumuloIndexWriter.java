package mil.nga.giat.geowave.datastore.accumulo;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.client.TableNotFoundException;
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
import mil.nga.giat.geowave.datastore.accumulo.operations.config.AccumuloOptions;
import mil.nga.giat.geowave.datastore.accumulo.util.AccumuloUtils;

/**
 * This class can write many entries for a single index by retaining a single
 * open writer. The first entry that is written will open a writer and it is the
 * responsibility of the caller to close this writer when complete.
 * 
 */
public class AccumuloIndexWriter<T> implements
		IndexWriter<T>
{
	private final static Logger LOGGER = Logger.getLogger(AccumuloIndexWriter.class);
	protected final PrimaryIndex index;
	protected final AccumuloOperations accumuloOperations;
	protected final AccumuloOptions accumuloOptions;
	protected final AccumuloDataStore dataStore;
	protected final IngestCallback<T> callback;
	protected Writer writer;

	protected final DataAdapter<T> adapter;
	protected final byte[] adapterId;
	final Closeable closable;

	// just need a reasonable threshold.

	protected final VisibilityWriter<?> customFieldVisibilityWriter;

	public AccumuloIndexWriter(
			final DataAdapter<T> adapter,
			final PrimaryIndex index,
			final AccumuloOperations accumuloOperations,
			final AccumuloOptions accumuloOptions,
			final AccumuloDataStore dataStore,
			final IngestCallback<T> callback,
			final Closeable closable,
			final VisibilityWriter<T> customFieldVisibilityWriter ) {
		this.index = index;
		this.accumuloOperations = accumuloOperations;
		this.accumuloOptions = accumuloOptions;
		this.dataStore = dataStore;
		this.customFieldVisibilityWriter = customFieldVisibilityWriter;
		this.callback = callback;
		this.adapter = adapter;
		this.adapterId = adapter.getAdapterId().getBytes();
		this.closable = closable;
	}

	private synchronized void ensureOpen() {
		if (writer == null) {
			try {
				writer = accumuloOperations.createWriter(
						StringUtils.stringFromBinary(index.getId().getBytes()),
						accumuloOptions.isCreateTable(),
						true,
						accumuloOptions.isEnableBlockCache(),
						index.getIndexStrategy().getNaturalSplits());
			}
			catch (final TableNotFoundException e) {
				LOGGER.error(
						"Unable to open writer",
						e);
			}
		}
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

	public List<ByteArrayId> writeInternal(
			final T entry,
			final VisibilityWriter<T> visibilityWriter ) {

		DataStoreEntryInfo entryInfo;
		synchronized (this) {

			ensureOpen();
			if (writer == null) {
				return Collections.emptyList();
			}
			entryInfo = AccumuloUtils.write(
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
			catch (IOException e) {
				LOGGER.error(
						"Cannot flush callbacks",
						e);
			}
		}
	}
}
