package mil.nga.giat.geowave.accumulo;

import java.util.List;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.StringUtils;
import mil.nga.giat.geowave.store.IndexWriter;
import mil.nga.giat.geowave.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.store.index.Index;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.log4j.Logger;

/**
 * This class can write many entries for a single index by retaining a single
 * open writer. The first entry that is written will open a writer and it is the
 * responsibility of the caller to close this writer when complete.
 * 
 */
public class AccumuloIndexWriter implements
		IndexWriter
{
	private final static Logger LOGGER = Logger.getLogger(AccumuloIndexWriter.class);
	private final Index index;
	private final AccumuloOperations accumuloOperations;
	private final AccumuloOptions accumuloOptions;
	protected final AccumuloDataStore dataStore;
	private Writer writer;

	public AccumuloIndexWriter(
			final Index index,
			final AccumuloOperations accumuloOperations,
			final AccumuloDataStore dataStore ) {
		this.index = index;
		this.accumuloOperations = accumuloOperations;
		this.dataStore = dataStore;
		this.accumuloOptions = new AccumuloOptions();
	}

	public AccumuloIndexWriter(
			final Index index,
			final AccumuloOperations accumuloOperations,
			final AccumuloOptions accumuloOptions,
			final AccumuloDataStore dataStore ) {
		this.index = index;
		this.accumuloOperations = accumuloOperations;
		this.accumuloOptions = accumuloOptions;
		this.dataStore = dataStore;
	}

	private synchronized void ensureOpen() {
		if (writer == null) {
			try {
				writer = accumuloOperations.createWriter(StringUtils.stringFromBinary(index.getId().getBytes()));
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
	public <T> List<ByteArrayId> write(
			final WritableDataAdapter<T> writableAdapter,
			final T entry ) {

		final String tableName = StringUtils.stringFromBinary(index.getId().getBytes());
		final byte[] adapterId = writableAdapter.getAdapterId().getBytes();

		dataStore.store(writableAdapter);
		dataStore.store(index);

		try {
			if (accumuloOptions.isUseLocalityGroups() && !accumuloOperations.localityGroupExists(
					tableName,
					adapterId)) {
				accumuloOperations.addLocalityGroup(
						tableName,
						adapterId);
			}
		}
		catch (AccumuloException | TableNotFoundException | AccumuloSecurityException e) {
			LOGGER.error(
					"Unable to determine existence of locality group [" + writableAdapter.getAdapterId() + "]",
					e);
		}

		synchronized (this) {
			ensureOpen();
			return AccumuloUtils.write(
					writableAdapter,
					index,
					entry,
					writer);
		}
	}

	@Override
	public void close() {
		// thread safe close
		closeInternal();
	}

}
