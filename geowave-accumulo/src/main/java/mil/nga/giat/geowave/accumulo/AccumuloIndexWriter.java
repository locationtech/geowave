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
	private Writer altIdxWriter;

	private boolean useAltIndex;
	private String tableName;
	private String altIdxTableName;

	public AccumuloIndexWriter(
			final Index index,
			final AccumuloOperations accumuloOperations,
			final AccumuloDataStore dataStore ) {
		this(
				index,
				accumuloOperations,
				new AccumuloOptions(),
				dataStore);
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
		initialize();
	}

	private void initialize() {
		tableName = StringUtils.stringFromBinary(index.getId().getBytes());
		altIdxTableName = tableName + AccumuloUtils.ALT_INDEX_TABLE;

		useAltIndex = accumuloOptions.isUseAltIndex();

		if (useAltIndex) {
			if (accumuloOperations.tableExists(tableName)) {
				if (!accumuloOperations.tableExists(altIdxTableName)) {
					useAltIndex = false;
					LOGGER.warn("Requested alternate index table [" + altIdxTableName + "] does not exist.");
				}
			}
			else {
				if (accumuloOperations.tableExists(altIdxTableName)) {
					accumuloOperations.deleteTable(altIdxTableName);
					LOGGER.warn("Deleting current alternate index table [" + altIdxTableName + "] as main table does not yet exist.");
				}
			}
		}
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
		if (useAltIndex && (altIdxWriter == null)) {
			try {
				altIdxWriter = accumuloOperations.createWriter(
						altIdxTableName,
						accumuloOptions.isCreateIndex());
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
		if (useAltIndex && (altIdxWriter != null)) {
			altIdxWriter.close();
			writer = null;
		}
	}

	@Override
	public <T> List<ByteArrayId> write(
			final WritableDataAdapter<T> writableAdapter,
			final T entry ) {

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
					"Unable to determine existence of locality group [" + writableAdapter.getAdapterId().getString() + "]",
					e);
		}

		List<ByteArrayId> rowIds;
		synchronized (this) {
			ensureOpen();
			rowIds = AccumuloUtils.write(
					writableAdapter,
					index,
					entry,
					writer);

			if (useAltIndex) {
				AccumuloUtils.writeAltIndex(
						writableAdapter,
						rowIds,
						entry,
						altIdxWriter);
			}
		}
		return rowIds;
	}

	@Override
	public void close() {
		// thread safe close
		closeInternal();
	}

}
