package mil.nga.giat.geowave.datastore.accumulo;

import java.io.Closeable;
import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
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
import mil.nga.giat.geowave.datastore.accumulo.operations.config.AccumuloOptions;
import mil.nga.giat.geowave.datastore.accumulo.util.AccumuloUtils;

/**
 * This class can write many entries for a single index by retaining a single
 * open writer. The first entry that is written will open a writer and it is the
 * responsibility of the caller to close this writer when complete.
 *
 */
public class AccumuloIndexWriter<T> extends
		DataStoreIndexWriter<T, Mutation>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(AccumuloIndexWriter.class);
	protected final AccumuloOperations accumuloOperations;
	protected final AccumuloOptions accumuloOptions;

	public AccumuloIndexWriter(
			final DataAdapter<T> adapter,
			final PrimaryIndex index,
			final AccumuloOperations accumuloOperations,
			final AccumuloOptions accumuloOptions,
			final IngestCallback<T> callback,
			final Closeable closable ) {
		super(
				adapter,
				index,
				accumuloOperations,
				accumuloOptions,
				callback,
				closable);
		this.accumuloOperations = accumuloOperations;
		this.accumuloOptions = accumuloOptions;
	}

	protected synchronized void ensureOpen() {
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

	@Override
	protected DataStoreEntryInfo getEntryInfo(
			final T entry,
			final VisibilityWriter<T> visibilityWriter ) {
		return AccumuloUtils.write(
				(WritableDataAdapter<T>) adapter,
				index,
				entry,
				writer,
				accumuloOperations,
				visibilityWriter);
	}
}