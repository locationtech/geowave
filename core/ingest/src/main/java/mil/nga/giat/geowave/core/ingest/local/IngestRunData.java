package mil.nga.giat.geowave.core.ingest.local;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.ingest.GeoWaveData;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.memory.DataStoreUtils;
import mil.nga.giat.geowave.core.store.memory.MemoryAdapterStore;

/**
 * A class to hold intermediate run data that must be used throughout the life
 * of an ingest process.
 */
public class IngestRunData implements
		Closeable
{
	private final Map<ByteArrayId, IndexWriter> indexIdToWriterCache = new HashMap<ByteArrayId, IndexWriter>();
	private final AdapterStore adapterCache;
	private final DataStore dataStore;
	private final String[] args;

	public IngestRunData(
			final List<WritableDataAdapter<?>> adapters,
			final DataStore dataStore,
			final String[] args ) {
		this.dataStore = dataStore;
		adapterCache = new MemoryAdapterStore(
				adapters.toArray(new WritableDataAdapter[adapters.size()]));
		this.args = args;
	}

	public WritableDataAdapter<?> getDataAdapter(
			final GeoWaveData<?> data ) {
		return data.getAdapter(adapterCache);
	}

	public synchronized IndexWriter getIndexWriter(
			final PrimaryIndex index ) {
		IndexWriter indexWriter = indexIdToWriterCache.get(index.getId());
		if (indexWriter == null) {
			indexWriter = dataStore.createIndexWriter(
					index,
					DataStoreUtils.DEFAULT_VISIBILITY);
			indexIdToWriterCache.put(
					index.getId(),
					indexWriter);
		}
		return indexWriter;
	}

	public String[] getArgs() {
		return args;
	}

	@Override
	public void close()
			throws IOException {
		synchronized (this) {
			for (final IndexWriter indexWriter : indexIdToWriterCache.values()) {
				indexWriter.close();
			}
			indexIdToWriterCache.clear();
		}
	}

	public void flush() {
		synchronized (this) {
			for (final IndexWriter indexWriter : indexIdToWriterCache.values()) {
				indexWriter.flush();
			}
		}
	}

}
