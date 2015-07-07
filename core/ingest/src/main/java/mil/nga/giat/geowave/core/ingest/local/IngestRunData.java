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
import mil.nga.giat.geowave.core.store.adapter.MemoryAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.index.Index;

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

	public IngestRunData(
			final List<WritableDataAdapter<?>> adapters,
			final DataStore dataStore ) {
		this.dataStore = dataStore;
		adapterCache = new MemoryAdapterStore(
				adapters.toArray(new WritableDataAdapter[adapters.size()]));
	}

	public WritableDataAdapter<?> getDataAdapter(
			final GeoWaveData<?> data ) {
		return data.getAdapter(adapterCache);
	}

	public synchronized IndexWriter getIndexWriter(
			final Index index ) {
		IndexWriter indexWriter = indexIdToWriterCache.get(index.getId());
		if (indexWriter == null) {
			indexWriter = dataStore.createIndexWriter(index);
			indexIdToWriterCache.put(
					index.getId(),
					indexWriter);
		}
		return indexWriter;
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
