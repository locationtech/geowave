package mil.nga.giat.geowave.ingest;

import java.util.List;

import mil.nga.giat.geowave.store.IndexWriter;
import mil.nga.giat.geowave.store.adapter.AdapterStore;
import mil.nga.giat.geowave.store.adapter.MemoryAdapterStore;
import mil.nga.giat.geowave.store.adapter.WritableDataAdapter;

public class IngestRunData
{
	private final IndexWriter indexWriter;
	private final AdapterStore adapterCache;

	public IngestRunData(
			final IndexWriter indexWriter,
			final List<WritableDataAdapter<?>> adapters ) {
		this.indexWriter = indexWriter;
		adapterCache = new MemoryAdapterStore(
				adapters.toArray(new WritableDataAdapter[] {}));
	}

	public WritableDataAdapter<?> getDataAdapter(
			final GeoWaveData<?> data ) {
		return data.getAdapter(adapterCache);
	}

	public IndexWriter getIndexWriter() {
		return indexWriter;
	}

}
