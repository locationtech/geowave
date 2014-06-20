package mil.nga.giat.geowave.ingest;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.adapter.AdapterStore;
import mil.nga.giat.geowave.store.adapter.DataAdapter;
import mil.nga.giat.geowave.store.adapter.WritableDataAdapter;

import org.apache.log4j.Logger;

public class GeoWaveData<T>
{
	private final static Logger LOGGER = Logger.getLogger(IngestRunData.class);
	private final ByteArrayId adapterId;
	private final WritableDataAdapter<T> adapter;
	private final T data;

	public GeoWaveData(
			final ByteArrayId adapterId,
			final T data ) {
		this.adapterId = adapterId;
		this.data = data;

		// in this case the actual adapter is meant to be looked up using the ID
		this.adapter = null;
	}

	public GeoWaveData(
			final WritableDataAdapter<T> adapter,
			final T data ) {
		this.adapter = adapter;
		this.data = data;

		this.adapterId = adapter.getAdapterId();
	}

	public WritableDataAdapter<T> getAdapter(
			final AdapterStore adapterCache ) {
		if (adapter != null) {
			return adapter;
		}
		final DataAdapter<?> adapter = adapterCache.getAdapter(adapterId);
		if (adapter instanceof WritableDataAdapter) {
			return (WritableDataAdapter<T>) adapter;
		}
		LOGGER.warn("Adapter is not writable");
		return null;
	}

	public T getData() {
		return data;
	}
}
