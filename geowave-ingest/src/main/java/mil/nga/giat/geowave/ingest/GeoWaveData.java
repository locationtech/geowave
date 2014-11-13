package mil.nga.giat.geowave.ingest;

import mil.nga.giat.geowave.accumulo.mapreduce.output.GeoWaveOutputKey;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.ingest.local.IngestRunData;
import mil.nga.giat.geowave.store.adapter.AdapterStore;
import mil.nga.giat.geowave.store.adapter.DataAdapter;
import mil.nga.giat.geowave.store.adapter.WritableDataAdapter;

import org.apache.log4j.Logger;

/**
 * This models any information that is necessary to ingest an entry into
 * GeoWave: the adapter and index you wish to use as well as the actual data
 * 
 * @param <T>
 *            The java type for the actual data being ingested
 */
public class GeoWaveData<T>
{
	private final static Logger LOGGER = Logger.getLogger(GeoWaveData.class);
	private final ByteArrayId adapterId;
	private final ByteArrayId indexId;
	private final WritableDataAdapter<T> adapter;
	private final T data;

	public GeoWaveData(
			final ByteArrayId adapterId,
			final ByteArrayId indexId,
			final T data ) {
		this.adapterId = adapterId;
		this.indexId = indexId;
		this.data = data;

		// in this case the actual adapter is meant to be looked up using the ID
		this.adapter = null;
	}

	public GeoWaveData(
			final WritableDataAdapter<T> adapter,
			final ByteArrayId indexId,
			final T data ) {
		this.adapter = adapter;
		this.data = data;
		this.indexId = indexId;

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

	public ByteArrayId getIndexId() {
		return indexId;
	}

	public GeoWaveOutputKey getKey() {
		return new GeoWaveOutputKey(
				adapterId,
				indexId);
	}

	public T getValue() {
		return data;
	}
}
