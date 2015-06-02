/**
 * 
 */
package mil.nga.giat.geowave.core.ingest;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.datastore.hbase.mapreduce.output.GeoWaveHBaseOutputKey;

import org.apache.log4j.Logger;

/**
 * @author viggy Functionality similar to <code> GeoWaveData </code>
 */
public class GeoWaveHBaseData<T>
{
	private final static Logger LOGGER = Logger.getLogger(GeoWaveHBaseData.class);
	private final ByteArrayId adapterId;
	private final ByteArrayId indexId;
	private final WritableDataAdapter<T> adapter;
	private final T data;

	public GeoWaveHBaseData(
			final ByteArrayId adapterId,
			final ByteArrayId indexId,
			final T data ) {
		this.adapterId = adapterId;
		this.indexId = indexId;
		this.data = data;

		// in this case the actual adapter is meant to be looked up using the ID
		this.adapter = null;
	}

	public GeoWaveHBaseData(
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

	public GeoWaveHBaseOutputKey getKey() {
		return new GeoWaveHBaseOutputKey(
				adapterId,
				indexId);
	}

	public T getValue() {
		return data;
	}
}
