package mil.nga.giat.geowave.store.adapter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.store.CloseableIterator;

/**
 * This is a simple HashMap based in-memory implementation of the AdapterStore
 * and can be useful if it is undesirable to persist and query objects within
 * another storage mechanism such as an Accumulo table.
 */
public class MemoryAdapterStore implements
		AdapterStore
{
	private Map<ByteArrayId, DataAdapter<?>> adapterMap;

	public MemoryAdapterStore(
			DataAdapter<?>[] adapters ) {
		adapterMap = new HashMap<ByteArrayId, DataAdapter<?>>();
		for (DataAdapter<?> adapter : adapters) {
			adapterMap.put(
					adapter.getAdapterId(),
					adapter);
		}
	}

	@Override
	public void addAdapter(
			DataAdapter<?> adapter ) {
		adapterMap.put(
				adapter.getAdapterId(),
				adapter);
	}

	@Override
	public DataAdapter<?> getAdapter(
			ByteArrayId adapterId ) {
		return adapterMap.get(adapterId);
	}

	@Override
	public boolean adapterExists(
			ByteArrayId adapterId ) {
		return adapterMap.containsKey(adapterId);
	}

	@Override
	public CloseableIterator<DataAdapter<?>> getAdapters() {
		return new CloseableIterator.Wrapper<DataAdapter<?>>(
				new ArrayList<DataAdapter<?>>(
						adapterMap.values()).iterator());
	}
}
