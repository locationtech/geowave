package mil.nga.giat.geowave.core.store.adapter;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;

/**
 * This is a simple HashMap based in-memory implementation of the AdapterStore
 * and can be useful if it is undesirable to persist and query objects within
 * another storage mechanism such as an Accumulo table.
 */
public class MemoryAdapterStore implements
		AdapterStore,
		Serializable
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Map<ByteArrayId, DataAdapter<?>> adapterMap;

	public MemoryAdapterStore(
			final DataAdapter<?>[] adapters ) {
		adapterMap = new HashMap<ByteArrayId, DataAdapter<?>>();
		for (final DataAdapter<?> adapter : adapters) {
			adapterMap.put(
					adapter.getAdapterId(),
					adapter);
		}
	}

	@Override
	public void addAdapter(
			final DataAdapter<?> adapter ) {
		adapterMap.put(
				adapter.getAdapterId(),
				adapter);
	}

	@Override
	public DataAdapter<?> getAdapter(
			final ByteArrayId adapterId ) {
		return adapterMap.get(adapterId);
	}

	@Override
	public boolean adapterExists(
			final ByteArrayId adapterId ) {
		return adapterMap.containsKey(adapterId);
	}

	@Override
	public CloseableIterator<DataAdapter<?>> getAdapters() {
		return new CloseableIterator.Wrapper<DataAdapter<?>>(
				new ArrayList<DataAdapter<?>>(
						adapterMap.values()).iterator());
	}

	private void writeObject(
			final java.io.ObjectOutputStream out )
			throws IOException {
		final int count = adapterMap.size();
		out.writeInt(count);
		for (final Map.Entry<ByteArrayId, DataAdapter<?>> entry : adapterMap.entrySet()) {
			out.writeObject(entry.getKey());
			final byte[] val = PersistenceUtils.toBinary(entry.getValue());
			out.writeObject(val);
		}
	}

	private void readObject(
			final java.io.ObjectInputStream in )
			throws IOException,
			ClassNotFoundException {
		final int count = in.readInt();
		adapterMap = new HashMap<ByteArrayId, DataAdapter<?>>();
		for (int i = 0; i < count; i++) {
			final ByteArrayId id = (ByteArrayId) in.readObject();
			final byte[] data = (byte[]) in.readObject();
			adapterMap.put(
					id,
					PersistenceUtils.fromBinary(
							data,
							DataAdapter.class));
		}
	}

}
