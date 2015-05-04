package mil.nga.giat.geowave.analytic;

import java.io.IOException;
import java.io.Serializable;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * Support for adapter stores that are Serializable. Rather than for an adapter
 * store to serialize its state, wrap an adapter store. If the adapter store is
 * not serializable, then log a warning message upon serialization.
 * 
 * 
 */

public class SerializableAdapterStore implements
		AdapterStore,
		Serializable
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	final static Logger LOGGER = LoggerFactory.getLogger(SerializableAdapterStore.class);

	transient AdapterStore adapterStore;

	public SerializableAdapterStore() {

	}

	public SerializableAdapterStore(
			AdapterStore adapterStore ) {
		super();
		this.adapterStore = adapterStore;
	}

	private AdapterStore getAdapterStore() {
		if (adapterStore == null) {
			throw new IllegalStateException(
					"AdapterStore has not been initialized");
		}
		return adapterStore;
	}

	@Override
	public void addAdapter(
			final DataAdapter<?> adapter ) {
		getAdapterStore().addAdapter(
				adapter);
	}

	@Override
	public DataAdapter<?> getAdapter(
			final ByteArrayId adapterId ) {
		return getAdapterStore().getAdapter(
				adapterId);
	}

	@Override
	public boolean adapterExists(
			final ByteArrayId adapterId ) {
		return getAdapterStore().adapterExists(
				adapterId);
	}

	@Override
	public CloseableIterator<DataAdapter<?>> getAdapters() {
		return getAdapterStore().getAdapters();
	}

	private void writeObject(
			final java.io.ObjectOutputStream out )
			throws IOException {
		if (adapterStore instanceof Serializable) {
			out.writeBoolean(true);
			out.writeObject(adapterStore);
		}
		else {
			out.writeBoolean(false);
		}
	}

	private void readObject(
			final java.io.ObjectInputStream in )
			throws IOException,
			ClassNotFoundException {
		if (in.readBoolean()) {
			adapterStore = (AdapterStore) in.readObject();
		}
		else {
			LOGGER.warn("Unable to initialized AdapterStore; the store is not serializable");
		}
	}
}
