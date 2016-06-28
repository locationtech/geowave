package mil.nga.giat.geowave.core.store.adapter;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;

/**
 * This is responsible for persisting data adapters (either in memory or to disk
 * depending on the implementation).
 */
public interface AdapterStore
{
	/**
	 * Add the adapter to the store
	 * 
	 * @param adapter
	 *            the adapter
	 */
	public void addAdapter(
			DataAdapter<?> adapter );

	/**
	 * Get an adapter from the store by its unique ID
	 * 
	 * @param adapterId
	 *            the unique adapter ID
	 * @return the adapter, null if it doesn't exist
	 */
	public DataAdapter<?> getAdapter(
			ByteArrayId adapterId );

	/**
	 * Check for the existence of the adapter with the given unique ID
	 * 
	 * @param adapterId
	 *            the unique ID to look up
	 * @return a boolean flag indicating whether the adapter exists
	 */
	public boolean adapterExists(
			ByteArrayId adapterId );

	/**
	 * Get the full set of adapters within this store
	 * 
	 * @return an iterator over all of the adapters in this store
	 */
	public CloseableIterator<DataAdapter<?>> getAdapters();

	public void removeAll();
}
