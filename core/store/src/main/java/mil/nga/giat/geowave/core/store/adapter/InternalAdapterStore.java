package mil.nga.giat.geowave.core.store.adapter;

import mil.nga.giat.geowave.core.index.ByteArrayId;

/**
 * This is responsible for persisting adapter/Internal Adapter mappings (either
 * in memory or to disk depending on the implementation).
 */
public interface InternalAdapterStore
{

	public ByteArrayId getAdapterId(
			short internalAdapterId );

	public Short getInternalAdapterId(
			ByteArrayId adapterId );

	/**
	 * If an adapter is already associated with an internal Adapter returns
	 * false. Adapter can only be associated with internal adapter once.
	 *
	 *
	 * @param adapterId
	 *            the adapter
	 * @return the internal ID
	 */
	public short addAdapterId(
			ByteArrayId adapterId );

	/**
	 * Adapter Id to Internal Adapter Id mappings are maintain without regard to
	 * visibility constraints.
	 *
	 * @param adapterId
	 */
	public boolean remove(
			ByteArrayId adapterId );

	public boolean remove(
			short internalAdapterId );

	public void removeAll();
}
